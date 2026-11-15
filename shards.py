# -*- coding: utf-8 -*-
"""SQLite 分片读取器。

包含两代实现：

1. baseline 保留：``seed`` / ``Shard.top`` —— 简单 topN 下推，
   跨分片 WITH TIES 丢同键尾部的已知缺陷由 baseline_demo.py 复现，勿改。
2. 本次联合查询组件：``seed_fact`` 建表（含可 NULL 的 score/day 与
   business_id），``ShardAdapter`` 经 snapshot.SnapshotRegistry 打开
   真实只读快照，提供
     - read_page      ：键集 seek 或 OFFSET 两种有序分页
     - read_tie_page  ：tie 能力分片的业务键边界探测下推
   每次返回都做单调性/游标校验，拒绝任何乱序页。
"""
import sqlite3
import time

from capabilities import SORT_SCORE_DAY_LOCAL
from snapshot import SnapshotExpired, ReopenBudgetExceeded

# ----------------------------------------------------------------------
# baseline 实现（保持原行为，baseline_demo 依赖）
# ----------------------------------------------------------------------

def seed(path, rows):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE fact(id INTEGER PRIMARY KEY,score INTEGER,payload TEXT)")
    c.executemany("INSERT INTO fact VALUES(?,?,?)", rows)
    c.commit()
    c.close()


class Shard:
    def __init__(self, path, name):
        self.path = path
        self.name = name
        self.log = []

    def top(self, n):
        c = sqlite3.connect(self.path)
        r = c.execute(
            "SELECT id,score,payload FROM fact ORDER BY score DESC,id LIMIT ?", (n,)
        ).fetchall()
        c.close()
        self.log.append({"limit": n, "rows": len(r)})
        return r


# ----------------------------------------------------------------------
# 新 schema 与排序定义
# ----------------------------------------------------------------------

FACT_DDL = """
CREATE TABLE fact(
  local_id    INTEGER PRIMARY KEY,
  score       INTEGER,
  day         INTEGER,
  business_id TEXT    NOT NULL,
  payload     TEXT
)
"""
# tie 探测索引：等值 (score,day,business_id) 后按 local_id 顺序扫
TIE_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_fact_tie "
    "ON fact(score, day, business_id, local_id)"
)

# 固定排序语义（与 capabilities.SORT_SCORE_DAY_LOCAL 对应）：
#   score DESC NULLS LAST, day ASC NULLS FIRST, local_id ASC
ORDER_SQL = "score IS NOT NULL DESC, score DESC, day IS NULL DESC, day ASC, local_id ASC"
# 说明：SQLite 中 ASC 缺省 NULLS FIRST、DESC 缺省 NULLS LAST，
# 上面的表达式把 NULL 位置显式化，score 非 NULL 的行排在 NULL 行之前，
# day 为 NULL 的行排在非 NULL 之前；local_id 是 NOT NULL 主键。

SELECT_COLS = "local_id, score, day, business_id, payload"
SELECT_SQL = "SELECT %s FROM fact" % SELECT_COLS


def seed_fact(path, rows):
    """建新 schema 分片库。rows: (local_id, score, day, business_id, payload)。"""
    c = sqlite3.connect(path)
    try:
        c.execute(FACT_DDL)
        c.executemany("INSERT INTO fact VALUES(?,?,?,?,?)", rows)
        c.execute(TIE_INDEX_DDL)
        c.commit()
    finally:
        c.close()


# ----------------------------------------------------------------------
# 键集 seek 的 NULL 感知游标条件
# ----------------------------------------------------------------------
# 排序键三列，每列生成“该行在此列上严格晚于游标”的 SQL 表达式；
# 词典序 = 在某列严格落后（排序意义上更靠后）且此前各列 NULL 感知相等。

def _after_score(p):
    # DESC NULLS LAST：游标非 NULL 时，排在其后的是
    #   (a) 更小的非 NULL score；(b) NULL score（NULL 统一沉底）。
    # 游标为 NULL 时其后不再有任何行（NULL 已是最后层级）。
    return ("(:%s IS NOT NULL AND ((score IS NOT NULL AND score < :%s) "
            "OR score IS NULL))" % (p, p))


def _after_day(p):
    # ASC NULLS FIRST，且本臂只在 score NULL 感知相等时成立：
    #  - 游标 day 为 NULL：其后是所有非 NULL day（NULL 已在该 score 段最前）
    #  - 游标 day 非 NULL：其后是更大的非 NULL day；NULL day 在此 score 段
    #    排在游标之前，不能包含（由 score 臂的跨段情形另行处理）
    return ("((:%s IS NULL AND day IS NOT NULL) "
            "OR (:%s IS NOT NULL AND day IS NOT NULL AND day > :%s))"
            % (p, p, p))


def _after_local(p):
    # local_id ASC，NOT NULL 主键
    return "local_id > :%s" % p


def build_seek_where(cursor):
    """cursor=(score,day,local_id) → (WHERE 片段, 参数字典)；None 表示无游标。"""
    if cursor is None:
        return "", {}
    s, d, lid = cursor
    params = {"s0": s, "d0": d, "l0": lid}
    clauses = [
        _after_score("s0"),
        "(score IS :s0 AND " + _after_day("d0") + ")",
        "(score IS :s0 AND day IS :d0 AND " + _after_local("l0") + ")",
    ]
    return "WHERE " + " OR ".join(clauses), params


# ----------------------------------------------------------------------
# 快照-backed adapter
# ----------------------------------------------------------------------

class AdapterOrderError(RuntimeError):
    """分片返回页违反固定排序语义或游标约束（绝不本地重排，直接失败）。"""


class ShardAdapter:
    """单分片只读 adapter；所有读取落在绑定版本的快照上。

    生命周期内持有最多一条连接，断开后重开计入 registry 预算（最多两次）。
    自身不缓存任何页：每次 read_* 的结果即该分片“唯一的 adapter 页”，
    由 merge 层持有或持久化。
    """

    sort_id = SORT_SCORE_DAY_LOCAL

    def __init__(self, shard_id, binding_rec, capability, audit_log):
        self.shard_id = shard_id
        self._rec = binding_rec            # snapshot.SnapshotBinding
        self.cap = dict(capability)
        self._audit = audit_log            # list，query 级调用审计
        self._conn = None
        self.attempts = 0                  # 连接建立次数（含初次）
        if self.cap["sort"] != self.sort_id:
            raise AdapterOrderError(
                "shard %s 排序语义 %r 与 adapter 固定语义不一致"
                % (shard_id, self.cap["sort"]))

    # -- 连接管理 --------------------------------------------------------
    def _get_conn(self):
        if self._conn is None:
            self._conn = self._rec.open()   # 内部校验快照与重开预算
            self.attempts = self._rec.opened
        return self._conn

    def drop_connection(self):
        """显式断开；下次读取触发一次重开（测试/故障注入用）。"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # -- 调用审计 --------------------------------------------------------
    def _log_call(self, op, requested, returned, exhausted, mode, extra=None):
        rec = {
            "shard_id": self.shard_id,
            "op": op,
            "mode": mode,                     # seek / offset / tie_probe
            "requested": requested,
            "returned": returned,
            "exhausted": exhausted,
            "ts": time.time(),
        }
        if extra:
            rec.update(extra)
        self._audit.append(rec)
        return rec

    # -- 单调性校验 ------------------------------------------------------
    @staticmethod
    def _row_key(row):
        _lid, score, day, _bid, _payload = row
        return (
            score is None,                       # score: 非 NULL 在前
            -(score if score is not None else 0),
            day is not None,                     # day: NULL 在前
            day if day is not None else 0,
            row[0],                              # local_id ASC
        )

    def _check_page(self, rows, cursor):
        prev_key = None
        for row in rows:
            key = self._row_key(row)
            if prev_key is not None and key <= prev_key:
                raise AdapterOrderError(
                    "shard %s 返回页非严格有序: %r 后出现 %r"
                    % (self.shard_id, prev_key, key))
            prev_key = key
        if cursor is not None and rows:
            # 首行必须严格晚于游标；游标格式 (score,day,local_id)
            cur_row = (cursor[2], cursor[0], cursor[1], None, None)
            if self._row_key(rows[0]) <= self._row_key(cur_row):
                raise AdapterOrderError(
                    "shard %s 返回页首行未越过已消费游标 %r" % (self.shard_id, cursor))

    # -- 有序分页 --------------------------------------------------------
    def read_page(self, page_size, cursor=None, offset=None):
        """读取一页有序行。

        seek 能力分片：cursor=(score,day,local_id) 或 None（首页）。
        仅 offset 分片：cursor 必须为 None，offset 为已消费行数。
        返回 dict(rows, exhausted, cursor_last, offset_next)。
        """
        if not (1 <= page_size <= self.cap["max_page"]):
            raise ValueError("page_size %d 超出分片 max_page=%d"
                             % (page_size, self.cap["max_page"]))
        if self.cap["seek"]:
            where, params = build_seek_where(cursor)
            sql = "%s %s ORDER BY %s LIMIT :lim" % (
                SELECT_SQL, where, ORDER_SQL)
            params = dict(params, lim=page_size)
            mode = "seek"
        else:
            if cursor is not None:
                raise AdapterOrderError(
                    "shard %s 仅支持 offset，不接受 seek 游标" % self.shard_id)
            if offset is None:
                raise ValueError("offset-only 分片必须提供 offset")
            sql = "%s ORDER BY %s LIMIT :lim OFFSET :off" % (SELECT_SQL, ORDER_SQL)
            params = {"lim": page_size, "off": offset}
            mode = "offset"

        conn = self._get_conn()
        t0 = time.time()
        rows = conn.execute(sql, params).fetchall()
        wait_ms = round((time.time() - t0) * 1000, 3)
        self._check_page(rows, cursor if self.cap["seek"] else None)
        last_cursor = (rows[-1][1], rows[-1][2], rows[-1][0]) if rows else cursor
        next_offset = None
        if not self.cap["seek"]:
            # offset 模式下 LIMIT 短页即真耗尽
            exhausted = len(rows) < page_size
            next_offset = (offset or 0) + len(rows)
        elif len(rows) < page_size:
            # seek 模式带 WHERE，短页不代表耗尽：再探测一页确认
            exhausted = self._probe_exhausted(last_cursor, page_size)
        else:
            exhausted = False
        self._log_call("read_page", page_size, len(rows), exhausted, mode,
                       {"wait_ms": wait_ms,
                        "cursor": list(cursor) if cursor else None,
                        "offset": offset})
        return {
            "rows": [tuple(r) for r in rows],
            "exhausted": exhausted,
            "cursor_last": last_cursor,
            "offset_next": next_offset,
        }

    def _probe_exhausted(self, cursor, page_size):
        """seek 短页后探测是否还有晚于游标的行；探测本身计入审计。"""
        where, params = build_seek_where(cursor)
        sql = "%s %s ORDER BY %s LIMIT 1" % (SELECT_SQL, where, ORDER_SQL)
        conn = self._get_conn()
        t0 = time.time()
        probe = conn.execute(sql, params).fetchall()
        wait_ms = round((time.time() - t0) * 1000, 3)
        self._log_call("probe_exhausted", 1, len(probe), len(probe) == 0,
                       "seek_probe", {"wait_ms": wait_ms,
                                      "cursor": list(cursor) if cursor else None})
        return len(probe) == 0

    # -- tie 探测（仅 with_ties 能力分片） -------------------------------
    def read_tie_page(self, business_key, cursor_local_id, page_size):
        """按业务键 (score,day,business_id) 直接探测 local_id 之后的同键行。

        全局 TIE 阶段的有界下推：只取该业务键的剩余行，不跨键多取。
        """
        if not self.cap["with_ties"]:
            raise AdapterOrderError(
                "shard %s 未声明 with_ties 能力，禁止 tie 探测下推" % self.shard_id)
        if not (1 <= page_size <= self.cap["max_page"]):
            raise ValueError("page_size %d 超出分片 max_page=%d"
                             % (page_size, self.cap["max_page"]))
        score, day, business_id = business_key
        sql = (
            "%s WHERE score IS :s AND day IS :d AND business_id = :b "
            "AND local_id > :l ORDER BY %s LIMIT :lim"
            % (SELECT_SQL, ORDER_SQL)
        )
        params = {"s": score, "d": day, "b": business_id,
                  "l": cursor_local_id, "lim": page_size}
        conn = self._get_conn()
        t0 = time.time()
        rows = conn.execute(sql, params).fetchall()
        wait_ms = round((time.time() - t0) * 1000, 3)
        # 探测结果必须全部命中同一业务键且严格跟在游标之后
        for r in rows:
            if (r[1], r[2], r[3]) != business_key or r[0] <= cursor_local_id:
                raise AdapterOrderError(
                    "shard %s tie 探测返回越界行 %r" % (self.shard_id, r))
        exhausted = len(rows) < page_size
        last_cursor = (rows[-1][1], rows[-1][2], rows[-1][0]) if rows else None
        self._log_call("read_tie_page", page_size, len(rows), exhausted,
                       "tie_probe", {"wait_ms": wait_ms,
                                     "business_key": list(business_key),
                                     "after_local_id": cursor_local_id})
        return {
            "rows": [tuple(r) for r in rows],
            "exhausted": exhausted,
            "cursor_last": last_cursor,
            "offset_next": None,
        }

    def close(self):
        self.drop_connection()
