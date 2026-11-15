# -*- coding: utf-8 -*-
"""联合查询组件：open-query / next-page / close-query / audit。

设计要点（详见 design.md）：

- 每个分片在 open-query 时绑定独立 snapshot_token（版本向量），后续每次
  adapter 调用都读该绑定版本；快照缺失/过期 -> 整次查询不可继续，不混新旧页。
- 全局排序 score DESC NULLS LAST, day ASC NULLS FIRST, 稳定尾键
  (shard_id UTF8, local_id ASC)；tie 仅按业务键 (score,day,business_id)。
- 流式 k 路归并：必须先拿到每个未耗尽分片的头行才能决定全局下一行；
  每个分片任意时刻最多保留一个 adapter 页（内存或落盘）+ 头/边界元数据。
- 三种阶段：SKIP（全局 offset）-> EMIT（全局 limit）-> TIE（WITH TIES 补齐）。
- TIE 阶段两种安全策略：
    tie 能力分片 -> 缓冲排干后 read_tie_page 业务键探测（有界、不跨键多取）；
    无 ties 分片 -> 有序前瞻，头行层级越过边界层级即暂停（页保留不丢）。
- continuation 状态持久化每片“已消费游标 + 未消费缓冲”，预读未输出行
  随页落盘保留，绝不跳过。
"""
from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

from capabilities import (
    SORT_SCORE_DAY_LOCAL, PAGE_SIZE_MIN, PAGE_SIZE_MAX,
    check_query_compatible, CapabilityError,
)
from shards import ShardAdapter, AdapterOrderError  # noqa: F401
from snapshot import (
    SnapshotRegistry, SnapshotExpired, ReopenBudgetExceeded, create as snap_create,
)

# ----------------------------------------------------------------------
# 键工具
# ----------------------------------------------------------------------

def level_key(row):
    """(score,day) 层级键，不含 local_id / shard_id；用于 tie 层级比较。"""
    score, day = row[2], row[3]
    return (
        score is None,                          # score 非 NULL 在前 (DESC NL)
        -(score if score is not None else 0),
        day is not None,                        # day NULL 在前 (ASC NF)
        day if day is not None else 0,
    )


def global_key(row):
    """全局排序键：业务排序 + 稳定尾键 (shard_id UTF8, local_id ASC)。"""
    return level_key(row) + (row[0], row[1])    # shard_id, local_id


def business_key(row):
    """同业务键才是 ties。"""
    return (row[2], row[3], row[4])             # score, day, business_id


# ----------------------------------------------------------------------
# 持久化文件名
# ----------------------------------------------------------------------
STATE_NAME = "query_state.json"
STATE_TMP = "query_state.tmp"
PAGES_DIR = "pages"
AUDIT_NAME = "audit.jsonl"


class QueryUnavailable(RuntimeError):
    """查询不可继续（快照过期/缺失、状态损坏）；已交付页保留但 query 未完成。"""


# ----------------------------------------------------------------------
# 联合查询句柄
# ----------------------------------------------------------------------
class JointQuery:

    PHASE_SKIP = "SKIP"
    PHASE_EMIT = "EMIT"
    PHASE_TIE = "TIE"
    PHASE_DONE = "DONE"

    def __init__(self, query_id, state_dir, params, shard_defs, registry,
                 crash_inject=None):
        self.query_id = query_id
        self.state_dir = Path(state_dir)
        self.pages_dir = self.state_dir / PAGES_DIR
        self.params = dict(params)              # offset/limit/with_ties/page_size
        self.shard_defs = shard_defs            # 建库与能力声明（重开时不再建库）
        self.registry = registry
        self.crash_inject = crash_inject or {}  # {"state_write": n} 第 n 次写崩
        self._write_count = 0
        self.audit_log = []
        self.adapters = {}
        self.streams = {}
        self.phase = self.PHASE_SKIP
        self.skipped = 0
        self.emitted = 0
        self.delivered_rows = 0
        self.boundary_key = None                # 业务键
        self.boundary_level = None              # (score,day) 层级
        self.peak_mem_pages = 0
        self.persist_bytes = 0
        self.persist_writes = 0
        self.failure = None
        self.created_at = time.time()
        self._row_bound = None
        # offset=0 时直接进入 EMIT（limit=0 则在 open/reopen 处直接 DONE），
        # 不能在 SKIP 分支先弹一行再判断，否则会误吞首行。
        self.phase = self.PHASE_EMIT if params["offset"] == 0 else self.PHASE_SKIP
        self._init_streams()

    def _apply_zero_limit(self):
        if self.params["limit"] == 0:
            self.phase = self.PHASE_DONE

    # -- 初始化 ----------------------------------------------------------
    def _init_streams(self):
        for sid in sorted(self.shard_defs):
            cap = self.shard_defs[sid]["cap"]
            rec = self.registry.get(sid)
            self.adapters[sid] = ShardAdapter(sid, rec, cap, self.audit_log)
            self.streams[sid] = {
                "exhausted": False,             # 分片数据已全部消费完
                "past_boundary": False,         # TIE 前瞻：头行已越过边界层级
                "probe_done": False,            # TIE 探测：该业务键已无更多行
                "probe_mode": False,            # TIE 中是否已切探测模式
                "cursor": None,                 # 已消费边界 (score,day,local_id)
                "offset_consumed": 0,           # offset-only 分片已消费行数
                "buffer": [],                   # 当前 adapter 页（内存）
                "buffer_on_disk": None,         # 落盘页文件名（与 buffer 二选一）
                "page_exhausted": True,         # 当前缓冲页是否已是最后一页
                "page_version": 0,              # adapter 页不可变版本号
            }

    # ------------------------------------------------------------------
    # open-query
    # ------------------------------------------------------------------
    @classmethod
    def open(cls, shard_defs, offset, limit, with_ties, page_size, work_root,
             crash_inject=None, query_id=None):
        """打开联合查询：建快照、注册版本向量、做能力兼容检查。"""
        if not isinstance(offset, int) or offset < 0:
            raise CapabilityError("offset 必须是非负整数")
        if not isinstance(limit, int) or limit < 0:
            raise CapabilityError("limit 必须是非负整数（含 0）")
        if not (PAGE_SIZE_MIN <= page_size <= PAGE_SIZE_MAX):
            raise CapabilityError("page_size 必须在 %d..%d"
                                  % (PAGE_SIZE_MIN, PAGE_SIZE_MAX))
        caps = {sid: d["cap"] for sid, d in shard_defs.items()}
        check_query_compatible(caps, SORT_SCORE_DAY_LOCAL, page_size, with_ties)

        query_id = query_id or ("q-" + uuid.uuid4().hex[:12])
        state_dir = Path(work_root) / query_id
        if state_dir.exists():
            raise ValueError("query 状态目录已存在: %s" % state_dir)
        (state_dir / PAGES_DIR).mkdir(parents=True)

        registry = SnapshotRegistry()
        bindings = {}
        token_vector = {}
        for sid in sorted(shard_defs):
            d = shard_defs[sid]
            token = "%s:%s" % (query_id, sid)
            snap_path = state_dir / ("snap_%s.sqlite" % sid)
            binding = snap_create(d["source"], snap_path, token)
            registry.register(sid, binding)
            bindings[sid] = binding
            token_vector[sid] = token

        params = {"offset": offset, "limit": limit,
                  "with_ties": bool(with_ties), "page_size": page_size}
        q = cls(query_id, state_dir, params, shard_defs, registry, crash_inject)
        q.bindings = bindings
        q.token_vector = token_vector
        q._apply_zero_limit()
        q._persist_locked()
        return q

    # ------------------------------------------------------------------
    # 重开
    # ------------------------------------------------------------------
    @classmethod
    def reopen(cls, state_dir, crash_inject=None):
        """从持久状态重开查询；快照缺失/过期或状态损坏 -> QueryUnavailable。"""
        state_dir = Path(state_dir)
        state_file = state_dir / STATE_NAME
        tmp_file = state_dir / STATE_TMP
        if not state_file.exists():
            if tmp_file.exists():
                raise QueryUnavailable(
                    "仅发现未完成的状态临时文件 %s，无可用已提交状态" % STATE_TMP)
            raise QueryUnavailable("查询状态缺失: %s" % state_file)
        try:
            state = json.loads(state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            raise QueryUnavailable("状态文件损坏: %s" % e)
        # 清理上次写入中断留下的临时文件（已提交状态仍有效）
        if tmp_file.exists():
            tmp_file.unlink()

        registry = SnapshotRegistry()
        for sid, binding in state["bindings"].items():
            registry.register(sid, binding)
        ok, bad = registry.all_live(list(state["bindings"]))
        if not ok:
            raise QueryUnavailable(
                "分片快照过期/缺失，整次查询不可继续: %s（已交付页保留，query 未完成）"
                % bad)

        q = cls(state["query_id"], state_dir, state["params"],
                state["shard_defs"], registry, crash_inject)
        q.bindings = state["bindings"]
        q.token_vector = state["token_vector"]
        q.phase = state["phase"]
        if state.get("failure"):
            # 失败查询重开后仍保持失败：已交付页保留，明确 query 未完成，
            # 不允许继续混新旧页
            q.failure = state["failure"]
        q.skipped = state["skipped"]
        q.emitted = state["emitted"]
        q.delivered_rows = state["delivered_rows"]
        q.boundary_key = tuple(state["boundary_key"]) if state["boundary_key"] else None
        q.boundary_level = tuple(state["boundary_level"]) if state["boundary_level"] else None
        q.peak_mem_pages = state["peak_mem_pages"]
        q.persist_bytes = state["persist_bytes"]
        q.persist_writes = state["persist_writes"]
        q.failure = state.get("failure")
        # 恢复每片流状态与未消费缓冲（内存或落盘）
        for sid, st in state["streams"].items():
            s = q.streams[sid]
            for k in ("exhausted", "past_boundary", "probe_done", "probe_mode",
                      "cursor", "offset_consumed", "page_exhausted", "page_version"):
                s[k] = st[k]
            s["cursor"] = tuple(st["cursor"]) if st["cursor"] else None
            if st.get("buffer_on_disk"):
                page_path = q.pages_dir / st["buffer_on_disk"]
                if not page_path.exists():
                    # 原子持久化保证已提交状态引用的页文件必然存在；
                    # 缺失即状态/页目录被外部破坏，明确失败而非猜测重取。
                    raise QueryUnavailable(
                        "分片 %s 的持久未消费页丢失，continuation 不完整" % sid)
                s["buffer_on_disk"] = st["buffer_on_disk"]
                s["buffer"] = []
            else:
                s["buffer"] = [tuple(r) for r in st["buffer"]]
                s["buffer_on_disk"] = None
        # 恢复连接尝试计数（重开预算跨进程累计）
        for sid, meta in state["snapshot_meta"].items():
            rec = registry._bindings[sid]
            rec.opened = meta["opened"]
            rec.reopens = meta["reopens"]
            q.adapters[sid].attempts = meta["opened"]
        return q

    # ------------------------------------------------------------------
    # 缓冲与持久页
    # ------------------------------------------------------------------
    def _page_file(self, sid, version):
        # 每次 adapter 取页一个不可变版本文件：已提交状态永远不会指向被
        # 后续页覆盖的内容（写入中断也只留下新的 .tmp，旧版本保持可读）。
        return self.pages_dir / ("page_%s_%d.json" % (sid, version))

    def _persist_page(self, sid, rows):
        """把未消费 adapter 页落盘，按实际字节统计。"""
        s = self.streams[sid]
        version = s["page_version"] = s.get("page_version", 0) + 1
        path = self._page_file(sid, version)
        payload = json.dumps([list(r) for r in rows], ensure_ascii=False).encode("utf-8")
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(payload)
        os.replace(tmp, path)
        self.persist_bytes += len(payload)
        self.persist_writes += 1
        return path.name, len(payload)

    def _load_page(self, sid):
        s = self.streams[sid]
        path = self.pages_dir / s["buffer_on_disk"]
        rows = [tuple(r) for r in json.loads(path.read_text(encoding="utf-8"))]
        s["buffer"] = rows
        s["buffer_on_disk"] = None
        return rows

    def _mem_pages(self):
        return sum(1 for s in self.streams.values() if s["buffer"])

    def _note_mem(self):
        cur = self._mem_pages()
        if cur > self.peak_mem_pages:
            self.peak_mem_pages = cur
        # 硬不变量：每个分片至多一个内存 adapter 页 => 内存页数 <= 分片数
        assert cur <= len(self.streams), "内存 adapter 页数超过分片数"

    # ------------------------------------------------------------------
    # adapter 取页（含快照失败语义）
    # ------------------------------------------------------------------
    def _fetch_next_page(self, sid):
        """分片当前页耗尽后取下一 adapter 页；返回 False 表示分片耗尽。"""
        s = self.streams[sid]
        adapter = self.adapters[sid]
        cap = self.shard_defs[sid]["cap"]
        page_size = cap["max_page"]
        try:
            if s["probe_mode"]:
                # TIE 业务键探测：游标 local_id 之后的同键行
                last_local = s["cursor"][2] if s["cursor"] else -1
                page = adapter.read_tie_page(self.boundary_key, last_local, page_size)
                if not page["rows"] and page["exhausted"]:
                    s["probe_done"] = True
                    return False
            elif cap["seek"]:
                page = adapter.read_page(page_size, cursor=s["cursor"])
            else:
                page = adapter.read_page(page_size, offset=s["offset_consumed"])
        except (SnapshotExpired, ReopenBudgetExceeded) as e:
            self.failure = {"type": type(e).__name__, "shard": sid, "msg": str(e)}
            self._safe_persist()
            raise QueryUnavailable(
                "shard %s 快照不可用，整次查询不可继续（不混新旧页）: %s"
                % (sid, e)) from e
        if not page["rows"]:
            s["exhausted"] = True
            return False
        # 装饰物理身份：全局行 = (shard_id, local_id, score, day, business_id, payload)
        s["buffer"] = [(sid,) + tuple(r) for r in page["rows"]]
        s["page_exhausted"] = page["exhausted"]
        if cap["seek"] or s["probe_mode"]:
            s["cursor"] = page["cursor_last"]
        else:
            s["offset_consumed"] = page["offset_next"]
        self._note_mem()
        return True

    def _head(self, sid):
        s = self.streams[sid]
        if s["buffer_on_disk"]:
            self._load_page(sid)
            self._note_mem()
        return s["buffer"][0] if s["buffer"] else None

    def _active_shards(self):
        out = []
        for sid, s in self.streams.items():
            if s["exhausted"] or s["past_boundary"] or s["probe_done"]:
                continue
            if self.phase == self.PHASE_TIE and self._pause_lookahead(sid):
                s["past_boundary"] = True    # 暂停前瞻，当前页原样保留
                continue
            out.append(sid)
        return out

    def _ensure_heads(self):
        """保证每个未耗尽分片都有头行（内存或从持久页恢复），否则取新页。"""
        for sid in self._active_shards():
            s = self.streams[sid]
            if self._head(sid) is None:
                if not self._fetch_next_page(sid):
                    continue
            self._note_mem()

    # ------------------------------------------------------------------
    # 行推进
    # ------------------------------------------------------------------
    def _pop_head(self, sid):
        s = self.streams[sid]
        if s["buffer_on_disk"]:
            self._load_page(sid)
        row = s["buffer"].pop(0)
        # 推进已消费边界（continuation 不能跳过预读未输出行：未弹出的仍在缓冲）。
        # cursor 是“已消费”边界，供 seek 重开/下一页使用；
        # offset_consumed 只在 _fetch_next_page 按实际取回行数推进（=已取回行数），
        # 绝不在此按 pop 自增，否则 OFFSET 会重复跳过已取回页中的行。
        s["cursor"] = (row[2], row[3], row[1])       # score, day, local_id
        if not s["buffer"]:
            # 当前 adapter 页消费完。普通模式不据 page_exhausted 提前判耗尽
            # （seek 带 WHERE 的短页不等于耗尽），交由下一次
            # _fetch_next_page 取空页时置 exhausted；探测模式短页即该键耗尽。
            if s["probe_mode"] and s["page_exhausted"]:
                s["probe_done"] = True
        self._after_pop(sid)
        return row

    def _after_pop(self, sid):
        s = self.streams[sid]
        cap = self.shard_defs[sid]["cap"]
        if self.phase != self.PHASE_TIE:
            return
        if s["buffer_on_disk"] or s["buffer"]:
            return
        # 缓冲已排干：
        # - tie 能力分片切业务键探测模式（有界、不跨键多取），
        #   之后只回该业务键的行，未命中即 probe_done；
        # - 无 ties 分片保持普通分页，由 _pause_lookahead 在头行越过
        #   边界层级时暂停（同层级外键行不会被误丢）。
        if cap["with_ties"] and not s["probe_mode"] and not s["exhausted"]:
            s["probe_mode"] = True

    def _pause_lookahead(self, sid):
        """TIE 阶段判断该分片是否应暂停（暂停后页保留，绝不再向前消费）。

        同一 (score,day) 层级内不同业务键按稳定尾键交错，因此头行业务键
        不同不代表边界键不会再出现；只有头行“层级”严格越过边界层级时，
        分片内有序性才保证边界键彻底不可能再出现，此时暂停并保留整页。
        探测模式分片只回边界层级的边界键行，永远不会满足此条件。
        """
        head = self._head(sid)
        if head is None:
            return False
        return level_key(head) > self.boundary_level

    def _pick_min(self):
        """必须先有每个未耗尽分片的头行，才能定全局下一行。"""
        best_sid, best_row = None, None
        for sid in self._active_shards():
            row = self._head(sid)
            if row is None:
                continue
            if best_row is None or global_key(row) < global_key(best_row):
                best_sid, best_row = sid, row
        return best_sid, best_row

    # ------------------------------------------------------------------
    # next-page
    # ------------------------------------------------------------------
    def _total_snapshot_rows(self):
        """绑定快照中的物理总行数（进度守卫上界；结果缓存，不逐页重算）。"""
        if getattr(self, "_row_bound", None) is None:
            total = 0
            for sid, adapter in self.adapters.items():
                conn = adapter._get_conn()
                total += conn.execute("SELECT COUNT(*) FROM fact").fetchone()[0]
            self._row_bound = total
        return self._row_bound

    def _liveness_gate(self):
        """next-page 前置门禁：任一分片快照过期/缺失，整次查询不可继续。

        不混新旧页：失败前不会取任何新数据；已交付页保留，query 标记未完成。
        """
        ok, bad = self.registry.all_live(list(self.streams), strong=True)
        if not ok:
            self.failure = {
                "type": "SnapshotExpired",
                "shard": bad,
                "msg": "快照过期/缺失: %s" % bad,
            }
            self._safe_persist()
            raise QueryUnavailable(
                "分片快照过期/缺失，整次查询不可继续: %s（已交付页保留，query 未完成）"
                % bad)

    def next_page(self, page_size=None):
        """取下一网络页。limit 针对整个查询；本方法只负责分批交付。"""
        if self.failure:
            raise QueryUnavailable("query 已处于失败状态: %s" % self.failure)
        if self.phase == self.PHASE_DONE:
            return {"rows": [], "done": True, "query_complete": True,
                    "query_id": self.query_id}
        page_size = page_size or self.params["page_size"]
        if not (PAGE_SIZE_MIN <= page_size <= PAGE_SIZE_MAX):
            raise CapabilityError("page_size 必须在 %d..%d"
                                  % (PAGE_SIZE_MIN, PAGE_SIZE_MAX))
        self._liveness_gate()
        out, ties_emitted = [], 0
        # 防御性进度守卫：稳态下每次内层迭代都会 pop 一行（skip/emit/tie
        # 前瞻），唯一无 pop 的路径是 row is None -> DONE。用 pop 计数驱动
        # 上限，超过“已绑定快照总行数”仍未结束即状态机不收敛，显式失败。
        bound = self._total_snapshot_rows()
        pops = 0
        try:
            while len(out) < page_size:
                self._ensure_heads()
                sid, row = self._pick_min()
                if row is None:
                    self.phase = self.PHASE_DONE
                    break
                if self.phase == self.PHASE_DONE:
                    # limit 已满足（含 limit=0）或上一步刚切到 DONE：
                    # 分片可能仍有缓冲行，但本查询不再消费，必须跳出
                    break
                pops += 1
                if pops > bound:
                    raise RuntimeError(
                        "next-page 状态机不收敛：phase=%s pops 超过快照总行数 %d"
                        % (self.phase, bound))
                if self.phase == self.PHASE_SKIP:
                    self._pop_head(sid)
                    self.skipped += 1
                    if self.skipped >= self.params["offset"]:
                        self.phase = (self.PHASE_DONE if self.params["limit"] == 0
                                      else self.PHASE_EMIT)
                    continue
                if self.phase == self.PHASE_EMIT:
                    self._pop_head(sid)
                    out.append(row)
                    self.emitted += 1
                    if self.emitted >= self.params["limit"]:
                        if self.params["with_ties"]:
                            self.phase = self.PHASE_TIE
                            self.boundary_key = business_key(row)
                            self.boundary_level = level_key(row)
                        else:
                            self.phase = self.PHASE_DONE
                    continue
                if self.phase == self.PHASE_TIE:
                    if level_key(row) > self.boundary_level:
                        # 防御性不变量：越过层级的分片应已在 _active_shards 暂停
                        raise RuntimeError(
                            "TIE 阶段选到越过边界层级的行 %r（boundary=%s）"
                            % (row, self.boundary_level))
                    if business_key(row) == self.boundary_key:
                        self._pop_head(sid)
                        out.append(row)
                        self.emitted += 1
                        ties_emitted += 1
                    else:
                        # 同层级、不同业务键的交错行：推进但不输出。
                        # 排干后 tie 能力分片切探测模式；无 ties 分片继续
                        # 普通分页，直到其头行越过边界层级被暂停。
                        self._pop_head(sid)
                    continue
        except QueryUnavailable:
            self._safe_persist()
            raise
        self.delivered_rows += len(out)
        complete = self.phase == self.PHASE_DONE
        self._persist_locked()
        return {
            "rows": out,
            "done": complete,
            "query_complete": complete,
            "query_id": self.query_id,
            "phase": self.phase,
            "ties_in_page": ties_emitted,
            "delivered_total": self.delivered_rows,
        }

    def exhaust(self):
        """便捷方法：收完全部页，返回全量有序行（测试/oracle 对比用）。"""
        all_rows = []
        while True:
            page = self.next_page()
            all_rows.extend(page["rows"])
            if page["query_complete"]:
                return all_rows

    # ------------------------------------------------------------------
    # close-query
    # ------------------------------------------------------------------
    def close(self, delete_snapshots=True):
        for sid, adapter in self.adapters.items():
            adapter.close()
        closed = self.registry.close_all()
        if delete_snapshots:
            for sid in closed:
                p = self.state_dir / ("snap_%s.sqlite" % sid)
                if p.exists():
                    p.unlink()
        self.phase = self.PHASE_DONE if not self.failure else self.phase
        self._safe_persist()
        return {"closed_shards": closed, "query_id": self.query_id,
                "failure": self.failure}

    # ------------------------------------------------------------------
    # 状态持久化
    # ------------------------------------------------------------------
    def _stream_state(self, sid):
        s = self.streams[sid]
        # 内存中的未消费页在状态写入前落盘到“新不可变版本”（不删旧版本）；
        # 旧版本的清理推迟到状态原子 rename 成功之后（_commit_page_cleanup），
        # 保证任意写入中断点上，已提交状态引用的页文件都仍然存在。
        buffer_on_disk = s["buffer_on_disk"]
        if s["buffer"] and not buffer_on_disk:
            name, _n = self._persist_page(sid, s["buffer"])
            buffer_on_disk = name
            s["buffer_on_disk"] = name
            s["buffer"] = []
        return {
            "exhausted": s["exhausted"],
            "past_boundary": s["past_boundary"],
            "probe_done": s["probe_done"],
            "probe_mode": s["probe_mode"],
            "cursor": list(s["cursor"]) if s["cursor"] else None,
            "offset_consumed": s["offset_consumed"],
            "buffer": [],
            "buffer_on_disk": buffer_on_disk,
            "page_exhausted": s["page_exhausted"],
            "page_version": s["page_version"],
        }

    def _commit_page_cleanup(self, streams_state):
        """状态 rename 成功后：删除各分片未被新状态引用的历史页文件。"""
        keep = {sid: st["buffer_on_disk"] for sid, st in streams_state.items()}
        for sid, keep_name in keep.items():
            for p in self.pages_dir.glob("page_%s_*.json" % sid):
                if p.name != keep_name:
                    p.unlink()

    def _state_payload(self, streams_state):
        return {
            "query_id": self.query_id,
            "params": self.params,
            "shard_defs": self.shard_defs,
            "bindings": getattr(self, "bindings", None) or self._bindings_from_reg(),
            "token_vector": getattr(self, "token_vector", None)
                            or {sid: b["token"] for sid, b in
                                (getattr(self, "bindings", {}) or {}).items()},
            "phase": self.phase,
            "skipped": self.skipped,
            "emitted": self.emitted,
            "delivered_rows": self.delivered_rows,
            "boundary_key": list(self.boundary_key) if self.boundary_key else None,
            "boundary_level": list(self.boundary_level) if self.boundary_level else None,
            "peak_mem_pages": self.peak_mem_pages,
            "persist_bytes": self.persist_bytes,
            "persist_writes": self.persist_writes,
            "failure": self.failure,
            "snapshot_meta": self.registry.audit(),
            "streams": streams_state,
            "saved_at": time.time(),
        }

    def _bindings_from_reg(self):
        return {sid: rec.binding for sid, rec in self.registry._bindings.items()}

    def _persist_locked(self):
        """原子写状态；crash_inject 可在临时写后、rename 前中断。

        顺序保证崩溃安全：先把新页写成不可变版本（不动旧页）-> 写状态 tmp
        -> 原子 rename 提交 -> 提交后才清理旧页。因此任意中断点上，已提交
        状态引用的持久页都仍然存在，重开不会丢失预读未输出行。
        """
        self._write_count += 1
        state_file = self.state_dir / STATE_NAME
        tmp_file = self.state_dir / STATE_TMP
        streams_state = {sid: self._stream_state(sid)
                         for sid in sorted(self.streams)}
        data = json.dumps(self._state_payload(streams_state),
                          ensure_ascii=False, indent=2)
        tmp_file.write_text(data, encoding="utf-8")
        if self.crash_inject.get("state_write") == self._write_count:
            raise IOError("注入故障：第 %d 次状态写入在 rename 前中断" % self._write_count)
        os.replace(tmp_file, state_file)
        self._commit_page_cleanup(streams_state)

    def _safe_persist(self):
        try:
            self._persist_locked()
        except IOError:
            pass  # 故障路径下保留 tmp，交由 reopen 的恢复/失败语义处理

    def save_state(self):
        self._persist_locked()
        return str(self.state_dir / STATE_NAME)

    # ------------------------------------------------------------------
    # audit
    # ------------------------------------------------------------------
    def audit(self):
        adapter_calls = {}
        for rec in self.audit_log:
            d = adapter_calls.setdefault(
                rec["shard_id"],
                {"read_page": 0, "read_tie_page": 0, "probe_exhausted": 0,
                 "rows_fetched": 0,
                 "seek_calls": 0, "offset_calls": 0, "tie_probe_calls": 0,
                 "seek_probe_calls": 0})
            d[rec["op"]] += 1
            d["rows_fetched"] += rec["returned"]
            d[rec["mode"] + "_calls"] += 1
        return {
            "query_id": self.query_id,
            "phase": self.phase,
            "params": self.params,
            "token_vector": getattr(self, "token_vector", None),
            "snapshots": self.registry.audit(),
            "adapter_calls": adapter_calls,
            "total_rows_fetched": sum(r["returned"] for r in self.audit_log),
            "total_adapter_calls": len(self.audit_log),
            "skipped": self.skipped,
            "emitted": self.emitted,
            "delivered_rows": self.delivered_rows,
            "peak_mem_pages": self.peak_mem_pages,
            "shard_count": len(self.streams),
            "persist_bytes": self.persist_bytes,
            "persist_writes": self.persist_writes,
            "failure": self.failure,
            "calls": list(self.audit_log),
        }
