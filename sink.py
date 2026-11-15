"""SQLite 下游:幂等投递 + 可验证确认。

cycles 行与 acks 行在同一事务提交 —— "已提交但确认丢失"只可能丢失
*响应消息*,确认记录本身与数据行同生共死。采集端回收账本前必须
verify_ack 通过;恢复时对账(ack_of)即可恰好保留一个周期。

deliver 幂等:cycle_id 已存在时直接返回既有 token,重复投递不产生
第二行。若出现"周期行在、确认行缺失"(理论上同事务不会发生,防御
性处理),deliver 会补写确认行后正常返回。

故障注入(演示/测试用):paused 暂停接收;drop_acks 提交后丢弃 n 次
确认(模拟 commit 成功、响应丢失)。
"""
import hashlib
import json
import secrets
import sqlite3


class SinkPaused(Exception):
    pass


class AckLost(Exception):
    """提交已成功,但确认在返回途中丢失。"""


class Sink:
    def __init__(self, path, hooks=None):
        self.c = sqlite3.connect(path)
        self.c.executescript('''
CREATE TABLE IF NOT EXISTS cycles(
  n INTEGER PRIMARY KEY AUTOINCREMENT,
  cycle_id TEXT UNIQUE,
  payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS acks(
  cycle_id TEXT PRIMARY KEY,
  commit_seq INTEGER NOT NULL,
  token TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);''')
        self.c.commit()
        self.epoch = self._meta_get('sink_epoch')
        if self.epoch is None:
            self.epoch = secrets.token_hex(8)
            self._meta_set('sink_epoch', self.epoch)
        self.paused = False
        self.drop_acks = 0
        self.hooks = hooks or (lambda point: None)

    def _meta_get(self, k):
        r = self.c.execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone()
        return r[0] if r else None

    def _meta_set(self, k, v):
        self.c.execute('INSERT INTO meta VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v', (k, v))
        self.c.commit()

    def _next_commit_seq(self, cur):
        r = cur.execute("SELECT v FROM meta WHERE k='commit_seq'").fetchone()
        n = (int(r[0]) if r else 0) + 1
        cur.execute("INSERT INTO meta VALUES('commit_seq',?) "
                    "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(n),))
        return n

    def deliver(self, cycle):
        """投递一个周期,返回可验证确认 token。重复投递幂等。"""
        if self.paused:
            raise SinkPaused(cycle['cycle_id'])
        cid = cycle['cycle_id']
        cur = self.c.cursor()
        try:
            cur.execute('BEGIN IMMEDIATE')
            row = cur.execute('SELECT token FROM acks WHERE cycle_id=?', (cid,)).fetchone()
            if row:
                self.c.commit()
                return row[0]  # 重复投递:返回既有确认,不产生第二行
            seq = self._next_commit_seq(cur)
            token = hashlib.sha256(f'{self.epoch}|{cid}|{seq}'.encode()).hexdigest()
            cur.execute('INSERT OR IGNORE INTO cycles(cycle_id,payload) VALUES(?,?)',
                        (cid, json.dumps(cycle, sort_keys=True)))
            cur.execute('INSERT INTO acks VALUES(?,?,?)', (cid, seq, token))
            self.hooks('before_sink_commit')
            self.c.commit()
        except BaseException:
            self.c.rollback()
            raise
        self.hooks('after_sink_commit_before_ack')
        if self.drop_acks:
            self.drop_acks -= 1
            raise AckLost(cid)
        self.hooks('after_ack_delivered')
        return token

    # ---- 确认的可验证性 ----
    def verify_ack(self, cycle_id, token):
        r = self.c.execute('SELECT token FROM acks WHERE cycle_id=?', (cycle_id,)).fetchone()
        return bool(r) and r[0] == token

    def ack_of(self, cycle_id):
        r = self.c.execute('SELECT token FROM acks WHERE cycle_id=?', (cycle_id,)).fetchone()
        return r[0] if r else None

    def has_cycle(self, cycle_id):
        return bool(self.c.execute('SELECT 1 FROM cycles WHERE cycle_id=?', (cycle_id,)).fetchone())

    # ---- 读取(含基线接口) ----
    def cycles_with_ids(self):
        return [(r[0], r[1], json.loads(r[2])) for r in self.c.execute(
            'SELECT n, cycle_id, payload FROM cycles ORDER BY n')]

    def rows(self):
        return [json.loads(x[0]) for x in self.c.execute('SELECT payload FROM cycles ORDER BY n')]

    def send(self, cycle):
        """基线接口:无身份普通插入(旧正常路径,不参与幂等)。"""
        self.c.execute('INSERT INTO cycles(payload) VALUES(?)', (json.dumps(cycle),))
        self.c.commit()

    def close(self):
        self.c.close()
