"""持久投递账本(SQLite,WAL)。

采集端唯一的持久状态:
  cycles    已完成但尚未被下游可验证确认的周期(磁盘预算 <= 10)
  frags     已消费片段 (receiver,generation,seq) -> sha1,用于重复片段校验
  gens      每 (receiver,generation) 的持久装配状态:seq 高水位、代内
            周期序号计数器、行缓冲、pending 半周期(序列化 JSON)
  meta      杂项

关键准则:已确认(ACK)片段的内容必须持久。consume_fragment 把
"片段hash + 高水位 + 序号计数器 + 行缓冲 + pending + 新周期"放进
同一事务 —— 崩溃后装配状态可从账本精确重建,源重送判为重复,
未提交片段由源重送后重新消费,不存在半消费状态。
(早期版本只持久化高水位,崩溃丢失易失 pending 导致周期内容丢失,
由 killpoints 实验暴露;序号计数器必须持久化且单调,否则账本回收
后序号回退会复用旧周期身份。)
"""
import json
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS cycles(
  n INTEGER PRIMARY KEY AUTOINCREMENT,
  cycle_id TEXT UNIQUE NOT NULL,
  receiver TEXT NOT NULL,
  generation INTEGER NOT NULL,
  ordinal INTEGER NOT NULL,
  payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS frags(
  receiver TEXT NOT NULL, generation INTEGER NOT NULL, seq INTEGER NOT NULL,
  sha1 TEXT NOT NULL, PRIMARY KEY(receiver, generation, seq));
CREATE TABLE IF NOT EXISTS gens(
  receiver TEXT NOT NULL, generation INTEGER NOT NULL,
  hw INTEGER NOT NULL DEFAULT 0,
  next_ordinal INTEGER NOT NULL DEFAULT 1,
  buffer BLOB NOT NULL DEFAULT X'',
  pending TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY(receiver, generation));
"""


class Ledger:
    def __init__(self, path):
        self.path = path
        self.c = sqlite3.connect(path)
        if path != ':memory:':
            self.c.execute('PRAGMA journal_mode=WAL')
        self.c.execute('PRAGMA synchronous=FULL')
        self.c.executescript(SCHEMA)
        self.c.commit()

    def close(self):
        self.c.close()

    # ---- 持久装配状态 / 去重 ----
    def gen_state(self, receiver, generation):
        """返回 {'hw','next_ordinal','buffer','pending'};无记录给默认值。"""
        r = self.c.execute(
            'SELECT hw, next_ordinal, buffer, pending FROM gens '
            'WHERE receiver=? AND generation=?', (receiver, generation)).fetchone()
        if not r:
            return {'hw': 0, 'next_ordinal': 1, 'buffer': b'', 'pending': {}}
        return {'hw': r[0], 'next_ordinal': r[1], 'buffer': bytes(r[2]),
                'pending': json.loads(r[3])}

    def high_water(self, receiver, generation):
        return self.gen_state(receiver, generation)['hw']

    def next_ordinal(self, receiver, generation):
        """持久化的代内周期序号:单调不回退,与账本回收无关(见模块 docstring)。"""
        return self.gen_state(receiver, generation)['next_ordinal']

    def frag_hash(self, receiver, generation, seq):
        r = self.c.execute('SELECT sha1 FROM frags WHERE receiver=? AND generation=? AND seq=?',
                           (receiver, generation, seq)).fetchone()
        return r[0] if r else None

    def clear_assembly(self, receiver, generation, hooks=None):
        """断连/代际更替:丢弃行缓冲与 pending(保留高水位/序号/片段hash)。"""
        self.c.execute(
            "UPDATE gens SET buffer=X'', pending='{}' WHERE receiver=? AND generation=?",
            (receiver, generation))
        self.c.commit()

    # ---- 消费事务(采集确认边界) ----
    def consume_fragment(self, receiver, generation, seq, sha1, cycles, *,
                         next_ordinal, buffer, pending, hooks=None):
        """原子记录一个已消费片段及其产生的全部状态。cycles: payload dict 列表。"""
        cur = self.c.cursor()
        try:
            cur.execute('BEGIN IMMEDIATE')
            cur.execute('INSERT INTO frags VALUES(?,?,?,?)', (receiver, generation, seq, sha1))
            cur.execute(
                'INSERT INTO gens(receiver,generation,hw,next_ordinal,buffer,pending) '
                'VALUES(?,?,?,?,?,?) '
                'ON CONFLICT(receiver,generation) DO UPDATE SET hw=excluded.hw, '
                'next_ordinal=excluded.next_ordinal, buffer=excluded.buffer, '
                'pending=excluded.pending',
                (receiver, generation, seq, next_ordinal, buffer, json.dumps(pending)))
            for cyc in cycles:
                cur.execute('INSERT INTO cycles(cycle_id,receiver,generation,ordinal,payload) '
                            'VALUES(?,?,?,?,?)',
                            (cyc['cycle_id'], receiver, generation, cyc['ordinal'],
                             cyc['payload_json']))
            if hooks:
                hooks('before_ledger_commit')
            self.c.commit()
            if hooks:
                hooks('after_ledger_commit')
        except BaseException:
            self.c.rollback()
            raise

    # ---- 待确认周期(磁盘预算内) ----
    def pending_count(self):
        return self.c.execute('SELECT COUNT(*) FROM cycles').fetchone()[0]

    def pending_all(self):
        return [dict(zip(('n', 'cycle_id', 'receiver', 'generation', 'ordinal', 'payload'), r))
                for r in self.c.execute(
                    'SELECT n,cycle_id,receiver,generation,ordinal,payload FROM cycles ORDER BY n')]

    def pending_receivers(self):
        return [r[0] for r in self.c.execute('SELECT DISTINCT receiver FROM cycles ORDER BY receiver')]

    def next_pending(self, exclude=frozenset(), policy='rr', rr_state=None):
        """取下一个待投递周期。policy='fifo' 全局先进先出;'rr' 接收器轮询(公平排空)。"""
        rows = self.pending_all()
        rows = [r for r in rows if r['cycle_id'] not in exclude]
        if not rows:
            return None, rr_state
        if policy == 'fifo':
            return rows[0], rr_state
        receivers = sorted({r['receiver'] for r in rows})
        start = 0
        if rr_state in receivers:
            start = (receivers.index(rr_state) + 1) % len(receivers)
        ordered = receivers[start:] + receivers[:start]
        for rx in ordered:
            for r in rows:
                if r['receiver'] == rx:
                    return r, rx
        return None, rr_state

    def reclaim(self, cycle_id):
        """收到可验证确认后回收账本条目。"""
        self.c.execute('DELETE FROM cycles WHERE cycle_id=?', (cycle_id,))
        self.c.commit()

    # ---- 报告 ----
    def payload_bytes(self):
        r = self.c.execute('SELECT COALESCE(SUM(LENGTH(payload)),0) FROM cycles').fetchone()
        return r[0]
