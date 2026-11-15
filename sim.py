"""确定性源、驱动器与故障注入。

Source:停等式源。当前事件未被确认前一直重送同一事件(源保留未确认
片段);ACK/DUPLICATE 才前进。BACKPRESSURE/NACK_GAP 原地保留。

Driver:每步 accept_event 一次,然后消费一个 schedule 令牌并 pump。
令牌语义(对应 fixtures/sink-schedule.json):
  pause     下游暂停,投递被拒绝(可观测背压)
  receive   解除暂停,允许投递(程序步:接收点)
  commit    解除暂停,允许投递(程序步:提交点)
  lose_ack  下一次投递提交成功后丢弃确认(AckLost)
  retry     触发采集端 reconcile(用可验证确认对账回收)
  resume    清除全部故障模式,恢复正常

CrashHook:命名控制点故障注入。控制点:
  before_ledger_commit / after_ledger_commit
  before_sink_commit / after_sink_commit_before_ack
  after_ack_delivered / after_reclaim
real=True 时 os._exit(3) 真实杀进程;否则抛 SimulatedCrash。
"""
import json
import os
from collections import deque

from collector import ACK, BACKPRESSURE, DUPLICATE, NACK_GAP, REJECT


class SourceRejected(Exception):
    pass


class SimulatedCrash(Exception):
    pass


class CrashHook:
    def __init__(self, point, hit=1, real=False):
        self.point, self.hit, self.real = point, hit, real
        self.count = 0

    def __call__(self, p):
        if p == self.point:
            self.count += 1
            if self.count == self.hit:
                if self.real:
                    os._exit(3)
                raise SimulatedCrash(p)


def load_events(path):
    return json.loads(open(path).read())


def load_schedule(path):
    return json.loads(open(path).read())


def split_sizes(wire, sizes):
    out, i = [], 0
    for s in sizes:
        out.append(wire[i:i + s])
        i += s
    if i < len(wire):
        out.append(wire[i:])
    return [p for p in out if p]


def fragment_events(events, splitter=None):
    """把 data 事件 wire 按 splitter(wire)->[pieces] 切分;
    每 (receiver,generation) 重新从 1 递增编号 seq(与源保证一致)。"""
    out, seqc = [], {}
    for ev in events:
        if ev.get('kind') != 'data':
            out.append(dict(ev))
            continue
        pieces = splitter(ev['wire']) if splitter else [ev['wire']]
        for p in pieces:
            key = (ev['receiver'], ev['generation'])
            seqc[key] = seqc.get(key, 0) + 1
            out.append({'kind': 'data', 'receiver': key[0], 'generation': key[1],
                        'seq': seqc[key], 'wire': p})
    return out


class Source:
    def __init__(self, events):
        self.events = list(events)
        self.pos = 0

    def current(self):
        return self.events[self.pos] if self.pos < len(self.events) else None

    def advance(self, res):
        if res.status in (ACK, DUPLICATE):
            self.pos += 1
        elif res.status in (BACKPRESSURE, NACK_GAP):
            pass  # 未确认:保留,下次重送同一事件
        elif res.status == REJECT:
            raise SourceRejected(res.reason)


class Driver:
    def __init__(self, collector, sink, events, *, schedule=(),
                 checkpoint_path=None, checkpoint_every=3, resume=False):
        self.collector = collector
        self.sink = sink
        self.source = Source(events)
        self.schedule = deque(schedule)
        self.checkpoint_path = checkpoint_path
        self.checkpoint_every = checkpoint_every
        self.log = []
        self.steps = 0
        if resume and checkpoint_path and os.path.exists(checkpoint_path):
            self.source.pos = json.loads(open(checkpoint_path).read())['pos']

    def apply_token(self, tok):
        s = self.sink
        if tok == 'pause':
            s.paused = True
        elif tok in ('receive', 'commit'):
            s.paused = False
        elif tok == 'lose_ack':
            s.paused = False
            s.drop_acks += 1
        elif tok == 'retry':
            self.collector.reconcile()
        elif tok == 'resume':
            s.paused = False
            s.drop_acks = 0
        else:
            raise ValueError(f'unknown schedule token: {tok}')

    def _checkpoint(self):
        if not self.checkpoint_path:
            return
        tmp = self.checkpoint_path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump({'pos': self.source.pos}, f)
        os.replace(tmp, self.checkpoint_path)

    def step(self):
        ev = self.source.current()
        if ev is None:
            return False
        res = self.collector.accept_event(ev)
        self.log.append({'event': ev, 'status': res.status, 'reason': res.reason})
        self.source.advance(res)
        if self.schedule:
            tok = self.schedule.popleft()
            self.apply_token(tok)
            self.log.append({'token': tok})
        self.collector.pump()
        self.steps += 1
        if self.steps % self.checkpoint_every == 0:
            self._checkpoint()
        return True

    def run(self, max_steps=20000):
        while self.step():
            if self.steps > max_steps:
                raise RuntimeError('driver not converging')
        # 排空:继续消费剩余调度令牌,直到账本与 flight 清空
        while self.collector.ledger.pending_count() > 0 or self.collector.flight:
            if self.schedule:
                tok = self.schedule.popleft()
                self.apply_token(tok)
                self.log.append({'token': tok})
            before = (self.collector.ledger.pending_count(), len(self.collector.flight))
            self.collector.pump()
            after = (self.collector.ledger.pending_count(), len(self.collector.flight))
            if before == after and not self.schedule:
                raise RuntimeError(f'drain stuck at {after} (sink paused?)')
        self._checkpoint()
        return self
