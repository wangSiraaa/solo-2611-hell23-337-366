"""采集器:按连接代装配 GSV 周期,有界、可恢复、背压显式。

不变量:
  I1  pending 键含 (receiver,generation,talker):新代第 2 句不能补齐旧代第 1 句。
  I2  同代新第 1 句淘汰同 talker 未完成周期,并记录原因。
  I3  周期身份 = receiver/generation/代内完成序号,与卫星内容无关;
      内容相同的两个真实周期是两个观测(序号不同)。
  I5  重复片段校验 seq 与原字节 sha1:相同重送幂等再确认,冲突重送拒绝。
  I6  界限:<=4 接收器、每接收器 <=2 未完成周期、内存待确认 <=3(flight)、
      磁盘待确认 <=10(ledger);达到界限返回显式 BACKPRESSURE,不消费、不确认。
  I10 账本条目只在 sink.verify_ack 通过后回收。

事件 API:
  accept_event({'kind':'data','receiver','generation','seq','wire'})
  accept_event({'kind':'disconnect','receiver','generation'})
返回 Result(status=ack/duplicate/nack_gap/backpressure/reject)。

兼容:Collector() 无参构造进入 legacy 模式,accept(receiver,generation,line)
与 .output 保持基线行为(demo.py 可原样运行);legacy 模式无持久投递语义。
"""
import hashlib
import json
from dataclasses import dataclass, field

from ledger import Ledger
from nmea import MAX_LINE, MAX_WIRE, NmeaError, decode
from sink import AckLost, SinkPaused

ACK = 'ack'
DUPLICATE = 'duplicate'
NACK_GAP = 'nack_gap'
BACKPRESSURE = 'backpressure'
REJECT = 'reject'


@dataclass
class Result:
    status: str
    reason: str = ''
    cycles: list = field(default_factory=list)  # 本片段完成的周期 payload
    detail: dict = field(default_factory=dict)


@dataclass
class Notice:
    kind: str
    receiver: str = ''
    generation: int = 0
    reason: str = ''
    detail: dict = field(default_factory=dict)


class _PumpStop(Exception):
    pass


class _GenState:
    """单 (receiver,generation) 的装配状态。

    每次片段消费后整体持久化到账本(与确认同事务),崩溃恢复时从账本
    精确重建 —— 已确认片段的内容永不因进程崩溃丢失。"""

    __slots__ = ('buffer', 'pending', 'next_ordinal')

    def __init__(self, next_ordinal, buffer=b'', pending=None):
        self.buffer = buffer
        self.pending = pending or {}  # talker -> {'total':t,'parts':{part:{d,seq,sha,line}}}
        self.next_ordinal = next_ordinal

    @classmethod
    def restore(cls, ledger, rx, gen):
        gs = ledger.gen_state(rx, gen)
        pending = {t: {'total': p['total'],
                       'parts': {int(k): v for k, v in p['parts'].items()}}
                   for t, p in gs['pending'].items()}
        return cls(gs['next_ordinal'], gs['buffer'], pending)

    def dump_pending(self):
        return {t: {'total': p['total'],
                    'parts': {str(k): v for k, v in p['parts'].items()}}
                for t, p in self.pending.items()}


class Collector:
    def __init__(self, ledger_path=None, sink=None, *, max_receivers=4,
                 max_pending_per_receiver=2, ledger_capacity=10, flight_capacity=3,
                 drain_policy='rr', hooks=None, legacy=None):
        if legacy is None:
            legacy = ledger_path is None and sink is None
        self.legacy = legacy
        self.ledger = Ledger(ledger_path or ':memory:')
        self.sink = sink
        self.max_receivers = max_receivers
        self.max_pending_per_receiver = max_pending_per_receiver
        self.ledger_capacity = ledger_capacity
        self.flight_capacity = flight_capacity
        self.drain_policy = drain_policy
        self.hooks = hooks or (lambda point: None)
        self.states = {}          # (rx,gen) -> _GenState
        self.last_gen = {}        # rx -> 已见最大代
        self.flight = {}          # cycle_id -> payload(内存待确认,<= flight_capacity)
        self.rr_state = None
        self.notices = []
        self.output = []          # legacy: 已完成周期(基线形状)
        self._legacy_seq = {}
        self.stats = dict(accepted=0, duplicates=0, rejects=0, conflicts=0,
                          backpressure=0, nack_gap=0, malformed=0, evicted=0,
                          cycles_completed=0, delivered=0, recovered=0,
                          ledger_hw=0, flight_hw=0, bytes_persisted=0)
        if self.sink is not None:
            self.recover()

    # ------------------------------------------------------------------ 事件入口
    def accept_event(self, ev):
        kind = ev.get('kind')
        if kind == 'data':
            return self._accept_data(ev['receiver'], int(ev['generation']),
                                     int(ev['seq']), ev['wire'])
        if kind == 'disconnect':
            return self._accept_disconnect(ev['receiver'], int(ev['generation']))
        return Result(REJECT, 'unknown_event')

    def _notice(self, kind, receiver='', generation=0, reason='', detail=None, **kw):
        d = dict(detail or {})
        d.update(kw)
        n = Notice(kind, receiver, generation, reason, d)
        self.notices.append(n)
        return n

    def _active_receivers(self):
        rx = {r for (r, _g) in self.states}
        rx.update(self.ledger.pending_receivers())
        return rx

    def _accept_data(self, rx, gen, seq, wire):
        try:
            wb = wire.encode('ascii') if isinstance(wire, str) else bytes(wire)
        except (UnicodeEncodeError, UnicodeDecodeError):
            self.stats['rejects'] += 1
            return Result(REJECT, 'fragment_not_ascii')
        if len(wb) > MAX_WIRE:
            self.stats['rejects'] += 1
            return Result(REJECT, 'fragment_too_large')

        # 代际:更老的代是陈旧输入;更新的代使旧代全部未完成状态作废
        last = self.last_gen.get(rx)
        if last is not None and gen < last:
            self.stats['rejects'] += 1
            self._notice('reject', rx, gen, 'stale_generation', last_gen=last)
            return Result(REJECT, 'stale_generation')
        if last is None or gen > last:
            if last is not None:
                self._drop_generation(rx, last, 'generation_superseded')
            if rx not in self._active_receivers() and len(self._active_receivers()) >= self.max_receivers:
                self.stats['rejects'] += 1
                self._notice('reject', rx, gen, 'receiver_limit', max=self.max_receivers)
                return Result(REJECT, 'receiver_limit')
            self.last_gen[rx] = gen
        elif rx not in self._active_receivers() and (rx, gen) not in self.states:
            if len(self._active_receivers()) >= self.max_receivers:
                self.stats['rejects'] += 1
                self._notice('reject', rx, gen, 'receiver_limit', max=self.max_receivers)
                return Result(REJECT, 'receiver_limit')

        # 顺序与重复:seq 严格递增;重复只可能是原 seq 的重送
        hw = self.ledger.high_water(rx, gen)
        sha = hashlib.sha1(wb).hexdigest()
        if seq <= hw:
            old = self.ledger.frag_hash(rx, gen, seq)
            if old is not None and old != sha:
                self.stats['conflicts'] += 1
                self._notice('conflict', rx, gen, 'conflicting_duplicate',
                             seq=seq, want=old, got=sha)
                return Result(REJECT, 'conflict', detail={'seq': seq})
            self.stats['duplicates'] += 1
            return Result(DUPLICATE, detail={'seq': seq})  # 幂等再确认
        if seq != hw + 1:
            self.stats['nack_gap'] += 1
            self._notice('nack_gap', rx, gen, 'gap', seq=seq, expected=hw + 1)
            return Result(NACK_GAP, 'gap', detail={'expected': hw + 1})

        state = self.states.get((rx, gen))
        if state is None:
            state = _GenState.restore(self.ledger, rx, gen)
            self.states[(rx, gen)] = state

        # 背压:预留本片段最多可完成的周期数,超出账本容量则整体拒绝,
        # 不消费任何字节、不给确认 —— 源可稍后原样重送。
        prospective = state.buffer + wb
        n_lines = prospective.count(b'\r\n')
        if self.ledger.pending_count() + n_lines > self.ledger_capacity:
            self.stats['backpressure'] += 1
            self._notice('backpressure', rx, gen, 'ledger_full',
                         pending=self.ledger.pending_count(), capacity=self.ledger_capacity)
            return Result(BACKPRESSURE, 'ledger_full')

        # 消费:重组 CRLF 句并装配
        state.buffer = prospective
        lines = state.buffer.split(b'\r\n')
        state.buffer = lines.pop()  # 末尾半句留待后续片段
        cycles = []
        for lb in lines:
            cyc = self._consume_line(rx, gen, state, lb, seq)
            if cyc:
                cycles.append(cyc)
        if len(state.buffer) + 2 > MAX_LINE:
            self.stats['malformed'] += 1
            self._notice('malformed', rx, gen, 'line_too_long', buffered=len(state.buffer))
            state.buffer = b''
            self._evict_all_pending(rx, gen, state, 'malformed_line')

        # 持久化:片段hash + 高水位 + 序号计数器 + 行缓冲 + pending + 完成周期,
        # 同一事务(采集确认边界:ACK 之后这些内容必须持久)
        stored = [] if self.legacy else [self._stored_payload(c) for c in cycles]
        self.ledger.consume_fragment(rx, gen, seq, sha, stored,
                                     next_ordinal=state.next_ordinal,
                                     buffer=state.buffer,
                                     pending=state.dump_pending(),
                                     hooks=self.hooks)
        self.stats['accepted'] += 1
        self.stats['cycles_completed'] += len(cycles)
        self.stats['bytes_persisted'] += sum(len(c['payload_json']) for c in stored)
        self.stats['ledger_hw'] = max(self.stats['ledger_hw'], self.ledger.pending_count())
        if self.legacy:
            for c in cycles:
                self.output.append(self._legacy_shape(c))
            for c in stored:  # legacy 无投递跟踪,立即回收
                self.ledger.reclaim(c['cycle_id'])
        return Result(ACK, cycles=cycles, detail={'seq': seq})

    def _consume_line(self, rx, gen, state, lb, seq):
        line_sha = hashlib.sha1(lb).hexdigest()
        try:
            d = decode(lb)
        except NmeaError as e:
            self.stats['malformed'] += 1
            self._notice('malformed', rx, gen, e.reason, line=lb[:32].decode('ascii', 'replace'))
            self._evict_all_pending(rx, gen, state, 'malformed_sentence')
            return None
        return self._assemble(rx, gen, state, d, seq, line_sha, lb.decode('ascii'))

    def _assemble(self, rx, gen, state, d, seq, line_sha, line_str):
        key = d['talker']
        pend = state.pending.get(key)
        reason = None
        if pend is not None:
            if d['part'] == 1:
                reason = 'superseded_by_part1'      # 新第 1 句淘汰同代未完成周期
            elif pend['total'] != d['total']:
                reason = 'inconsistent_total'
            elif d['part'] in pend['parts']:
                reason = 'duplicate_part'
        if reason:
            self._evict(rx, gen, key, pend, reason)
            pend = None
        if pend is None:
            pend = {'total': d['total'], 'parts': {}}
            state.pending[key] = pend
            if len(state.pending) > self.max_pending_per_receiver:
                # GP/GL 两 talker 下不会触发;防御性显式淘汰最旧
                oldest = next(iter(state.pending))
                self._evict(rx, gen, oldest, state.pending.pop(oldest), 'pending_limit')
                pend = state.pending[key]
        pend['parts'][d['part']] = {'d': d, 'seq': seq, 'sha': line_sha, 'line': line_str}
        if len(pend['parts']) == pend['total']:
            del state.pending[key]
            return self._complete(rx, gen, state, key, pend)
        return None

    def _complete(self, rx, gen, state, talker, pend):
        ordinal = state.next_ordinal
        state.next_ordinal += 1
        parts = [pend['parts'][i] for i in sorted(pend['parts'])]
        cycle_id = f'{rx}/g{gen}/c{ordinal:04d}'
        return {
            'cycle_id': cycle_id, 'receiver': rx, 'generation': gen, 'talker': talker,
            'total': pend['total'], 'ordinal': ordinal,
            'first_seq': parts[0]['seq'], 'last_seq': parts[-1]['seq'],
            'sentences': [{'seq': p['seq'], 'sha1': p['sha'], 'line': p['line']} for p in parts],
            'satellites': [s for p in parts for s in p['d']['satellites']],
        }

    @staticmethod
    def _stored_payload(cycle):
        c = dict(cycle)
        c['payload_json'] = json.dumps(cycle, sort_keys=True)
        return c

    @staticmethod
    def _legacy_shape(cycle):
        return {'receiver': cycle['receiver'], 'generation': cycle['generation'],
                'talker': cycle['talker'],
                'parts': [decode(s['line']) for s in cycle['sentences']]}

    def _evict(self, rx, gen, talker, pend, reason):
        self.stats['evicted'] += 1
        self._notice('evicted', rx, gen, reason, talker=talker,
                     parts_present=sorted(pend['parts']), total=pend['total'],
                     first_seq=min(p['seq'] for p in pend['parts'].values()))

    def _evict_all_pending(self, rx, gen, state, reason):
        for talker in list(state.pending):
            self._evict(rx, gen, talker, state.pending.pop(talker), reason)

    def _drop_generation(self, rx, gen, reason):
        state = self.states.pop((rx, gen), None)
        if state:
            self._evict_all_pending(rx, gen, state, reason)
        self.ledger.clear_assembly(rx, gen)  # 持久态同步丢弃(保留去重状态)

    def _accept_disconnect(self, rx, gen):
        self._drop_generation(rx, gen, 'disconnect')
        self._notice('disconnected', rx, gen)
        return Result(ACK)

    # ------------------------------------------------------------------ 投递泵
    def pump(self):
        """把账本中的周期经有界 flight 窗口送往下游;公平排空(默认接收器轮询)。"""
        if self.sink is None:
            return
        try:
            for cid in list(self.flight):           # 先重试内存中未确认的
                self._deliver_one(cid, self.flight[cid], is_retry=True)
            while len(self.flight) < self.flight_capacity:
                row, self.rr_state = self.ledger.next_pending(
                    exclude=set(self.flight), policy=self.drain_policy, rr_state=self.rr_state)
                if row is None:
                    break
                payload = json.loads(row['payload'])
                self.flight[row['cycle_id']] = payload
                self.stats['flight_hw'] = max(self.stats['flight_hw'], len(self.flight))
                self._deliver_one(row['cycle_id'], payload, is_retry=False)
        except _PumpStop:
            pass

    def _deliver_one(self, cid, payload, is_retry):
        try:
            token = self.sink.deliver(payload)
        except SinkPaused:
            self._notice('sink_paused', detail={'cycle_id': cid})
            raise _PumpStop
        except AckLost:
            # 提交已成功、确认丢失:留在 flight/ledger,由下一轮 pump 的
            # 幂等重投或 reconcile(可验证确认对账)回收,不在此处自愈。
            self._notice('ack_lost', payload.get('receiver', ''), payload.get('generation', 0),
                         detail={'cycle_id': cid})
            return
        if token and self.sink.verify_ack(cid, token):
            self.ledger.reclaim(cid)              # 仅可验证确认后回收账本
            self.flight.pop(cid, None)
            self.stats['delivered'] += 1
            self._notice('delivered', payload.get('receiver', ''), payload.get('generation', 0),
                         detail={'cycle_id': cid, 'token': token[:12], 'retry': is_retry})
            self.hooks('after_reclaim')

    # ------------------------------------------------------------------ 恢复
    def reconcile(self):
        """对账:账本中每个待确认周期向下游求证可验证确认,命中即回收。"""
        if self.sink is None:
            return
        for row in self.ledger.pending_all():
            cid = row['cycle_id']
            token = self.sink.ack_of(cid)
            if token and self.sink.verify_ack(cid, token):
                self.ledger.reclaim(cid)
                self.flight.pop(cid, None)
                self.stats['recovered'] += 1
                self._notice('recovered', row['receiver'], row['generation'],
                             detail={'cycle_id': cid, 'token': token[:12]})

    def recover(self):
        """进程重启入口:易失装配状态丢弃(源重送),账本与下游对账。"""
        self.reconcile()
        self._notice('recover', detail={'ledger_pending': self.ledger.pending_count()})

    # ------------------------------------------------------------------ 基线兼容
    def accept(self, receiver, generation, line):
        """legacy 模式:与基线 Collector.accept 同签名同返回形状。"""
        if not self.legacy:
            raise RuntimeError('durable 模式请使用 accept_event')
        key = (receiver, generation)
        self._legacy_seq[key] = self._legacy_seq.get(key, 0) + 1
        res = self.accept_event({'kind': 'data', 'receiver': receiver, 'generation': generation,
                                 'seq': self._legacy_seq[key], 'wire': line})
        if res.cycles:
            return self._legacy_shape(res.cycles[0])
        return None

    def close(self):
        self.ledger.close()
