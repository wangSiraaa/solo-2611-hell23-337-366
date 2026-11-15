"""确定性演示场景与运行器。

run_case:单次运行(可带崩溃点);run_with_faults:故障运行 + 无故障
参照运行,比较完整周期集合(身份+内容规范化哈希)是否一致。

四个命名场景(deliverable 4):
  slow-consumer        下游暂停,账本到界,显式背压,恢复后无丢失
  crash-after-complete 周期完成后在命名控制点杀进程,恢复后继续
  lost-ack             提交后丢确认(fixtures/sink-schedule.json)
  reconnect            断连跨代重连(fixtures/events.json)
"""
import hashlib
import json
import os
import shutil

from collector import Collector
from nmea import gsv_sentence
from sim import CrashHook, Driver, SimulatedCrash, load_events, load_schedule
from sink import Sink

REPO = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(REPO, 'fixtures')

CRASH_POINTS = ('before_ledger_commit', 'after_ledger_commit',
                'before_sink_commit', 'after_sink_commit_before_ack',
                'after_ack_delivered', 'after_reclaim')


def fresh_dir(path):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    return path


def data(rx, gen, wire):
    return {'kind': 'data', 'receiver': rx, 'generation': gen, 'wire': wire}


def disc(rx, gen):
    return {'kind': 'disconnect', 'receiver': rx, 'generation': gen}


def cycle_events(rx, gen, total, prns_per_part, talker='GP'):
    """一个完整周期的 data 事件序列(seq 由 Driver 侧 Source 不重编 ——
    这里直接显式编号,调用方保证每 (rx,gen) 递增)。"""
    return [data(rx, gen, gsv_sentence(talker, total, i + 1, len(prns_per_part[i]), prns_per_part[i]))
            for i in range(total)]


class Numberer:
    """显式 seq 编号器:每 (receiver,generation) 从 1 严格递增。"""

    def __init__(self):
        self.n = {}

    def assign(self, events):
        out = []
        for ev in events:
            if ev['kind'] == 'data':
                key = (ev['receiver'], ev['generation'])
                self.n[key] = self.n.get(key, 0) + 1
                ev = dict(ev, seq=self.n[key])
            out.append(ev)
        return out


class RunResult:
    def __init__(self, collector, sink, driver, workdir):
        self.collector, self.sink, self.driver, self.workdir = collector, sink, driver, workdir

    def cycles(self):
        return {cid: payload for _n, cid, payload in self.sink.cycles_with_ids()}

    def signature(self):
        """完整周期集合的规范化签名(身份 + 内容哈希)。"""
        sig = []
        for cid, payload in sorted(self.cycles().items()):
            canon = json.dumps(payload, sort_keys=True)
            sig.append((cid, hashlib.sha256(canon.encode()).hexdigest()[:16]))
        return sig

    def chain_lines(self):
        """原句 -> 周期 -> 投递确认 的身份链。"""
        lines = []
        delivered = {n.detail.get('cycle_id'): n.detail
                     for n in self.collector.notices if n.kind == 'delivered'}
        for n, cid, payload in self.sink.cycles_with_ids():
            lines.append(f"cycle {cid}  (rx={payload['receiver']} gen={payload['generation']} "
                         f"talker={payload['talker']} total={payload['total']})")
            for s in payload['sentences']:
                lines.append(f"    sentence seq={s['seq']} sha1={s['sha1'][:10]}  {s['line']}")
            ack = self.sink.ack_of(cid)
            d = delivered.get(cid, {})
            lines.append(f"    -> ledger(持久) -> sink row#{n} "
                         f"ack={ack[:12] if ack else '?'}… verified={self.sink.verify_ack(cid, ack) if ack else False}"
                         + (f" (retry={d['retry']})" if d.get('retry') else ''))
        return lines

    def byte_report(self):
        lb = os.path.join(self.workdir, 'ledger.db')
        sb = os.path.join(self.workdir, 'sink.db')
        return {
            'ledger_payload_bytes_pending': self.collector.ledger.payload_bytes(),
            'ledger_file_bytes': os.path.getsize(lb) if os.path.exists(lb) else 0,
            'sink_file_bytes': os.path.getsize(sb) if os.path.exists(sb) else 0,
            'stats': dict(self.collector.stats),
        }


def run_case(events, workdir, *, schedule=(), crash_at=None, crash_hit=1,
             drain_policy='rr', resume=False, collector_kw=None, real_kill=False):
    """单次运行。crash_at 命中时:real_kill 则 os._exit(3),否则抛 SimulatedCrash。"""
    fresh = not resume
    if fresh:
        fresh_dir(workdir)
    hook = CrashHook(crash_at, crash_hit, real=real_kill) if crash_at else None
    sink = Sink(os.path.join(workdir, 'sink.db'), hooks=hook)
    kw = {'drain_policy': drain_policy}
    kw.update(collector_kw or {})
    col = Collector(os.path.join(workdir, 'ledger.db'), sink, hooks=hook, **kw)
    drv = Driver(col, sink, events, schedule=schedule,
                 checkpoint_path=os.path.join(workdir, 'driver.json'), resume=resume)
    drv.run()
    return RunResult(col, sink, drv, workdir)


def run_with_faults(events, workdir, *, schedule=(), crash_at=None, crash_hit=1,
                    collector_kw=None):
    """故障运行(可含崩溃+恢复)与无故障运行,返回 (fault_result, clean_result, equal)。"""
    clean = run_case(events, os.path.join(workdir, 'clean'))
    if crash_at:
        try:
            run_case(events, os.path.join(workdir, 'fault'),
                     schedule=schedule, crash_at=crash_at, crash_hit=crash_hit,
                     collector_kw=collector_kw)
            raise AssertionError('crash point never fired')
        except SimulatedCrash:
            pass
        fault = run_case(events, os.path.join(workdir, 'fault'),
                         schedule=schedule, resume=True, collector_kw=collector_kw)
    else:
        fault = run_case(events, os.path.join(workdir, 'fault'),
                         schedule=schedule, collector_kw=collector_kw)
    return fault, clean, fault.signature() == clean.signature()


# ------------------------------------------------------------------ 场景事件
def ev_slow_consumer():
    num = Numberer()
    evs = []
    for i in range(12):  # R1 突发 12 个单句周期(足以填满账本容量 10)
        evs += cycle_events('R1', 1, 1, [[10 + i]])
    for i in range(2):  # R2 两个单句周期
        evs += cycle_events('R2', 1, 1, [[30 + i]], talker='GL')
    return num.assign(evs)


def ev_crash_after_complete():
    num = Numberer()
    evs = []
    for i in range(3):
        evs += cycle_events('R1', 1, 2, [[40 + i], [50 + i]])
    evs += cycle_events('R2', 1, 1, [[60]])
    return num.assign(evs)


def ev_lost_ack():
    num = Numberer()
    evs = cycle_events('R1', 1, 2, [[70], [71]])
    evs += cycle_events('R2', 1, 1, [[72]], talker='GL')
    evs += cycle_events('R1', 1, 1, [[73]])
    return num.assign(evs)


def ev_reconnect():
    return load_events(os.path.join(FIXTURES, 'events.json'))


def fixture_schedule():
    return load_schedule(os.path.join(FIXTURES, 'sink-schedule.json'))


# ------------------------------------------------------------------ 命名场景
def scenario_slow_consumer(workdir):
    schedule = ['pause'] * 12 + ['resume']
    fault, clean, equal = run_with_faults(ev_slow_consumer(), workdir, schedule=schedule)
    return {'name': 'slow-consumer', 'equal': equal, 'fault': fault, 'clean': clean,
            'expect': {'cycles': 14, 'backpressure_seen': fault.collector.stats['backpressure'] > 0,
                       'ledger_hw_le_10': fault.collector.stats['ledger_hw'] <= 10,
                       'flight_hw_le_3': fault.collector.stats['flight_hw'] <= 3}}


def scenario_crash_after_complete(workdir):
    fault, clean, equal = run_with_faults(
        ev_crash_after_complete(), workdir,
        crash_at='after_ledger_commit', crash_hit=2)
    return {'name': 'crash-after-complete', 'equal': equal, 'fault': fault, 'clean': clean,
            'expect': {'cycles': 4}}


def scenario_lost_ack(workdir):
    fault, clean, equal = run_with_faults(ev_lost_ack(), workdir, schedule=fixture_schedule())
    ack_lost = any(n.kind == 'ack_lost' for n in fault.collector.notices)
    recovered = fault.collector.stats['recovered'] + sum(
        1 for n in fault.collector.notices if n.kind == 'delivered' and n.detail.get('retry'))
    return {'name': 'lost-ack', 'equal': equal, 'fault': fault, 'clean': clean,
            'expect': {'cycles': 3, 'ack_lost_seen': ack_lost, 'recovered_seen': recovered > 0}}


def scenario_reconnect(workdir):
    fault, clean, equal = run_with_faults(ev_reconnect(), workdir)
    return {'name': 'reconnect', 'equal': equal, 'fault': fault, 'clean': clean,
            'expect': {'cycles': 2,
                       'cycle_ids': sorted(fault.cycles()),
                       'no_cross_generation': all(
                           p['generation'] == int(cid.split('/g')[1].split('/')[0])
                           for cid, p in fault.cycles().items())}}


SCENARIOS = {'slow-consumer': scenario_slow_consumer,
             'crash-after-complete': scenario_crash_after_complete,
             'lost-ack': scenario_lost_ack,
             'reconnect': scenario_reconnect}
