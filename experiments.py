"""工程取舍的可复现实验。

exp_backpressure  有界账本+显式背压(本方案) vs 无界队列(放弃方案):
                  同一慢下游输入,报告内存/磁盘占用与丢失数。
exp_fairness      接收器轮询 RR(本方案) vs 全局 FIFO(放弃方案):
                  R1 突发时 R2 周期的排空延迟。
exp_killpoints    SQLite 单事务账本(本方案)的命名控制点崩溃矩阵:
                  每个 kill 点崩溃+恢复后,周期集合必须与无故障一致。
                  (JSONL 追加日志需要额外的行校验和/截断恢复才能达到
                  同样的原子性,这是放弃它的原因 —— 见 README。)
"""
import os

from scenarios import (CRASH_POINTS, Numberer, cycle_events, fresh_dir, run_case,
                       run_with_faults)
from sim import Driver
from collector import Collector
from sink import Sink


def exp_backpressure(workdir):
    """14 个周期、下游先暂停:有界方案在账本=10 处背压,源保留未确认片段。"""
    fresh_dir(workdir)
    num = Numberer()
    evs = []
    for i in range(14):
        evs += cycle_events('R1', 1, 1, [[i + 1]])
    events = num.assign(evs)
    schedule = ['pause'] * 16 + ['resume']
    res = run_case(events, workdir, schedule=schedule)
    st = res.collector.stats
    unbounded_memory = 14  # 对照:无界队列会把全部 14 个周期堆在采集端内存
    return {
        'delivered': len(res.cycles()), 'lost': 14 - len(res.cycles()),
        'backpressure_events': st['backpressure'],
        'ledger_high_water': st['ledger_hw'], 'flight_high_water': st['flight_hw'],
        'bounded_memory_cycles': st['flight_hw'],
        'bounded_disk_cycles': st['ledger_hw'],
        'unbounded_memory_cycles': unbounded_memory,
        'source_retained': st['backpressure'],  # 背压期间由源保留的片段数
    }


def exp_fairness(workdir):
    """R1 突发 6 周期 + R2 1 周期在下游暂停期全部积压在账本,
    flight=1 恢复排空:R2 周期的排空位次衡量公平性。"""
    num = Numberer()
    evs = []
    for i in range(6):
        evs += cycle_events('R1', 1, 1, [[i + 1]])
    evs += cycle_events('R2', 1, 1, [[99]], talker='GL')
    events = num.assign(evs)
    out = {}
    for policy in ('fifo', 'rr'):
        res = run_case(events, os.path.join(workdir, policy),
                       schedule=['pause'] * 7 + ['resume'],
                       collector_kw={'drain_policy': policy, 'flight_capacity': 1})
        order = [n.detail['cycle_id'] for n in res.collector.notices if n.kind == 'delivered']
        out[policy] = {'r2_drain_position': order.index('R2/g1/c0001') + 1,
                       'drain_order': order}
    return out


def exp_killpoints(workdir):
    """每个命名控制点崩溃一次并恢复,校验周期集合与无故障一致。"""
    num = Numberer()
    evs = []
    for i in range(3):
        evs += cycle_events('R1', 1, 2, [[10 + i], [20 + i]])
    evs += cycle_events('R2', 1, 1, [[77]], talker='GL')
    events = num.assign(evs)
    matrix = {}
    for point in CRASH_POINTS:
        for hit in (1, 2):
            try:
                _fault, _clean, equal = run_with_faults(
                    events, os.path.join(workdir, f'{point}-{hit}'),
                    crash_at=point, crash_hit=hit)
                matrix[f'{point}#{hit}'] = 'PASS' if equal else 'FAIL(set mismatch)'
            except Exception as e:  # noqa: BLE001 - 实验报告需要真实异常
                matrix[f'{point}#{hit}'] = f'FAIL({type(e).__name__}: {e})'
    return matrix
