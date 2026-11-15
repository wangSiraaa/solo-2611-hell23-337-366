"""恢复与恰好一次:丢确认、崩溃各控制点、可验证确认、无故障等价。"""
import os
import sqlite3

import pytest

from collector import Collector
from scenarios import (CRASH_POINTS, Numberer, cycle_events, run_case,
                       run_with_faults)
from sim import SimulatedCrash
from sink import Sink


def three_cycle_events():
    num = Numberer()
    evs = []
    for i in range(3):
        evs += cycle_events('R1', 1, 2, [[10 + i], [20 + i]])
    evs += cycle_events('R2', 1, 1, [[77]], talker='GL')
    return num.assign(evs)


class TestLostAck:
    def test_commit_then_lose_ack_exactly_once(self, tmp_path):
        """下游已提交但确认丢失:重试后恰好保留一个周期。"""
        wd = str(tmp_path)
        sink = Sink(os.path.join(wd, 's.db'))
        col = Collector(os.path.join(wd, 'l.db'), sink)
        num = Numberer()
        evs = num.assign(cycle_events('R1', 1, 1, [[11]]))
        for e in evs:
            col.accept_event(e)
        sink.drop_acks = 1
        col.pump()                       # 提交成功,确认丢失
        assert sink.has_cycle('R1/g1/c0001')
        assert col.ledger.pending_count() == 1   # 未回收
        col.pump()                       # 重试:幂等返回既有确认
        assert col.ledger.pending_count() == 0
        assert len(sink.cycles_with_ids()) == 1  # 恰好一行

    def test_reconcile_after_restart_recovers_lost_ack(self, tmp_path):
        wd = str(tmp_path)
        sink = Sink(os.path.join(wd, 's.db'))
        col = Collector(os.path.join(wd, 'l.db'), sink)
        num = Numberer()
        for e in num.assign(cycle_events('R1', 1, 1, [[11]])):
            col.accept_event(e)
        sink.drop_acks = 1
        col.pump()
        # 模拟重启:新 Collector 用同一账本与下游
        sink2 = Sink(os.path.join(wd, 's.db'))
        col2 = Collector(os.path.join(wd, 'l.db'), sink2)   # __init__ 自动 recover
        assert col2.ledger.pending_count() == 0
        assert len(sink2.cycles_with_ids()) == 1

    def test_reclaim_requires_verifiable_ack(self, tmp_path):
        """确认行被人为移除(不可验证)时,账本条目不得回收。"""
        wd = str(tmp_path)
        sink = Sink(os.path.join(wd, 's.db'))
        col = Collector(os.path.join(wd, 'l.db'), sink)
        num = Numberer()
        for e in num.assign(cycle_events('R1', 1, 1, [[11]])):
            col.accept_event(e)
        sink.drop_acks = 1
        col.pump()
        sink.c.execute("DELETE FROM acks WHERE cycle_id='R1/g1/c0001'")
        sink.c.commit()
        col.reconcile()
        assert col.ledger.pending_count() == 1   # 不可验证 → 保留
        col.pump()                               # 重投自愈:补写确认行
        assert col.ledger.pending_count() == 0
        assert len(sink.cycles_with_ids()) == 1  # 仍恰好一行


class TestCrashRecovery:
    @pytest.mark.parametrize('point', CRASH_POINTS)
    @pytest.mark.parametrize('hit', [1, 2])
    def test_killpoint_recovery_matches_no_fault(self, tmp_path, point, hit):
        fault, clean, equal = run_with_faults(
            three_cycle_events(), str(tmp_path / 'wd'), crash_at=point, crash_hit=hit)
        assert equal, f'{point}#{hit}'
        assert len(fault.cycles()) == 4

    def test_crash_mid_fragment_source_resends(self, tmp_path):
        """片段消费中途崩溃(提交前):源重送,结果与无故障一致。"""
        fault, clean, equal = run_with_faults(
            three_cycle_events(), str(tmp_path / 'wd'),
            crash_at='before_ledger_commit', crash_hit=3)
        assert equal

    def test_pending_half_cycle_survives_crash(self, tmp_path):
        """已确认的半周期在崩溃后仍能由后续片段完成(装配状态持久)。"""
        wd = str(tmp_path)
        from nmea import sentence
        col = Collector(os.path.join(wd, 'l.db'))
        col.accept_event({'kind': 'data', 'receiver': 'R1', 'generation': 1,
                          'seq': 1, 'wire': sentence(2, 1, 11)})
        col2 = Collector(os.path.join(wd, 'l.db'))   # 重启
        r = col2.accept_event({'kind': 'data', 'receiver': 'R1', 'generation': 1,
                               'seq': 2, 'wire': sentence(2, 2, 12)})
        assert [c['cycle_id'] for c in r.cycles] == ['R1/g1/c0001']
        assert r.cycles[0]['satellites'][:4] == ['11', '30', '100', '40']


class TestNoFaultEquivalence:
    def test_fault_schedule_equals_clean_run(self, tmp_path):
        from scenarios import fixture_schedule, ev_lost_ack
        fault, clean, equal = run_with_faults(ev_lost_ack(), str(tmp_path),
                                              schedule=fixture_schedule())
        assert equal
        assert len(fault.cycles()) == 3

    def test_slow_consumer_equals_clean_run(self, tmp_path):
        from scenarios import ev_slow_consumer
        fault, clean, equal = run_with_faults(
            ev_slow_consumer(), str(tmp_path), schedule=['pause'] * 12 + ['resume'])
        assert equal
        assert fault.collector.stats['backpressure'] > 0
        assert fault.collector.stats['ledger_hw'] <= 10
