"""界限与背压:接收器数、pending 数、内存/磁盘待确认、显式背压与重送、排空隔离。"""
import os

import pytest

from collector import (ACK, BACKPRESSURE, REJECT, Collector)
from nmea import sentence
from scenarios import Numberer, cycle_events, data, run_case
from sink import Sink


def ev(rx, gen, seq, wire):
    return {'kind': 'data', 'receiver': rx, 'generation': gen, 'seq': seq, 'wire': wire}


class TestReceiverLimit:
    def test_fifth_receiver_rejected_explicitly(self, tmp_path):
        col = Collector(str(tmp_path / 'l.db'))
        for i, rx in enumerate(['R1', 'R2', 'R3', 'R4']):
            r = col.accept_event(ev(rx, 1, 1, sentence(2, 1, 10 + i)))
            assert r.status == ACK
        r = col.accept_event(ev('R5', 1, 1, sentence(2, 1, 20)))
        assert r.status == REJECT and r.reason == 'receiver_limit'
        assert any(n.reason == 'receiver_limit' for n in col.notices if n.kind == 'reject')

    def test_disconnect_frees_receiver_slot(self, tmp_path):
        col = Collector(str(tmp_path / 'l.db'))
        for i, rx in enumerate(['R1', 'R2', 'R3', 'R4']):
            col.accept_event(ev(rx, 1, 1, sentence(2, 1, 10 + i)))
        col.accept_event({'kind': 'disconnect', 'receiver': 'R1', 'generation': 1})
        r = col.accept_event(ev('R5', 1, 1, sentence(2, 1, 20)))
        assert r.status == ACK


class TestPendingLimit:
    def test_pending_never_exceeds_two_per_receiver(self, tmp_path):
        """GP/GL 各一个 pending;同 talker 新第1句是淘汰而非堆积。"""
        from nmea import gsv_sentence
        col = Collector(str(tmp_path / 'l.db'))
        col.accept_event(ev('R1', 1, 1, sentence(3, 1, 11)))                  # GP pending
        col.accept_event(ev('R1', 1, 2, gsv_sentence('GL', 3, 1, 1, [21])))   # GL pending
        st = col.states[('R1', 1)]
        assert len(st.pending) == 2
        col.accept_event(ev('R1', 1, 3, sentence(3, 1, 31)))                  # 淘汰 GP
        st = col.states[('R1', 1)]
        assert len(st.pending) <= col.max_pending_per_receiver == 2
        assert st.pending['GP']['parts'][1]['d']['satellites'][0] == '31'


class TestBackpressure:
    def _fill_ledger(self, col, n):
        num = 0
        for i in range(n):
            r = col.accept_event(ev('R1', 1, i + 1, sentence(1, 1, 10 + i)))
            assert r.status == ACK, r.reason
            num += 1
        return num

    def test_ledger_capacity_explicit_backpressure_then_resume(self, tmp_path):
        sink = Sink(str(tmp_path / 's.db'))
        sink.paused = True
        col = Collector(str(tmp_path / 'l.db'), sink)
        self._fill_ledger(col, 10)
        assert col.ledger.pending_count() == 10
        r = col.accept_event(ev('R1', 1, 11, sentence(1, 1, 99)))
        assert r.status == BACKPRESSURE and r.reason == 'ledger_full'
        # 未消费:hw 未推进,源可原样重送
        assert col.ledger.high_water('R1', 1) == 10
        sink.paused = False
        col.pump()
        r = col.accept_event(ev('R1', 1, 11, sentence(1, 1, 99)))
        assert r.status == ACK
        col.pump()
        while col.ledger.pending_count():
            col.pump()
        assert len(sink.cycles_with_ids()) == 11

    def test_backpressure_not_silent_drop(self, tmp_path):
        """背压期间到达的片段之后必须能原样重送并成功。"""
        sink = Sink(str(tmp_path / 's.db'))
        sink.paused = True
        col = Collector(str(tmp_path / 'l.db'), sink)
        self._fill_ledger(col, 10)
        for _ in range(3):
            assert col.accept_event(ev('R1', 1, 11, sentence(1, 1, 99))).status == BACKPRESSURE
        sink.paused = False
        for _ in range(5):
            col.pump()
        assert col.accept_event(ev('R1', 1, 11, sentence(1, 1, 99))).status == ACK
        for _ in range(5):
            col.pump()
        assert len(sink.cycles_with_ids()) == 11
        assert col.stats['cycles_completed'] == 11

    def test_flight_capacity_bounds_memory(self, tmp_path):
        sink = Sink(str(tmp_path / 's.db'))
        col = Collector(str(tmp_path / 'l.db'), sink, flight_capacity=3)
        self._fill_ledger(col, 8)
        sink.drop_acks = 10          # 提交成功但确认全丢:flight 堆积到上限
        col.pump()
        assert len(col.flight) == 3
        assert col.stats['flight_hw'] == 3
        col.reconcile()              # 可验证确认对账:仅回收已提交的 3 个
        assert len(col.flight) == 0
        assert col.ledger.pending_count() == 5
        sink.drop_acks = 0
        for _ in range(4):
            col.pump()
        assert col.ledger.pending_count() == 0
        assert len(sink.cycles_with_ids()) == 8

    def test_multi_line_fragment_reservation_atomic(self, tmp_path):
        """一个片段含多句、空间不足时整体拒绝,不部分消费。"""
        sink = Sink(str(tmp_path / 's.db'))
        sink.paused = True
        col = Collector(str(tmp_path / 'l.db'), sink)
        self._fill_ledger(col, 9)
        two = sentence(1, 1, 21) + sentence(1, 1, 22)   # 一个片段两个周期
        r = col.accept_event(ev('R1', 1, 10, two))
        assert r.status == BACKPRESSURE                  # 9+2 > 10 → 整体拒绝
        assert col.ledger.high_water('R1', 1) == 9
        sink.paused = False
        col.pump(); col.pump()
        r = col.accept_event(ev('R1', 1, 10, two))
        assert r.status == ACK and len(r.cycles) == 2


class TestDrainIsolation:
    def test_malformed_flood_does_not_block_other_receivers(self, tmp_path):
        """R1 持续畸形输入,R2/R3 的完整周期仍全部排空。"""
        sink = Sink(str(tmp_path / 's.db'))
        col = Collector(str(tmp_path / 'l.db'), sink)
        bad = '$GPGSV,1,1,02,11,30,100,40*00\r\n'
        num = Numberer()
        evs = []
        for i in range(3):
            evs += cycle_events('R2', 1, 1, [[30 + i]], talker='GL')
            evs += cycle_events('R3', 1, 1, [[40 + i]])
            evs += [data('R1', 1, bad)]
        for e in num.assign(evs):
            col.accept_event(e)
            col.pump()
        while col.ledger.pending_count():
            col.pump()
        got = sorted(cid for _n, cid, _p in sink.cycles_with_ids())
        assert got == [f'R2/g1/c000{i}' for i in (1, 2, 3)] + \
                      [f'R3/g1/c000{i}' for i in (1, 2, 3)]
        assert col.stats['malformed'] == 3
