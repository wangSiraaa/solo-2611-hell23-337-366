"""重复片段:同字节幂等、冲突拒绝、崩溃后仍可识别。"""
import os

from collector import ACK, DUPLICATE, NACK_GAP, REJECT, Collector
from nmea import sentence
from scenarios import Numberer, cycle_events, data


def ev(rx, gen, seq, wire):
    return {'kind': 'data', 'receiver': rx, 'generation': gen, 'seq': seq, 'wire': wire}


class TestDuplicates:
    def test_same_seq_same_bytes_idempotent(self, tmp_path):
        col = Collector(str(tmp_path / 'ledger.db'))
        w1, w2 = sentence(2, 1, 11), sentence(2, 2, 12)
        assert col.accept_event(ev('R1', 1, 1, w1)).status == ACK
        r = col.accept_event(ev('R1', 1, 1, w1))          # 原样重送
        assert r.status == DUPLICATE and r.cycles == []
        assert col.accept_event(ev('R1', 1, 2, w2)).cycles[0]['cycle_id'] == 'R1/g1/c0001'
        r = col.accept_event(ev('R1', 1, 2, w2))          # 完成句重送不再产周期
        assert r.status == DUPLICATE and r.cycles == []
        assert col.stats['cycles_completed'] == 1

    def test_conflicting_duplicate_rejected_not_overwritten(self, tmp_path):
        col = Collector(str(tmp_path / 'ledger.db'))
        w1 = sentence(2, 1, 11)
        col.accept_event(ev('R1', 1, 1, w1))
        r = col.accept_event(ev('R1', 1, 1, sentence(2, 1, 99)))  # 同 seq 不同字节
        assert r.status == REJECT and r.reason == 'conflict'
        assert col.stats['conflicts'] == 1
        # 原周期仍按原字节完成
        c = col.accept_event(ev('R1', 1, 2, sentence(2, 2, 12))).cycles[0]
        assert c['satellites'][:4] == ['11', '30', '100', '40']

    def test_conflict_detected_after_restart(self, tmp_path):
        path = str(tmp_path / 'ledger.db')
        col = Collector(path)
        col.accept_event(ev('R1', 1, 1, sentence(2, 1, 11)))
        col2 = Collector(path)  # 模拟重启:去重状态来自持久账本
        r = col2.accept_event(ev('R1', 1, 1, sentence(2, 1, 99)))
        assert r.status == REJECT and r.reason == 'conflict'
        r = col2.accept_event(ev('R1', 1, 1, sentence(2, 1, 11)))
        assert r.status == DUPLICATE

    def test_gap_nack_not_silent(self, tmp_path):
        col = Collector(str(tmp_path / 'ledger.db'))
        r = col.accept_event(ev('R1', 1, 2, sentence(2, 2, 12)))  # 缺 seq1
        assert r.status == NACK_GAP and r.detail['expected'] == 1
        # 补上后照常推进
        assert col.accept_event(ev('R1', 1, 1, sentence(2, 1, 11))).status == ACK
        assert col.accept_event(ev('R1', 1, 2, sentence(2, 2, 12))).status == ACK

    def test_duplicate_does_not_double_count_cycle(self, tmp_path):
        """重送整个序列:周期集合不变。"""
        num = Numberer()
        evs = num.assign(cycle_events('R1', 1, 2, [[11], [12]]))
        col = Collector(str(tmp_path / 'ledger.db'))
        ids = []
        for e in evs:
            ids += [c['cycle_id'] for c in col.accept_event(e).cycles]
        for e in evs:  # 整体重送
            assert col.accept_event(e).cycles == []
        assert ids == ['R1/g1/c0001']
        assert col.stats['cycles_completed'] == 1
