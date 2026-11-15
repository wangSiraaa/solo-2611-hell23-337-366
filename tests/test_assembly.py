"""装配不变量:跨代隔离、淘汰原因、稳定身份、切分/交织不变性、合法族变体。"""
import json
import os
import random

import pytest

from collector import ACK, Collector, REJECT
from nmea import gsv_sentence, sentence
from scenarios import Numberer, cycle_events, data, disc, ev_reconnect
from sim import fragment_events

FIXTURES = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'fixtures')


def run_events(events, **kw):
    """无 sink 纯装配(内存账本):返回 (cycles, collector)。"""
    col = Collector(**kw)
    cycles = []
    for ev in events:
        res = col.accept_event(ev)
        assert res.status != REJECT, res.reason
        cycles.extend(res.cycles)
    return cycles, col


class TestCrossGeneration:
    def test_fixture_no_cross_generation_splice(self):
        """fixtures/events.json:旧代半句不得拼入新代;同代新第1句淘汰旧pending。"""
        cycles, col = run_events(ev_reconnect())
        by_id = {c['cycle_id']: c for c in cycles}
        assert set(by_id) == {'R1/g2/c0001', 'R2/g4/c0001'}
        # R1/g1 的第 1 句(PRN 11)与 R1/g2 的第 2 句(PRN 22)不得出现在任何周期
        all_sats = [s for c in cycles for s in c['satellites']]
        assert '11' not in all_sats and '22' not in all_sats
        assert by_id['R1/g2/c0001']['satellites'][:4] == ['41', '30', '100', '40']
        # 淘汰原因被记录:断连淘汰 + 新第1句淘汰
        reasons = [(n.kind, n.reason) for n in col.notices if n.kind == 'evicted']
        assert ('evicted', 'disconnect') in reasons
        assert ('evicted', 'superseded_by_part1') in reasons

    def test_generation_key_isolates_pending(self):
        """同 receiver 两代各自半周期互不补齐。"""
        num = Numberer()
        evs = num.assign([data('R1', 1, sentence(2, 1, 11)),
                          disc('R1', 1),
                          data('R1', 2, sentence(2, 2, 22))])
        cycles, col = run_events(evs)
        assert cycles == []
        assert any(n.reason == 'disconnect' for n in col.notices if n.kind == 'evicted')

    def test_stale_generation_rejected(self):
        num = Numberer()
        evs = num.assign([data('R1', 2, sentence(1, 1, 11))])
        _, col = run_events(evs)
        res = col.accept_event({'kind': 'data', 'receiver': 'R1', 'generation': 1,
                                'seq': 1, 'wire': sentence(1, 1, 12)})
        assert res.status == REJECT and res.reason == 'stale_generation'


class TestEviction:
    def test_new_part1_evicts_same_generation_pending(self):
        num = Numberer()
        evs = num.assign([data('R1', 1, sentence(3, 1, 11)),
                          data('R1', 1, sentence(3, 2, 12)),
                          data('R1', 1, sentence(2, 1, 21)),   # 淘汰 {1,2}
                          data('R1', 1, sentence(2, 2, 22))])
        cycles, col = run_events(evs)
        assert len(cycles) == 1
        assert cycles[0]['satellites'][0] == '21'
        ev = [n for n in col.notices if n.kind == 'evicted']
        assert ev and ev[0].reason == 'superseded_by_part1'
        assert ev[0].detail['parts_present'] == [1, 2]

    def test_disconnect_records_reason(self):
        num = Numberer()
        evs = num.assign([data('R1', 1, sentence(2, 1, 11)), disc('R1', 1)])
        _, col = run_events(evs)
        ev = [n for n in col.notices if n.kind == 'evicted']
        assert len(ev) == 1 and ev[0].reason == 'disconnect'

    def test_inconsistent_total_evicts(self):
        num = Numberer()
        evs = num.assign([data('R1', 1, sentence(3, 2, 11)),
                          data('R1', 1, sentence(2, 2, 12)),   # total 不一致
                          data('R1', 1, sentence(2, 1, 13))])
        cycles, col = run_events(evs)
        assert cycles == []
        assert any(n.reason == 'inconsistent_total' for n in col.notices if n.kind == 'evicted')


class TestCycleIdentity:
    def test_identity_not_derived_from_satellite_content(self):
        """两个内容完全相同的真实周期仍是两个观测(身份不同)。"""
        num = Numberer()
        one = cycle_events('R1', 1, 2, [[11], [12]])
        evs = num.assign(one + one)  # 同内容再来一遍
        cycles, _ = run_events(evs)
        assert len(cycles) == 2
        assert cycles[0]['cycle_id'] != cycles[1]['cycle_id']
        assert cycles[0]['satellites'] == cycles[1]['satellites']

    def test_identity_stable_across_replay(self):
        num = Numberer()
        evs = num.assign(cycle_events('R1', 1, 2, [[11], [12]]))
        ids1 = [c['cycle_id'] for c in run_events(evs)[0]]
        ids2 = [c['cycle_id'] for c in run_events(evs)[0]]
        assert ids1 == ids2 == ['R1/g1/c0001']

    def test_identity_uses_generation_and_ordinal(self):
        num = Numberer()
        evs = num.assign(cycle_events('R1', 1, 1, [[11]]) + [disc('R1', 1)]
                         + cycle_events('R1', 2, 1, [[11]]))
        cycles, _ = run_events(evs)
        assert [c['cycle_id'] for c in cycles] == ['R1/g1/c0001', 'R1/g2/c0001']


class TestFragmentation:
    @pytest.mark.parametrize('offset', range(1, 60))
    def test_every_split_offset_same_cycles(self, offset):
        """句内任意切分点:同一输入族的所有切分产出同一周期集合。"""
        wire = gsv_sentence('GP', 2, 1, 2, [11, 12]) + gsv_sentence('GP', 2, 2, 2, [13, 14])
        num = Numberer()
        whole = num.assign([data('R1', 1, wire)])
        base = {c['cycle_id']: c['satellites'] for c in run_events(whole)[0]}
        num2 = Numberer()
        frag = num2.assign([data('R1', 1, wire[:offset]), data('R1', 1, wire[offset:])])
        got = {c['cycle_id']: c['satellites'] for c in run_events(frag)[0]}
        assert got == base

    def test_random_fragmentation_seeded(self):
        rng = random.Random(20261115)
        num = Numberer()
        evs = num.assign(cycle_events('R1', 1, 3, [[1], [2], [3]])
                         + cycle_events('R2', 1, 1, [[4]], talker='GL'))
        base = sorted(c['cycle_id'] for c in run_events(evs)[0])
        for _ in range(20):
            def splitter(w, rng=rng):
                pts = sorted(rng.sample(range(1, len(w)), min(3, len(w) - 1)))
                out, i = [], 0
                for p in pts:
                    out.append(w[i:p]); i = p
                out.append(w[i:])
                return [p for p in out if p]
            frag = fragment_events(evs, splitter)
            got = sorted(c['cycle_id'] for c in run_events(frag)[0])
            assert got == base

    def test_random_interleaving_seeded(self):
        """多接收器事件任意交织(保持各自序)产出同一周期集合。"""
        rng = random.Random(7)
        num = Numberer()
        r1 = num.assign(cycle_events('R1', 1, 2, [[1], [2]]))
        r2 = num.assign(cycle_events('R2', 1, 2, [[3], [4]], talker='GL'))
        base = sorted(c['cycle_id'] for c in run_events(r1 + r2)[0])
        for _ in range(20):
            mixed = []
            pools = [list(r1), list(r2)]
            while any(pools):
                pool = rng.choice([p for p in pools if p])
                mixed.append(pool.pop(0))
            got = sorted(c['cycle_id'] for c in run_events(mixed)[0])
            assert got == base


class TestFamilyVariants:
    @pytest.mark.parametrize('talker', ['GP', 'GL'])
    @pytest.mark.parametrize('total', [1, 2, 3, 4])
    @pytest.mark.parametrize('nsats', [1, 2, 3, 4])
    def test_legal_family(self, talker, total, nsats):
        num = Numberer()
        evs = num.assign(cycle_events('R1', 1, total, [[10 + i] * nsats for i in range(total)],
                                      talker=talker))
        cycles, _ = run_events(evs)
        assert len(cycles) == 1
        assert cycles[0]['total'] == total
        assert len(cycles[0]['satellites']) == 4 * nsats * total

    def test_fragment_over_256_rejected(self):
        _, col = run_events([])
        res = col.accept_event({'kind': 'data', 'receiver': 'R1', 'generation': 1,
                                'seq': 1, 'wire': 'x' * 257})
        assert res.status == REJECT and res.reason == 'fragment_too_large'


class TestMalformedIsolation:
    def test_bad_checksum_does_not_block_other_receiver(self):
        """R1 畸形句不阻止 R2 已有完整周期(隔离 + 原因记录)。"""
        num = Numberer()
        bad = '$GPGSV,1,1,02,11,30,100,40*00\r\n'
        evs = num.assign(cycle_events('R2', 1, 1, [[31]], talker='GL')
                         + [data('R1', 1, bad)]
                         + cycle_events('R2', 1, 1, [[32]], talker='GL'))
        cycles, col = run_events(evs)
        assert [c['cycle_id'] for c in cycles] == ['R2/g1/c0001', 'R2/g1/c0002']
        assert any(n.kind == 'malformed' and n.receiver == 'R1' and n.reason == 'checksum'
                   for n in col.notices)

    def test_malformed_evicts_own_pending_only(self):
        num = Numberer()
        bad = '$GPGSV,2,2,02,99,30,100,40*00\r\n'
        evs = num.assign([data('R1', 1, sentence(2, 1, 11)),   # R1 pending
                          data('R2', 1, sentence(2, 1, 21)),   # R2 pending
                          data('R1', 1, bad),                  # R1 畸形
                          data('R2', 1, sentence(2, 2, 22))])  # R2 完成
        cycles, col = run_events(evs)
        assert [c['cycle_id'] for c in cycles] == ['R2/g1/c0001']
        ev = [n for n in col.notices if n.kind == 'evicted']
        assert any(n.receiver == 'R1' and n.reason == 'malformed_sentence' for n in ev)
        assert not any(n.receiver == 'R2' for n in ev)

    def test_overlong_line_resets_buffer_only(self):
        num = Numberer()
        evs = num.assign([data('R1', 1, '$' + 'A' * 100 + '\r\n'),
                          data('R1', 1, sentence(1, 1, 11))])
        cycles, col = run_events(evs)
        assert [c['cycle_id'] for c in cycles] == ['R1/g1/c0001']
        assert any(n.reason == 'too_long' or n.reason == 'line_too_long'
                   for n in col.notices if n.kind == 'malformed')
