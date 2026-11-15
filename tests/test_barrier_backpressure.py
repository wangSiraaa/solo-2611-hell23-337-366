"""不变量 4：分阶段协商、schema 屏障、背压、失败原子性。"""

import pyarrow as pa

from unionx import Backpressure, Exchange, Producer, ShardStore
from unionx.protocol import Envelope, IPC_METADATA_KEY
from unionx.schema import (
    MISSING_BRANCH_ID,
    BranchSpec,
    FieldSpec,
    IncompatibleSchema,
    ProducerDescriptor,
)

from tests._base import ExchangeTestCase


class BarrierBackpressureTests(ExchangeTestCase):
    def test_batch_waits_for_schema_acceptance(self):
        """未接受目标 revision 前，批只能停在屏障，绝不能按旧布局解码。"""
        v1, v2, _ = self.struct_descriptors()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        ex.submit(Producer("P1", v1).build_batch(
            [{"branch_id": "branch-record", "value": {"field-quantity": 1}}], seq=1))

        ex.propose(v2)  # 提案在，未接受
        waiting = ex.submit(Producer("P1", v2).build_batch(
            [{"branch_id": "branch-record",
              "value": {"field-quantity": 2, "field-note": "x"}}], seq=2))
        self.assertFalse(waiting.committed)
        self.assertEqual(ex.stats()["queued_batches"], 1)
        # 存储里此时只有 rev1 的 1 行
        self.assertEqual(ex.store.count(), 1)
        ex.accept("P1", 2)  # 屏障放行
        self.assertEqual(ex.stats()["queued_batches"], 0)
        ex.close()
        store = ShardStore.reopen(self.store_root)
        self.assertEqual(store.count(), 2)

    def test_slow_consumer_backpressure_limits(self):
        d1, d2 = self.descriptors_simple()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(d1); ex.propose(d2)  # 都不接受 => 批全部常驻
        p1, p2 = Producer("P1", d1), Producer("P2", d2)
        ex.submit(p1.build_batch([{"branch_id": "branch-number", "value": 1}], seq=1))
        ex.submit(p2.build_batch([{"branch_id": "branch-text", "value": "u"}], seq=1))
        ex.submit(p1.build_batch([{"branch_id": "branch-number", "value": 2}], seq=2))
        # 全局第 4 批 => 背压
        bp = ex.submit(p2.build_batch([{"branch_id": "branch-text", "value": "v"}], seq=2))
        self.assertIsInstance(bp, Backpressure)
        # 每 producer 第 3 批 => 同样背压
        bp2 = ex.submit(p1.build_batch([{"branch_id": "branch-number", "value": 3}], seq=3))
        self.assertIsInstance(bp2, Backpressure)
        # 背压消息不保留、无提交确认
        self.assertEqual(ex.stats()["queued_batches"], 3)
        self.assertEqual(ex.stats()["peaks"]["global_batches"], 3)
        self.assertEqual(ex.stats()["peaks"]["per_producer_batches"], 2)
        # 接受后排空，被背压的批可重投
        ex.accept("P2", 1)
        ex.accept("P1", 1)
        self.assertEqual(ex.stats()["queued_batches"], 0)
        retry = ex.submit(p2.build_batch(
            [{"branch_id": "branch-text", "value": "v"}], seq=2))
        self.assertTrue(retry.committed)
        ex.close()

    def test_incompatible_proposal_does_not_block_other_stream(self):
        d1, d2 = self.descriptors_simple()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(d1); ex.propose(d2)
        p1, p2 = Producer("P1", d1), Producer("P2", d2)
        ex.submit(p1.build_batch([{"branch_id": "branch-number", "value": 1}], seq=1))
        ex.submit(p2.build_batch([{"branch_id": "branch-text", "value": "u"}], seq=1))
        # P1 的坏 rev2（改 payload 类型）
        bad = ProducerDescriptor(
            "P1", 2, {3: "branch-number", 7: "branch-text"},
            (
                BranchSpec("branch-number", "number", "text"),  # int64 -> text
                BranchSpec("branch-text", "text", "text"),
            ),
        )
        with self.assertRaises(IncompatibleSchema):
            ex.propose(bad)
        # P2 的已接受 schema 流照常推进
        ex.accept("P2", 1)
        ack = ex.submit(p2.build_batch([{"branch_id": "branch-text", "value": "w"}], seq=2))
        self.assertTrue(ack.committed)
        # P1 修好后也能继续（坏提案没污染 pending）
        ex.accept("P1", 1)
        ok2 = ProducerDescriptor(
            "P1", 2, {3: "branch-number", 7: "branch-text"},
            (
                BranchSpec("branch-number", "number", "int64"),
                BranchSpec("branch-text", "text", "text"),
            ),
        )
        ex.propose(ok2); ex.accept("P1", 2)
        self.assertTrue(ex.submit(Producer("P1", ok2).build_batch(
            [{"branch_id": "branch-number", "value": 9}], seq=2)).committed)
        ex.close()

    def test_failed_batch_is_atomic_and_stream_recovers(self):
        v1, _, _ = self.struct_descriptors()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        p = Producer("P1", v1)
        ex.submit(p.build_batch(
            [{"branch_id": "branch-record", "value": {"field-quantity": 1}}], seq=1))

        # 手工构造违反非空约束的真实 IPC（非空键显式 NULL）
        rec = pa.array(
            [{"field-quantity": None}],
            type=pa.struct([pa.field("field-quantity", pa.int64())]),
        )
        miss = pa.nulls(1, pa.null())
        u = pa.UnionArray.from_dense(
            pa.array([1], pa.int8()), pa.array([0], pa.int32()),
            [rec, miss],
            field_names=["branch-record", MISSING_BRANCH_ID], type_codes=[1, 2],
        )
        table = pa.table({"value": u})
        env = Envelope("P1", 1, 2, dict(v1.branch_codes), v1, b"")
        schema = table.schema.with_metadata(
            {IPC_METADATA_KEY: env.metadata_bytes()}
        )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, schema) as w:
            w.write_table(table)
        env.ipc = sink.getvalue().to_pybytes()

        with self.assertRaises(IncompatibleSchema):
            ex.submit(env)
        # 原子性：输出仍只有第一批；失败留档；队列清空；seq 可复用
        self.assertEqual(ex.store.count(), 1)
        self.assertEqual(len(ex.failures), 1)
        self.assertEqual(ex.stats()["queued_batches"], 0)
        recovered = ex.submit(p.build_batch(
            [{"branch_id": "branch-record", "value": {"field-quantity": 2}}], seq=2))
        self.assertTrue(recovered.committed)
        ex.close()
        store = ShardStore.reopen(self.store_root)
        self.assertEqual([r["seq"] for r in store.query()], [1, 2])

    def test_malformed_ipc_rejected_atomically(self):
        d1, _ = self.descriptors_simple()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(d1); ex.accept("P1", 1)
        # 构造 Arrow code 与 descriptor 不一致的批
        u = pa.UnionArray.from_dense(
            pa.array([9], pa.int8()), pa.array([0], pa.int32()),
            [pa.array([1], pa.int64()), pa.array(["x"], pa.string())],
            field_names=["branch-number", "branch-text"], type_codes=[9, 8],
        )
        table = pa.table({"value": u})
        env = Envelope("P1", 1, 1, dict(d1.branch_codes), d1, b"")
        schema = table.schema.with_metadata({IPC_METADATA_KEY: env.metadata_bytes()})
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, schema) as w:
            w.write_table(table)
        env.ipc = sink.getvalue().to_pybytes()
        with self.assertRaises(IncompatibleSchema):
            ex.submit(env)
        self.assertEqual(ex.store.count(), 0)

    def test_interleaved_producers_keep_per_producer_seq(self):
        d1, d2 = self.descriptors_simple()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(d1); ex.accept("P1", 1)
        ex.propose(d2); ex.accept("P2", 1)
        p1, p2 = Producer("P1", d1), Producer("P2", d2)
        for seq in range(1, 4):
            ex.submit(p1.build_batch(
                [{"branch_id": "branch-number", "value": seq}], seq=seq))
            ex.submit(p2.build_batch(
                [{"branch_id": "branch-text", "value": f"t{seq}"}], seq=seq))
        ex.close()
        store = ShardStore.reopen(self.store_root)
        p1_seq = [r["seq"] for r in store.query() if r["producer"] == "P1"]
        p2_seq = [r["seq"] for r in store.query() if r["producer"] == "P2"]
        self.assertEqual(p1_seq, [1, 2, 3])
        self.assertEqual(p2_seq, [1, 2, 3])

    def test_gap_in_sequence_rejected(self):
        d1, _ = self.descriptors_simple()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(d1); ex.accept("P1", 1)
        p1 = Producer("P1", d1)
        ex.submit(p1.build_batch([{"branch_id": "branch-number", "value": 1}], seq=1))
        with self.assertRaises(IncompatibleSchema):
            ex.submit(p1.build_batch([{"branch_id": "branch-number", "value": 3}], seq=3))
