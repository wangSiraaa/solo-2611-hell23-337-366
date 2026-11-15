"""边界限制：2 producer / 16 分支 / struct 8 字段 / 每批 1000 行；内存预算披露。"""

from unionx import Exchange, Producer, ShardStore
from unionx.schema import (
    LIMITS,
    MISSING_BRANCH_ID,
    BranchSpec,
    FieldSpec,
    IncompatibleSchema,
    ProducerDescriptor,
)

from tests._base import ExchangeTestCase


class LimitTests(ExchangeTestCase):
    def test_max_branches_enforced(self):
        branches = tuple(
            BranchSpec(f"branch-b{i}", f"b{i}", "int64")
            for i in range(LIMITS["max_branches"] + 1)
        )
        codes = {i: f"branch-b{i}" for i in range(len(branches))}
        desc = ProducerDescriptor("P1", 1, codes, branches)
        with self.assertRaises(IncompatibleSchema):
            desc.validate()

    def test_max_struct_fields_enforced(self):
        fields = tuple(
            FieldSpec(f"field-{i}", f"n{i}", "int64", nullable=True)
            for i in range(LIMITS["max_struct_fields"] + 1)
        )
        desc = ProducerDescriptor(
            "P1", 1, {1: "b-s"},
            (BranchSpec("b-s", "s", "struct", fields),),
        )
        with self.assertRaises(IncompatibleSchema):
            desc.validate()

    def test_max_rows_per_batch_enforced(self):
        desc = ProducerDescriptor(
            "P1", 1, {1: "b-x"}, (BranchSpec("b-x", "x", "int64"),)
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(desc); ex.accept("P1", 1)
        with self.assertRaises(IncompatibleSchema):
            Producer("P1", desc).build_batch(
                [{"branch_id": "b-x", "value": i}
                 for i in range(LIMITS["max_rows_per_batch"] + 1)]
            )

    def test_max_two_producers(self):
        descs = [
            ProducerDescriptor(
                f"P{i}", 1, {1: f"branch-x{i}"},
                (BranchSpec(f"branch-x{i}", "x", "int64"),),
            )
            for i in range(3)
        ]
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(descs[0]); ex.propose(descs[1])
        with self.assertRaises(Exception):
            ex.propose(descs[2])

    def test_empty_batch_supported(self):
        desc = ProducerDescriptor(
            "P1", 1, {1: "b-x"}, (BranchSpec("b-x", "x", "int64"),)
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(desc); ex.accept("P1", 1)
        ack = ex.submit(Producer("P1", desc).build_batch([], seq=1))
        self.assertTrue(ack.committed)
        ack2 = ex.submit(Producer("P1", desc).build_batch(
            [{"branch_id": "b-x", "value": 1}], seq=2))
        self.assertTrue(ack2.committed)
        ex.close()
        store = ShardStore.reopen(self.store_root)
        self.assertEqual(store.count(), 1)

    def test_100_batch_cap(self):
        desc = ProducerDescriptor(
            "P1", 1, {1: "b-x"}, (BranchSpec("b-x", "x", "int64"),)
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(desc); ex.accept("P1", 1)
        p = Producer("P1", desc)
        for seq in range(1, 101):
            ex.submit(p.build_batch([{"branch_id": "b-x", "value": seq}], seq=seq))
        with self.assertRaises(IncompatibleSchema):
            ex.submit(p.build_batch([{"branch_id": "b-x", "value": 101}], seq=101))
        ex.close()

    def test_memory_accounting_includes_conversion_buffer(self):
        desc = ProducerDescriptor(
            "P1", 1, {1: "b-x", 2: MISSING_BRANCH_ID},
            (
                BranchSpec("b-x", "x", "int64"),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
            ),
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(desc); ex.accept("P1", 1)
        env = Producer("P1", desc).build_batch(
            [{"branch_id": "b-x", "value": i} for i in range(500)], seq=1
        )
        ex.submit(env)
        stats = ex.stats()["memory"]
        # 峰值必须为正数且包含三个组成项的口径说明
        self.assertGreater(stats["peak_resident_bytes"], 0)
        self.assertIn("conversion_buffer", stats["note"])
        ex.close()
