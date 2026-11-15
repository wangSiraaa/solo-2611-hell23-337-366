"""不变量 1：局部 typecode 重排 / 重用不得跨批按整数 code 拼接。

覆盖题目核心 bug：a 批 code 3=number，b 批 code 7=number（交换），
canonical 重编码后 20 必须落在 number 分支且带来源 provenance。
另含同 producer 跨 revision 的 typecode 重用场景。
"""

import pyarrow as pa

from unionx import Backpressure, Exchange, Producer, ShardStore
from unionx.protocol import Envelope
from unionx.schema import IncompatibleSchema

from tests._base import ExchangeTestCase


class TypecodeTests(ExchangeTestCase):
    def test_swapped_typecodes_two_producers(self):
        from unionx import Exchange as E
        d1, d2 = self.descriptors_simple()
        ex = E(ShardStore(self.store_root))
        ex.propose(d1); ex.accept("P1", 1)
        ex.propose(d2); ex.accept("P2", 1)

        e1 = Producer("P1", d1).build_batch(
            [
                {"branch_id": "branch-number", "value": 10},
                {"branch_id": "branch-text", "value": "x"},
                {"branch_id": "branch-number", "value": None},
            ],
            seq=1,
        )
        e2 = Producer("P2", d2).build_batch(
            [
                {"branch_id": "branch-number", "value": 20},
                {"branch_id": "branch-text", "value": "y"},
                {"branch_id": "branch-number", "value": 21},
            ],
            seq=1,
        )
        ex.submit(e1)
        ex.submit(e2)
        ex.close()

        store = ShardStore.reopen(self.store_root)
        rows = store.query()
        by_val = {r["value"]: r for r in rows if r["value"] is not None}
        # 旧 receiver 的错误：20 会被标成 text；这里必须是 number
        self.assertEqual(by_val[20]["branch_id"], "branch-number")
        self.assertEqual(by_val[20]["producer"], "P2")
        self.assertEqual(by_val[20]["original_child_offset"], 0)
        self.assertEqual(by_val["x"]["branch_id"], "branch-text")
        self.assertEqual(by_val["y"]["branch_id"], "branch-text")
        # canonical code 与两个 producer 的局部 code 都不同且稳定
        code = store.manifest["canonical_history"][-1]["code"]
        self.assertEqual(code["branch-number"], 0)
        self.assertEqual(code["branch-text"], 1)

        # 真实 IPC 读回验证：落盘 Union 的 type-code 是 canonical 私有码
        import glob
        f = glob.glob(str(self.store_root / "shards" / "*.arrow"))[0]
        t = pa.ipc.open_file(pa.memory_map(f, "r")).read_all()
        u = t.column(0).chunk(0)
        self.assertEqual(sorted(u.type.type_codes), [0, 1])

    def test_typecode_reuse_across_revisions_same_producer(self):
        """同一 producer：rev1 用 code 1 表示 number，rev2 把 code 1 改派给 text。"""
        from unionx.schema import ProducerDescriptor, BranchSpec
        rev1 = ProducerDescriptor(
            "P1", 1, {1: "branch-number", 2: "branch-text"},
            (
                BranchSpec("branch-number", "number", "int64"),
                BranchSpec("branch-text", "text", "text"),
            ),
        )
        rev2 = ProducerDescriptor(
            "P1", 2, {1: "branch-text", 2: "branch-number"},  # code 1 改派
            (
                BranchSpec("branch-number", "number", "int64"),
                BranchSpec("branch-text", "text", "text"),
            ),
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(rev1); ex.accept("P1", 1)
        ex.submit(Producer("P1", rev1).build_batch(
            [{"branch_id": "branch-number", "value": 100}], seq=1))
        ex.propose(rev2); ex.accept("P1", 2)
        ex.submit(Producer("P1", rev2).build_batch(
            [{"branch_id": "branch-text", "value": "z"}], seq=2))
        ex.close()
        rows = ShardStore.reopen(self.store_root).query()
        self.assertEqual(rows[0]["branch_id"], "branch-number")
        self.assertEqual(rows[1]["branch_id"], "branch-text")
        self.assertEqual(rows[1]["value"], "z")

    def test_arbitrary_typecode_values_legal(self):
        """输入 typecode 可合法任意选择（Arrow IPC 允许的 [0,127] 内稀疏取值）。"""
        from unionx.schema import ProducerDescriptor, BranchSpec
        desc = ProducerDescriptor(
            "P1", 1, {7: "branch-number", 120: "branch-text"},
            (
                BranchSpec("branch-number", "number", "int64"),
                BranchSpec("branch-text", "text", "text"),
            ),
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(desc); ex.accept("P1", 1)
        ack = ex.submit(Producer("P1", desc).build_batch(
            [{"branch_id": "branch-text", "value": "sparse-code"}], seq=1))
        self.assertTrue(ack.committed)
        ex.close()
