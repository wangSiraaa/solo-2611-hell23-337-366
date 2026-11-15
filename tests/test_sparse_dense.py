"""不变量 2：稀疏/稠密输入互换，canonical 统一为稠密，语义与来源不丢。"""

from unionx import Exchange, Producer, ShardStore
from unionx.schema import MISSING_BRANCH_ID

from tests._base import ExchangeTestCase


class SparseDenseTests(ExchangeTestCase):
    def _desc(self):
        v1, _, _ = self.struct_descriptors()
        return v1

    def test_sparse_then_dense_same_identity(self):
        desc = self._desc()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(desc); ex.accept("P1", 1)

        rows = [
            {"branch_id": "branch-record", "value": {"field-quantity": 5}},
            {"branch_id": MISSING_BRANCH_ID, "value": None},
            {"branch_id": "branch-record", "value": None},  # child NULL
        ]
        ack_s = ex.submit(Producer("P1", desc, mode="sparse").build_batch(rows, seq=1))
        ack_d = ex.submit(Producer("P1", desc, mode="dense").build_batch(rows, seq=2))
        self.assertTrue(ack_s.committed and ack_d.committed)
        ex.close()

        store = ShardStore.reopen(self.store_root)
        all_rows = store.query()
        self.assertEqual(len(all_rows), 6)
        # input_mode provenance 保留原始形态
        modes = {r["seq"]: r["input_mode"] for r in all_rows}
        self.assertEqual(modes, {1: "sparse", 2: "dense"})
        # 两个批语义逐行一致
        seq1 = [r for r in all_rows if r["seq"] == 1]
        seq2 = [r for r in all_rows if r["seq"] == 2]
        for a, b in zip(seq1, seq2):
            self.assertEqual(a["branch_id"], b["branch_id"])
            self.assertEqual(a["present"], b["present"])
            self.assertEqual(a["value"], b["value"])
        # 落盘统一是 dense union
        import glob, pyarrow as pa
        f = glob.glob(str(self.store_root / "shards" / "*.arrow"))[0]
        u = pa.ipc.open_file(pa.memory_map(f, "r")).read_all().column(0).chunk(0)
        self.assertEqual(u.type.mode, "dense")
        # original_child_offset：sparse 输入下等于父行号，dense 下为紧凑偏移
        self.assertEqual(seq1[0]["original_child_offset"], 0)
        self.assertEqual(seq1[1]["original_child_offset"], 1)

    def test_dense_offsets_preserved_as_provenance(self):
        desc = self._desc()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(desc); ex.accept("P1", 1)
        env = Producer("P1", desc, mode="dense").build_batch(
            [
                {"branch_id": MISSING_BRANCH_ID, "value": None},
                {"branch_id": "branch-record", "value": {"field-quantity": 9}},
                {"branch_id": "branch-record", "value": {"field-quantity": 10}},
            ],
            seq=1,
        )
        ex.submit(env)
        ex.close()
        rows = ShardStore.reopen(self.store_root).query(branch_id="branch-record")
        # dense child 紧凑排列：两条 record 的 original_child_offset 是 0 和 1
        self.assertEqual([r["original_child_offset"] for r in rows], [0, 1])
        self.assertEqual([r["parent_row"] for r in rows], [1, 2])
