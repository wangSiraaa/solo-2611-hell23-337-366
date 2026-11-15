"""不变量 5：持久化、重开、按稳定身份查询与 provenance；fixtures 端到端。"""

import json
from pathlib import Path

import pyarrow as pa

from unionx import Exchange, Producer, ShardStore
from unionx.schema import MISSING_BRANCH_ID

from tests._base import ExchangeTestCase

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


class PersistenceTests(ExchangeTestCase):
    def test_reopen_and_provenance_query(self):
        v1, v2, p2 = self.struct_descriptors()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        ex.propose(p2); ex.accept("P2", 1)
        ex.submit(Producer("P1", v1, mode="sparse").build_batch(
            [
                {"branch_id": "branch-record", "value": {"field-quantity": 5}},
                {"branch_id": MISSING_BRANCH_ID, "value": None},
                {"branch_id": "branch-record", "value": None},
            ],
            seq=1,
        ))
        ex.submit(Producer("P2", p2).build_batch(
            [{"branch_id": "branch-record", "value": {"field-quantity": 77}}], seq=1))
        ex.propose(v2); ex.accept("P1", 2)
        ex.submit(Producer("P1", v2).build_batch(
            [{"branch_id": "branch-record",
              "value": {"field-quantity": 7, "field-note": "added"}}], seq=2))
        ex.close()

        # 重开：manifest 自描述
        store = ShardStore.reopen(self.store_root)
        self.assertEqual(store.count(), 5)
        self.assertTrue(store.manifest["closed"])
        # 每片绑定 canonical 快照，演进产生 2 个分片
        self.assertEqual(len(store.manifest["shards"]), 2)
        for shard in store.manifest["shards"]:
            self.assertIn("canonical_revision", shard)
            self.assertIn("fingerprint", shard)

        # 全量查询含完整来源
        rows = store.query()
        first = rows[0]
        self.assertEqual(first["producer"], "P1")
        self.assertEqual(first["parent_row"], 0)
        self.assertEqual(first["original_child_offset"], 0)
        self.assertEqual(first["input_mode"], "sparse")

        # missing 与 present-null 不混同
        missing = store.query_missing()
        self.assertEqual(len(missing), 1)
        self.assertFalse(missing[0]["present"])
        present_null = [
            r for r in store.query(branch_id="branch-record") if r["value"] is None
        ]
        self.assertEqual(len(present_null), 1)
        self.assertTrue(present_null[0]["present"])

        # 旧片按 field_id 读时对齐新字段 => NULL
        note = store.query(branch_id="branch-record", field_id="field-note")
        self.assertEqual([r["value"] for r in note], [None, None, None, "added"])

    def test_fixture_schedule_end_to_end(self):
        """用真实 fixtures/*.arrow 跑 schedule.json，复现并纠正基线 typecode bug。"""
        from unionx import scenarios
        ex = scenarios.run_fixture_schedule(FIXTURES, self.store_root)
        store = ShardStore.reopen(self.store_root)
        rows = store.query()
        self.assertEqual(store.count(), 6)
        # b 批的 20 在交换 typecode 后仍必须是 number
        twenty = next(r for r in rows if r["value"] == 20)
        self.assertEqual(twenty["branch_id"], "branch-number")
        self.assertEqual(twenty["producer"], "P2")
        # 来源 offset 与原始 dense union 一致
        self.assertEqual(twenty["parent_row"], 0)
        self.assertEqual(twenty["original_child_offset"], 0)
        # 屏障语义：schedule 中 P2 rev2 在 accept 前到达
        self.assertEqual(ex.failures, [])

    def test_fixture_struct_v1_v2_identity(self):
        """struct-v1（sparse）与 struct-v2（dense，改名+加字段）按 field_id 对齐。"""
        from unionx import scenarios
        from unionx.schema import ProducerDescriptor, BranchSpec, FieldSpec
        from unionx.protocol import Envelope

        evo = json.loads((FIXTURES / "struct-evolution.json").read_text())
        v1_ids = evo["v1"]
        v1 = ProducerDescriptor(
            "P1", 1,
            {int(c): b for c, b in v1_ids["type_codes"].items()},
            (
                BranchSpec("branch-record", "record", "struct", (
                    FieldSpec(v1_ids["branch-record"]["qty"], "qty", "int64", False),)),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
            ),
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)

        # 直接消费真实 struct-v1.arrow IPC
        table = pa.ipc.open_file(
            pa.memory_map(str(FIXTURES / "struct-v1.arrow"), "r")
        ).read_all()
        env = Envelope(
            "P1", 1, 1, dict(v1.branch_codes), v1, b"",
            struct_arrow_names={"branch-record": {"qty": "field-quantity"}},
        )
        schema = table.schema.with_metadata(
            {b"x-unionx-envelope": env.metadata_bytes()}
        )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, schema) as w:
            w.write_table(table)
        env.ipc = sink.getvalue().to_pybytes()
        ex.submit(env)
        ex.close()

        store = ShardStore.reopen(self.store_root)
        rec = store.query(branch_id="branch-record")
        # fixture: row0 qty=5, row2 是 child NULL
        self.assertEqual(rec[0]["value"], {"field-quantity": 5})
        self.assertIsNone(rec[1]["value"])
        self.assertTrue(rec[1]["present"])
        miss = store.query_missing()
        self.assertEqual(len(miss), 1)
        self.assertEqual(miss[0]["parent_row"], 1)

    def test_real_struct_v1_to_v2_fixtures_identity_align(self):
        """真实 struct-v1(sparse) -> struct-v2(dense)：改名+加字段+新 typecode。"""
        from unionx.schema import ProducerDescriptor, BranchSpec, FieldSpec
        from unionx.protocol import Envelope

        evo = json.loads((FIXTURES / "struct-evolution.json").read_text())
        v1 = ProducerDescriptor(
            "P1", 1,
            {int(c): b for c, b in evo["v1"]["type_codes"].items()},
            (
                BranchSpec("branch-record", "record", "struct", (
                    FieldSpec("field-quantity", "qty", "int64", False),)),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
            ),
        )
        v2 = ProducerDescriptor(
            "P1", 2,
            {int(c): b for c, b in evo["v2"]["type_codes"].items()},
            (
                BranchSpec("branch-record", "renamed_record", "struct", (
                    FieldSpec("field-quantity", "count", "int64", False),
                    FieldSpec("field-note", "note", "text", True),
                )),
            ),
        )

        def wrap(arrow_name, env, mapping):
            table = pa.ipc.open_file(
                pa.memory_map(str(FIXTURES / arrow_name), "r")
            ).read_all()
            env.struct_arrow_names = mapping
            schema = table.schema.with_metadata(
                {b"x-unionx-envelope": env.metadata_bytes()}
            )
            sink = pa.BufferOutputStream()
            with pa.ipc.new_file(sink, schema) as w:
                w.write_table(table)
            env.ipc = sink.getvalue().to_pybytes()
            return env

        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        ex.submit(wrap(
            "struct-v1.arrow",
            Envelope("P1", 1, 1, dict(v1.branch_codes), v1, b""),
            {"branch-record": {"qty": "field-quantity"}},
        ))
        ex.propose(v2); ex.accept("P1", 2)
        ex.submit(wrap(
            "struct-v2.arrow",
            Envelope("P1", 2, 2, dict(v2.branch_codes), v2, b""),
            {"branch-record": {"count": "field-quantity", "note": "field-note"}},
        ))
        ex.close()

        store = ShardStore.reopen(self.store_root)
        self.assertEqual(len(store.manifest["shards"]), 2)
        # 与 struct-evolution.json expected_old_nulls 一致
        self.assertEqual(
            [r["value"] for r in store.query(
                branch_id="branch-record", field_id="field-quantity")],
            [5, None, 7],
        )
        self.assertEqual(
            [r["value"] for r in store.query(
                branch_id="branch-record", field_id="field-note")],
            [None, None, "added"],
        )
        missing = store.query_missing()
        self.assertEqual([(m["parent_row"], m["branch_id"]) for m in missing],
                         [(1, MISSING_BRANCH_ID)])
