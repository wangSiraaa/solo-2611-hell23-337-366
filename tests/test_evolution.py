"""不变量 3：schema 演进身份规则。

* 改名不改变 branch_id / field_id；
* 新增 nullable 字段，旧行补 NULL；
* 删除后同名重建必须用新 field_id，不能与旧值合并；
* 类型不兼容整体拒绝；
* 同 revision 不同 schema 整体拒绝；
* 跨 producer 同 branch/field 身份共享，同名不同 ID 是不同字段。
"""

from unionx import Exchange, Producer, ShardStore
from unionx.schema import (
    KIND_INT64,
    KIND_TEXT,
    MISSING_BRANCH_ID,
    BranchSpec,
    FieldSpec,
    IncompatibleSchema,
    ProducerDescriptor,
)

from tests._base import ExchangeTestCase


class EvolutionTests(ExchangeTestCase):
    def test_rename_keeps_identity_new_field_backfills_null(self):
        v1, v2, _ = self.struct_descriptors()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        ex.submit(Producer("P1", v1).build_batch(
            [{"branch_id": "branch-record", "value": {"field-quantity": 7}}], seq=1))
        ex.propose(v2); ex.accept("P1", 2)
        ex.submit(Producer("P1", v2).build_batch(
            [{"branch_id": "branch-record",
              "value": {"field-quantity": 8, "field-note": "n"}}], seq=2))
        ex.close()

        store = ShardStore.reopen(self.store_root)
        # field-quantity 身份不变（显示名 qty->count 无关）
        q = store.query(branch_id="branch-record", field_id="field-quantity")
        self.assertEqual([r["value"] for r in q], [7, 8])
        # 旧行在新字段 field-note 上读时对齐补 NULL
        note = store.query(branch_id="branch-record", field_id="field-note")
        self.assertEqual([r["value"] for r in note], [None, "n"])
        # canonical field 顺序：quantity 先、note 后
        snap = store.manifest["canonical_history"][-1]
        names = [f["field_id"] for f in snap["branches"][0]["fields"]]
        self.assertEqual(names, ["field-quantity", "field-note"])

    def test_same_name_different_field_id_is_distinct_field(self):
        """名字相同而 ID 不同 => 不同字段，两个值都保留，不合并。"""
        v1 = ProducerDescriptor(
            "P1", 1, {1: "b-s"},
            (BranchSpec("b-s", "s", "struct", (
                FieldSpec("field-alpha", "label", KIND_TEXT, nullable=True),)),),
        )
        # 先加一个新字段 field-beta（不同 ID），再删除 alpha，用第三个 ID 重建同名 label
        v2 = ProducerDescriptor(
            "P1", 2, {1: "b-s"},
            (BranchSpec("b-s", "s", "struct", (
                FieldSpec("field-alpha", "label", KIND_TEXT, nullable=True),
                FieldSpec("field-beta", "beta", KIND_TEXT, nullable=True),)),),
        )
        v3 = ProducerDescriptor(
            "P1", 3, {1: "b-s"},
            (BranchSpec("b-s", "s", "struct", (
                # alpha 删除；gamma 是全新身份，且不复用历史显示名 label
                FieldSpec("field-beta", "beta", KIND_TEXT, nullable=True),
                FieldSpec("field-gamma", "caption", KIND_TEXT, nullable=True),)),),
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        ex.submit(Producer("P1", v1).build_batch(
            [{"branch_id": "b-s", "value": {"field-alpha": "old"}}], seq=1))
        ex.propose(v2); ex.accept("P1", 2)
        ex.propose(v3); ex.accept("P1", 3)
        ex.submit(Producer("P1", v3).build_batch(
            [{"branch_id": "b-s",
              "value": {"field-beta": "b", "field-gamma": "new"}}], seq=2))
        ex.close()
        store = ShardStore.reopen(self.store_root)
        # 旧 alpha 值仍可按旧 field_id 查到，不会被 gamma 覆盖合并
        alpha = store.query(branch_id="b-s", field_id="field-alpha")
        self.assertEqual([r["value"] for r in alpha], ["old", None])
        gamma = store.query(branch_id="b-s", field_id="field-gamma")
        self.assertEqual([r["value"] for r in gamma], [None, "new"])

    def test_recreated_field_reusing_old_display_name_rejected(self):
        v1, _, _ = self.struct_descriptors()
        bad = ProducerDescriptor(
            "P1", 2, {5: "branch-record", 9: MISSING_BRANCH_ID},
            (
                BranchSpec("branch-record", "record", "struct", (
                    FieldSpec("field-new-quantity", "qty", KIND_INT64, nullable=True),
                    FieldSpec("field-note", "note", KIND_TEXT, nullable=True),
                )),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
            ),
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        with self.assertRaises(IncompatibleSchema):
            ex.propose(bad)

    def test_incompatible_type_change_rejected(self):
        v1, _, _ = self.struct_descriptors()
        bad = ProducerDescriptor(
            "P1", 2, {5: "branch-record", 9: MISSING_BRANCH_ID},
            (
                BranchSpec("branch-record", "record", "struct", (
                    FieldSpec("field-quantity", "qty", KIND_TEXT, nullable=False),
                )),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
            ),
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        with self.assertRaises(IncompatibleSchema):
            ex.propose(bad)
        # canonical 未被污染
        self.assertEqual(
            ex.canonical.get_branch("branch-record").fields[0].kind, KIND_INT64
        )

    def test_same_revision_different_schema_rejected(self):
        d1, _ = self.descriptors_simple()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(d1); ex.accept("P1", 1)
        altered = ProducerDescriptor(
            "P1", 1, {3: "branch-number", 7: "branch-extra"},
            (
                BranchSpec("branch-number", "number", KIND_INT64),
                BranchSpec("branch-extra", "extra", KIND_TEXT),
            ),
        )
        with self.assertRaises(IncompatibleSchema):
            ex.propose(altered)

    def test_cross_producer_shared_identity_and_name_collision(self):
        # 两 producer 用同 branch_id + 同 field_id；P2 局部 code 完全不同
        v1, _, p2 = self.struct_descriptors()
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        ex.propose(p2); ex.accept("P2", 1)
        ex.submit(Producer("P1", v1).build_batch(
            [{"branch_id": "branch-record", "value": {"field-quantity": 1}}], seq=1))
        ex.submit(Producer("P2", p2).build_batch(
            [{"branch_id": "branch-record", "value": {"field-quantity": 2}}], seq=1))
        ex.close()
        store = ShardStore.reopen(self.store_root)
        q = store.query(branch_id="branch-record", field_id="field-quantity")
        self.assertEqual([(r["value"], r["producer"]) for r in q], [(1, "P1"), (2, "P2")])

    def test_cross_producer_type_conflict_rejected(self):
        v1, _, _ = self.struct_descriptors()
        p2_bad = ProducerDescriptor(
            "P2", 1, {42: "branch-record"},
            (BranchSpec("branch-record", "record", "struct", (
                FieldSpec("field-quantity", "qty", KIND_TEXT, nullable=True),)),),
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        with self.assertRaises(IncompatibleSchema):
            ex.propose(p2_bad); ex.accept("P2", 1)

    def test_new_branch_and_non_nullable_addition_rejected(self):
        v1, _, _ = self.struct_descriptors()
        # 新分支合法
        with_branch = ProducerDescriptor(
            "P1", 2, {1: "branch-record", 2: MISSING_BRANCH_ID, 3: "branch-extra"},
            (
                BranchSpec("branch-record", "record", "struct", (
                    FieldSpec("field-quantity", "qty", KIND_INT64, nullable=False),)),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
                BranchSpec("branch-extra", "extra", KIND_TEXT),
            ),
        )
        ex = Exchange(ShardStore(self.store_root))
        ex.propose(v1); ex.accept("P1", 1)
        ex.propose(with_branch); ex.accept("P1", 2)
        self.assertIn("branch-extra", ex.canonical.branch_ids())

        # 新增非 nullable 字段非法
        bad = ProducerDescriptor(
            "P1", 3, {1: "branch-record", 2: MISSING_BRANCH_ID},
            (
                BranchSpec("branch-record", "record", "struct", (
                    FieldSpec("field-quantity", "qty", KIND_INT64, nullable=False),
                    FieldSpec("field-mandatory", "m", KIND_INT64, nullable=False),
                )),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
            ),
        )
        with self.assertRaises(IncompatibleSchema):
            ex.propose(bad)
