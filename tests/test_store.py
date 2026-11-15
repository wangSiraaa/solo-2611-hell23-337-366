"""共享/循环对象数据仓的不变量测试（标准库 unittest，无需 pytest）。

覆盖：
- 类型化键（int 1 与 bool true 共存；bytes/text 键）
- 容器身份：同值不共享对象不合并、别名、环
- 覆盖层 patch：共享节点一改全改、旧版本冻结、句柄失效
- 原子批处理：坏操作整批回滚、引用完整性、类型化键校验
- 导出/再导入一致性（真实 CBOR，tag28/29）
- 保留窗口（>=5 根版本）、drop + gc（环回收、活版本共享不误删）
- 物理位置回收复用后旧句柄不指向新对象
- patch/gc 四个中断点的崩溃恢复
- 1 万节点链每版 20 改动的存储计数上限
"""

import os
import tempfile
import unittest

from repostore.cbor_codec import (
    NodeArray,
    TypedKey,
    TypedMap,
    decode_cbor,
    encode_cbor,
)
from repostore.store import (
    MAX_CONTENT_ROWS_PER_VERSION,
    MAX_RETAINED_VERSIONS,
    ConflictError,
    Store,
    ValidationError,
    make_handle,
    parse_handle,
)

FIX = os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures")


def fixture_bytes(name):
    with open(os.path.join(FIX, name), "rb") as f:
        return f.read()


def build_shared_map():
    """{1: shared, '1': shared, 'cycle': <self>}，shared=[10,20]。"""
    shared = NodeArray([10, 20])
    root = TypedMap()
    root.entries = [
        (TypedKey("int", 1), shared),
        (TypedKey("text", "1"), shared),
        (TypedKey("text", "cycle"), root),
    ]
    return root


def fresh_store():
    tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
    tmp.close()
    os.unlink(tmp.name)
    return Store(tmp.name), tmp.name


class CodecTests(unittest.TestCase):
    def test_typed_keys_fixture_preserves_both_keys(self):
        data = fixture_bytes("typed-keys.cbor")
        root, _, _ = decode_cbor(data)
        self.assertIsInstance(root, TypedMap)
        keys = {(k.type, k.value) for k, _ in root.entries}
        self.assertIn(("int", 1), keys)
        self.assertIn(("bool", True), keys)
        self.assertEqual(len(root.entries), 2)

    def test_cbor2_would_have_lost_info(self):
        # 证明成熟库入口先丢信息：cbor2 合并 int(1)/bool(true)
        import cbor2
        data = fixture_bytes("typed-keys.cbor")
        self.assertEqual(len(cbor2.loads(data)), 1)  # 只剩一个键

    def test_identity_and_cycle_fixture(self):
        data = fixture_bytes("shared.cbor")
        root, nodes, edges = decode_cbor(data)
        self.assertIs(root.get("text", "cycle"), root)
        self.assertIs(root.get("int", 1), root.get("text", "1"))
        self.assertEqual(nodes, 2)
        self.assertEqual(edges, 5)

    def test_equal_but_unshared_containers_stay_distinct(self):
        mp = TypedMap([
            (TypedKey("text", "a"), NodeArray([1, 2])),
            (TypedKey("text", "b"), NodeArray([1, 2])),
        ])
        blob = encode_cbor(mp)
        back, _, _ = decode_cbor(blob)
        self.assertIsNot(back.entries[0][1], back.entries[1][1])

    def test_roundtrip_byte_identical_to_fixtures(self):
        for name in ("typed-keys.cbor", "shared.cbor"):
            blob = fixture_bytes(name)
            root, _, _ = decode_cbor(blob)
            self.assertEqual(encode_cbor(root), blob, name)

    def test_self_cycle_and_two_node_cycle(self):
        a = NodeArray()
        a.items = [a]
        r, _, _ = decode_cbor(encode_cbor(a))
        self.assertIs(r, r.items[0])

        x, y = NodeArray(), NodeArray()
        x.items = [y]
        y.items = [x]
        r2, _, _ = decode_cbor(encode_cbor(x))
        self.assertIs(r2.items[0].items[0], r2)

    def test_dag_not_tagged_shareable(self):
        dag = NodeArray([NodeArray([1]), NodeArray([2])])
        self.assertNotIn(b"\xd8\x1c", encode_cbor(dag))

    def test_all_scalar_key_types(self):
        mp = TypedMap([
            (TypedKey("int", -7), "i"),
            (TypedKey("text", "k"), "t"),
            (TypedKey("bytes", b"\x01"), "b"),
            (TypedKey("bool", False), "f"),
            (TypedKey("bool", True), "t2"),
        ])
        back, _, _ = decode_cbor(encode_cbor(mp))
        self.assertEqual(len(back.entries), 5)
        self.assertEqual(back.get("bool", False), "f")

    def test_malformed_cbor_rejected(self):
        from repostore.cbor_codec import CBORError
        cases = {
            "非法tag": bytes([0xd8, 0x20, 0x01]),
            "截断": bytes([0x82, 0x01]),
            "悬空tag29": bytes([0xd8, 0x1d, 0x05]),
            "嵌套悬空": bytes([0x81, 0xd8, 0x1d, 0x00]),
            "float键": bytes([0xa1, 0xfa, 0x3f, 0x80, 0x00, 0x00, 0x01]),
            "多余字节": bytes([0x01, 0x02]),
        }
        for name, blob in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(CBORError):
                    decode_cbor(blob)


class ImportAndOverlayTests(unittest.TestCase):
    def setUp(self):
        self.store, self.path = fresh_store()

    def tearDown(self):
        self.store.close()
        for suffix in ("", "-wal", "-shm"):
            p = self.path + suffix if suffix else self.path
            if os.path.exists(p):
                os.unlink(p)

    def test_import_preserves_alias_and_cycle(self):
        vid = self.store.import_cbor(
            fixture_bytes("shared.cbor"))
        g = self.store.materialize(vid)
        self.assertIs(g.get("int", 1), g.get("text", "1"))
        self.assertIs(g.get("text", "cycle"), g)

    def test_typed_keys_land_in_storage(self):
        vid = self.store.import_cbor(
            fixture_bytes("typed-keys.cbor"))
        g = self.store.materialize(vid)
        self.assertEqual(len(g.entries), 2)
        self.assertEqual(g.get("int", 1), "a")
        self.assertEqual(g.get("bool", True), "b")

    def _array_lid_handle(self, vid):
        row = self.store.conn.execute(
            "SELECT lid FROM node_content WHERE kind='array' LIMIT 1"
        ).fetchone()
        lid = row["lid"]
        return make_handle(lid, self.store._resolve_rev(lid, vid)), lid

    def test_shared_node_edit_visible_via_all_aliases_old_frozen(self):
        v1 = self.store.import_cbor(
            fixture_bytes("shared.cbor"))
        h, lid = self._array_lid_handle(v1)
        v2 = self.store.patch(v1, [
            {"op": "array_set", "handle": h, "index": 0,
             "value": {"scalar": 99}}])
        old = self.store.materialize(v1)
        new = self.store.materialize(v2)
        self.assertEqual(old.get("int", 1).items, [10, 20])  # 旧版冻结
        self.assertEqual(new.get("int", 1).items, [99, 20])
        self.assertIs(new.get("int", 1), new.get("text", "1"))  # 别名同改
        self.assertIs(new.get("text", "cycle"), new)
        # 只新增 1 条内容记录，未整树复制
        self.assertEqual(self.store.content_rows_for_version(v2), 1)

    def test_stale_handle_rejected_not_silently_rebased(self):
        v1 = self.store.import_cbor(
            fixture_bytes("shared.cbor"))
        h, lid = self._array_lid_handle(v1)
        self.store.patch(v1, [
            {"op": "array_set", "handle": h, "index": 0,
             "value": {"scalar": 99}}])
        # 旧修订句柄在新版本上必须被拒绝，而不是悄悄指向新修订
        with self.assertRaises(ConflictError):
            self.store.patch(v1 + 1, [
                {"op": "array_set", "handle": h, "index": 1,
                 "value": {"scalar": 7}}])

    def test_map_typed_key_set_and_delete(self):
        v1 = self.store.import_cbor(
            fixture_bytes("typed-keys.cbor"))
        row = self.store.conn.execute(
            "SELECT lid FROM node_content WHERE kind='map' LIMIT 1").fetchone()
        lid = row["lid"]

        def handle(vid):
            return make_handle(lid, self.store._resolve_rev(lid, vid))

        # int(1) 与 bool(true) 分别写不同值，互不覆盖
        v2 = self.store.patch(v1, [
            {"op": "map_set", "handle": handle(v1),
             "key": {"type": "int", "value": 1}, "value": {"scalar": "II"}},
            {"op": "map_set", "handle": handle(v1),
             "key": {"type": "bool", "value": True}, "value": {"scalar": "BB"}},
        ])
        g = self.store.materialize(v2)
        self.assertEqual(g.get("int", 1), "II")
        self.assertEqual(g.get("bool", True), "BB")
        self.assertEqual(len(g.entries), 2)
        v3 = self.store.patch(v2, [
            {"op": "map_delete", "handle": handle(v2),
             "key": {"type": "int", "value": 1}}])
        g3 = self.store.materialize(v3)
        with self.assertRaises(KeyError):
            g3.get("int", 1)
        self.assertEqual(g3.get("bool", True), "BB")

    def test_set_root_to_scalar_and_back(self):
        v1 = self.store.import_cbor(
            fixture_bytes("shared.cbor"))
        v2 = self.store.patch(v1, [{"op": "set_root",
                                    "value": {"scalar": None}}])
        self.assertIsNone(self.store.materialize(v2))
        # 旧版仍在；把根指回旧根节点（用旧版句柄）
        rh = self.store.root_handle(v1)
        v3 = self.store.patch(v2, [{"op": "set_root",
                                    "value": {"ref": rh}}])
        g3 = self.store.materialize(v3)
        self.assertIsInstance(g3, TypedMap)

    def test_atomic_batch_rolls_back_on_bad_op(self):
        v1 = self.store.import_cbor(
            fixture_bytes("shared.cbor"))
        h, _ = self._array_lid_handle(v1)
        before = self.store.storage_counts()
        with self.assertRaises(ValidationError):
            self.store.patch(v1, [
                {"op": "array_set", "handle": h, "index": 0,
                 "value": {"scalar": 1}},
                {"op": "array_set", "handle": h, "index": 99,
                 "value": {"scalar": 2}},  # 越界
            ])
        # 没有新版本、没有残留内容/边
        self.assertEqual(self.store.storage_counts(), before)
        self.assertEqual([v["vid"] for v in self.store.list_versions()], [v1])

    def test_dangling_reference_rejected(self):
        v1 = self.store.import_cbor(
            fixture_bytes("shared.cbor"))
        with self.assertRaises(ConflictError):
            self.store.patch(v1, [{"op": "set_root",
                                   "value": {"ref": "L99999:R1"}}])

    def test_batch_size_limit(self):
        v1 = self.store.import_cbor(
            fixture_bytes("typed-keys.cbor"))
        row = self.store.conn.execute(
            "SELECT lid FROM node_content LIMIT 1").fetchone()
        h = make_handle(row["lid"], 1)
        ops = [{"op": "map_set", "handle": h,
                "key": {"type": "text", "value": f"k{i}"},
                "value": {"scalar": i}} for i in range(21)]
        with self.assertRaises(ValidationError):
            self.store.patch(v1, ops)


class ExportReimportTests(unittest.TestCase):
    def test_roundtrip_preserves_types_aliases_cycles(self):
        store, path = fresh_store()
        try:
            v1 = store.import_cbor(
                fixture_bytes("shared.cbor"))
            h_row = store.conn.execute(
                "SELECT lid FROM node_content WHERE kind='array'").fetchone()
            h = make_handle(h_row["lid"], 1)
            v2 = store.patch(v1, [
                {"op": "array_set", "handle": h, "index": 0,
                 "value": {"scalar": 99}}])
            blob = store.export_cbor(v2)
            v3 = store.import_cbor(blob, label="reimport")
            g = store.materialize(v3)
            self.assertEqual(g.get("int", 1).items, [99, 20])
            self.assertIs(g.get("int", 1), g.get("text", "1"))
            self.assertIs(g.get("text", "cycle"), g)
            # 独立再解码验证类型化键（构造含 int/bool 键的版本）
            t_v1 = store.import_cbor(
                fixture_bytes("typed-keys.cbor"))
            t_blob = store.export_cbor(t_v1)
            back, _, _ = decode_cbor(t_blob)
            self.assertEqual({(k.type, k.value) for k, _ in back.entries},
                             {("int", 1), ("bool", True)})
        finally:
            store.close()


class RetentionAndGCTests(unittest.TestCase):
    def setUp(self):
        self.store, self.path = fresh_store()

    def tearDown(self):
        self.store.close()
        for suffix in ("", "-wal", "-shm"):
            p = self.path + suffix if suffix else self.path
            if os.path.exists(p):
                os.unlink(p)

    def test_retain_window_keeps_five_roots(self):
        vid = self.store.import_cbor(
            fixture_bytes("typed-keys.cbor"))
        row = self.store.conn.execute(
            "SELECT lid FROM node_content LIMIT 1").fetchone()
        h = make_handle(row["lid"], 1)
        for i in range(6):
            vid = self.store.patch(vid, [
                {"op": "map_set", "handle":
                 make_handle(row["lid"],
                             self.store._resolve_rev(row["lid"], vid)),
                 "key": {"type": "text", "value": f"k{i}"},
                 "value": {"scalar": i}}])
        active = self.store.list_versions()
        self.assertLessEqual(len(active), MAX_RETAINED_VERSIONS)
        self.assertEqual(len(active), MAX_RETAINED_VERSIONS)

    def test_gc_collects_orphan_cycle_but_keeps_live_shared(self):
        # 版本1: 根数组 [A, S, S]，A<->B 构成环；S 被两个槽共享
        a, b, s = NodeArray(), NodeArray(), NodeArray([1])
        a.items = [b]
        b.items = [a]
        root1 = NodeArray([a, s, s])
        v1 = self.store.import_cbor(encode_cbor(root1))

        # 从同一份物化索引按结构定位 lid（materialize 每次构造新对象，
        # 必须在同一对象集内比较身份）
        built = self.store.materialize_index(v1)
        root_lid = self._root_lid(v1)
        g1 = built[root_lid]
        a_obj, s_obj = g1.items[0], g1.items[1]
        b_obj = a_obj.items[0]
        lid_by_obj = {id(o): lid for lid, o in built.items()}
        a_lid, b_lid, s_lid = (lid_by_obj[id(a_obj)],
                               lid_by_obj[id(b_obj)],
                               lid_by_obj[id(s_obj)])

        v2 = self.store.patch(v1, [
            {"op": "array_set",
             "handle": make_handle(root_lid,
                                   self.store._resolve_rev(root_lid, v1)),
             "index": 0, "value": {"scalar": 0}}])
        self.store.drop_version(v1)  # 唯一引用 A/B 环的根退役
        report = self.store.gc()
        self.assertGreaterEqual(report["removed_content"], 2)
        # 环内容确实被删（有环无外部根也能回收）
        for lid in (a_lid, b_lid):
            self.assertIsNone(self.store.conn.execute(
                "SELECT 1 FROM node_content WHERE lid=?", (lid,)).fetchone())
        # S 仍被活版本共享引用，不能误删
        self.assertIsNotNone(self.store.conn.execute(
            "SELECT 1 FROM node_content WHERE lid=?", (s_lid,)).fetchone())
        g2 = self.store.materialize(v2)
        self.assertIs(g2.items[1], g2.items[2])

    def _root_lid(self, vid):
        return self.store.conn.execute(
            "SELECT root_lid FROM versions WHERE vid=?", (vid,)
        ).fetchone()["root_lid"]

    def test_old_handle_never_points_to_reused_location(self):
        # 制造内容 -> gc 清空部分 lid 的所有修订 -> 新导入分配新 lid
        a, b = NodeArray(), NodeArray()
        a.items = [b]
        b.items = [a]
        v1 = self.store.import_cbor(encode_cbor(NodeArray([a])))
        old_handles = [
            make_handle(r["lid"], r["rev"])
            for r in self.store.conn.execute(
                "SELECT lid, rev FROM node_content").fetchall()]
        self.store.drop_version(v1)
        self.store.gc()
        self.assertEqual(self.store.storage_counts()["content_rows"], 0)
        # 新导入：lid 单调不复用，旧句柄解析必须失败
        v2 = self.store.import_cbor(
            fixture_bytes("typed-keys.cbor"))
        for h in old_handles:
            lid, rev = parse_handle(h)
            with self.assertRaises(ConflictError):
                self.store._resolve_rev(lid, v2)
        # 物理 rowid 可能被复用，但逻辑身份不会撞车
        self.assertTrue(all(
            self.store.conn.execute(
                "SELECT 1 FROM node_content WHERE lid=? AND rev=?",
                parse_handle(h)).fetchone() is None
            for h in old_handles))

    def test_reopen_branches_without_copying_content(self):
        v1 = self.store.import_cbor(fixture_bytes("shared.cbor"))
        before = self.store.storage_counts()["content_rows"]
        v_branch = self.store.reopen_version(v1)
        # 分支版本不新增任何内容记录，只新增版本索引
        self.assertEqual(
            self.store.storage_counts()["content_rows"], before)
        # 在分支上独立修改，不影响原版本
        arr_row = self.store.conn.execute(
            "SELECT lid FROM node_content WHERE kind='array'").fetchone()
        h = make_handle(arr_row["lid"],
                        self.store._resolve_rev(arr_row["lid"], v_branch))
        v3 = self.store.patch(v_branch, [
            {"op": "array_set", "handle": h, "index": 0,
             "value": {"scalar": 31337}}])
        # 原版本完全冻结
        self.assertEqual(
            self.store.materialize(v1).get("int", 1).items, [10, 20])
        # 分支点版本本身也冻结（修订提交在其后代 v3）
        self.assertEqual(
            self.store.materialize(v_branch).get("int", 1).items, [10, 20])
        # 修改只在新版本可见
        self.assertEqual(
            self.store.materialize(v3).get("int", 1).items, [31337, 20])

    def test_sibling_branches_do_not_leak_revisions(self):
        # v1 -> reopen 得 v2；v2 与直接基于 v1 的另一条线互不可见
        v1 = self.store.import_cbor(fixture_bytes("shared.cbor"))
        v2 = self.store.reopen_version(v1)
        arr_row = self.store.conn.execute(
            "SELECT lid FROM node_content WHERE kind='array'").fetchone()
        lid = arr_row["lid"]
        # 分支 A：v2 上改数组
        h2 = make_handle(lid, self.store._resolve_rev(lid, v2))
        va = self.store.patch(v2, [
            {"op": "array_set", "handle": h2, "index": 0,
             "value": {"scalar": 111}}])
        # 分支 B：直接从 v1 拉另一条线并改不同值
        v_other = self.store.reopen_version(v1)
        ho = make_handle(lid, self.store._resolve_rev(lid, v_other))
        vb = self.store.patch(v_other, [
            {"op": "array_set", "handle": ho, "index": 0,
             "value": {"scalar": 222}}])
        # 两条兄弟分支各见各的值，互不泄漏
        self.assertEqual(
            self.store.materialize(va).get("int", 1).items, [111, 20])
        self.assertEqual(
            self.store.materialize(vb).get("int", 1).items, [222, 20])
        # 共同祖先 v1 仍冻结
        self.assertEqual(
            self.store.materialize(v1).get("int", 1).items, [10, 20])
        # 两条分支对同一 lid 解析到各自独立的修订，互不复用编号
        rev_a = self.store._resolve_rev(lid, va)
        rev_b = self.store._resolve_rev(lid, vb)
        self.assertNotEqual(rev_a, rev_b)
        # 分支 A 的修订号在分支 B 谱系中不可见
        self.assertFalse(self.store._is_ancestor_or_self(
            self.store.conn.execute(
                "SELECT vid FROM node_content WHERE lid=? AND rev=?",
                (lid, rev_a)).fetchone()["vid"], vb))


class CrashRecoveryTests(unittest.TestCase):
    def _store_with_crash(self, path, hook_point, operation):
        s = Store(path)
        state = {"armed": True}

        def hook(point, op):
            if state["armed"] and point == hook_point and op == operation:
                state["armed"] = False
                raise RuntimeError(f"injected crash at {op}:{point}")
        s.crash_hook = hook
        return s, state

    def _cleanup(self, path, s):
        s.close()
        for suffix in ("", "-wal", "-shm"):
            p = path + suffix if suffix else path
            if os.path.exists(p):
                os.unlink(p)

    def test_patch_crash_pre_and_post_commit(self):
        for point in ("pre_commit", "post_commit"):
            with self.subTest(point=point):
                _, path = fresh_store()
                s, _ = self._store_with_crash(path, point, "patch")
                v1 = s.import_cbor(
                    fixture_bytes("shared.cbor"))
                h_row = s.conn.execute(
                    "SELECT lid FROM node_content WHERE kind='array'").fetchone()
                h = make_handle(h_row["lid"], 1)
                with self.assertRaises(RuntimeError):
                    s.patch(v1, [{"op": "array_set", "handle": h,
                                  "index": 0, "value": {"scalar": 5}}])
                s.close()
                # 重新打开：版本结果必须与崩溃点语义一致
                s2 = Store(path)
                versions = [v["vid"] for v in s2.list_versions()]
                if point == "pre_commit":
                    self.assertEqual(versions, [v1])  # 事务回滚
                    g = s2.materialize(v1)
                    self.assertEqual(g.get("int", 1).items, [10, 20])
                else:
                    # commit 已成功，钩子在提交后抛错
                    self.assertEqual(versions, [v1, v1 + 1])
                    self.assertEqual(
                        s2.materialize(v1 + 1).get("int", 1).items, [5, 20])
                self._cleanup(path, s2)

    def test_gc_crash_after_mark_and_before_sweep(self):
        for point in ("post_mark", "pre_sweep"):
            with self.subTest(point=point):
                _, path = fresh_store()
                s, _ = self._store_with_crash(path, point, "gc")
                a, b = NodeArray(), NodeArray()
                a.items = [b]
                b.items = [a]
                v1 = s.import_cbor(encode_cbor(NodeArray([a])))
                keep = s.import_cbor(
                    fixture_bytes("typed-keys.cbor"))
                s.drop_version(v1)
                with self.assertRaises(RuntimeError):
                    s.gc()
                s.close()

                s2 = Store(path)
                # 中断后活版本仍可完整读取
                self.assertIsNotNone(s2.materialize(keep))
                # 续跑 gc：完成回收，结果与一次性执行一致
                report = s2.gc()
                self.assertGreater(report["removed_content"], 0)
                # 活版本依旧完好
                g = s2.materialize(keep)
                self.assertEqual(len(g.entries), 2)
                self._cleanup(path, s2)


    def test_gc_state_invalidated_by_intervening_write(self):
        # GC 标记后中断；期间发生新写入，旧标记必须作废、按新快照重算
        _, path = fresh_store()
        s, _ = self._store_with_crash(path, "post_mark", "gc")
        a, b = NodeArray(), NodeArray()
        a.items = [b]
        b.items = [a]
        v1 = s.import_cbor(encode_cbor(NodeArray([a])))
        s.drop_version(v1)
        with self.assertRaises(RuntimeError):
            s.gc()
        # gc_state 已写入标记
        self.assertIsNotNone(s.conn.execute(
            "SELECT 1 FROM gc_state").fetchone())
        # 一次新写入应清除陈旧标记
        keep = s.import_cbor(fixture_bytes("typed-keys.cbor"))
        self.assertIsNone(s.conn.execute(
            "SELECT 1 FROM gc_state").fetchone())
        s.close()

        s2 = Store(path)
        report = s2.gc()  # 重新标记，而不是复用旧 live 集合
        self.assertGreater(report["removed_content"], 0)
        self.assertEqual(len(s2.materialize(keep).entries), 2)
        self._cleanup(path, s2)


class LargeChainStorageTests(unittest.TestCase):
    def test_10k_nodes_20_edits_per_version_stays_under_cap(self):
        store, path = fresh_store()
        try:
            # 1 万容器节点、深度仅 2（满足深度 <=100 边界）：
            # 根数组持有 9999 个子数组，每个子数组持有一个标量。
            children = [NodeArray([i]) for i in range(9_999)]
            root = NodeArray(children)
            v1 = store.import_cbor(encode_cbor(root))
            self.assertEqual(store.storage_counts()["content_rows"], 10_000)

            # 取根与子节点的稳定 lid（不依赖分配顺序）
            built = store.materialize_index(v1)
            root_lid = store.conn.execute(
                "SELECT root_lid FROM versions WHERE vid=?", (v1,)
            ).fetchone()["root_lid"]
            g1 = built[root_lid]
            self.assertEqual(len(g1.items), 9_999)
            obj_to_lid = {id(obj): lid for lid, obj in built.items()}
            child_lids = [obj_to_lid[id(child)] for child in g1.items]
            self.assertEqual(len(child_lids), 9_999)

            # 每版改 20 个不同子节点（改其标量项），连续 6 版触发保留窗口
            vid = v1
            for version_i in range(6):
                ops = []
                for j in range(20):
                    target_lid = child_lids[version_i * 20 + j]
                    rev = store._resolve_rev(target_lid, vid)
                    ops.append({
                        "op": "array_set",
                        "handle": make_handle(target_lid, rev),
                        "index": 0,
                        "value": {"scalar": 100_000 + version_i * 20 + j},
                    })
                vid = store.patch(vid, ops)
                added = store.content_rows_for_version(vid)
                self.assertLessEqual(added, MAX_CONTENT_ROWS_PER_VERSION)
                self.assertEqual(added, 20)  # 精确：只写被改节点
            counts = store.storage_counts()
            # 内容记录 = 1 万初始 + 6*20 修订（退役不删内容，gc 才删）
            self.assertEqual(counts["content_rows"], 10_000 + 6 * 20)
            # 旧版本完全冻结
            self.assertEqual(store.materialize(v1).items[5].items[0], 5)
            # 新版本改动可见，未改部分保持
            g_latest = store.materialize(vid)
            self.assertEqual(g_latest.items[0].items[0], 100_000)
            # 索引 500 从未被任何版本修改，保持初始值 500
            self.assertEqual(g_latest.items[500].items[0], 500)

            # gc 后：退役版本独有修订被清，活版本共享的初始内容保留
            report = store.gc()
            # 6 版中前 1 版退役；退役后不可达的只有被改过的旧修订
            self.assertGreater(report["removed_content"], 0)
            still = store.storage_counts()
            # 活版本仍可完整物化（1 万节点身份不丢）
            g_after = store.materialize(vid)
            self.assertEqual(len(g_after.items), 9_999)
            self.assertEqual(g_after.items[0].items[0], 100_000)
            self.assertLessEqual(still["content_rows"], 10_000 + 6 * 20)
        finally:
            store.close()
            for suffix in ("", "-wal", "-shm"):
                p = path + suffix if suffix else path
                if os.path.exists(p):
                    os.unlink(p)


if __name__ == "__main__":
    unittest.main(verbosity=2)
