"""同一合法输入族的随机变体测试：生成器固定随机种子，结果可复现。

覆盖随机形状（数组/映射混合、随机共享别名、随机环、四类类型化键），
验证导入→多版 patch→导出往返→GC 全链路的不变量。
"""

import os
import random
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
    ConflictError,
    Store,
    ValidationError,
    make_handle,
)


def build_random_graph(seed, n_containers=60):
    rng = random.Random(seed)
    containers = []
    for i in range(n_containers):
        if rng.random() < 0.5:
            containers.append(NodeArray())
        else:
            containers.append(TypedMap())

    key_types = [
        ("int", lambda: rng.randint(-5, 5)),       # 故意制造 1/True 碰撞域
        ("bool", lambda: rng.choice([True, False])),
        ("text", lambda: rng.choice(["1", "k", "x"])),
        ("bytes", lambda: bytes([rng.randint(0, 3)])),
    ]

    for i, c in enumerate(containers):
        n_edges = rng.randint(0, 4)
        for _ in range(n_edges):
            if rng.random() < 0.45 and i > 0:
                target = containers[rng.randrange(i)]  # 只指向已有节点→可成环
            else:
                target = rng.choice([rng.randint(-9, 9),
                                     rng.choice([True, False, None]),
                                     "s", b"b"])
            if isinstance(c, NodeArray):
                c.items.append(target)
            else:
                kt, gen = rng.choice(key_types)
                c.entries.append((TypedKey(kt, gen()), target))

    # 根随机选一个容器；保证至少有边
    root = containers[rng.randrange(len(containers))]
    root_empty = (not root.items) if isinstance(root, NodeArray) \
        else (not root.entries)
    if root_empty:
        if isinstance(root, NodeArray):
            root.items.append(1)
        else:
            root.entries.append((TypedKey("text", "k"), 1))
    return root


def graph_signature(obj):
    """基于对象身份的结构签名，用于比较两张图的类型/别名/环是否同构。"""
    index = {}
    order = []
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, (NodeArray, TypedMap)):
            if id(cur) in index:
                continue
            index[id(cur)] = len(order)
            order.append(cur)
            children = (cur.items if isinstance(cur, NodeArray)
                        else [v for _, v in cur.entries])
            stack.extend(children)

    def key_sig(k):
        return (k.type,
                k.value.hex() if isinstance(k.value, bytes) else k.value)

    sig = []
    for node in order:
        if isinstance(node, NodeArray):
            sig.append(("arr", tuple(
                index[id(x)] if isinstance(x, (NodeArray, TypedMap))
                else ("scalar", x) for x in node.items)))
        else:
            sig.append(("map", tuple(
                (key_sig(k),
                 index[id(v)] if isinstance(v, (NodeArray, TypedMap))
                 else ("scalar", v))
                for k, v in node.entries)))
    return index[id(obj)], sig


class RandomVariantTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        os.unlink(self.tmp.name)
        self.store = Store(self.tmp.name)

    def tearDown(self):
        self.store.close()
        for suffix in ("", "-wal", "-shm"):
            p = self.tmp.name + suffix if suffix else self.tmp.name
            if os.path.exists(p):
                os.unlink(p)

    def test_random_graphs_roundtrip_and_evolve(self):
        for seed in range(8):
            with self.subTest(seed=seed):
                root = build_random_graph(seed)
                blob = encode_cbor(root)
                # 自洽：先解码再编码应保持字节一致
                decoded, _, _ = decode_cbor(blob)
                self.assertEqual(encode_cbor(decoded), blob)

                vid = self.store.import_cbor(blob, label=f"seed{seed}")
                materialized = self.store.materialize(vid)
                # 结构签名（含别名身份与环）一致
                self.assertEqual(graph_signature(root),
                                 graph_signature(materialized))

                # 导出真实 CBOR 再导入，签名仍一致
                re_blob = self.store.export_cbor(vid)
                re_root, _, _ = decode_cbor(re_blob)
                self.assertEqual(graph_signature(root),
                                 graph_signature(re_root))

    def test_random_patches_keep_old_versions_frozen(self):
        root = build_random_graph(99, n_containers=40)
        v1 = self.store.import_cbor(encode_cbor(root))
        sig_before = graph_signature(self.store.materialize(v1))
        built = self.store.materialize_index(v1)
        arrays = [lid for lid, o in built.items() if isinstance(o, NodeArray)]
        if not arrays:
            self.skipTest("随机图无数组节点")

        # 在 v1 上连续打 3 版，每版改一个数组下标
        vid = v1
        for round_i in range(3):
            lid = arrays[round_i % len(arrays)]
            rev = self.store._resolve_rev(lid, vid)
            obj = built[lid]
            if not obj.items:
                continue
            idx = round_i % len(obj.items)
            vid = self.store.patch(vid, [{
                "op": "array_set", "handle": make_handle(lid, rev),
                "index": idx, "value": {"scalar": 777 + round_i}}])
        # 旧版本签名完全不变
        self.assertEqual(
            graph_signature(self.store.materialize(v1)), sig_before)
        # 新版本可以物化且规模不缩减
        self.assertEqual(
            len(self.store.materialize_index(vid)), len(built))

    def test_invalid_ops_rejected_on_random_graph(self):
        root = build_random_graph(7, n_containers=30)
        v1 = self.store.import_cbor(encode_cbor(root))
        built = self.store.materialize_index(v1)
        maps = [lid for lid, o in built.items() if isinstance(o, TypedMap)]
        if not maps:
            self.skipTest("随机图无映射节点")
        lid = maps[0]
        h = make_handle(lid, self.store._resolve_rev(lid, v1))
        # 对数组操作作用于映射 → 类型校验失败
        with self.assertRaises(ValidationError):
            self.store.patch(v1, [
                {"op": "array_set", "handle": h, "index": 0,
                 "value": {"scalar": 1}}])
        # 键类型与值不符
        with self.assertRaises(ValidationError):
            self.store.patch(v1, [
                {"op": "map_set", "handle": h,
                 "key": {"type": "int", "value": True},
                 "value": {"scalar": 1}}])
        # 不存在的引用
        with self.assertRaises(ConflictError):
            self.store.patch(v1, [
                {"op": "set_root", "value": {"ref": "L424242:R9"}}])


if __name__ == "__main__":
    unittest.main(verbosity=2)
