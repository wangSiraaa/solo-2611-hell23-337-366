"""方案取舍的可复现实验：版本覆盖层 vs 整树复制。

两种表示在同一条“N 个节点、每版只改 1 个节点”的版本链上比较持久化的
对象内容记录数：

- 整树复制（基线 Store 的做法，每个版本完整快照全部节点）：
  内容记录 = 版本数 * N，随版本线性全量膨胀；
- 版本覆盖层（本实现，稳定 lid + 不可变修订，只追加被改节点）：
  内容记录 = N + (版本数-1) * 每版改动数，与图规模解耦。

运行：python experiments/storage_tradeoff.py
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from repostore.cbor_codec import NodeArray, encode_cbor  # noqa: E402
from repostore.store import Store, make_handle  # noqa: E402


def build_graph(n):
    children = [NodeArray([i]) for i in range(n - 1)]
    return NodeArray(children), children


def overlay_records(n, versions):
    """覆盖层：实际写入 SQLite 的 node_content 行数。"""
    path = tempfile.mktemp(suffix=".sqlite")
    s = Store(path)
    try:
        root, _ = build_graph(n)
        vid = s.import_cbor(encode_cbor(root))
        built = s.materialize_index(vid)
        root_lid = s.conn.execute(
            "SELECT root_lid FROM versions WHERE vid=1").fetchone()["root_lid"]
        o2l = {id(o): lid for lid, o in built.items()}
        child_lids = [o2l[id(c)] for c in built[root_lid].items]
        for i in range(1, versions):
            lid = child_lids[i % len(child_lids)]
            rev = s._resolve_rev(lid, vid)
            vid = s.patch(vid, [{
                "op": "array_set", "handle": make_handle(lid, rev),
                "index": 0, "value": {"scalar": 1000 + i}}])
        return s.storage_counts()["content_rows"]
    finally:
        s.close()
        for suffix in ("", "-wal", "-shm"):
            p = path + suffix
            if os.path.exists(p):
                os.unlink(p)


def full_copy_records(n, versions):
    """整树复制基线的理论内容记录数：每版都重写全部 N 个容器。"""
    return n * versions


def main():
    n = 1000
    print(f"图规模 N={n} 容器节点，每版改 1 个节点\n")
    print(f"{'版本数':>6} | {'整树复制':>10} | {'覆盖层(实测)':>12} | 节省")
    print("-" * 52)
    for versions in (2, 5, 10, 20):
        full = full_copy_records(n, versions)
        overlay = overlay_records(n, versions)
        ratio = full / overlay
        print(f"{versions:>6} | {full:>10} | {overlay:>12} | "
              f"{ratio:>4.1f}x 少写")
    print("\n结论：覆盖层内容记录 = N + (版本-1)*改动数，与图规模解耦；")
    print("整树复制 = 版本*N。版本越多、图越大，覆盖层优势越明显。")
    print("放弃整树复制的原因：它无法同时满足节点身份语义（共享/环）与")
    print("小更新存储预算（每版 <=100 内容记录）。")


if __name__ == "__main__":
    main()
