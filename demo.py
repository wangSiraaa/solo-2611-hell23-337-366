"""升级后的可运行演示（保留“导入共享对象并编辑”的正常路径）。

与基线 demo.py 的区别：
- 走持久化 Store（SQLite），不再是内存里共享可变字典；
- 编辑产生新版本，旧版本完全冻结，共享节点的所有别名一致看到修改；
- 打印真实持久记录计数，证明小更新不整树复制。

运行：python demo.py
"""

import json
import os
import tempfile

from repostore.cbor_codec import decode_cbor
from repostore.store import Store, make_handle


def main():
    db_path = os.path.join(tempfile.gettempdir(), "demo_repostore.sqlite")
    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(db_path + suffix):
            os.unlink(db_path + suffix)

    store = Store(db_path)
    data = open("fixtures/shared.cbor", "rb").read()

    # 基线能力：导入真实 CBOR（保留共享别名与环）
    v1 = store.import_cbor(data)
    g1 = store.materialize(v1)
    print("v1 根映射键:",
          [(k.type, k.value) for k, _ in g1.entries])
    print("v1 共享数组（两个别名同一对象）:", g1.get("int", 1).items,
          "alias-identity:", g1.get("int", 1) is g1.get("text", "1"))

    # 新能力：基于稳定句柄修改共享数组节点
    arr_lid = store.conn.execute(
        "SELECT lid FROM node_content WHERE kind='array'").fetchone()["lid"]
    handle = make_handle(arr_lid, store._resolve_rev(arr_lid, v1))
    v2 = store.patch(v1, [{
        "op": "array_set", "handle": handle, "index": 0,
        "value": {"scalar": 99}}])

    old, new = store.materialize(v1), store.materialize(v2)
    print("v1（旧版冻结）:", old.get("int", 1).items)
    print("v2 两个别名都看到修改:",
          new.get("int", 1).items,
          new.get("text", "1").items,
          "alias-identity:", new.get("int", 1) is new.get("text", "1"))
    print("v2 环仍闭合:", new.get("text", "cycle") is new)
    print("本版新增内容记录数（不是整树复制）:",
          store.content_rows_for_version(v2))
    print("持久计数:", json.dumps(store.storage_counts()))

    # 导出为真实 CBOR 并可被标准 tag28/29 重新读入
    blob = store.export_cbor(v2)
    re_root, _, _ = decode_cbor(blob)
    print("导出再读回，别名/环保持:",
          re_root.get("int", 1) is re_root.get("text", "1"),
          re_root.get("text", "cycle") is re_root)
    store.close()


if __name__ == "__main__":
    main()
