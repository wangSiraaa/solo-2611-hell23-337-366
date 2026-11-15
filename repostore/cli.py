"""命令行入口。

用法：
  python -m repostore.cli import   DB.cbor.sqlite FILE.cbor
  python -m repostore.cli versions DB [--all]
  python -m repostore.cli query    DB VID [--handle H] [--raw-cbor OUT]
  python -m repostore.cli patch    DB BASE VID_OPS.json
  python -m repostore.cli export   DB VID OUT.cbor
  python -m repostore.cli drop-version DB VID
  python -m repostore.cli gc       DB
  python -m repostore.cli stats    DB

patch JSON 为操作数组，例如：
  [{"op":"array_set","handle":"L1:R1","index":0,"value":{"scalar":99}},
   {"op":"map_set","handle":"L2:R1","key":{"type":"int","value":1},
    "value":{"ref":"L3:R1"}},
   {"op":"set_root","value":{"scalar":null}}]
"""

import argparse
import json
import sys

from .cbor_codec import NodeArray, TypedMap
from .store import Store, StoreError


def _node_to_jsonable(obj, seen=None):
    """把物化对象图转为可打印 JSON；共享/环用 {ref: lid} 摘要表示。"""
    if seen is None:
        seen = {}
    if isinstance(obj, NodeArray):
        lid = id(obj)
        if lid in seen:
            return {"ref": f"#node{seen[lid]}"}
        seen[lid] = len(seen)
        return {"node": seen[lid],
                "array": [_node_to_jsonable(x, seen) for x in obj.items]}
    if isinstance(obj, TypedMap):
        lid = id(obj)
        if lid in seen:
            return {"ref": f"#node{seen[lid]}"}
        seen[lid] = len(seen)
        return {"node": seen[lid],
                "map": [{"key_type": k.type, "key_value": _jsonable(k.value),
                         "value": _node_to_jsonable(v, seen)}
                        for k, v in obj.entries]}
    return _jsonable(obj)


def _jsonable(v):
    if isinstance(v, bytes):
        return {"__bytes__": v.hex()}
    return v


def _normalize_legacy_op(op, store, current_vid):
    """把基线 patches.json 的裸 node/id 写法转成新 op 形式。

    旧形式 {"base":v,"node":"<lid>","index":i,"value":{"scalar":x}}
    新形式 {"op":"array_set","handle":"L..:R..", ...}
    裸 lid 在当前版本上解析为当前修订句柄，保证语义仍是“基于某版本”。
    """
    if "op" in op:
        return op
    if "node" in op and "index" in op:
        lid = int(op["node"])
        rev = store._resolve_rev(lid, current_vid)  # noqa: SLF001
        return {"op": "array_set",
                "handle": f"L{lid}:R{rev}",
                "index": op["index"], "value": op["value"]}
    raise ValueError(f"无法识别的批处理条目: {op}")


def _run_batch(store, ops_json_path):
    entries = json.loads(open(ops_json_path, encoding="utf-8").read())
    results = []
    current = None
    for entry in entries:
        if "drop_version" in entry:
            store.drop_version(entry["drop_version"])
            results.append({"drop_version": entry["drop_version"]})
            continue
        if "reopen_version" in entry:
            current = store.reopen_version(entry["reopen_version"])
            results.append({"reopen_version": entry["reopen_version"],
                            "new_version": current})
            continue
        if "base" in entry and "op" not in entry and "node" in entry:
            base = entry["base"]
            current = store.patch(base, [_normalize_legacy_op(
                entry, store, base)])
            results.append({"patch_base": base, "new_version": current})
            continue
        if "op" in entry:
            base = entry.pop("base", current)
            if base is None:
                raise ValueError("op 条目缺少 base 版本")
            current = store.patch(base, [entry])
            results.append({"patch_base": base, "new_version": current})
            continue
        raise ValueError(f"无法识别的批处理条目: {entry}")
    return {"steps": results, "final_version": current,
            "counts": store.storage_counts()}


def main(argv=None):
    p = argparse.ArgumentParser(prog="repostore",
                                description="可修订共享循环对象数据仓")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_db(sp):
        sp.add_argument("db")

    sp = sub.add_parser("import", help="导入 CBOR 为新版本")
    add_db(sp); sp.add_argument("file")

    sp = sub.add_parser("versions", help="列出版本")
    add_db(sp); sp.add_argument("--all", action="store_true",
                                help="包含已退役版本")

    sp = sub.add_parser("query", help="查询版本/节点")
    add_db(sp); sp.add_argument("vid", type=int)
    sp.add_argument("--handle", help="只看某节点句柄的内容")
    sp.add_argument("--raw-cbor", help="同时导出原始 CBOR 到文件")

    sp = sub.add_parser("patch", help="原子应用一批修改")
    add_db(sp); sp.add_argument("base", type=int); sp.add_argument("ops_json")

    sp = sub.add_parser("export", help="导出版本为真实 CBOR")
    add_db(sp); sp.add_argument("vid", type=int); sp.add_argument("out")

    sp = sub.add_parser("drop-version", help="退役一个版本根")
    add_db(sp); sp.add_argument("vid", type=int)

    sp = sub.add_parser("reopen", help="从旧版本拉出分支新版本")
    add_db(sp); sp.add_argument("vid", type=int)

    sp = sub.add_parser("batch",
                        help="按 patches.json 混合格式顺序执行多步操作")
    add_db(sp); sp.add_argument("ops_json")

    sp = sub.add_parser("gc", help="回收不可达内容")
    add_db(sp)

    sp = sub.add_parser("stats", help="持久记录计数")
    add_db(sp)

    args = p.parse_args(argv)
    store = Store(args.db)
    try:
        if args.cmd == "import":
            data = open(args.file, "rb").read()
            vid = store.import_cbor(data, label=f"import:{args.file}")
            print(json.dumps({"imported": args.file, "version": vid,
                              "counts": store.storage_counts()},
                             ensure_ascii=False))
        elif args.cmd == "versions":
            print(json.dumps(store.list_versions(args.all), ensure_ascii=False))
        elif args.cmd == "query":
            if args.raw_cbor:
                with open(args.raw_cbor, "wb") as f:
                    f.write(store.export_cbor(args.vid))
            root = store.materialize(args.vid)
            if args.handle:
                from .store import parse_handle
                lid, rev = parse_handle(args.handle)
                kind = store._content_kind(lid, rev)  # noqa: SLF001
                edges = store._edges(lid, rev)        # noqa: SLF001
                out = {"handle": args.handle, "kind": kind,
                       "edges": [dict(e) for e in edges]}
            else:
                out = {"version": args.vid,
                       "root_handle": (store.root_handle(args.vid)
                                       if isinstance(root, (NodeArray, TypedMap))
                                       else None),
                       "graph": _node_to_jsonable(root)}
            print(json.dumps(out, ensure_ascii=False, default=str, indent=2))
        elif args.cmd == "patch":
            ops = json.loads(open(args.ops_json, encoding="utf-8").read())
            vid = store.patch(args.base, ops)
            print(json.dumps({"new_version": vid,
                              "content_rows_added":
                                  store.content_rows_for_version(vid),
                              "counts": store.storage_counts()},
                             ensure_ascii=False))
        elif args.cmd == "export":
            with open(args.out, "wb") as f:
                f.write(store.export_cbor(args.vid))
            print(json.dumps({"exported": args.out, "version": args.vid}))
        elif args.cmd == "drop-version":
            store.drop_version(args.vid)
            print(json.dumps({"dropped": args.vid,
                              "active": store.list_versions()}))
        elif args.cmd == "reopen":
            new_vid = store.reopen_version(args.vid)
            print(json.dumps({"reopened_from": args.vid,
                              "new_version": new_vid}))
        elif args.cmd == "batch":
            print(json.dumps(_run_batch(store, args.ops_json),
                             ensure_ascii=False))
        elif args.cmd == "gc":
            print(json.dumps(store.gc(), ensure_ascii=False))
        elif args.cmd == "stats":
            print(json.dumps(store.storage_counts(), ensure_ascii=False))
    except StoreError as e:
        print(f"错误: {e}", file=sys.stderr)
        return 2
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
