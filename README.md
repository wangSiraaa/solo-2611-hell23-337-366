# 共享循环对象的可修订版本数据仓

把真实 CBOR（标准 tag28/29 共享值、类型化映射键、共享别名与环）导入为
可持久化、可修订、可回收的节点表。支持按“基版本 + 节点稳定句柄”做
原子 patch，旧版本冻结、共享节点一处改处处见，并可导出为真实 CBOR。

## 依赖

- Python 3.11+，持久化用标准库 sqlite3（无额外依赖即可运行核心）。
- cbor2 仅用于一个对照测试，证明成熟库入口会先合并 `int(1)/bool(true)`
  键而丢信息（见 `tests/test_store.py::CodecTests`）。

## 命令行

```bash
python -m repostore.cli import   DB.sqlite FILE.cbor
python -m repostore.cli versions DB.sqlite [--all]
python -m repostore.cli query    DB.sqlite VID [--handle L1:R1] [--raw-cbor out.cbor]
python -m repostore.cli patch    DB.sqlite BASE_VID ops.json
python -m repostore.cli export   DB.sqlite VID out.cbor
python -m repostore.cli drop-version DB.sqlite VID
python -m repostore.cli reopen   DB.sqlite VID      # 从旧版本拉分支
python -m repostore.cli batch    DB.sqlite patches.json
python -m repostore.cli gc       DB.sqlite
python -m repostore.cli stats    DB.sqlite
```

patch 操作示例（一批原子提交，最多 20 个节点修改）：

```json
[
  {"op": "array_set", "handle": "L2:R1", "index": 0, "value": {"scalar": 99}},
  {"op": "array_insert", "handle": "L2:R1", "index": 1, "value": {"ref": "L3:R1"}},
  {"op": "array_delete", "handle": "L2:R1", "index": 2},
  {"op": "map_set", "handle": "L1:R1", "key": {"type": "int", "value": 1}, "value": {"scalar": "x"}},
  {"op": "map_delete", "handle": "L1:R1", "key": {"type": "bool", "value": true}},
  {"op": "set_root", "value": {"scalar": null}}
]
```

句柄 `L<lid>:R<rev>` 指向不可变内容修订；基于某版本 patch 前先用
`query` 获取该版本的当前句柄。

## 运行测试与实验

```bash
python -m unittest discover -s tests -v     # 30 个不变量/随机变体测试
python experiments/storage_tradeoff.py      # 覆盖层 vs 整树复制取舍
python demo.py                              # 端到端演示
```

## 文档

设计取舍、持久结构、回收与崩溃恢复见 [DESIGN.md](DESIGN.md)。
