# 稀疏镜像差量应用与跨代范围校验

这是可运行但存在已说明缺口的工程起点。主需求以 contract.md 为准；legacy 是旧格式示例，其旧范围不能覆盖本次新要求。

使用 Python 3.11：`python -m pip install -r requirements.txt`，然后在当前仓库根目录执行 `PYTHONDONTWRITEBYTECODE=1 python baseline_demo.py`。它会真实运行现有模块、显示一个已知缺陷；成功退出表示复现正确，不表示新目标已完成。

所有输入路径相对当前仓库根目录。候选需实现 contract.md 全部范围并自行补验证，不能只修改演示打印。

## 实现（sparse_archive/）

完整目标已实现于 `sparse_archive/` 包（设计取舍见 `DESIGN.md`）：

- `sparse_archive/archive.py` — `Archive`：`ingest_base` / `apply_delta` /
  `open_reader().read_range()` / `compact` / `verify` / `restore` /
  `storage_stats` / `status`；reader pin、单写者锁、原子发布与崩溃恢复。
- `sparse_archive/model.py` — delta 声明、校验块、后写胜出归一化、幂等 id。
- `sparse_archive/oracle.py` — 独立 bytearray oracle（测试对照）。
- `sparse_archive/cli.py` — 命令行：`python3 -m sparse_archive.cli
  {ingest-base,apply-delta,read-range,compact,verify,stats,status,restore}`。

运行验证与演示（均在仓库根目录，只依赖标准库）：

```
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v
PYTHONDONTWRITEBYTECODE=1 python3 demo_full.py        # 归档留在 demo_out/ 可重读
```

`demo_full.py` 用 `inputs/` fixtures 复现基线缺陷后跑完整协议（含显式
拒绝结果与存储统计），输出 JSON 行日志。
