# 时间规则修订下的事件关联与可撤回物化

工程起点与主需求见 contract.md；规则/索引设计与全量 vs 局部重算比较见
DESIGN.md。legacy 是旧格式示例，其旧范围不能覆盖本次新要求。

## 旧基线（保留的缺口复现）

使用 Python 3.11：`python -m pip install -r requirements.txt`，然后在当前
仓库根目录执行 `PYTHONDONTWRITEBYTECODE=1 python baseline_demo.py`
（无 python 命令时用 python3）。它真实运行旧模块、复现已知缺陷：
`update_rules` 只换 metadata，关联事实仍旧。成功退出表示复现正确。

## 新系统：ingest / install-rules / query / explain

所有路径相对当前仓库根目录，全部整数纳秒、不经浮点。

```sh
python3 catalog.py ingest        --db out/demo_db --events inputs/events.json --config inputs/config.json
python3 catalog.py install-rules --db out/demo_db --rules inputs/r1.json
python3 catalog.py install-rules --db out/demo_db --rules inputs/r2.json
python3 catalog.py query         --db out/demo_db --version latest     # latest|previous|N
python3 catalog.py explain       --db out/demo_db --pair u t           # 或 --event ID
python3 catalog.py verify        --db out/demo_db --version 2          # 全量 oracle 比对
```

- 规则修订联合产生规范事件净变更、配对 insert/retract/update 与窗口
  count delta，落盘于 `out/demo_db/changes/delta_N.json`，可被外部消费者
  重放至与 query 完全相等；同一对撤回后再插入可由 explain 完整解释。
- 规则安装失败（含 `CATALOG_FAULT_AT=pre_association|pre_commit` 注入）
  不暴露一半新规范时间和一半旧配对。
- `python3 demo_upgrade.py` 一键重建上述演示并写出真实变更日志。
- `python3 verify.py` 运行 20 项验证（真实读原始事件、逐版重建独立
  oracle、逐配对与窗口值比较、delta 独立重放）。
