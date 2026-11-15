# OSM增量依赖传播与几何隔离查询

可运行的持久增量区域数据目录：`import`（OSM XML）→ `apply-diff`（有序、原子、
带版本的 diff）→ `search`（真实面 ∩ 查询矩形，带版本）→ `explain`（每环
node/way 版本与方向，可由当前源重建）。失效关系进入隔离并从空间索引撤下，
修复后自动恢复；普通 diff 只对反向依赖闭包重做几何，实际重建的 relation_id
逐条记入日志。

## 运行

Python 3.11：

```
python3 -m pip install -r requirements.txt
PYTHONDONTWRITEBYTECODE=1 python3 baseline_demo.py     # 旧缺陷复现(冻结副本) + 新引擎同场景已修复
PYTHONDONTWRITEBYTECODE=1 python3 run_scenarios.py     # 全场景 + 独立oracle全量重建比对
```

`run_scenarios.py` 真实生成 XML/diff 到 `out/` 并运行：共享 node 移动使一个关系
失效另一个有效、删除 way 使多关系隔离、修复节点恢复、删除关系后再改旧成员、
改 way 成员导致依赖转移、错误版本重放/幂等重放、索引更新失败回滚。每个已提交
版本都与独立 oracle（全量重建 + Shapely 校验）比对几何、隔离集、命中 ID 和实际
重建集合；任何不一致即非零退出。联动日志见 `out/*/apply.log`，机器可读比对见
`out/oracle_report.json`，每版本完整状态见 `out/*/catalog/versions/`。

## 模块

- `osm_source.py` — OSM XML 导入（版本缺省 1）、diff 规范化/原子应用、版本与幂等规则
- `geometry.py` — legacy 拼环（node 身份不焊接、方向记录）、孔洞条件、有效性
- `catalog.py` — MVCC 快照、反向依赖闭包、几何缓存、隔离、STRtree 空间索引、持久化
- `oracle.py` — 独立全量重建 oracle（自写拼环器 + Shapely 校验）
- `run_scenarios.py` — 场景生成与端到端验证
- `baseline_frozen.py` — 原始起点模块的逐字冻结副本（仅供复现旧缺陷）
- `DESIGN.md` — 状态设计与全量/受影响/延迟重建策略比较

`legacy/areas.osm`（多 outer/inner、反向分段 way、同坐标不同 node 的缺口）与
`inputs/` 给定夹具在同一工程中支持；原始缺陷复现保留在 `baseline_demo.py`。
