# 状态设计与重建策略比较

## 1. 状态模型（每个已提交 sequence 一份不可变快照）

```
Snapshot(sequence)
├── graph      SourceGraph   节点/way/关系，含 version 与删除墓碑；node 身份仅按 id
├── cache      rid -> BuiltRelation{geometry, rings(provenance), relation_version, built_at}
├── quarantine rid -> {reason, detail, since}     无效关系，已从空间索引撤下
└── index      SpatialIndex  STRtree，只覆盖 cache 中有效关系的真实面
```

- **发布**：`apply_diff` 在旧快照之外完整构建新快照，持久化成功后一次指针交换
  （`self._snap = snap2`）。读连接 `connect()` 固定引用某份快照，因此应用中间
  两个读连接只能读到旧整体或新整体，绝不会读到混合态。
- **回滚**：源图校验、闭包计算、几何重建、索引构建、持久化全部发生在交换之前；
  任一步失败（含注入的索引构建失败）旧快照原封不动，sequence 不前进。
- **持久化**（`out/*/catalog/`）：`journal.jsonl` 逐条追加（含 diff 原文、changed、
  实际 rebuilt relation_id、隔离/恢复/移除集合）；`versions/state-NNNNNN.json`
  每提交版本一份完整状态；`state.json` 指向最新。`Catalog.load` 可整体重读，
  日志可重放到任意前缀，重放结果与在线状态逐字节一致（由 run_scenarios 验证）。

## 2. 反向依赖索引与几何缓存组织

- **反向索引**：`node→ways`、`way→relations` 两张派生映射，仅含存活对象。
  每次 diff 对变更前/后的图各建一份（O(way+relation)，纯元数据，非几何运算），
  闭包 = 变更关系 ∪ 变更 way 的关系 ∪ 变更 node 的 way 的关系（前后两代索引取并）。
  删除的关系即刻离开索引——之后修改它的旧成员 way 时闭包为空（d05/d06 验证），
  旧依赖不会再触发它。way 改成员后，新旧两代索引的并集保证依赖转移不丢传播
  （d07–d09 验证）。
- **几何缓存**：按 rid 存 BuiltRelation 与环来源（每环 way/node 版本与方向），
  `built_at` 记录构建时的 sequence。只有闭包内的关系允许重做几何；闭包外条目
  的 `built_at` 必须不变（oracle 每版本校验，防止"全库重算伪称增量"）。
  隔离关系保留源定义但撤出索引；其依赖被修复后它自然进入闭包并恢复
  （d04 节点修复、d11 way 恢复、d13 按身份闭合缺口）。

## 3. 重建策略比较（失效隔离与查询可见性）

| | 全量重建 | 受影响重建（采用） | 延迟计算 |
|---|---|---|---|
| 每 diff 几何代价 | O(全部关系) | O(反向闭包) | O(1) 标记，查询时 O(脏集) |
| 失效隔离 | 好（全部重判） | 好（闭包精确覆盖依赖） | 差：隔离判定推迟到被查询，未触达的坏关系仍"看似有效" |
| 查询可见性 | 提交即一致 | 提交即一致；无关关系缓存与索引项不动 | 查询路径被迫写入/构建，破坏读快照语义，延迟不可预测 |
| 主要风险 | 掩盖依赖索引错误；违反"只做闭包"要求 | 依赖索引必须正确（由 oracle 独立闭包比对兜底） | 陈旧命中或查询时阻塞，违反"不能沿用旧有效 geometry" |

选择受影响重建：契约明确要求普通 diff 只对反向闭包重做几何并记录实际
relation_id；import 是唯一诚实的全量重建点。

## 4. 拼环与孔洞条件（legacy 规则）

- 同角色成员 way 按 **node id 身份** 贪心+回溯拼成闭环，way 可反向使用并记录方向；
  每个 way 恰好用一次。坐标相同的不同 node **不焊接**——legacy 501 与主场景 1004
  因此判定为 open-ring 并隔离。
- 环至少 4 个节点；inner 必须严格落在某一 outer 内（嵌套时取面积最小者）；
  最终 (multi)polygon 须经 Shapely `is_valid` 校验（自交、孔出界、孔相交均无效）。
- 多 outer 合法，结果为各 outer 多边形之并（legacy 500：两个 outer + 一个 inner）。

## 5. 版本与幂等规则

- diff 必须 `sequence == 当前+1` 且 `base == 当前`；已提交 sequence 的重放与
  日志中 canonical 字节完全一致 → 幂等 no-op，否则整批拒绝。
- 对象级：`new_version < 当前` 版本倒退拒绝；`new_version == 当前` 且内容字节相同
  → no-op，内容不同 → 整批拒绝；`new_version > 当前` 要求 `old_version == 当前`。
- 关系成员仅 way、角色仅 outer/inner、同关系内 way 不重复——元数据校验，违例整批拒绝。
- 删除保留墓碑（版本继续参与校验），恢复 = 更高版本的 undelete。

## 6. 独立 oracle 的验证边界

- **独立**：从持久化的 base XML + journal 文件重建每个已提交版本的图；自写拼环器
  （邻接走查，与 geometry.py 不同实现）；Shapely 校验有效性；全量重建全部关系；
  独立计算期望闭包；全表扫描计算期望搜索命中。
- **共享**：diff 解析/版本规则（`osm_source`）与 Shapely 库本身——属同一规格的唯一
  实现点，且由拒绝类测试直接钉死行为。
- 每个已提交版本比对：几何（normalize 后 WKB/WKB-hex）、隔离集合、搜索命中 ID、
  实际重建集合、闭包外条目 `built_at` 不变。

## 7. 非目标

地图渲染、道路导航、几何自动修复、跨日期线、嵌套 relation、分布式流处理。
