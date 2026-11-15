# 设计决策与正确性论证

本文件说明可持久重开的注释数据目录如何组织状态、如何保证版本原子性与隔离闭包正确性，
以及增量维护与全量重算各自的适用条件。所有结论由 `verify.py`（32,487 项断言，全通过）
与 `demo_session.log` 的真实 SQLite 执行支撑。

## 1. 数据模型：SCD 版本行 + 当前态物理索引

SQLite 单库 `annotation.db`，WAL 日志模式。表分两类：

**版本表（缓慢变化维，slowly-changing-dimension）**，每行带 `version_from / version_to`：
- `entity_version(id, version_from, version_to, status, reason, body,
  content_version, status_version)`
- `segment_version(id, ord, lo, hi, version_from, version_to)` —— 物理段，0 基半开
- `edge_version(id, parent, version_from, version_to)` —— 每条父子边独立成行，
  多父实体不复制段
- `provenance(id, ord, lo, hi, version, source)` —— 只增不删，段来源可重建
- `patch_record(patch_id, base_version, new_version, payload, summary)`

当前版本的行 `version_to IS NULL`。发布新版本 = 关闭旧行（写 `version_to=新版本号`）
并只插入真正变化实体的新行。

**当前态表（无版本列）**：
- `seg_index(id, ord, lo, hi)` 只保存**当前可信实体的实际物理段**，带 `(lo,hi)` 索引。
  这正是修复基线缺陷的关键：`query` 永不读取已删父的残留段，也永不把隔离实体的段
  当作完整数据；命中的是实际段而不是包络（envelope）。

为什么不用"每个版本一套完整新表 + 切换名字"的分阶段方案：该方案每次补丁都要复制全部
实体/段，违背"写审计只能包含实际变化"。SCD 行让未变实体一行都不写，同时仍能按版本
做时间区间查询（`version_from <= v AND (version_to IS NULL OR version_to > v)`）。

## 2. 原子性与读者视图：单写事务 + WAL 一致读

每个补丁在**一个** `BEGIN IMMEDIATE` 事务内完成：闭包计算 → 关闭/插入 SCD 行 →
切换 `seg_index` → 写补丁记录 → 更新 `head_version` → `COMMIT`。任何一步抛错都
`ROLLBACK`，已发布表与索引不会留下半成品（`verify.py` 测试 8 在三个控制点注入异常，
逐字节比对回滚前后全表快照）。

读者在另一连接里 `BEGIN` 持有读事务后，WAL 快照保证它在整个事务期间只看到提交前的
旧完整版本；写者提交后新开的读事务看到新完整版本。读者绝不会看到混合态（测试 9 用
两个真实连接，在写者 `after_closure / after_index_switch / before_commit` 三个钩子里
反复从读者连接查询，确认 head 与命中集合始终冻结在旧版本）。"补丁中途进入的读者只能
看到旧完整版本或新完整版本"由此成立。

## 3. 状态判定：缺父、环、身份不一致与沿边传播

纯函数模块 `closure.py` 不含任何 I/O，可同时服务于写路径与独立 oracle：

- **缺父**：实体的某个 parent id 在全图中不存在（含被本补丁删除）→
  `missing_parent:<p>`。
- **环**：迭代 Tarjan SCC 找出所有环节点 → `cycle`。
- **身份/坐标不一致**：GFF3 解析期产生（`identity_conflict`、
  `outside_sequence_region` 等），以 `extra_reasons` 注入闭包。
- **传播**：上述直接原因沿 parent→child 边 BFS 到不动点，子项得到
  `bad_parent:<p>`。多父实体只要有一个父坏就被隔离，**边一条不删**——修复后无需
  重建关系即可恢复完整父集合（测试 2、6 验证两父同时删除时两条边与两个原因都在）。

环只隔离环本身及依赖它们的子树，其它组件继续可信。

## 4. 增量闭包 vs 全量重算的适用条件

- **全量重算**（`full_closure`，O(V+E)）：import 时使用；并作为 `verify.py` 的
  **独立 oracle**，对每个保留版本重算整张图，再枚举所有物理段对照每个 query。
  这是正确性基准，不在常规写路径上使用。
- **增量闭包**（`incremental_closure`，常规写路径）：先求"受影响锥"
  （`affected_set`：目标 + 在旧图或新图父→子边下可达的全部后代），只对该子图重算。
  正确性依据：未受影响节点到任何变化边都不可达，其状态不可能改变；任何被变化边触及
  的环都整体落在受影响集内，所以子图内 SCC 与全图 SCC 对这些节点等价。锥外父节点的
  当前状态/存在性只按 id 单独取行，不把整图拉进内存。

**写审计的局部性**（契约硬性要求）：统计区分"读取实体数 / 改写段数 / 索引行数"，
触及 ≠ 改写。删除共享父 `t1` 时只读到锥内 4 实体、改写 0 段（纯状态变化不重写物理
段）、索引仅删除 3 行（t1 自身 1 行 + 被连带隔离的 e 的 2 行），未受影响的 `g`
内容版本与状态版本都停在 1，绝不虚增。全表删除重建在写路径中不存在。

## 5. 版本号、保留窗口与过期 base

- 每个实体带两个独立版本：`content_version`（body 变化才前进）与
  `status_version`（status 或 reason 变化才前进）。原因集合变化但状态不变（如隔离中
  又坏一个父）也会产生新的 status 版本行，保证 `inspect` 能解释完整原因演化。
- 只保留最近两个已发布版本；裁剪与发布在同一事务内完成，不存在可观察的中间裁剪态。
  读取更老版本返回 `version_unavailable`。
- 补丁绑定 base，`base != head` 明确拒绝（`stale_base`），因此不可能覆盖新版本；
  失败补丁不推进 head、不写任何已发布表。

## 6. 来源与可解释性

`provenance` 追加保存每个物理段在哪个版本、由哪个来源（import 文件名或
`patch:<id>`）写入。任意当前段都能由 provenance 重建（测试 11）。`patch_record`
保存补丁原始 payload 与变更摘要，`inspect --patch` 可回看原补丁；
`inspect --entity` 对隔离实体同样返回 body、段、全部父边与原因。

## 7. GFF3 旧格式要点（legacy/semantics.md）

- 先按 `;` 拆属性再 unquote，Parent 在 unquote 后按逗号拆，编码逗号是普通文本；
  Parent 去重，ID/Parent 只允许一次。
- 1 基闭区间 → 0 基半开；sequence-region 覆盖校验；strand/phase 约束；`##FASTA`
  后停止解释并记录 `sequence_payload_ignored`；无 ID 行生成 `anon@文件:行号`，
  其它行不允许 Parent 指向匿名身份。
- 同 ID 多行仅在 seqid/type/strand/Parent 集合完全一致时合并，否则该 ID 全部行进
  隔离（`identity_conflict`），不任选一行（`conflicting-parts.gff3` 验证）。
- 前向 Parent 可解析；不做任何生物推断（无蛋白翻译、无几何包含约束）。

## 8. 显式不做的事

匿名跨版本身份、蛋白翻译、分布式服务、通用图数据库均不在范围内。模块边界：
`annotation.py`（解析）/ `closure.py`（纯图算法）/ `store.py`（持久化与事务）/
`cli.py`（命令面）/ `verify.py`（oracle 验证）。

## 9. 容量与控制点

10000 实体 / 30000 段 / 100 补丁 / 每补丁 100 原子操作，超限在写事务内拒绝。
`AnnotationStore.fault`（`before_index_switch / after_index_switch /
before_commit` 抛异常）与 `hook`（不抛异常的观察点）是仅供验证使用的控制点。
