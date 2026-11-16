# 设计说明：SDMX DSD 挂接迁移统计仓

## 1. 目标与边界

离线 SDMX 2.1 子集：固定三维 `COUNTRY / MEASURE / TIME`（TIME 为时间维）、有限本地
codelist、单个 GenericData 数据集。规模上限 100 000 观测、4 个 DSD 版本、50 个修订。
维度身份在版本间不变；不做代码语义映射、单位换算、统计预测或网络抓取。DSD 允许属性在
dataset / group（任意非时间维子集）/ series（全部非时间维）/ dimset（非时间维真子集）/
observation 之间迁移，允许新增必需属性、删除可选属性。

## 2. 核心设计选择：原始声明边 + 规范有效属性事实表（混合架构）

题目明确要求在「规范有效属性事实表」与「原层级声明存储」之间作选择。单一方案都不成立，
因此仓内同时维护两类表，职责严格分开：

### 2.1 原始层（append-only，永不覆盖）

* `obs_decl`：每个观测值的原始声明，含原文 `raw_value`、`explicit_empty`、来源
  （`origin` = xml/revision/migration）、操作类型（upsert/delete）。修订以新行追加，
  删除是逻辑墓碑而不是物理删除，因此任何历史修订都能回查。
* `decl_edge`：每条属性声明是一条「边」——`(attr_id, layer, group_type, scope_key,
  raw_value, explicit_empty, valid, invalid_reason)`，带 `revision_seq`、
  `revoked_revision`、`supersedes_edge`、`derived_from_edge`。同一作用域键的重复声明、
  错层声明、codelist 违规都原样保留，只标记 `valid=0` 与原因，迁移器不删除、不静默修复。
* `migration_provenance`：迁移产生的每条派生边到其来源边的多对多边，粒度到
  `(plan, derived_edge, source_edge, obs_key)`。

**取舍——来源规模**：原始层只存声明边而不是每版全量 XML。一条 series 级声明只占一行，
无论它覆盖多少观测；10 万观测的库中原始边规模约为观测数的小常数倍（见
`logs/scale_log.json` 实测），而全量 XML 复制会随版本数线性膨胀且无法用 SQL 关联。
代价是原始 XML 的元素顺序、注释等与语义无关的文本信息不保留——本子集的查询语义只依赖
键、值、挂接层和显式空标记，这些都被完整保留。

### 2.2 规范层（按修订物化的快照）

* `obs_key_dim`：观测键字典表，完整 JSON 键与展开维度只存一次，分配整数
  `obs_key_id`。
* `obs_fact`：每个修订每个观测一行（以整数键存储）——规范化值状态
  （`finite/missing/empty`）、规范十进制文本、隔离标志与原因集合。
* `attr_fact`：每个修订每个观测 × 当前 DSD 每个属性一行（以整数键存储）——存在性、
  原文值、显式空标记、裁决层、有效性、**来源边 id 与来源修订**。
* `v_obs_fact / v_attr_fact` 是事实表回连 `obs_key_dim` 的视图，对外暴露可读 JSON 键与
  维度列；`current_obs_fact / current_attr_fact` 再在其上限定最大修订号。因此 `current`
  只是历史事实的一个投影，从不覆盖历史原文。

**取舍——查询与粗化验证**：有效属性的裁决（多层声明覆盖、同键冲突、缺失必需属性）在发布
事务里计算一次并物化，所有读路径（`query-version`、兼容视图、审计）都是普通 SQL 投影，
不需要在读取时重放 XML，也不需要把解析逻辑复制到 SQL 里。同一发布事务内兼容评估复用已
裁决的事实（带修订号+DSD 版本号的缓存，下一次写事务开始即失效），避免重复解析。代价是
每个修订产生 `O(观测数 × 属性数)` 的事实行；整数代理键把长 JSON 键的重复存储压到字典表
一次，在 10 万 × 4 属性上限内这是可接受的固定成本，实测见 `logs/scale_log.json`，事实表
为 `WITHOUT ROWID` 聚簇表、按修订号索引。快照总量随修订数线性增长，这是「每版事实可回查
且不复制 XML」约束下的预期成本，审计如实报告实际字节而不设节省比例。

### 2.3 为什么不是二选一

* 只存声明边：粗化校验需要「同一新 scope 内所有旧有效值是否相同」，纯边表上要对每个
  属性做多层覆盖连接再 GROUP BY，且无法直接表达「显式缺失」与「显式空文本」的区别；
  兼容视图也无从投影。
* 只存事实表：会丢掉声明的原始挂接层与同键冲突的两条原文，无法回答「这个 obs 值来自
  哪条 series 声明」，也无法在不复制 XML 的情况下审计修订前的原文；冲突被裁决后即不可
  复查，违背「不能最后值覆盖」。

## 3. 值与存在性的三态区分

`normalize_value`（`warehouse.py`）产出互斥三态：

| 输入 | state | 规范值 | 说明 |
| --- | --- | --- | --- |
| `"0"`、`"1.2300"`、`"5.000"` | `finite` | `0`、`1.23`、`5` | 有限十进制，去尾零，原文另存 |
| `NaN`/`Inf`/无 ObsValue | `missing` | NULL | 数学缺失，与 0 严格区分 |
| `""`（显式空文本） | `empty` | NULL | 声明存在但文本为空 |

属性同理区分三种情形：**absent**（无声明，`present=0`）、**显式空文本**
（`present=1, explicit_empty=1, raw_value=''`）、**有值**。必需属性收到显式空文本按
`empty_mandatory` 隔离对应观测，而不是当作满足或当作缺失后填默认值。

## 4. 有效属性裁决与隔离

`_effective_facts` 对每个 `(观测, 属性)` 收集所有覆盖它的活动声明边，一次裁决：

1. 多条**有效**边且值（含显式空标记）互不相同 → `scope_conflict`，隔离真实受影响观测，
   事实行保留其中一条边作为来源并带冲突原因；不做最后值覆盖语义上的「胜出」。
2. 恰有一个不同有效值 → 正常生效，记录来源边。
3. 只有无效边（错层、codelist 违规、空必需值）→ 值仍带出来但 `valid=0` 并附原因，
   无效边不会遮蔽同观测上的有效边。
4. 无任何边且 DSD 标记 Mandatory → 追加 `missing_mandatory` 隔离原因；Conditional 则
   合法 absent。
5. 维度代码违规按观测隔离；dataset 级无效声明影响全部观测；group 级只影响匹配 group 键
   的观测；未知 group 类型或 group 键无法确定覆盖时，整个数据集在导入期拒绝。

## 5. 迁移计划的语义

计划绑定 `(from_dsd_version, to_dsd_version, bound_revision_seq, bound_content_hash)`，
以**每个旧观测的有效属性事实**为基准（不是以声明边文本）：

* **细化**（新 scope ⊃ 旧 scope，含 series→obs、group→obs、dataset→任意层）：对每个旧
  有效事实展开一条新层派生边，`obs_keys` 与 `source_edge_ids` 全量记录，原始声明来源
  保留。不产生冲突。
* **同层迁移**：边身份按新 scope 键重建，来源保留。
* **粗化**（新 scope ⊂ 旧 scope，含 obs→series、obs→group、series→group）：按新 scope
  键对观测分组，仅当组内「所有旧有效值完全相同」**且**组内不存在 required 缺失时才生成
  一条粗化边；值分歧产出 `coarsening_conflict`，逐条列出受影响观测及其来源边 id；必需
  缺失产出 `required_missing_in_coarsened_scope`。计划非无损时 `apply-migration` 直接
  抛 `MigrationConflict`，仓状态不变。
* **不可比 scope**（如 group(COUNTRY) → group(MEASURE)，互不为子集）：每个相关观测列入
  `incomparable_scope`，迁移器不猜测归属。
* **新增必需属性而无任何来源**：对应观测进入 `isolated_observations`
  （`new_mandatory_without_source` / `newly_mandatory_missing`），绝不构造假默认。
* **删除属性**：Mandatory 属性禁止删除（计划期结构拒绝）；Conditional 属性消失后，新版
  事实表不再包含该属性列行，旧版事实表保持可查。
* codelist 收紧导致旧值在新 DSD 下非法时隔离对应观测而不是改写值。

冲突的唯一消除途径是输入显式修订（可带旧值前置条件 `preconditions`），修订提交后重新
计划；迁移器自身不改单位、不换算、不填默认。

## 6. 计划绑定、陈旧性与原子发布

* 计划只在 `planned` 状态可应用。应用时复核当前修订号与内容哈希；期间任何新观测/修订
  都会使哈希不符，`apply-migration` 抛 `PlanStale` 并拒绝重算，计划置 `invalidated`，
  必须重新 `plan-migration`。
* 每个发布（导入、修订、迁移）在单事务内完成：写 DSD 版本/修订行、派生边、provenance、
  物化 `obs_fact/attr_fact`、重建 current 视图、写兼容评估，要么全部可见要么全部回滚。
  WAL + `synchronous=FULL` 下，另一个读连接在事务中途只能看到上一个完整修订。
* 修订身份由「修订序号 + 完整观测键」决定：同序号同内容请求幂等重放；同序号异内容抛
  `RevisionConflict`；序号不连续拒绝。删除/重插以墓碑 + 新声明实现，历史行不被覆盖。

## 7. 旧 DSD 兼容导出的正确性取舍

迁移后每个迁移修订都对旧 DSD 做一次逐属性可表达性评估（`compat_assessment`）：

* 新挂接**粗于或等于**旧挂接：粗值向细观测广播天然可表达。
* 新挂接**细于**旧挂接（典型：UNIT 从 series 改到 obs 后，某次 obs 修订使同一 series
  内出现 count/USD 分歧）：按旧 scope 键分组，任一组内存在性或值不一致即
  `divergence_within_old_scope`，导出返回 `expressible=false`，证据列出每个分歧观测、
  当前值与来源边 id；**不静默回退到旧数据**。
* 全部可表达时创建真实 SQL 视图 `compat_r<n>_dsd<m>`（事实表投影，不是 XML 复制），
  `export-compat` 直接读视图。

取舍：兼容视图只在发布时评估一次并登记，读取零计算成本；代价是每次修订都要重评估，
该成本与事实表物化为同一量级。

## 8. 审计

`audit` 输出实测而非估算：各表实际行数、每修订物化行数与隔离数、活动/总声明边数、
provenance 边数（按计划分组）、数据库与 WAL 文件字节、来源 XML 文件字节合计。不设任何
「节省比例」指标。审计报告自身也落表（`audit_run`）可追溯。

## 9. 验证方式

* `oracle.py` 是不依赖仓库裁决代码的独立实现，直接读 `obs_decl/decl_edge` 与 DSD
  描述符，按完整键独立重算每版每观测的有效属性。
* `validate.py` 在真实 XML 夹具上覆盖：series→obs→group 细化/粗化、count/USD 原冲突、
  required 缺失、显式空 NOTE、0/missing、迁移期间修订、obs 修订导致旧版不可表达、
  同 revision 幂等与冲突、迁移中断回滚、不可比 scope、结构拒绝，并逐观测比较值、存在性、
  隔离原因与来源边。
* `scale_test.py` 在 100 000 观测真实 XML 上跑导入、迁移与 50 个修订并抽样 oracle 比对，
  实测耗时与字节写入 `logs/scale_log.json`。
* `queries.sql` / `make_readback_log.py` 用真实 SQLite 语句读回修订谱系、逐版 UNIT、粗化
  分歧证据、隔离观测、迁移 provenance、声明历史与兼容评估，输出 `logs/query_readback.log`。

### 9.1 100 000 观测实测（logs/scale_log.json）

导入约 7 s、迁移计划约 3 s、迁移应用约 10 s、50 个修订合计约 6–7 分钟（每修订全量重算
10 万 × 4 事实并原子发布，这是可回查快照的固定成本）；最终库约 1.6 GB，其中
`obs_fact` 520 万行、`attr_fact` 2 080 万行（52 个修订 × 10 万 × 4）、原始声明边约
31 万条、provenance 20 万条，oracle 抽样零差异。声明边相对事实表小一个数量级，印证
2.3 节「不复制 XML、以声明边 + 事实快照」的规模取舍。

## 10. 命令面

`cli.py` 暴露 `init / plan-migration / apply-migration / ingest-revision /
query-version / export-compat / audit`；冲突类错误（迁移冲突、计划陈旧、修订冲突、
前置条件失败、结构拒绝）以非零退出码和结构化 JSON 返回，不产生半成品修订。

## 11. 已知范围外事项

Header/agency/concept 元数据、多数据集消息、跨版本维度重构、单位/代码语义映射、远程
registry 均不在本子集内；维度身份变更在计划期直接结构拒绝。
