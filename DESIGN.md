# 设计说明：可审阅且执行结果与批准一致的外键删除计划

本文件与 `contract.md` 配套，说明 `retention.py`（plan/apply/锁内 verify）、
`verifier.py`（独立复制库 oracle + 联合判断）、`auditor.py`（审计/读回）、
`schedules.py`（真实两连接日程）的设计取舍。所有路径相对仓库根目录。

## 1. 模块与边界

| 模块 | 职责 | 是否写业务库 |
| --- | --- | --- |
| `database.py` | 外键常启连接、12 表业务库 schema 与种子 | 仅建库/种子 |
| `retention.py` | 目录校验、静态语义推演、指纹、plan、锁内重验与单事务 apply | apply 是唯一业务写路径 |
| `verifier.py` | 复制库 FK 引擎 oracle、保留规则×批准写集×引擎三方联合判断、提交后读回 | 只写复制库 |
| `auditor.py` | JSONL 审计轨迹（事务边界/受影响行/拒绝原因/统计）、逐表读回 | 只写审计文件 |
| `schedules.py` | 另一连接在三控制点尝试写入、占锁线程 | rival 连接按日程写 |

外键在每条连接上都以 `PRAGMA foreign_keys=ON` 打开；代码中不存在任何
`PRAGMA foreign_keys=OFF` 或等价绕过路径。真实外键动作只由 SQLite 引擎在
apply 的单事务内触发，Python 侧不手工逐行删除来“模拟”级联。

## 2. 支持的目录形状（硬校验，不满足即 RetentionError）

- 每表**单列 `INTEGER PRIMARY KEY`**；外键单列且指向父表 PK；最多 12 表 /
  30000 行（`retention_config.json` 可调）。
- FK 动作仅 `CASCADE` / `SET NULL` / `NO ACTION`。`NO ACTION` 必须是
  `DEFERRABLE INITIALLY DEFERRED`（从 CREATE TABLE 文本解析，立即型
  RESTRICT 不在范围）。CASCADE/SET NULL 的 deferrability 子句被 SQLite 忽略，
  仅 NO ACTION 强制该约束。
- 拒绝视图与**任意触发器**（`sqlite_master` 中出现非 table/index 对象即拒）。
- 配置把表标记为 `root`（恰好一张）、`retained` 或 `ordinary`。
- FK 可成环（自引用、两表互指、多表环），环表种子先建行后互指，延期约束在
  提交点整体校验。

## 3. 静态推演顺序（analyze，只读，绝不“先删一遍”）

1. **候选根**：显式根（必须落在 root 表且存在，否则 `MISSING_ROOT`/
   `INVALID_ROOT` 阻断项）∪ cutoff 谓词（root 表谓词列 `<= cutoff`）。
2. **CASCADE 固定点优先**：从候选根沿 `ON DELETE CASCADE` 边 BFS 到不再扩大。
   每条边预建 `{fk值: [子PK]}` 索引，固定点为线性；环靠“已在删除集则不重复
   入队”天然收敛。逻辑 NULL 不入索引，因此 NULL 永不被当作父引用。
3. **幸存子行的 SET NULL / 延期 NO ACTION**：只看“指向被删父行且自身不在
   删除集”的子行。
   - 多路径优先级：一行同时被 CASCADE 与 SET NULL 路径触及时，**删除优先**，
     不再产生置空项（reasons 中保留两条路径的解释）。
   - SET NULL 落到 NOT NULL 列 → `SET_NULL_NOT_NULL` 阻断，计划里解释列与父键。
   - 延期 NO ACTION：同批已删除的子行不算冲突；幸存行仍引用被删父行 →
     `SURVIVING_NO_ACTION` 阻断（这正是延期同批解除的语义）。
4. **有效 hold 裁决**：对每张 retained 表按本次逻辑时钟判断
   `expires_at IS NULL`（永久）或 `expires_at > clock`（有效）；
   `expires_at == clock` 即已过期。有效 hold：
   - hold 行自身在删除闭包 → `HOLD_ROW_DELETED`；
   - 其受保护父行（非 NULL）在删除闭包 → `HOLD_PROTECTED_DELETED`。
   任一成立整份计划 `BLOCKED`。把 hold 行本身放进根/闭包不能解锁——它在第 4
   步仍被独立判定。

输出 `READY`/`BLOCKED`，并给出逐表删除 PK、置空（PK,列,原因）、阻断项与每个
键的多原因 `reasons`。阻断态计划仍返回完整预测写集供审阅，但 apply 会以
`REJECTED/blocked_plan_not_applicable` 拒绝执行。

## 4. 计划格式（format_version=1）

`retention.plan()` 返回、`save_plan/load_plan` 落 JSON：

```json
{
  "format_version": 1,
  "plan_id": "<sha256: 对除 plan_id 外整体 canonical JSON 的哈希>",
  "database_path": "…",
  "embedded_config": { … 完整角色/谓词/retained 配置，计划自带语义 … },
  "catalog_fingerprint": "<sha256: 规范化后的列/PK/FK 动作/角色/谓词/retained>",
  "data_fingerprint": "<sha256: 每表结构作用域内行指纹的再哈希>",
  "data_fingerprint_tables": { "表": {"sha256": "…", "row_count": N} },
  "request": {"explicit_roots": [...], "cutoff": 150, "plan_clock": 1000},
  "verdict": {
    "status": "READY | BLOCKED",
    "candidate_roots": [{"table": "account", "pk": 3}],
    "delete_rows": {"account": [3], "event": [30]},
    "null_updates": {"subscription": [{"pk": 1, "column": "account_id",
                                       "reasons": ["set_null:…"]}]},
    "blockers": [{"kind": "HOLD_PROTECTED_DELETED", "...": "…"}],
    "reasons": {"account|3": ["root:explicit"], "…": "…"},
    "clock": 1000, "cutoff": 150
  },
  "stats": {"rows_read": 50}
}
```

- **catalog 指纹绑定完整语义，不绑定文件 mtime**：规范化列定义、单列 PK、每条
  FK 的父子列与 `on_delete/on_update/deferred`、角色、root 谓词、retained 定义。
  新增索引只出现在 `sqlite_master` 但不进入该结构，因此**不会**使计划失效。
- **data 指纹只覆盖闭包/谓词作用域**，避免“无关行一改就失效”：
  - root 表与 retained 表整表（根数据与全部 hold 都参与裁决）；
  - ordinary 表取 批准删除/置空/阻断键 ∪ 当前所有“非空 FK 指向批准删除父行”
    的行。后者是用 FK 结构谓词在 plan 与 apply 两侧分别重算的，能捕获幻影
    cascade/幻影置空子行；而 NULL FK、指向未删父行的无关行（如 `audit_log`
    的 NULL 引用、无关账户的 label）永不入指纹。
- `plan_id` 覆盖整个文档（含两个指纹与 verdict）。任何对批准写集的篡改都会让
  apply 端重算的 id 不符 → `REJECTED/plan_id_mismatch`，且原库不动。

## 5. apply 并发协议：有限锁时长内消灭检查后写入窗口

不从人工 plan 一直锁到 apply。采用 **`BEGIN IMMEDIATE` 写锁 + 锁内三层重验 +
单事务执行核对**：

1. `before_lock` 控制点：rival 可在此自由写入（apply 尚未持锁）。
2. apply 连接设 `busy_timeout=0`，显式 `BEGIN IMMEDIATE` 抢保留锁（写锁）。
   抢锁失败即感知 `SQLITE_BUSY`：**只允许一次明确重试**（`lock_retries` 计数，
   睡眠 `busy_retry_delay` 后再试一次），第二次仍失败 → `BUSY` 终止，不做无
   饥饿保证，不无限等待。
3. 持锁后做**三层重验**（任何一层不过即回滚并 `STALE_PLAN`，原库保留）：
   1. `catalog_fingerprint` 比对（目录被改成不满足约束时，`load_catalog` 抛
      `RetentionError` 也归类为 `STALE_PLAN/catalog_changed`）；
   2. 用执行时钟重新 `analyze` 并重算 `data_fingerprint`，比对数据指纹；
   3. 比对重算裁决的 `status/candidate_roots/delete_rows/null_updates/blockers`
      与批准 verdict（canonical 比较）。这一层能抓住**数据字节不变、仅执行时钟
      不同导致 hold 有效性翻转**的情形（见 run_tests 的 clock_flip 用例）。
4. `after_revalidation` 控制点：此刻 apply 已持写锁，rival 的写入会被
   `SQLITE_BUSY` 挡住（日程用例断言 rival `blocked`）。
5. 锁内对每表拍快照，只对**候选根 PK** 发 `DELETE ... IN (?)`（分批 500），
   全部 CASCADE/SET NULL/延期 NO ACTION 由引擎在同一事务触发。
6. 提交前在**同一事务**逐表核对：实际删除集 == 预测删除集；预测置空列确为
   NULL；不得出现任何未预测的行变化/插入；再跑 `PRAGMA foreign_key_check`。
   任一不符 → 回滚，`VERIFY_MISMATCH`，无成功回执。
7. `before_commit` 控制点：rival 仍被锁挡住；随后 `commit()`。提交失败（罕见
   commit 期 BUSY）回滚并 `COMMIT_BUSY`。

**为什么没有检查后写入窗口**：重验、快照、引擎删除、核对、提交全部发生在同一个
`BEGIN IMMEDIATE` 事务里，SQLite 保留锁在该事务期间阻止任何其它连接取得写锁，
因此“重验之后、提交之前”不存在可并发提交写入的间隙。锁只覆盖重验+执行的短窗口
（毫秒到秒级，取决于写集），不覆盖人工审批间隔。

**取舍**：
- *锁竞争*：选 `busy_timeout=0` + 一次显式重试，而不是长忙等，避免 apply 连接
  长时间挂起和饥饿不透明；竞争方持续占锁时我们快速、明确地以 `BUSY` 失败并
  保留原库，调用方可稍后重新 plan/apply。
- *重验范围*：用结构作用域指纹而非整库哈希，保证“新增无关索引、改无关行不失效”
  的同时，任何能改变预测写集或裁决的修改（幻影子行、新 hold、根谓词列变化、
  FK 动作变化、时钟翻转）都被三层中的某一层拦下。
- *批准语义*：计划内嵌完整配置与语义指纹，apply 不依赖外部配置文件是否被改；
  静默扩大删除集在原理上不可能——引擎实际写集在事务内被逐表比对，超出批准集即
  回滚。

## 6. 独立 oracle 与联合判断（verifier.py）

只看 `foreign_key_check` 通过不够（它只证明无孤立引用，证明不了删的是批准过的
行）。因此：

1. 在 plan 后 apply 前复制业务库文件为 pre-image（apply 后被删行已不存在，无法
   复放）。
2. 在**隔离复制库**上用外键常启的单事务执行同样的原始根删除，让 SQLite 引擎
   产出真实 FK 结果 oracle：提交/`integrity_error`、逐表删除 PK、逐列置空、
   `foreign_key_check`。
3. 联合判断：
   - READY 计划：oracle 必须提交，且 oracle 删除集（按全表补空表对齐）与置空
     `(PK,列)` 集必须与批准写集**逐键相等**（reasons 是解释材料，不参与引擎
     一致性比较），oracle 的 `foreign_key_check` 必须干净。
   - BLOCKED 计划：若阻断来自延期 NO ACTION/NOT NULL，oracle 必须在提交点报
     `integrity_error`；若仅来自 hold 策略，引擎本身往往会提交——这恰好演示
     “FK 检查通过但策略不允许”，并断言 oracle 确实会删掉该 hold/受保护行，而
     静态闭包也已把它们标为阻断。
4. apply 已提交时再用**独立新连接**读回：批准删除键不存在、置空列为 NULL、无 FK
   违反；未提交路径则比对当前库与 pre-image 的规范化行指纹，断言原库字节级保留。

## 7. 审计（auditor.py）

`AuditTrail` 追加写 JSONL，**不读墙钟**：事件只有单调 `seq` 与调用方提供的逻辑
`clock`。记录 `trail_open/apply_begin/三控制点 hook/lock_acquired/lock_retry/
revalidation/apply_committed/apply_stale/apply_busy/apply_verify_mismatch/
apply_exec_failed/apply_commit_busy/apply_rejected`。`summarize()` 汇总事务边界、
拒绝原因、实际受影响行（deletes/null_updates/引擎 changes 计数）、读行/写行与
锁重试次数。`remaining()` 逐表读回，`readback_verify()` 做提交后数据核对。

## 8. 验收与复跑

- `PYTHONDONTWRITEBYTECODE=1 python run_tests.py`：58 项检查，覆盖多路径删/置空
  优先级、延期同批解除 NO ACTION、自引用/两表环、hold 过期边界（含 == 与 NULL
  永久）、幻影 cascade/置空、新 hold、无关行与无关索引、FK 目录变更、时钟翻转、
  锁竞争（持续占锁 BUSY、释放后一次重试成功）、计划篡改、NULL 非引用、cutoff。
- `PYTHONDONTWRITEBYTECODE=1 python baseline_demo.py`：复现旧缺口并证明新实现
  拦截为 STALE_PLAN、保留原库。
- 真实两连接日程在 `inputs/schedules.json`，由 `schedules.py` 的 rival 连接在
  before_lock/after_revalidation/before_commit 三点尝试写入；锁竞争用
  `rival_holds_lock` 线程模拟占锁与定时释放。
- 规模：29,901 行浅分支树 plan≈0.12s 并正确提交；超深级联链触及 SQLite 自身
  `SQLITE_MAX_TRIGGER_DEPTH` 时归类为 `EXEC_CONSTRAINT/
  engine_trigger_recursion_limit` 并回滚（直接执行同样语句引擎也会失败）。

## 9. 明确不做

生产部署、用户/权限系统、任意触发器、RESTRICT 立即检查、多列 PK/FK、非整数 PK、
跨库清理、脱敏与最小删除优化均不在范围内。`legacy/` 的旧 TEXT-PK 材料原样保留，
仅作历史参考，不被新模块依赖。
