# 冻结匹配的并发数据合并执行器

planner.py在SQLite快照中给source生成update/update+insert，executor.py稍后执行。单次合并可用，但冻结计划后另一个写者改了amount，旧计划直接覆盖新值；observed_rev被记录却没有验证。

运行基础工程：`python demo.py`。依赖见 requirements.txt；无依赖时使用 Python 3.11 标准库。

基础工程（planner.py / executor.py / demo.py）保留为可运行的旧能力基线；升级实现位于 `merge/` 包、`merge_cli.py`、`tests/`、`experiments/`。

## 升级系统

### 结构

- `merge/ast_eval.py` — 受限 MERGE AST v1 的结构校验与求值：字段引用、常量、比较、整数加减、AND/OR/NOT 三值逻辑；深度上限 8；int64 溢出/非整数运算拒绝整计划；文本比较按 SQLite BINARY（UTF-8 字节序与码点序一致），混合类型按 SQLite 类型序（整数 < 文本）。
- `merge/planner.py` — 在一条读事务内冻结：每个 source 按 business_key 等值匹配（NULL 不相等、永不匹配），matched 分支顺序首命中 update/delete，not-matched 首命中 insert，赋值在冻结快照上求值成具体值。计划期间不写任何数据，因此动作不可能导致后续 source 重新匹配。计划内确定性冲突（多 source 命中同 target、多个动作产生相同新唯一键）在此检测，与执行顺序无关；某条分支最终 no-op 不能规避同 target 冲突（判定基于匹配而非动作）。
- `merge/executor.py` — 提交阶段：`BEGIN IMMEDIATE` 短事务内先按键索引探测重验读集，再按 source 顺序应用冻结动作，同事务写入回执；任何验证失败或约束兜底失败整体回滚。
- `merge/writer.py` — 外部写入的 revision 维护入口（短事务、rev+1）。schema 中的 rev 守卫触发器让忘记维护 rev 的直写直接报错；绕过触发器的独立直写不在合法输入内。
- `merge/schema.py` — target/parent 表 + `typeof` CHECK + rev 守卫触发器 + `merge_receipt` 回执表。
- `merge_cli.py` — 命令行入口（见下）。

### 读集表达（含缺失键）

冻结计划携带的读集有两类条目：

- `rows[]`：每个匹配 source 一行 `{key, id, cols}`。`cols` 只包含求值**实际读过**的 target 列（匹配本身读 business_key；分支条件与赋值表达式读到的列由求值器动态记录）。验证时按 business_key 走 UNIQUE 索引取回该行，比对 id 与这些列的冻结值：行消失→`target_deleted`，id 变了→`target_replaced`，列值变了→`target_modified`，均回报冲突身份（key、target_id、列、期望值/实际值）。
- `absent[]`：缺失键断言——未匹配的非 NULL source 键，以及计划将产生（insert/改名）且在快照中不存在的非 NULL 新键。验证时对该键做 `SELECT 1 ... LIMIT 1` 索引探测，出现即 `phantom` 冲突。NULL 键不做缺席断言（NULL 不相等，UNIQUE 也允许多个 NULL）。

每个探测都是 business_key 唯一索引上的点查，验证读取行数 ≤ 读集规模（≤ 2×source 数量级），与 target 表总量（1 万）无关；回执的 `validation.rows_read`/`probes` 输出实际读取行数，测试里另有 `EXPLAIN QUERY PLAN` 断言走索引。

### 验证哪些列版本：方案取舍与实验

- **采用：列级（细粒度）**。只比较计划实际读过的列。无关列（如未引用的 parent_id）被并发改写不会误拒计划——这正是"验证影响匹配/分支/赋值的读集"的语义。
- **放弃：行级 rev（粗粒度）**。只比对触发器维护的行 rev，实现略简，但同一行的任何列变化都会拒绝计划。fixtures 的 disjoint 场景（不同键）两种方案都能过；真正的分歧在"同键无关列"交错：计划只读 amount，外部写者只改 parent_id，行级会误拒一个本可串行化的合法计划。

可复现实验（1 万 target 行、20 source/计划、重复 200 次验证）：

```
$ python3 experiments/granularity_experiment.py
== interleaving outcomes by validation mode ==
scenario                           column-mode  row/rev-mode
A unrelated column, same rows      committed    conflict     <- 行级误拒
B unrelated keys                   committed    committed
C true conflict on amount          conflict     conflict     <- 两者都正确拒绝

== validation cost (rows read / mean latency) ==
sources  column-mode            row/rev-mode
1          1 rows /     1.7 us    1 rows /     1.6 us
5          5 rows /     7.4 us    5 rows /     6.7 us
10        10 rows /    14.6 us   10 rows /    13.0 us
20        20 rows /    27.8 us   20 rows /    25.8 us
```

结论：两种模式读取行数完全相同（都受读集规模约束，远低于表量），列级仅多约 8% 的比较耗时（微秒级），却消除了同键无关列的误拒；故默认 `validation_mode="column"`，`row` 模式保留用于对照实验与测试。

### 约束兜底点

计划期做确定性检查（AST、类型、溢出、计划内冲突）；与并发数据相关的核验全部留给提交事务内的数据库约束兜底，失败即整体回滚、回执 `failed`：

- UNIQUE(business_key)：计划产生的新键与**现存且不被本计划改名让位**的键相撞（如 A→B 而 B 存在）；SQLite 按语句即时检查唯一性。
- FOREIGN KEY(parent_id)：插入/改名的 parent 在提交时不存在。
- NOT NULL / typeof CHECK：amount 为 NULL 或非整数等（计划期已做确定性类型预判，CHECK 作为纵深防御）。
- rev 守卫触发器：insert 必须 rev=1、update 必须 rev+1，否则 RAISE(ABORT)。

### 原子性与幂等

所有动作与 `merge_receipt` 回执在同一事务提交或回滚，约束失败后没有半更新（测试：FK 失败时同计划已验证的 update 也回滚）。`plan_id` 是计划内容（source+分支）规范 JSON 的 SHA-256，提交前重算校验绑定；已提交 plan_id 的重送在计划之前直接命中回执存储，返回字节相同的回执、不重复加金额（提交后丢确认场景）。被拒绝/失败的计划不落回执，可修正后重提——无冲突计划必能提交。

### 事务与锁

计划阶段一条读事务（WAL 快照），计划完成立即释放（测试断言 `in_transaction` 为 False 且另一连接可立刻写入）；等待期不持任何锁。提交阶段 `BEGIN IMMEDIATE` 短写事务，只做读集验证+应用+回执，允许这期间的短写锁。最多 2 并发计划在合法输入包络内，靠 SQLite 写串行化与 busy_timeout 即可。

## 命令行入口

```bash
python merge_cli.py init DB --seed          # 建库（含 fixtures 种子数据）
python merge_cli.py plan DB PLAN.json --out FROZEN.json   # 冻结计划到文件
python merge_cli.py write DB update-amount B 999          # 另一写者（revision 入口）
python merge_cli.py commit DB FROZEN.json   # 短事务验证+应用+回执
python merge_cli.py submit DB PLAN.json     # 计划+提交一步
python merge_cli.py receipt DB PLAN_ID      # 查已存回执
python merge_cli.py rows DB                 # 看 target 内容
python merge_cli.py run-fixture DB          # 跑 fixtures/merge-plan.json
python merge_cli.py scenario DB NAME        # lost-update|phantom|rename|disjoint|fk-fail|lost-confirm
```

回执打印为 JSON；committed 退出码 0，其余 1。注意 fixtures/merge-plan.json 的 `plan_id: "example-P"` 是占位标签，与内容哈希不符会被确定性拒绝（`run-fixture` 会剥掉占位符并注明；权威 plan_id 恒为内容哈希）。

## 测试与真实运行结果

```bash
python3 -m unittest discover -s tests     # 44 个用例
python3 experiments/granularity_experiment.py
python3 demo.py                            # 旧基线仍可用
```

实测（本仓库当前代码，Python 3.11.2 / SQLite 3.40.1）：

- `python3 -m unittest discover -s tests` → `Ran 44 tests ... OK`。覆盖：两连接确定性交错（lost-update、缺失键幻影、冻结匹配后改名、分支三值逻辑、FK 失败原子回滚、提交后丢确认幂等重放）、fixtures/interleavings.json 逐步骤驱动（lost_update/absent_insert/disjoint）、fixtures/merge-plan.json 端到端（A→A2 改名+金额 15、插入 C=7）、计划内冲突（同 target/同新键/no-op 不规避/冻结匹配不被自身 insert 重匹配）、溢出与非整数拒绝、AST 深度、20 source 上限、1 万行下验证 `rows_read=10`（≤ 读集规模）且 `EXPLAIN QUERY PLAN` 为索引 SEARCH、rev 守卫触发器拒绝不维护 rev 的直写。
- `python3 merge_cli.py scenario /tmp/s.db lost-update` → `conflict / target_modified {key A, column amount, expected 10, actual 100}`，A 保持 100。
- `... scenario phantom` → `conflict / phantom {key C}`，库中只有外部插入的 C=9。
- `... scenario rename` → `conflict / target_deleted {key A}`，A2 金额未动。
- `... scenario disjoint` → P、Q 均 committed，A=11、B=22。
- `... scenario fk-fail` → `failed / FOREIGN KEY constraint failed`，A 保持 10（无半更新）。
- `... scenario lost-confirm` → 重送返回同一回执（`identical_receipt: true`），A=15 只加一次。
- 两阶段 CLI 实测：`plan` 冻结到文件 → `write update-amount B 999`（无关键）→ `commit` 成功且 `validation.rows_read=1` → 再次 `commit` 同一文件 → 提示 `identical receipt replayed from the receipt store`，金额不重复加。

## 仍未覆盖的边界

- **改名环/链与现存键的语句序**：SQLite 唯一约束按语句即时检查（无 deferred unique），A→B 且 B→D 这类"最终状态合法但中间态冲突"的计划会在应用第一个 UPDATE 时被 UNIQUE 兜底拒绝（回执 `failed`，整体回滚，无半更新）。计划内动作按 source 顺序应用，不做重排。
- **并发吞吐**：合法输入包络为最多 2 并发计划；未做更多并发者下的压力测试（机制上由 SQLite 写串行化保证正确性）。
- **重试**：冲突/失败后由客户端重计划重提，系统不自动重试。
- **绕过触发器的直写**（如 DROP TRIGGER 后写）不在合法输入内，rev 守卫只能让"忘记维护 rev"的直写报错，不能防御恶意绕过。
- **冻结计划文件**由本系统产生与消费，plan_id 绑定 source 内容哈希；对冻结文件本身的恶意篡改不设防（信任边界在 CLI 文件处）。
- 比较语义实现了 SQLite BINARY 文本序与整数/文本混合类型序；BLOB 不在输入包络（键为可空 text、金额 int64）。
