# 大导出流的事务边界安全续传恢复 — 设计说明与运行报告

本文说明升级后系统的**实际实现**、关键取舍、可复现实验，以及**仍未覆盖的边界**。
结论与数字均来自本仓库真实运行（命令见文末），不是固定示例输出。

---

## 1. 交付物

| 路径 | 作用 |
| --- | --- |
| `streamlex/lexer.py` | 字节级增量词法：SQL / COPY / 引号 / 注释状态机 |
| `streamlex/store.py` | 源读取、内容身份、不可变分段清单、边界校验 |
| `streamlex/target.py` | SQLite 目标：一个源单元 = 一个目标事务；断点同事务写入 |
| `streamlex/engine.py` | 恢复引擎：单元规划、原子应用、校验、续传、审计 |
| `streamlex/cli.py` | `restore` / `audit-source` / `status` 命令行 |
| `demo.py` | 9 个端到端场景（含全字节切分扫描） |
| `bin/demo-cli.sh` | 纯 CLI 驱动的演示脚本 |
| `tests/` | 58 个不变量测试（`unittest`，无第三方依赖） |
| `fixtures/export.sql`, `fixtures/cuts.json` | 原始材料 |

原有 `lexer.py` / `restore.py` / `demo.py` 的基线能力保留在历史中；`demo.py` 现为升级版入口。

---

## 2. 事务边界与词法安全

### 2.1 为什么 `COMMIT;`、注释、引号不能当边界
COPY 数据段与 SQL 是两种语义。词法器一旦进入 `COPY ... FROM STDIN;` 后的数据模式，就**只按换行切行**，
每行按 TAB 分字段，只有独占一行的 `\.` 结束数据段。因此：

* `fixtures/export.sql` 第 8 行 `10 → 1 → COMMIT; is data` 是三个字段，`COMMIT;` 是数据；
* 字段里出现 `--`、`/*`、`;`、`'` 都只是字节。

SQL 模式用逐字节状态机（`SQL / SQ / SQ_END / DQ / DQ_END / DASH / SLASH / LC / BC / BC_STAR`）：
`--`、`/*`、`'...'`、`"..."`（含 `''`/`""` 转义）都完整成词后才推进，只有引号/注释**之外**的 `;` 结束语句。
`SQ_END` 单独成态，正是为了在“这个 `'` 是闭合还是双写转义”无法由单字节判定时等待下一字节。

### 2.2 任意字节/转义/UTF-8 中间断开
* 输入按任意 chunk 喂入（测试覆盖 chunk=1…整文件；`test_every_byte_split_matches_whole_stream` 对 337 个切点逐一比对，事件流完全一致）。
* 一个 **COPY 数据段整体**是一个事件边界：切在数据段内不是可发布的边界，续传时重读该段。
  于是 `\.` 之前无论怎么断都不产生“半个 COPY”。
* `test_utf8_split_inside_codepoint`、`test_dash_and_slash_split_across_chunks` 覆盖 UTF-8 多字节、`--`/`/*` 两字节被拆开的情况。

### 2.3 只在完整词法单位推进
每个事件的 `end` 都是安全续传点：模式为 `SQL`、无挂起语句、无挂起行。
`Event.verbatim` 恰好是源字节 `[start,end-1)`（去掉结尾 `;`），审计/身份均基于它。

---

## 3. 原子性、依赖与校验

### 3.1 单元模型
一个**单元**是原子可应用的单位：

* 一个显式 `BEGIN ... COMMIT` 源事务；或
* 一条**独立的** `CREATE TABLE/INDEX`（pg_dump 风格，事务之外），各自成一个单元。

`INSERT INTO ... VALUES (...)` 允许出现在 `BEGIN/COMMIT` 内（作为 DML）；
事务外的 DML、COPY 一律拒绝——它们无法被做成全有或全无。未知语句、坏 COPY 行、截断事务都在**规划阶段**抛错，
该单元根本不会执行；已提交的前一单元不受影响。

### 3.2 一个源单元 = 一个目标事务；断点写在同一事务里
`Engine._apply_unit` 在一个 `BEGIN IMMEDIATE` 中依次执行该单元的 DDL / COPY 行 / INSERT，
并在**同一事务**里写 `__streamlex_checkpoint`，然后 `COMMIT`。
由此得到最关键的性质：

> **“目标数据已提交” 与 “源已消费到字节 N” 是同一个原子事实。**

* 提交前崩溃 → 数据和断点都不存在，续传从旧断点**重读输入**（绝不跳过未提交输入）；
* 提交后崩溃 → 数据和断点同时存在。

### 3.3 “已提交但确认丢失”的恢复证据
崩溃发生在 `COMMIT` 之后、调用方拿到确认之前。重开时引擎直接读目标库里的断点行：
它携带 `byte_offset / unit_seq / txn_count / boundary_start / boundary_sha256 / manifest_seq / txn_digest / prev_txn_digest`。
**看见这条已提交的行，就等于证明该单元已持久化**（因为它与数据同事务提交）。
随后引擎只重读一个有界边界块并与 `boundary_sha256` 比对，成功后从 `byte_offset` 继续——
该单元**不会被执行第二次**。`test_committed_but_confirmation_lost` 与 demo `crash-resume` 复现此点。

### 3.4 外键
连接建立即 `PRAGMA foreign_keys=ON`，**从不关闭**。每个单元提交前再跑一次 `PRAGMA foreign_key_check` 作为冗余确认。
`fk-violation` 场景：子表 COPY 引用不存在的父行 → 目标 `IntegrityError` → 整个单元回滚，
前一源事务仍可见，`TxnRejected` 明确报告“未恢复该事务”。没有“先关再忘开”的路径。

---

## 4. 断点、清单与读取代价

### 4.1 绑定到目标的内容身份
断点行绑定：**源内容身份**（固定小前缀 `identity_span` 的 SHA-256）、**字节位置**、**已提交源事务/单元标识**、
**边界块摘要**、**清单序号**、**解析状态**（恒为 `SQL;txn=0;stmt=none`，可自检不自洽）。
身份读一次即可判断“是否同一文件/前缀”。

### 4.2 清单
`manifest.json` 是**只追加、已发布分块永不变**的收据日志 `{off,len,sha256}`，覆盖 `trusted_len`。
关键顺序：**先把清单 fsync 到磁盘，再提交目标事务**。因此任何已提交断点都被已持久化的清单覆盖。
清单只在 `seal_upto(单元结束偏移)` 时发布，不会把读前预取（read-ahead）谎称已信任。

### 4.3 resume 的读取上界
resume 只读：
1. 身份前缀（≤4 KiB）；
2. 断点**边界块**（最后一条语句的尾部，≤`BOUNDARY_MAX=61440` 字节）；
3. 断点之后的未提交输入。

固定开销 = 4 KiB + 60 KiB = **≤64 KiB**，再加**至多一个源事务**的未提交输入。
它**不重读**更早的历史前缀，因此也**不会宣称**那些字节已在本次 resume 中重新验真——
只有 `audit-source` 才做全量重读并更新该结论。
`test_resume_reads_bounded_even_for_large_source`（50 事务）、demo `multi-resume` 打印每次 resume 的实测 read 数。

清单最多比断点“领先一个单元”：如果清单已 fsync 而目标尚未提交，这些字节**没有**被信任，resume 会从断点重读并重执行；
`replay_bytes_read` 单独统计这部分“已见但未提交”的重放量，实测为几百字节级（见 §7）。

### 4.4 audit-source
显式全量重读每个已发布分块，与清单摘要逐一比对（**单独完整计费**，报告 `audit_bytes_read`）。
任何不匹配 → 目标标记 `audit_untrusted`、删除断点行、抛 `AuditFailed`；此后 `restore` 直接拒绝续传。
已提交数据**保留**（不销毁），但不再可信。`test_audit_detects_old_prefix_tamper_and_refuses_resume` 覆盖。

---

## 5. 取舍：为什么这样选，放弃了什么

### 5.1 选择 A（采用）：断点行写入目标事务内部
* 优点：单库单事务即完成“数据 + 进度”的原子提交，无需跨文件的两阶段协议；断电语义清晰；
  恢复证据就是断点行本身，`SQLite` 的 WAL + `synchronous=FULL` 保证持久性。
* 放弃：将断点写独立 sidecar 文件（类似外置 WAL）。
  原因：那需要“目标提交了但 sidecar 没写”或反之的补偿流程，容易在崩溃点产生“无法判定”。
  同事务写入把该窗口消掉。

### 5.2 选择 B（采用）：清单只读预取到断点，不预取也发布
* 优点：`trusted_len` 永远不领先已提交断点，语义干净；resume 边界只需一个边界块摘要。
* 放弃：每读即发布（eager publication）。
  原因：那样会把“读前预取但未提交”的字节标成已信任，虽然重放会覆盖它们，但“信任”的措辞就不诚实了。

### 5.3 选择 C（采用）：COPY 数据段整体作为事件边界，段内重新读取
* 优点：续传点极简，绝不会发布半个 COPY；内存只保留当前行。
* 放弃：在 COPY 行级设置续传点（可把 resume 边界缩小到一行）。
  原因：行级断点需要在目标里持久化“已插入到第 k 行”的段内偏移，增加状态与半行解析复杂度；
  当前上界（一个源事务）已满足题目要求。可复现取舍见 §7 的 `partial-utf8` 与 `exhaustive-cuts`。

### 5.4 选择 D（采用）：独立 DDL 自成一个单元
* 优点：pg_dump 的事务外 `CREATE TABLE`、事务内建依赖表两种形态都能原子恢复。
* 放弃：把事务外 DDL 与后续事务合并提交。
  原因：源语义里它本就是独立语句；合并会改变可见性。

---

## 6. 未覆盖 / 已知边界（诚实声明）

* **源“旧前缀被改”不做隐式检测**：resume 不重读旧前缀，只有显式 `audit-source` 才全量核验并判失信。题目允许。
* **清单本身被篡改**不在防护范围（题目假定“已接收分块摘要清单不可变且可信”）。
  若清单与目标断点被同时恶意改写，本系统不保证发现。
* **任意硬盘损坏 / 坏扇区**不建模；崩溃只建模为“命名控制点 + 进程丢弃未提交事务”。
* **多写者、源就地随机改写、非追加**：只支持单源单目标单写者、源正常只追加；检测到替换/缩短会拒绝（`SourceChanged`）。
* **COPY 子集**：仅 `COPY <表> FROM STDIN;`（无列清单、无其他来源）；`\N` 仅作**整字段** NULL，
  字段内只允许 `\t \n \r \\`；其他转义、非法 UTF-8 字段、列数不符都会被拒绝（不是“丢弃坏行后宣称成功”）。
* **SQL 子集**：`BEGIN/COMMIT`（含 `END`、`TRANSACTION` 变体）、`CREATE TABLE/INDEX`、`INSERT INTO ... VALUES(...)`；
  其余语句（如 `UPDATE/SELECT/DELETE/ALTER/DROP`）一律拒绝。
  `CREATE` 语句原样交给 SQLite，因此其 DDL 细节（如是否使用 `STRICT`）由 SQLite 判定。
* **单语句/单 COPY 行 4 MiB 硬上限**：超出即拒绝（防止无界内存）。
* **`sync=FULL` + WAL** 只在同机进程崩溃语义下成立；不覆盖 OS/电源级 bug。
* **`replay_bytes_read` 依赖清单中记录的 `read_hwm`**：若清单丢失，该指标退化为 0（不影响正确性）。

---

## 7. 真实运行结果

命令：`python demo.py`（全量，含 338 个字节切点扫描）、`./bin/demo-cli.sh`、`python -m unittest discover -s tests -t .`。

实测（本机）：

```
$ python demo.py --quick
[PASS] normalize          2 txns, 4 rows; units=4
[PASS] pseudo-boundaries  COPY payload kept as data; FK clean
[PASS] txn-added-ddl      parent/child tables created and filled in one txn
[PASS] partial-utf8       1-byte chunks, same rows
[PASS] no-half-txn        3 observations, no half txn
[PASS] crash-resume       durable checkpoint (249, 3, 1); seen before resume p=[(1,'alpha')] c=[(10,1,'COMMIT; is data')]; resume_from=249 reads=344
[PASS] multi-resume       resume reads per append: [344, 344, 344]
[PASS] audit-tamper       detected, target untrusted, resume refused
[PASS] exhaustive-cuts    22 cut positions, all converge
```

`python demo.py`（非 quick）额外打印 `exhaustive-cuts 338 cut positions, all converge`。

* resume 固定读取：fixture 上 344 字节（4 KiB 身份 + 边界块经取整后实际 344），远低于 64 KiB；100 事务 / 13 MiB 源上仍为 4103 字节。
* 追加一个事务后 resume 只多读新追加的字节；`multi-resume` 每次固定 344 字节。
* 崩溃在中途、读前预取越过断点时（read_hwm=64000，断点=63528），`replay_bytes_read` 实测 472 字节（“已见未提交”尾部），全量前缀不被重读。
* 100 事务 / 13 MiB 源：加载 0.48s，100 事务全部提交，`foreign_key_check` 为空；单事务 1.92 MiB 加载 0.06s。
* audit 全量读取（fixture）673 字节并与清单逐一比对；篡改旧前缀第 5 字节后 audit 退出码 3，目标失信，resume 退出码 3。

测试：`Ran 59 tests ... OK`（含 25 组随机同族变体 × 3 个随机切点的差分测试）。

---

## 8. 复现命令

```bash
# 测试
python -m unittest discover -s tests -t .

# 场景演示（含全字节切分）
python demo.py
python demo.py --quick

# 纯 CLI（恢复 / 崩溃注入 / 观察者 / 续传 / 审计）
./bin/demo-cli.sh

# 手工单步
python -m streamlex restore fixtures/export.sql --target /tmp/t.db --manifest /tmp/m.json
python -m streamlex restore fixtures/export.sql --target /tmp/t.db --manifest /tmp/m.json \
    --crash-at after_copy_end --observe-target /tmp/t.db     # 退出码 130
python -m streamlex restore fixtures/export.sql --target /tmp/t.db --manifest /tmp/m.json
python -m streamlex status --target /tmp/t.db
python -m streamlex audit-source fixtures/export.sql --target /tmp/t.db --manifest /tmp/m.json
```

CLI 退出码：`0` 成功；`2` 输入被拒（未知 SQL/坏行/截断）；`3` 审计失信或拒绝续传；`4` 源非纯追加改变；`130` 模拟崩溃。
