# 演进 Union 列的协商式数据交换（unionx）

两个 producer → 一个 receiver 的本地协商式 Union 数据交换。消费**真实 Arrow IPC**
dense/sparse Union，以稳定的 `branch_id` / `field_id` 身份解决局部 type-code
随 producer/revision 变化的问题，分阶段协商、受限缓冲、失败原子、可重开落盘。

基线 `demo.py` / `receiver.py` / `union_reader.py` 保留可运行（`python demo.py`），
它复现的正是本系统要修的 bug：b 批交换局部 type-code 后数值 20 被错标成文本分支。

## 快速开始

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# 1) 真实 fixtures（a/b 交换 typecode + schedule.json 屏障时序）
.venv/bin/python -m unionx.cli fixture  --fixtures fixtures --store out/fixture

# 2) struct 演进全场景（sparse→dense、改名、新增 nullable、missing vs child-null）
.venv/bin/python -m unionx.cli evolution --store out/evo

# 3) 重开存储，按稳定身份查询（原值 + 来源）
.venv/bin/python -m unionx.cli query --store out/evo --branch branch-record --field field-note
.venv/bin/python -m unionx.cli query --store out/evo --missing

# 4) 工程取舍可复现实验（结果见 experiments/）
.venv/bin/python -m unionx.cli experiment --rows 1000 --evolutions 8 --out experiments/result.json

# 测试
.venv/bin/python -m unittest discover -s tests -v
```

## 身份模型（核心）

| 概念 | 作用域 | 规则 |
|---|---|---|
| `branch_id` | 两 producer 全局命名空间 | 同一 branch_id 即同一逻辑分支；只增不删（保留 `branch-missing` 例外，见下） |
| `field_id` | 同一 branch_id 内跨 producer 共享 | 改名不换 ID；**同名不同 ID 是不同字段，值不合并**；已删 ID 不复活 |
| 局部 type-code | 单 producer 单 revision | 可在 Arrow IPC 允许的 `[0,127]` 任意选择、随 revision 重排/重用，永不跨批按整数拼接 |
| canonical code | receiver 私有 | branch 首次进入 canonical 时分配的永久稳定整数，与任何 producer 局部 code 无关 |

逻辑缺席用全局保留分支 `branch-missing`（null payload）**显式表示**。Arrow Union
本身没有父 validity，因此：

* 行选中 `branch-missing` → 逻辑缺席（查询 `present=false`）；
* 行选中数据分支但 child 是 NULL → 分支仍存在（`present=true, value=null`）。
  两者在落盘 Union 与查询结果中严格区分（`tests/test_persistence.py`、`test_barrier_backpressure.py`）。

## 三阶段协议

1. **propose**：descriptor（producer_id、schema_revision、局部 code→branch_id、带 field_id 的分支/字段）
   进入 pending。做结构自洽、revision 邻接、相对上一已接受 revision 的演进兼容、
   跨 revision 显示名历史裁决。不合即抛 `IncompatibleSchema`，canonical 不变。
2. **accept**：receiver 显式接受，descriptor 并入全局 canonical（只增不改），随后尝试放行屏障。
3. **submit**：数据批提交。目标 revision 未接受 → 进**屏障缓存**，绝不用旧布局解码；
   接受后按 canonical 重编码并落盘。

演进允许：字段改名（同 ID）、新增 **nullable** 字段（旧行读时补 NULL）、新分支。
演进拒绝：payload/field 类型变更、nullable 收紧、删除数据分支、新增非空字段、
已删 field_id 复活、删除后用新 field_id 占用历史显示名（同名重建）。同 revision
不同 schema 指纹整体拒绝。

## 受限缓冲与背压

* 每 producer 最多 **2** 批、全局最多 **3** 批常驻（含屏障缓存）；
* 超限 `submit` 显式返回 `Backpressure`：消息不保留、不给提交确认，调用方稍后重投同一 seq；
* 失败批原子拒绝：无任何部分进入输出，seq 占用与 resident 计数回滚，同 producer 下一批不被阻塞；
* 一个 producer 的不兼容提案不影响另一个已接受 schema 的流（屏障按 producer 分流）。

内存口径（`Exchange.stats()["memory"]`）：完整 Arrow 输入 IPC + 待重编码 + 输出批合计；
**已关闭落盘文件不计**。另披露一个为处理当前批而设的 `conversion_buffer`
（`child.to_pylist()`，≤1000 行），其估算字节计入峰值占用。

## 工程选择：canonical 如何增长、旧片怎么办

**采用：只增不改的分片 + 读时按身份对齐（append-only shards, read-time alignment）。**

* canonical 是只增快照：新分支/新 field_id 追加并分配稳定 canonical code；纯改名不产生新结构。
* 每次 canonical 结构增长就**滚动新分片**，旧分片字节永不重写；每片在 Arrow metadata
  与 `manifest.json` 中绑定写入时刻的 canonical revision + 指纹。
* 查询时按 `branch_id`/`field_id` 身份把旧片对齐到请求视图：旧片没有的字段天然返回 NULL。

**放弃：每次演进立即把全部旧片重编码到新 schema（eager rewrite）。**

可复现实验 `cli experiment`（同一份输入族跑两种方案，`experiments/tradeoff-*.json`）：

| 1000 行/批 × 8 次演进 | A 只增片+读时对齐（采用） | B 立即重编码旧片（放弃） |
|---|---:|---:|
| 旧片重写字节 | **0** | 2,540,500（随演进 O(K²)） |
| 提交总耗时 | 0.071 s | 重写额外 0.153 s |
| 新字段单次查询 | 0.044 s（扫全部旧片对齐） | 0.044 s |
| 峰值常驻 | ~282 KB | ~282 KB |

取舍：A 把成本从写入路径移到读取路径，提交延迟与演进次数解耦、失败半径最小；
代价是新字段查询要读时扫旧片。B 在写入时付出 O(K²) 重写，且重写期间放大失败半径，
在"演进频繁、查询多按身份点查"的本题负载下不划算。不做最优性证明。

## 落盘布局

```
<store>/
  manifest.json        # 分片清单、每片 canonical 快照、producer seq、canonical_history
  shards/shard-00000.arrow  # 真实 IPC：value=dense canonical union + provenance struct
```

provenance 每行带来源：`producer, seq, parent_row, original_child_offset, input_mode`。
`ShardStore.reopen()` 校验 manifest 已正常 close 后重开；所有查询走真实 `pa.ipc.open_file` 读回。

## 模块

| 文件 | 职责 |
|---|---|
| `unionx/schema.py` | 稳定身份、descriptor、兼容裁决、canonical 合并 |
| `unionx/protocol.py` | IPC 信封（metadata 自描述）、Ack/Backpressure、外部 Arrow 名→field_id 映射 |
| `unionx/producer.py` | 构造真实 dense/sparse Union IPC，局部 code 任意合法 |
| `unionx/encode.py` | 局部 code 解码 + 全部原子校验 + canonical dense 重编码 + provenance |
| `unionx/store.py` | 分片滚动、manifest、重开、身份查询、missing/present-null 区分 |
| `unionx/exchange.py` | 三阶段协调、屏障分流、背压、内存记账、失败回滚 |
| `unionx/scenarios.py` | fixtures 适配与合成演进场景 |
| `tests/` | 29 个 unittest，覆盖下列全部不变量 |

## 测试覆盖的不变量

* typecode 交换/重用/稀疏取值，canonical 码与局部码解耦（真实 IPC 读回断言）；
* sparse/dense 输入互换，统一落 dense，`original_child_offset` 与 `input_mode` 保留；
* 改名不改身份、新增 nullable 旧行补 NULL、同名不同 ID 不合并、同名重建/类型变更/同 revision 异 schema 拒绝；
* schema 屏障（未接受不解码）、慢消费者背压（2/3 上限）、坏提案不阻塞另一流、失败批原子与流恢复；
* 分片重开、按身份查询、provenance、missing vs child-null；真实 a/b 与 struct-v1/v2 fixtures 端到端；
* 16 分支 / 8 字段 / 1000 行 / 2 producer 上限与内存披露。

## 已知边界（未覆盖或刻意约束）

* Arrow IPC Union type-id 实际只支持非负 `[0,127]`（负数无法过 IPC 往返，为 Arrow 格式约束，非本系统限制）；
* 仅本地可控模拟传输（Envelope 字节 + 内存队列），无 RPC/TCP/Flight；
* 重开要求存储曾正常 `close()`（manifest 置 closed 标志），崩溃在途的未关闭存储按损坏拒绝；
* 背压是同步返回信号，没有内置定时重试/回调，由调用方决定重投节奏；
* 查询为全分片扫描式身份对齐，未做列级投影下推或二级索引；
* 演进实验受 8 字段上限约束，最多演示 8 次字段增长。
