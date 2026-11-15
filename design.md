# 设计说明：能力约束下的快照 WITH TIES 联合查询组件

## 1. 组件与文件

| 文件 | 职责 |
| --- | --- |
| `capabilities.py` | 固定排序语义、分片能力声明（`max_page`/`seek`/`with_ties`）与兼容性校验；不兼容排序直接 `CapabilityError` 拒绝，不做本地重排。保留 baseline 的 `validate`。 |
| `snapshot.py` | SQLite `backup` 生成真实只读快照、`mode=ro` 打开、sha256 校验；`SnapshotRegistry` 实现 token 注册/过期/close 与读者重开预算（每绑定初次打开 + 最多 2 次重开）。 |
| `shards.py` | baseline 的 `seed`/`Shard.top` 原样保留；新增 `seed_fact` 新 schema 与 `ShardAdapter`：NULL 感知键集 seek、OFFSET 分页、业务键 tie 探测，每次返回做严格单调性/游标校验。 |
| `query.py` | 联合查询句柄 `JointQuery`，提供 `open` / `next_page` / `reopen` / `close` / `audit`，含三阶段状态机、每片单页缓冲、崩溃安全的持久化。 |
| `oracle.py` | 独立真值：对快照裸 `SELECT`（不带 `ORDER BY`）取全量，在 Python 侧独立排序，模拟全局 OFFSET/LIMIT WITH TIES。 |
| `testdata.py` | 确定性数据构造、源库变更注入。 |
| `verify.py` | 12 组 50 个断言的真实快照验证（`FULL_SCALE=1` 时 8×50000）。 |

## 2. 排序与身份

全局排序固定为：

```
score DESC NULLS LAST, day ASC NULLS FIRST, shard_id UTF8 ASC, local_id ASC
```

- 前两列是业务排序，后两列 `(shard_id, local_id)` 是稳定尾键，保证全序确定。
- **tie 只看业务键 `(score, day, business_id)`**：同业务键跨行才补齐；不同 `business_id` 即使 `(score,day)` 相同也不是 tie。
- 业务 ID 可在多片重复，物理身份 `(shard_id, local_id)` 始终保留，不做去重合并。
- SQLite 侧排序表达式把 NULL 位置显式化：
  `score IS NOT NULL DESC, score DESC, day IS NULL DESC, day ASC, local_id ASC`。

## 3. open-query：版本向量

- 每个分片用 `snapshot.create` 经 online backup 生成**独立本地只读快照文件**，得到
  `{token, path, sha256}` 绑定并注册进 `SnapshotRegistry`，组成版本向量 `token_vector`。
- 不要求分片间同时刻的全局事务；只要求同一查询的后续每次读取都落在各自绑定版本上。
- 连接一律 `mode=ro` 打开；打开前强校验 sha256，文件缺失/被换即 `SnapshotExpired`。
- 源库可以继续写入，快照字节不变，旧查询结果不受影响（verify 用例 4 实证）。
- 每次 `next_page` 入口跑强一致 liveness 门禁：任一分片快照过期/缺失，**整次查询立即
  不可继续**，失败前不取任何新数据，不混新旧页；已交付页保留，状态里写明 `failure`。

## 4. 三阶段流式状态机

```
SKIP (跳过全局 offset)  ->  EMIT (输出全局 limit 行)  ->  TIE (补齐边界业务键)  -> DONE
```

- k 路归并每轮先 `_ensure_heads`：**每个未耗尽分片都必须有头行**（内存缓冲或从持久页
  恢复，否则发一次 adapter 取页），再用全局键选最小行。因此全局下一行只在所有活动分片
  的头都可见后才决定。
- `limit` 针对整个查询；`next_page` 的 `page_size` 只控制网络分批，ties 可以跨多次
  next-page（用例 11 用 page_size=1 实证一个 12 行 tie 组跨 13 次交付）。
- offset/limit 含 0；limit=0 在 open/reopen 时直接进入 DONE，不读任何数据行。

## 5. 按能力选择的安全下推与缓冲策略

### 5.1 seek 能力分片

- 下推 `ORDER BY ... LIMIT n` + NULL 感知键集游标 `(score,day,local_id)`，游标谓词用
  词典序三支（score 严格落后 / score 相等 day 落后 / 两者相等 local_id 更大），
  `IS` 做 NULL 感知相等。
- seek 带 WHERE 时“短页”不代表耗尽（WHERE 过滤可使一页少于 n 行），adapter 对短页
  额外发一次 `LIMIT 1` 探测（`probe_exhausted`，计入审计）确认是否真耗尽，避免提前
  终止——这是开发中实际踩到的缺陷。
- 暂停/恢复成本：恢复只需游标，不重扫；全局边界之外的页永不取回。

### 5.2 仅 offset 分片

- 下推 `LIMIT n OFFSET k`，`k` = 该分片**已取回行数**，只在取页时按实际返回行数推进，
  绝不在每行弹出时自增（否则 OFFSET 会重复跳过，这也是实际修掉的一个缺陷）。
- 成本：深 offset 时分片侧需扫过 k 行，受全局 offset 上界约束；恢复时从记录的
  `offset_consumed` 继续，缓冲页里已预读未输出的行随 continuation 保留，不重取。

### 5.3 WITH TIES 的两种策略

进入 TIE 时记录边界业务键 `boundary_key` 与层级 `boundary_level=(score,day)`：

- **有 `with_ties` 能力**：分片把当前缓冲页内的边界键行先输出；缓冲一排干就切到
  `probe_mode`，用 `read_tie_page` 做**有界业务键探测**（等值 `(score,day,business_id)`
  + `local_id > 游标`），只回该键剩余行，不跨业务键多取；探测为空即该分片 tie 完成。
- **无 ties 能力**：保持普通有序分页作为前瞻。因为同一 `(score,day)` 层级内不同业务键
  按稳定尾键交错，**只能按层级而非业务键暂停**：头行层级严格越过 `boundary_level` 才
  `past_boundary` 暂停并整页保留；同层级的外键交错行弹出但不输出。这保证不会丢掉某片
  超过 N 的同键尾部（baseline 的核心缺陷在此被根治）。

### 5.4 多取、暂停、恢复如何受全局边界约束

- 多取量上界 = 每片一个 `max_page` 页 + seek 短页的 1 行耗尽探测 + tie 探测页；任何
  分片都不会取回全局边界层级之后的数据（无 ties 片在头行越界时即暂停）。
- 低 limit 非并列基准实测：16000 行总量、offset 10/limit 5 仅取回 255 行（1.6%）；
  40 万行重负载 offset 1000/limit 200 仅取回 1505 行（0.38%），均远少于全量，统计口径
  包含 offset 跳过行与所有 lookahead/探测行（审计 `total_rows_fetched`）。
- 暂停态（`past_boundary`/`probe_done`）与未消费缓冲一起持久化，恢复后不重复前瞻。

## 6. 内存与持久化不变量

- 每个分片任意时刻**最多一个 adapter 页**在内存（`peak_mem_pages <= 分片数`，带断言
  守护），加头/边界/游标等少量元数据；输出是流式的，不做全量 sort。
- 状态写入（每个 next_page 成功末、open、失败时）把内存未消费页落盘到
  `pages/page_<shard>_<version>.json`，**版本号不可变**，按实际字节计入
  `persist_bytes`。
- 崩溃安全的写入顺序：
  1. 新页写成新不可变版本文件（先写 `.tmp` 再 `os.replace`），不动旧页；
  2. 状态写 `query_state.tmp`；
  3. `os.replace` 原子提交为 `query_state.json`；
  4. 提交成功后才清理未被新状态引用的历史页文件。

  因此在任意中断点，已提交状态引用的持久页必然存在；重开发现残留 `.tmp` 会清理并基于
  上一个检查点续接，预读未输出行不丢、已输出行不重放（用例 6 实证）。
- continuation 状态逐片记录 `cursor`（已消费边界）、`offset_consumed`、`probe_mode`、
  `past_boundary`、`probe_done` 以及未消费缓冲（内存或落盘文件名）。

## 7. 失败语义（保留可重读数据 + 明确失败结果）

| 情形 | 行为 |
| --- | --- |
| 快照文件被删/被改 | liveness 门禁或打开时 sha256 失败 -> `QueryUnavailable`，`failure` 持久化，reopen 同样失败；已交付页保留。 |
| 重开超过 2 次 | `ReopenBudgetExceeded` 包成 `QueryUnavailable`，查询不可继续。 |
| 分片返回乱序/游标越界页 | `AdapterOrderError`，拒绝而非本地重排。 |
| 不兼容排序/非法 page_size/负 offset | open 时 `CapabilityError`，拒绝建查询。 |
| 状态文件缺失/损坏、持久页丢失 | `QueryUnavailable`，不猜测重取。 |

## 8. 审计 `audit()`

逐分片给出 `read_page`/`read_tie_page`/`probe_exhausted` 次数、`seek`/`offset`/
`tie_probe` 模式分布、累计取回行数，以及快照 token、打开/重开次数、过期/关闭状态、
内存页峰值、持久化字节数与失败信息。verify 用它核对：每个分片内存页不超过 1、取回行数
不超过全量、低 limit 取回显著少于全量、tie 探测只发生在声明了 with_ties 的分片。

## 9. 明确的非目标

不实现任意 SQL 优化器、不做分布式共识、不部署远端数据库；所有分片都是本地真实 SQLite
文件，所有“网络分页”都是进程内分批交付的边界模拟。
