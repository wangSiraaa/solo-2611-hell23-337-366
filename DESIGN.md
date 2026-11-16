# 设计说明：可跨代范围读取的稀疏归档

实现 `contract.md` 的全部范围：`ingest-base`、`apply-delta`、`read-range`、
`compact`、`verify`。代码在 `sparse_archive/`，测试在 `tests/`，端到端演示
在 `demo_full.py`。基线模块（`source_image.py`、`delta.py`、`range_reader.py`）
保持原样用于复现缺陷；新实现位于其上的新包中。

## 1. 表示选择：分层差量 + 块级校验解析

候选方案与取舍：

| 方案 | 写入代价 | 读放大 | 稀疏性 | compaction |
|---|---|---|---|---|
| 覆盖区间索引（全局一张区间表，每代重写） | 每代重写整表 O(表) | 低 | 好 | 表重写与代际保留耦合，回收祖先要重写全表 |
| 纯块级映射（每块一个指针，写时复制） | 每代复制 16k 指针 | 低 | 好 | 索引字节随代线性增长（64MiB→每代 16k 项） |
| 纯分层差量（只存每代变更区段，读时解析） | 最小 | 随链长增长 | 好 | 需要 rebase 来截断链 |
| **本实现：分层差量 + 4096B 校验块解析** | 只写变更区段 | 每请求块一次组装，链长由 compact 封顶 | 区段级、子块粒度 | rebase 最老被需代为自包含快照 |

选择理由：

- **后写覆盖（last-write-wins）**：delta 内按声明顺序在 `normalize_ops`
  归一化为排序不相交区段（被覆盖的旧区段字节直接丢弃，不进 payload）；
  跨代由解析器"新代优先"天然实现。声明的区段表是权威变更日志，不做
  mtime/内容扫描找脏块。
- **稀疏性**：区段级存储，子块粒度。HOLE 区段永不占 payload；显式全零
  DATA 仍记为 `data`（映射诚实，不靠内容推断）；base 用
  `SEEK_DATA/SEEK_HOLE` 取文件系统自己的 data/hole 图（粗报为 data 就
  如实存 data，能力缺失则整文件记 data——两个方向都不丢字节）。
- **读放大**：`read-range` 只组装与请求相交的完整校验块（4096B），每块
  沿链向祖先解析，实际读盘仅为这些块内的 DATA 子区段；HOLE 合成零不占
  盘。放大上界 = 相交块数 × 4096，与文件大小无关；链长（每块需走的代数）
  由 compact 封顶。读者侧有块缓存，重复块不再读盘。
- **compaction**：把"最老仍被需要"的代（最近两代与 pin 代的最小者）
  物化为自包含快照（extent 级重写），其下所有代脱离并回收。快照
  payload 只含 DATA 区段——协议全洞块零 payload；混合块按子块区间
  存储并如实记录 data/hole 段映射。链中间的代仍作为解析链节保留
  （不可直接读，但为可读代提供祖先字节）。

### truncate-extend 语义

解析时每一代都按其 `logical_size` 裁剪：偏移 ≥ 该代逻辑长度的字节在
该代视图中就是 0，解析**停止**而不是继续向更老祖先下落——缩短后再扩展
的区间默认 hole，旧祖先字节不复活。oracle（`oracle.py`）用"先 resize
再按序应用 op"的 bytearray 语义独立表达同一规则，测试逐字节核对。

## 2. 完整性协议

- 每代每 4096 字节逻辑块一个 sha256（最后一块按实际长度，不填充）。
- delta 声明携带目标视图全部块摘要。`apply-delta` 验收：被触及块
  **组装后**（祖先字节 + 按序 op）再验；未触及且与基代同范围的块必须
  等于基代记录摘要（这就是祖先内容校验）；超出基代长度的块必须是零
  摘要。越界/摘要不符/祖先不符一律拒绝，且不发布任何半代。
- `read-range` 每块组装跨 DATA/HOLE/祖先边界后验摘要，通过才返回字节；
  空范围合法，越界拒绝。计量分开：祖先 payload、本代 payload、元数据、
  缓存命中、合成零。
- 只有 `verify`（全量重读重验每个存储代的每个块 + 链结构检查）可声明
  全代验真；局部读只证明它碰过的块（测试演示：块 A 损坏时读块 B 仍
  成功，但 verify 失败）。

## 3. 并发与故障原子性

- 单写者：进程内锁 + `writer.lock` 的 `flock`（进程死亡自动释放）。
  读者最多两个，pin 文件（`pins/*.json`）是跨进程可见的回收屏障。
- 发布顺序：payload+index 写入 `tmp/` 并 fsync →（暂停点 1：数据写完/
  索引未切换）→ rename 进 `generations/` → manifest 临时文件 + 原子
  rename 切换。读者按打开时的 manifest 快照解析，切换前只见旧代，
  切换后新读者只见完整目标代。失败清理 staging，不发布半代；
  打开时恢复（清 tmp、清未被 manifest 引用的孤儿目录）。
- compact 同理：快照物化 →（暂停点 2：compact 生成后/旧祖先回收前）
  → manifest 切换（替换最老需保留代、丢弃更老代、记录 pending_reclaim）
  → 回收。回收跳过任何 pin 快照窗口覆盖的代目录；pin 释放后
  `compact`/`reclaim` 重试。中断在两处暂停点都安全：切换前状态不变，
  切换后 pending_reclaim 持久化、下次补收。
- 幂等：`delta_id` = 规范化声明（base/target/new_size/有序 op 的
  payload 哈希/全部块摘要）的 sha256。同 target 同内容 → 幂等成功不
  重写；同 target 异内容 → 拒绝。

## 4. 保留与回收

- 可读 = 最近两代 ∪ 显式 pin 的更老代；更老代未 pin 则读拒绝。
- 存储保留 = 从最老被需代到 tip 的完整链（链节为解析服务）。
- 回收单位是代目录；只有 compact/reclaim 触发；pin 窗口（打开时的
  [oldest, tip]）覆盖的目录一律保留，释放后才可回收。

## 5. 计量与统计

- 读计量（每次 read-range）：`own_payload_bytes`、
  `ancestor_payload_bytes`、`metadata_bytes`、`cache_bytes`、
  `synthesized_zero_bytes`，另附实际读盘区段列表（代、偏移、长度），
  测试据此核对"实际读取范围"不超出相交校验块。
- 存储统计（`storage_stats`）：base/快照 payload、差量 payload、索引、
  manifest、pin、tmp 临时（含 compact 暂停中的 staging）、待回收目录、
  总持久字节——不只报净新增；测试与 `os.walk` 实际磁盘字节逐项相等。

## 6. 限制与明确的非目标

- 单常规文件 ≤ 64MiB；链内最多 20 代（达到后拒绝新 delta，compact 后
  释放名额）；最多 2 个并发读者。
- 不做压缩、内容去重、通用备份扫描、分布式块存储。稀疏性只来自
  文件系统 data/hole 图与协议显式 HOLE，不做内容零块推断。

## 7. 验证（tests/test_archive.py，33 例）

真实 data→hole（基线缺陷场景）、hole→全零 data（compact 后仍记 data）、
跨块后写胜出、缩小再扩展（含非对齐长度）、错误祖先（不存在/已回收/
摘要按错误内容计算）、payload 破坏（staged 拒绝、已提交检出）、跨代
范围读、compact 两种中断、reader pin 阻止/放行回收、单写者互斥、
双读者上限、暂停点可见性、幂等/冲突、20 代上限、读放大与计量分类、
存储统计对盘、CLI 端到端，以及随机序列对照 bytearray oracle：每代
对全部候选边界范围与随机范围比对、整代恢复逐字节比对、每步 verify
与磁盘总量核对。
