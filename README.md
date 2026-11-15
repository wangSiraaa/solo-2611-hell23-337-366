# 带压缩历史的消息档案随机取数（升级版）

本工程在原有 `frames.py` / `archive.py` 基线之上实现了一个可持久化的磁盘索引、
随机取数入口和针对不变量的测试。**没有依赖外部库**，仅用 Python 3.11 标准库
（`zlib` 提供 raw DEFLATE）。基线 `demo.py` 仍可运行，输出不变。

## 快速开始

```bash
# 1. 用 fixture 构建索引（写入 index.bin + manifest.json）
python -m wsarchive build \
  --archive fixtures/archive.frames \
  --negotiation fixtures/negotiation.json \
  --index-dir /tmp/wsidx

# 2. 任意次序、可重复地取数（输出按请求顺序）
python -m wsarchive query \
  --archive fixtures/archive.frames \
  --negotiation fixtures/negotiation.json \
  --index-dir /tmp/wsidx \
  --ids 99,0,50,50 --mode batch --json

# 3. 全量顺序解码，与索引中的长度/sha256 逐条核对
python -m wsarchive verify --archive fixtures/archive.frames \
  --negotiation fixtures/negotiation.json --index-dir /tmp/wsidx

# 4. 索引结构、大小预算
python -m wsarchive info --archive fixtures/archive.frames \
  --negotiation fixtures/negotiation.json --index-dir /tmp/wsidx

# 5. 复现实验：100x4096 负载 + 检查点密度扫描
python -m wsarchive bench --archive fixtures/archive.frames \
  --negotiation fixtures/negotiation.json
python -m wsarchive bench --archive fixtures/archive.frames \
  --negotiation fixtures/negotiation.json --density-sweep

# 6. 端到端演示
python demo_upgraded.py

# 7. 测试（53 项）
python run_tests.py -v
```

## 设计

### 外部数据模型（`wsarchive/wire.py`）

* 只接受服务端→客户端方向、未 mask 的帧；masked/截断/RSV2/RSV3 一律报错。
* 控制帧（ping/pong/close）不是消息，可插在数据分片之间；它们不进入任何消息
  负载，也不进入 deflate 历史。
* RSV1 只在**首数据帧**声明压缩；continuation 帧出现 RSV1 是协议错误。
* 一条消息的 FIN 帧结束消息；未压缩消息不喂给 inflater。
* 只支持 binary（text 帧被拒绝），因为任务边界是 binary 消息。

### 持久化解码检查点（`wsarchive/build.py` / `index.py`）

permessage-deflate + context takeover 实际上是一条连续的 deflate 流：每条消息以
`Z_SYNC_FLUSH` 结尾，线上剥掉了 4 字节尾标记 `00 00 FF FF`。所以每条消息之间
不是独立压缩流，后者会因越界距离而解码失败或得到错误字节。

检查点保存的是**消息边界处压缩明文的尾部窗口**（至多 `1 << window_bits` 字节），
而不是完整消息明文。恢复时用 `zlib.decompressobj(-wbits, zdict=snapshot)` 直接
接管：

* `Z_SYNC_FLUSH` 结束当前块并对齐字节，消息边界是干净的可接管点；
* `zdict` 恰好提供后续 back-reference 所需的历史窗口；
* 一个查询从最近的检查点起、用同一个 inflater 连续解压到目标消息，
  **跨分片**重建，绝不把每条消息的 flush 边界当成独立流。

索引还会记录每条消息的明文长度与 sha256（元数据，不是明文），用于严格校验：
任何解码失败、长度不符或摘要不符都会抛错，**绝不允许部分消息被当成成功产物**。
索引文件头同时绑定档案 sha256 与协商参数的规范化 sha256；档案被改动或协商参数
不匹配时，加载/取数会拒绝使用旧检查点。

### 索引格式

`index.bin` 是小端、无对齐填充的紧凑二进制（`index.py` 顶部有逐字段布局），
包含帧表、消息表、检查点表和有界字典 blob。`manifest.json` 记录 `index.bin`
的 sha256，供人阅读与二次校验。**不存任何完整明文消息**。

### 查询语义（`wsarchive/reader.py`）

* `cold`：每个请求的出现位置都从检查点独立解压；重复 id 会重复解压、重复计费，
  与多次独立 CLI 调用一致。
* `batch`：按检查点分组，组内共享重建前缀；重复 id 只物化一次，但**输出字节按
  出现次数累计**。输出始终按请求顺序重排。
* 任何模式下，实际 inflate 出的字节（含为恢复历史而丢弃的前缀、以及最终输出）
  都计入 `inflated_bytes`；原始压缩 body 读取量计入 `compressed_body_bytes`。
  没有明文旁路缓存：重复查询会再次 inflate 并再次计数。

### 预算与取舍

选择**检查点密度**：在永久状态 `<=256KiB` 的硬约束下取最细的网格。对 100 条
× 4096B、`window_bits=15`：

* 状态 = `(检查点数-1)*32768 + 检查点数*53`；
* 密度 12 需 9 个检查点，状态 262621B 略超 256KiB；
* 密度 13 需 8 个检查点，状态 229800B，**满足预算**；
* 单条冷查询最多重放 13 条消息 → `13*4096 = 53248B <= 64KiB`。

若把密度调得更细，冷查询放大更小但永久状态超预算（密度 6：状态 516997B）；
调得更粗则状态更省但单查询放大上升（密度 26：单查询 106496B 超 64KiB）。
实验 `--density-sweep` 输出这条曲线。我们**放弃**了“存每条消息完整明文或
`raw_deflate` 状态不可序列化”的替代方案：前者违反“禁止把明文作为索引保存”，
后者无法在 `zlib` 中导出/导入原始 inflate 内部状态，只有窗口快照既能持久化
又能被新进程独立使用。

## 实测结果（本机 Python 3.11）

### fixture（`fixtures/archive.frames`，100 条 × 4096B）

| 指标 | 值 |
|---|---|
| 帧 / 消息 / 检查点 | 300 / 100 / 8 |
| 密度 | 13 |
| 永久字典状态 | 229800 B（≤256KiB） |
| 普通索引元数据 | 16308 B（≤64KiB） |
| 消息 99 冷查询实际 inflate | 36864 B（前缀 32768 + 输出 4096） |
| 消息 99 sha256 与 source-truth 一致 | 是 |

### 指定 100×4096 负载（生成同族语料）

| 指标 | 值 |
|---|---|
| 线上字节 | 9379 B |
| 永久字典状态 | 229800 B（≤256KiB） |
| 索引元数据 | 18366 B（≤64KiB） |
| **最坏单条冷查询 inflate** | **53248 B（≤64KiB）** |
| 100 次单条冷查询 inflate 总和 | 2793472 B |
| 读取的原始压缩 body 字节总和 | 54736 B |
| 重复冷查询（同一条两次） | 73728 B，确实重复计数 |
| batch（100 条乱序）inflate | 409600 B = 1× 全量 |
| batch 相对逐条 cold 节省 | 2383872 B |
| batch 输出顺序保持 | 是 |

（“单条冷查询 ≤64KiB”是逐条上限；100 条输出本身就有 400KiB，所以跨 100 次的
总和不可能 ≤64KiB，报告总和仅为透明起见。）

### 密度取舍曲线（同一 100×4096 语料）

| 密度 | 检查点 | 字典状态 B | 最坏单条 cold B | 状态达标 | 查询达标 |
|---:|---:|---:|---:|:--:|:--:|
| 6 | 17 | 516997 | 24576 | ✗ | ✓ |
| 13 | 8 | 229800 | 53248 | ✓ | ✓ |
| 26 | 4 | 98516 | 106496 | ✓ | ✗ |
| 52 | 2 | 32874 | 212992 | ✓ | ✗ |
| 100 | 1 | 53 | 409600 | ✓ | ✗ |

## 测试覆盖（`tests/`）

* `test_framing.py`：控制帧非消息、RSV1 仅首帧、continuation RSV1/新数据帧/未结束
  消息/截断/掩码/text 拒绝；跨分片重建；未压缩消息不进历史；flush 边界不是独立流。
* `test_query.py`：全部单条 cold 正确性、任意次序与重复 id、输出顺序、cold/batch
  等价、重复查询重复计费、无明文旁路、预算、batch 节省量。
* `test_corruption.py`：检查点后字节损坏、前缀损坏、整档哈希失配、截断尾帧/未结束
  消息，异常不返回部分消息。
* `test_process.py`：序列化往返（含交错控制帧）、manifest 哈希绑定、**新进程**独立
  构建/查询检查点。
* `test_variants.py`：window_bits 9–15、压缩级别/策略、10 分片+控制帧、压缩/未压缩
  交错、空/极小消息、接近 64KiB 消息、no_context_takeover、密度单调取舍。
* `test_fuzz.py`：随机配置的差分测试（随机访问 vs 顺序解码 vs 真值）。

## 已知未覆盖边界

* 只处理服务端单向、未 mask、binary、单条 ≤64KiB、≤10 分片；不实现握手、
  关闭握手、文本消息、客户端方向。
* 只支持 `Z_SYNC_FLUSH` 作为消息边界（`Z_FULL_FLUSH`/`Z_FINISH` 不在档案族内）。
* `zdict` 接管依赖 `Z_SYNC_FLUSH` 的字节对齐；若未来允许其他 flush 模式需重新验证。
* 极端 `window_bits=15` + 4096B 消息 + 1000 条时，256KiB 状态预算只能给出密度 125
  （8 个检查点），逐条冷查询最坏约 512000B，必然超过 64KiB；同时帧/消息元数据约
  141KiB 也超过 64KiB。这超出题目“预算只约束 100×4096 负载”的范围，`build_stats`
  会如实报告 `query_worst_case_upper_bound` 与 `metadata_ok`，不会假装达标。
* 检查点快照保存完整 `1<<window_bits` 尾部窗口以保证一般正确性；没有尝试按实际
  back-reference 距离压缩该快照。
