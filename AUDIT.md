# 存储与查询审计

所有数字按**实际落盘文件字节**统计（`Store.audit()`，等价 `du` 口径），
恢复/extract/trace 会读取的文件全部计入；仅排除一份原件、独立测试 oracle
与不参与恢复的外部观测日志。本实现没有这类“更名日志”，所有持久文件都在
manifest 登记并参与查询。

## 1. 分类口径

| 类别 | 包含文件 | 是否参与恢复/查询 |
|---|---|---|
| origin | `origin.fa`（唯一一份原件，预算中排除） | 是，piece 回查原碱基 |
| sequence_delta | `rev-N.adds.bin`、`rev-N.seq.json` | 是，序列重放与新增来源回查 |
| feature_index | `feature-defs.json`、`rev-N.feat.json` | 是，feature 原子链与受影响分类 |
| metadata | `manifest.json` | 是，版本可见性与文件登记 |

`new_persistent_bytes = all - origin`，即除原件外的全部持久字节。

## 2. 预算测试：1MiB 源 × 20 次各插入 1 字节（无 feature）

输入 `inputs/large-reference.fa`（序列 1,048,576 字节，FASTA 文件 1,048,586）、
`inputs/large-revisions.json`（20 个修订，每修订 1 个 1 字节 N 插入）。

| 类别 | 字节 |
|---|---:|
| origin（排除） | 1,048,586 |
| sequence_delta（20×adds + 20×seq.json） | 5,711 |
| feature_index | 1,506 |
| metadata（manifest） | 6,036 |
| **新增持久字节合计** | **13,253** |
| 源序列长度 | 1,048,576 |
| **13,253 < 1,048,576** | **满足** |

新增量约为源长度的 **1.26%**。其中真正的新增序列数据只有 20 字节（20 个 N），
其余为 20 份小 JSON 编辑记录与清单的固定开销。

## 3. 含 100 个 feature 的同口径大测试

| 类别 | 字节 |
|---|---:|
| origin（排除） | 1,048,586 |
| sequence_delta | 5,711 |
| feature_index（defs + 20 份 feat 增量） | 25,621 |
| metadata | 6,036 |
| **新增持久字节合计** | **37,368** |
| **37,368 < 1,048,576** | **满足** |

feature 索引只记录原子链真正变化的段；未被任何编辑触及的 feature 零增量。

## 4. 小样本（12bp，1 修订，2 feature）

| 类别 | 字节 |
|---|---:|
| origin | 20 |
| sequence_delta | 333（adds 3 + seq.json 330） |
| feature_index | 393（defs 256 + feat.json 137） |
| metadata | 550 |
| all | 1,296 |

## 5. 查询与回查成本（实测，1MiB / 20 版）

| 操作 | 耗时 | 说明 |
|---|---:|---|
| 20 个修订全部发布（含 feature 引擎） | ≈0.07–0.08 s | 每修订 O(编辑 + 受影响段原子) |
| 重开 store 并重放重建 v20 完整序列 | ≈0.01 s | 顺序重放 20 份小 seq.json，不复制历史全量 |
| `extract(version,id)` | 与段原子数线性 | 由 trace 块逐字节拼回 |
| `trace(version,id)` | O(段数 × 原子数 × log piece) | 坐标靠 piece-table 二分反查 |
| `affected(version)` | O(1) 读 feat.json | 内容改变 / 仅坐标移动已在发布期分类 |

## 6. 无全量复制的核对

* 任何 `rev-N` 文件都不含该版本完整序列或完整 feature 文本；
* 重开后 `sequence(v)` 由 origin 片段 + 各 adds 片段现场重建，并与 seq.json 内
  记录的 sha256 核对；
* `extract(v,id)` 的每个字节都能在 `trace(v,id)` 中找到 `origin` 或 `edit` 来源，
  测试断言 trace 重建结果逐字节等于 extract；
* 审计列出的文件集合与磁盘实际文件集合一致（孤儿文件在打开时清理，测试有断言）。
