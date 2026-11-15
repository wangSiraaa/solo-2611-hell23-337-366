# 升级报告：可回查重复项的外存集合执行器

## 交付物

| 位置 | 内容 |
|---|---|
| `exmem/encoding.py` | 行键/出现记录/溢写帧的二进制编码（可全序、单射） |
| `exmem/meter.py` | 资源计量：常驻 payload 槽位、临时磁盘字节/行数、I/O 字节；超限即抛 |
| `exmem/streams.py` | 溢写文件、计量读写器、三类流（扫描/段/链）、外部排序与多路归并 |
| `exmem/engine.py` | 计划校验、AST 求值、归并连接、逐算子摘要、输出提交 |
| `exmem/cli.py` | `python3 -m exmem run …` 与 `python3 -m exmem bench …` |
| `runner.py` / `demo.py` | 兼容入口；`python3 demo.py` 走新引擎，输出与旧基线一致 |
| `tests/test_engine.py` | 33 个不变量测试（见第 6 节） |
| `experiments/hash_vs_sort.py` | 策略取舍的可复现实验 |
| `make_large.py` | 默认每源 10 万行、90% 单热点键 |

仅使用 Python 3.11 标准库；`requirements.txt` 保持为空。

## 1. 语义与不变量的实现

- **出现身份**：每个扫描叶在编译期获得 `(路径串, 路径元组, 叶id)`，出现记录 =
  `(path_id, rownum, row_key)`；见证串输出为 `root.L.R:a:17`，与参考实现
  `operators.py` 的格式逐字符一致。同一源被 AST 扫描两次 → 路径不同 → 两个独立
  出现族；相同值两行行号不同 → 身份不同（`test_occurrence_identity_double_scan`、
  `test_except_self_is_empty_per_occurrence`）。
- **括号语义**：不对 AST 做任何交换/结合改写；`(a∖b)∖c` 与 `a∖(b∖c)` 在同一数据上
  产生不同结果且各自与参考一致（`test_parentheses_preserved`）。
- **配额顺序**：EXCEPT/INTERSECT 的左侧按 `(row_key, path_tuple, rownum)` 外部排序，
  等值组内即左见证顺序；右侧只归约为排序的 `(key→count)` 溢写文件（右见证本就不需要）。
  归并连接单趟游标：EXCEPT 消耗每组最早 m 个左出现，INTERSECT 保留最早 min(n,m) 个。
- **输出顺序**：每个节点的输出流都按 `(path_tuple, rownum)` 全序——UNION 是左流接右流
  的零拷贝链（左子树路径元组恒小于右子树），EXCEPT/INTERSECT 的幸存者按见证键再做一次
  外部排序。排序键全序且唯一，因此 spill 分区/归并顺序不可能改变保留下来的来源及其顺序。
- **等值规则**：NULL 编码为同一字节故相等；text 按 UTF-8 字节精确相等 ⇔ Unicode 码点
  精确相等（组合态 é 与分解态 e+◌́ 不同键）；int/text/null 互不相等；无浮点规则。
- **可抽查摘要**：每个算子向报告追加计数（left_in/right_in/right_distinct/matched/
  emitted/conservation_ok）；`--audit-groups` 可输出逐组 `(key,n,m,consumed,emitted)`
  日志（作为已提交产物，tmp+rename 提交，不计临时空间）。

## 2. 引擎设计

**策略：外部排序 + 归并连接。** 每个 EXCEPT/INTERSECT 节点四步：
1. 右侧流 → 键外部排序 → 流式聚合为 `(key,count)` 溢写文件，随即删除右侧输入；
2. 左侧流 → 按 `(key, path, rownum)` 外部排序（run 缓冲 1024 条，扇入 128 多趟归并）；
3. 单趟归并连接消耗配额，写幸存者；
4. 幸存者按 `(path, rownum)` 重排序即节点输出。

**按表达式需求回收**：每个节点的结果是被父节点恰好消费一次的流；消费完毕立即删除其
溢写文件（`dispose`），UNION 链不产生任何临时字节。测试断言运行结束后计量器临时字节
为 0 且 workdir 为空。正在生成的输出以 `.tmp` 写入并计入临时空间，`os.replace` 提交后
才移出计量。

**热点键**：排序天然不受单键偏斜影响——90k 个相同键只是 run 里的一段，不需要任何
特殊回退路径。

## 3. 工程取舍：外部排序 vs 分区哈希

放弃分区哈希（grace hash / 混合）的原因：单热点键无法被再 hash 拆分——同一键必然落入
同一桶，换种子、换模数、递归再分都一样。可复现实验 `python3 experiments/hash_vs_sort.py`
的真实输出（10 万行/侧、90% 热点）：

```
H  hash-partition left into   64 buckets (seed graceH1): biggest bucket = 90140 rows (22x the payload budget)
H  hash-partition left into   64 buckets (seed graceH2): biggest bucket = 90161 rows (22x the payload budget)
H  hash-partition left into  256 buckets (seed graceH3): biggest bucket = 90031 rows (22x the payload budget)
H  re-hash the hot bucket with a new seed/modulus: biggest sub-bucket = 90000 rows (unchanged)
S  exmem sort-merge: payload_peak = 2308 slots (<= 4096), temp_bytes_peak = 3956994
```

分区哈希要么让热点桶常驻 ~9 万条（预算的 22 倍），要么对超大桶回退为排序——即混合策略
最终收敛到排序归并，还白付一轮分区 I/O。此外见证顺序语义本来就要求全序，排序的
确定性全序正好满足，故选外部排序。

**SQLite**：未使用。题目允许其内部缓存另限 4MiB，但自研溢写让每一字节、每一槽位都可
直接计量，无需把候选的大列表交给引擎名义隐藏。披露的库级缓冲仅为 Python 文件对象：
每文件 8KiB × 最多约 132 个并发文件 ≈ 1.06MiB < 4MiB（见报告 `config` 段）。

## 4. 资源预算与实测

常驻 payload 槽位预算（容量制记账，获取即计、释放即销，超限抛 `BudgetExceeded`）：

| 结构 | 槽位 |
|---|---|
| 排序 run 缓冲 + 排序装饰键 | 2×1024 = 2048 |
| 归并：128 路 × 4 条 + 堆 | 512 + 256 |
| 顺序读/写缓冲 | 128/个 |
| counts 游标 | 256 |
| **硬上限** | **4096** |

标量计数/文件偏移另统计：计量器自身约十余个 int、归并堆的 run 路径列表（最大中间结果
约 800 个文件名）、连接游标 1 个计数项、逐组审计的当前组 1 项——无藏匿行 payload 的
巨大序列；每组的计数值本身放在已计量的 counts 溢写文件里，不在内存。

**热点基准**（`python3 -m exmem bench`：make_large 每源 10 万行 90% 热点 +
`fixtures/resource-benchmark-plan.json` = `a EXCEPT ALL b`），真实运行：

| 指标 | 实测 | 约束 |
|---|---|---|
| 输入两源字节 | 2,300,001 | — |
| 峰值常驻 payload | **2,308**（构成：counts写128+排序run2048+scan4+键run写128） | ≤ 4096 ✓ |
| 峰值临时磁盘 | **3,956,994 B = 1.72× 输入** | ≤ 6× = 13,800,006 B ✓ |
| 峰值临时行数 | 210,001（左 runs 100k + 左有序 100k + counts 10k） | 披露 |
| 总读 / 总写字节 | 7,767,035 / 5,467,084 | 披露 |
| 输出 | 1 行（热点组 90k⊖90k 全抵消，冷键仅 id=90000 存活） | — |
| 墙钟 | ~0.7s | 无要求 |

**deep-skew-plan @10 万行**（深度 6、源 b 被引用 5 次；此 AST 无磁盘比例约束，仅披露）：
输出 200,000 行；payload 峰值 2,308；临时磁盘峰值 11,368,072 B（1.65× 输入）；
临时行峰值 410,001；读 44.3MB / 写 46.9MB；墙钟 ~3.3s；与内存参考 20 万行逐 witness
全量一致。UNION 高倍输出是已提交输出，不计临时空间，也未误套 6× 预算。

## 5. 验证（tests/test_engine.py，33 项，全绿）

- **小样本与独立内存参考一致**：fixtures 两个计划 + 105 个随机合法计划（深度≤6、叶≤16、
  源≤4，含 NULL/Unicode/热点）+ 20 个 deep-skew 家族变体，全部逐 `(row, witness)` 差分。
- **同一合法输入族的变体**：随机计划生成器 + 偏斜数据生成器（种子固定可复现）；
  另有 25 例在缩小预算（run=8、扇入=3）下强制多趟归并后仍与参考一致——直接验证
  "spill 分区顺序不改变保留来源"。
- **全量核验非仅总行数**：10 万行基准与 10 万行 deep-skew 均与参考做逐出现全量 diff；
  每个 set 算子的计数守恒恒等式（EXCEPT: emitted=left_in−matched；INTERSECT:
  emitted=matched）由引擎自检并断言；输出出现抽样回查源行值；逐组配额日志抽样重算。
- **资源不变量**：payload 峰值 ≤4096（多计划实测）；小预算 16 时必抛 `BudgetExceeded`；
  临时文件零泄漏（计量器与 workdir 双查）；基准临时磁盘 ≤6× 输入。
- **边界**：NULL 相等、码点精确、int/text/null 互异、自交为空、双重扫描身份、空源、
  单行/单列；非法输入（深度>6、叶>16、源>4、未知源/op、列数不齐、>4 列、int64 溢出、
  浮点、布尔、>1KiB 行、非空列出现 NULL）全部拒绝。

## 6. 仍未覆盖的边界

- 1KiB 最大行 × 10 万行的磁盘峰值未实测（基准行约 11–15B）；6× 约束本就只针对
  make_large 行型，但大行型下的临时字节会成比例增大，未给出实测值。
- 多趟归并在 10 万规模未触发（98 runs < 扇入 128）；其正确性由小预算测试覆盖，
  更大规模（>1.3 亿条中间结果）的多趟磁盘峰值为推算而非实测。
- 墙钟未优化：纯 Python 单线程，10 万行基准 ~0.7s、deep-skew ~3.3s；题目无墙钟要求。
- 恰好 1KiB 行、恰好 10 万行源等边界值只过了校验逻辑，未测恰好边界的资源峰值。
- `--audit-groups` 在超高基数的输入上会产生很大的日志文件（按需开启，已作为提交
  产物计量）；默认关闭。
- 浮点/NaN 不在 schema（拒绝），未定义其行为。

## 7. 复现命令

```bash
python3 demo.py                                   # 兼容基线入口（走新引擎）
python3 -m unittest discover -s tests             # 33 项不变量测试
python3 -m exmem bench --rows 100000              # 热点资源基准（6× 与 4096 校验）
python3 -m exmem run --plan fixtures/plan.json --source a=fixtures/a.jsonl \
    --source b=fixtures/b.jsonl --schema fixtures/schema.json \
    --out out.jsonl --workdir tmp --report report.json --audit-groups groups.jsonl
python3 experiments/hash_vs_sort.py               # 策略取舍实验
```
