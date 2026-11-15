# 设计说明：规则修订下的事件关联目录

本文档对应实现：`time_rules.py`（规则引擎）、`association.py`（关联引擎）、
`materializer.py`（版本化目录存储）、`catalog.py`（CLI：ingest /
install-rules / query / explain / verify）、`verify.py`（验证套件）、
`demo_upgrade.py`（真实变更日志演示）。旧 API 保留在原模块中，
`baseline_demo.py` 仍可复现旧缺口（元数据换了、关联事实没换）。

## 1. 时间模型与规则表

- 规范轴为整数纳秒 TAI：`tai_ns = (posix_label + TAI−UTC) * 1e9 + nanos`。
  全程整数运算，不经过浮点。
- UTC 文本允许正闰秒 `:60`；仅当规则表在次日午夜声明了 `+1` 跳变时才是
  有效瞬间，否则该事件在该版本下为 `unsupported(leap_second_not_in_table)`。
  `:60` 的覆盖判断用其前一秒标签（闰秒属于前一 UTC 日）。
- TAI 文本（无 `Z`）与 GPS 秒（`+315964800+19`）映射到绝对 TAI，
  **身份不随 UTC 规则表改变**，ingest 时一次算定。
- 规则表声明 `id`、`coverage`（半开 UTC 区间）、`sealed_before`、
  `initial_offset` 与 `changes`（offset 项）。校验：changes 按 effective
  严格递增、必须落在 UTC 午夜、offset 逐项 `+1`（仅正闰秒）、
  `coverage[0] <= sealed_before <= coverage[1]`、id 不与已装版本重复。
- 封存一致性：新表 `sealed_before` 不得回退；`initial_offset` 不得变；
  `sealed_before` 之前的 offset 项与 coverage 截断区间必须逐点相同。
  之后的暂定覆盖可更正（如删/改暂定闰秒）或延伸（释放原 unsupported 事件）。
- 上限：事件 100000、规则版本 3；每个版本快照都保留事件原始表示
  （scale/value）与所用规则表全文。

## 2. 规范化审计范围（只含规则可能改变的 UTC 事件）

`sealed_before` 之前两表逐点相同，因此一次修订的**影响范围**只有：
`label >= sealed_before` 的 UTC 事件（首版则为全部 UTC 事件）。
TAI/GPS 事件绝不进入审计（`tai_gps_events_not_rescanned` 计数）。
影响范围内重扫但未变化的事件只计入读取（`utc_events_rescanned`），
不计入结果变化（`events_changed`）——读取与结果变化分开计量。

## 3. 索引与物化

- **原始事件索引**：`id -> 原始事件`（ingest 校验后落盘 `events/raw.json`，
  含解析缓存；拒绝项带原因单列）。
- **规则影响范围索引**：UTC 事件按 naive label 排序，按 `sealed_before`
  二分切出影响集（实现上随原始索引一并维护解析缓存）。
- **配对索引**：每版本 `key -> {A: [(tai,id)…], B: […]}` 有序表，区间
  二分查询；配对结果按版本物化在 `versions/vN.json`（含每对的
  tai_a/tai_b/window 与窗口汇总），窗口汇总另存 `windows_by_index`
  供局部更新。
- **局部重算**：配对是否变化只取决于两端点的 tai，因此重算范围 =
  每个变化事件在其**旧位置与新位置**的 tolerance 邻域（旧索引查旧邻域、
  新索引查新邻域），可读到未变事件作邻居（`unchanged_neighbors_read`
  计量），配对候选扫描数（`pair_candidates_scanned`）单独计量。
  窗口汇总只重算受影响窗口：配对 insert/retract 改变成员；配对身份不变
  但 A 跨窗口时记为同一身份的显式 out+in（`pair_updates`）。

### 全量重算 vs 局部重算

| 场景 | 全量重算 | 局部重算（本实现） |
|---|---|---|
| 边界移动（闰秒前后 1s 移动） | 重扫全部事件与全部配对 | 只重扫影响集 UTC 事件；配对只查变化事件新旧邻域 |
| 失效事件（supported→unsupported 或反向） | 自然覆盖 | 旧位置邻域必在重算范围内，retract 精确产生 |
| 旧版本查询 | 需另存历史 | 每版本自包含快照，最近两版（实际全部 ≤3 版）可查 |
| 成本 | O(事件+配对) | O(影响集 + 邻域)；真实计量见 `out/demo_db/changes/delta_2.json`：`utc_events_rescanned=1`、`unchanged_neighbors_read=1`，而结果变化为 `events_changed=1`、`pairs_inserted=1` |

全量独立 oracle 仍然保留：`catalog.py verify` 与 `verify.py` 中的暴力
oracle 从原始事件重新构建每版结果，与物化快照逐配对、逐窗口值比较。

## 4. 原子安装与失败语义

`install-rules` 全部计算在内存完成后，先写 `versions/vN.json.tmp` 与
`changes/delta_N.json.tmp` 并 rename，最后原子替换 `catalog.json`
（提交点）。校验失败或注入异常（`CATALOG_FAULT_AT=pre_association |
pre_commit`，对应"关联更新前注入异常"）时目录保持旧版本字节级不变——
不会暴露一半新规范时间配一半旧配对。

## 5. 变更日志与可重放 delta

每次安装产生 `changes/delta_N.json`：

- `event_changes`：规范事件净变更（id、old、new，含 unsupported 原因）；
- `pair_inserts` / `pair_retracts` / `pair_updates`：配对身份级变更。
  身份恒为 `(A_id, B_id)`；两端同时移动而身份不变时记 `pair_updates`，
  不产生 insert/retract；同一对撤回后再插入在不同版本中各记一笔，
  `explain --pair` 可完整回放其时间线；
- `window_deltas`：每窗口 `pairs_in`/`pairs_out`（身份显式列出）+
  `count_delta` + 变更后汇总。净数为 0 也不抵消身份变化；
- `stats`：读取与结果变化分开的计量。

`verify.py` 的独立小消费者从空状态重放全部 delta，逐版本与
`query` 输出完全相等（含窗口汇总），并校验 logged window delta 与其
自身推导一致。

## 6. 验证覆盖（verify.py，20 项）

正闰秒前/中/后纳秒（…835/…836/…837 及 +1ns）、等价 TAI/GPS、
暂定表补闰秒（inputs r1→r2，关联 0→1）、覆盖延伸释放事件、
非法表与已封存历史变化拒绝、关联 1→0（暂定更正）、A 跨窗口边界、
两事件同时移动 pair 身份不变、关联更新前注入异常的原子性、
bag 不去重、窗口净零不抵消身份、读取/变化分别计量、3 版上限与
最近两版查询、100000 事件上限、旧缺口复现保持。
每版都真实重读原始事件并重建 oracle 逐值比较。

## 7. 非目标

DST、负闰秒、未来真实闰秒预测、NTP/时钟校准算法、通用流平台、
分布式时钟。
