# 远端变长列的版本一致投影引擎

基于 `range_server.py`（本地单机 HTTP、Range/ETag/If-Match、指定请求后切换对象）
和 `fixtures/` 的 FITS 子集，实现按需读取的版本一致投影。原 `query.py` 下载整文件
再交给 astropy 的行为已替换为 **元数据/行描述符计划 + 按需 heap 读取**，两者共享
同一版本身份（ETag）。

```bash
pip install -r requirements.txt          # numpy, astropy
python3 demo.py                           # 保留的基线正常路径（现在走范围读取）
python3 -m pytest tests/ -q               # 全部不变量测试
python3 -m fitsproj.experiment            # 计划器取舍的可复现实验
```

支持范围：单个 `BINTABLE`、固定 `J` 列 `id`、一个有符号 32 位变长列 `PJ()`、
96–512 行、每数组 0–2048 元素、大端、无压缩。其他布局会被显式拒绝。

## 1. 架构与交付物

```
fitsproj/
  errors.py      异常层次（含 DescriptorBoundsError / VersionConflictError）
  fitsmeta.py    用 Range 流式读取并解析主头/扩展头，推导字节布局
  httpclient.py  单传输通道的范围 GET、ETag 绑定、请求 trace、body 计费
  plan.py        有界区间缓存 + 区间合并（计划器）
  engine.py      prepare（描述符计划）→ execute（heap 读取），重试/版本语义
  generate.py    同族变体 FITS 生成器（含共享/部分重叠 heap、长度置换）
  spec.py        JSON 查询规格解析
  cli.py         命令行入口（query / plan）
  experiment.py  计划器取舍实验
query.py         兼容入口 query(url, rows, start, stop)
range_server.py  本地 Range 服务器（保留旧接口，新增确定性切换）
tests/           不变量测试
```

### 查询语义

* 请求行可任意次序、可重复；**返回顺序与请求严格一致**，每个结果带
  `occurrence`（第几次出现）与 `source_row`。
* 每列/每行可用元素区间 `[start, stop)`；允许乱序、重叠与空区间 `stop==start`；
  空区间返回 `samples: []` 且不产生 heap 读取。
* 两个行描述符可指向同一 heap：`fixtures/a.fits` 的 7 与 83 行即如此。结果中
  两者的 `source_row` 仍分别为 7、83，且共享对象只传输一次。

### 两步式计划（`engine.plan` / `engine.prepare`）

1. **元数据/描述符计划**：只读取 FITS 头块和请求行的 8 字节 P 描述符（`count`、
   `offset`），不读取任何 heap 字节。`engine.plan(...)` 返回可 JSON 序列化的
   计划（含 `etag`、布局、每行描述符、描述符读取区间、请求 trace、字节统计）。
2. **heap 读取**：`prepare(...)` 得到 `PreparedQuery`（绑定 ETag），
   `execute()` 在同一 ETag 下读取 heap。`engine.query(...)` 内部按此两步执行。

## 2. 版本一致性与重试

* 首个头块请求不带 `If-Match`（此时尚不知道版本），此后**所有**请求都带
  `If-Match: <etag>`；服务器对旧版本返回 `412`。
* 任一 `412`（或 206 响应的 ETag 与绑定值不符）都会**丢弃未提交计划**，并从
  头开始**完整重试至多一次**。
* 结果在内存中全部读齐后才返回，因此不会出现“旧描述符 + 新 heap”的混合。
* 若重试中再次切换（切换两次），抛 `VersionConflictError`：**不含任何行**，
  只带各次尝试的字节统计。`demo`/CLI 退出码为 3。
* 服务器保证“任意两个请求之间最多切换一次等长版本”；测试用谓词在
  “读完描述符、第一次读 heap 前”精确触发切换。

## 3. 缓存与字节预算

* **数据页常驻缓存上限 8 KiB**：描述符字节与 heap 字节共用同一预算。
* 已解析的 FITS 头块（本例 5760 字节）经原始 HTTP 客户端读取，**不进入**
  该缓存；其上限单独为 9 KiB（`fitsmeta.MAX_PARSED_HEADER_BYTES`）。
* 结果返回 `trace`（每次请求的 kind/range/If-Match/status/body 字节）、
  `response_body_bytes`（所有尝试的响应 body 总和，重试分别列在 `attempts`）、
  以及 `cache.peak_cache_bytes`。
* `fixtures/queries.json` 的两个冷启动查询实测 body：
  `[7,83]` 窗口 `[0,8)` → **5808 B**；`[4,5,4]` 窗口 `[1000,1024)` →
  **5972 B**，均 < 16 KiB。
* 失败/非法请求：`rows` 为空（结果 body 计为 0），只报告为获取头/描述符
  已消耗的字节。
* 不使用本地完整副本、不使用第二条传输通道。客户端会拒绝忽略 Range 的
  整文件 200 响应。

## 4. 计划器：方案与取舍

**采用方案：绝对偏移的有界区间（segment）缓存 + 缺失区间合并。**
另有两个调节旋钮：`page_size`（把远端读取向外对齐）与
`descriptor_merge_gap` / `heap_merge_gap`（把逻辑上相邻的区间合并）。

* 缓存按绝对文件偏移保存互不重叠的字节段，淘汰按 LRU，常驻总量受
  `cache_capacity`（默认 8192）约束；一次响应若大于预算，只保留尾部。
* 读取前先减去已驻留覆盖，再把**缺失区间**合并成尽量少的 HTTP 请求，因此
  重复行、共享 heap、重叠区间不会重复传输。
* 对齐只以调用方给出的 `[min_start, max_end)` 为界：描述符读取绝不越过
  `heap_start`，heap 读取绝不触碰表/头块——非法描述符不会读到其他区块。

**放弃的方案：固定页网格缓存。** 理由：
1. 描述符与 heap 区域相邻，页对齐会把描述符请求舍入进 heap，违反
   “非法区间不得读取其他区块”；
2. 固定页对 8 字节描述符过度读取，在小预算下浪费明显；
3. 在“描述符 + heap 共用预算”下，区间的精确扣减比页计数更直接。

**可复现实验（`python3 -m fitsproj.experiment`，稳定源上）** 展示实际取舍：

```
scenario                        config                    body  reqs   peak  <=16KiB
two_rows_window_0_8             exact/no-merge            5808     5     48      yes
two_rows_window_0_8             page512/desc-gap8         6912     5   1152      yes
two_rows_window_0_8             page4096/desc-gap8       11008     4   5248      yes
duplicate_row_window_1000_1024  exact/no-merge            5968     6    208      yes
duplicate_row_window_1000_1024  exact/desc-gap8           5972     5    212      yes
scattered_small                 exact/no-merge            5880    12    120      yes
scattered_small                 page4096/desc-gap8       27392     8   8192       NO
```

结论：精确读取（`page_size=0`）在字节上最优，但请求数更多；较大的
`page_size` 减少往返却快速放大 body，在分散小请求时甚至突破 16 KiB。
默认采用 `page_size=0`、`descriptor_merge_gap=8`，在预算内取得较少的请求数。
该实验只体现取舍，不声称最优。

## 5. 版本绑定如何贯穿缓存与未完成结果

* `IntervalCache` 构造时绑定 ETag；每次 206 都校验返回 ETag，不一致即
  `VersionMismatch`。
* `PreparedQuery` 持有 `plan.etag`；`query` 只有在两阶段全部成功后才组装
  并返回结果，异常时不会暴露部分行。
* 重试会新建 `HttpRangeClient` 与缓存，因此旧版本的缓存字节绝不会被新版本复用。

## 6. 关键测试

* `tests/test_semantics.py`：顺序/重复/每列区间/空区间、共享 heap 保留来源、
  生成器 oracle、计划不读 heap、越界行早失败。
* `tests/test_versions.py`：**读到描述符后、读 heap 前确定性切换一次** → 412
  后整体重试成功且全部来自新 ETag；切换两次 → 显式失败且无行；长度置换版本
  证明不会混合。
* `tests/test_bounds.py`：**heap 描述符越界整体拒绝**，且请求日志证明没有读取
  heap 或其他区块。
* `tests/test_budget.py`：fixture 预算、峰值缓存 ≤ 8 KiB、头不计入缓存、
  trace/body 求和一致、重复与共享行不重复传输。
* `tests/test_plan_split.py`：prepare→execute 共享 ETag；陈旧计划读新 heap 得 412。

## 7. 命令行

```bash
# 单个查询
python3 -m fitsproj.cli query --file fixtures/a.fits --rows 7,83 --slice 0:8

# 每行不同区间（分号分隔行，逗号分隔区间）
python3 -m fitsproj.cli query --file fixtures/a.fits \
  --rows 7,83 --intervals "0:3,10:13;1020:1024"

# fixture 全部冷启动查询并逐条核对预算
python3 -m fitsproj.cli query --file fixtures/a.fits \
  --spec fixtures/queries.json --all --out /tmp/all.json

# 只生成/检查描述符计划
python3 -m fitsproj.cli plan --file fixtures/a.fits --rows 7,83 --slice 0:8

# 读完描述符后、读 heap 前切换（--file 重复给出等长版本）
python3 -m fitsproj.cli query --file fixtures/a.fits --file fixtures/b.fits \
  --switch-on-heap --rows 7,83 --slice 0:8

# 切换两次：显式失败，退出码 3
python3 -m fitsproj.cli query --file fixtures/a.fits --file fixtures/b.fits \
  --file fixtures/a.fits --switch-on-heap --rows 7,83 --slice 0:8
```

## 8. 尚未覆盖的边界

* 仅支持单个 `BINTABLE`、`J`+`PJ()`、大端、无压缩；多扩展、其他变长类型
  （`PD()` 等）、`HEAP`/`THEAP` 异常、压缩 HDU 均显式拒绝。
* 不支持跨版本不同的 `PCOUNT`/行宽（切换契约要求等长，这一前提被直接依赖）。
* Header 中 `THEAP` 目前只用于计算 heap 起点；未处理 `THEAP` 小于表尾等
  非标准但可解析的布局。
* HTTP 层固定使用 `urllib`；未实现连接复用/流水线，请求数指标偏保守。
* 大数组与全表查询的总 body 远超 16 KiB（预算只约束 `fixtures` 的两个冷启动
  查询）；这是有意的数据量差异，不是绕过。
