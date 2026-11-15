# 设计说明：可验证 STAC 副本联合取数

本文件随源码一起交付，说明四个命令（`build` / `plan-query` / `fetch-query`
/ `inspect`）背后的空间索引与精确过滤、副本选择策略、范围缓存与并发控制。
真实运行日志见 `out/verification-log.json` 与 `out/verify-run.txt`。

## 1. 模块划分

| 模块 | 职责 |
| --- | --- |
| `catalog.py` | STAC 1.0 受限子集的发现、严格校验；真实矩形部件抽取；日期线拆分；闭时间区间与精确矩形谓词。保留基线 `discover`/`search`。 |
| `manifest.py` | 资产清单 sidecar：身份 `(collection,item,asset_key,content_sha256)`、副本元数据、4096 字节块摘要。 |
| `replica.py` | 本地真实文件适配器（整文件型 / 可范围型）、HEAD/GET 真实读取与故障注入。保留基线 `Origin`。 |
| `store.py` | 不可变、按 `build_id` 内容寻址的 SQLite 目录，含空间候选索引与构建事件。 |
| `planner.py` | `plan-query`：固定 build、候选→精确过滤、记录命中部件与每个资产身份。 |
| `cache.py` | 内容寻址的“已验真”对象/块缓存，原子发布、每键锁、来源溯源、计划引用计数与回收。 |
| `fetcher.py` | `fetch-query`：选择策略、副本回退、按块/整文件验真、范围包与不完整包。 |
| `inspect_cmd.py` | 仅重读 SQLite 与包清单（不碰源 JSON / 原文件），并对包内字节重新哈希。 |
| `cli.py` | 四个命令的命令行入口。 |

## 2. 空间候选索引与精确过滤

普通 Item `bbox` 跨越日期线时会退化成“覆盖整个地球”的宽矩形，不能作为命中
依据。设计因此分两层：

1. **候选索引（SQLite `item_parts` 表）**：存储的是几何的**真实矩形部件**，
   而不是宽 bbox。`MultiPolygon` 的日期线几何被存成两条独立部件
   （如 `[179,-1,180,1]` 与 `[-180,-1,-179,1]`）。表上建有
   `(build_id, w, e, s, n)` 索引，查询用边界重叠条件取候选。
2. **精确过滤（Python 谓词）**：候选必须再通过真实部件的精确矩形相交
   （边界相交易于命中）与闭时间区间相交 `item_start <= q_end and
   item_end >= q_start`。宽 bbox 永远只用于候选，不用于判定。

`west > east` 的查询在进入索引前被 `split_bbox` 拆成两块普通矩形
（`[west,180]` 与 `[-180,east]`），每块都满足 `west<=east`，再分别取候选、
合并。计划里同时记录命中的部件下标与对应的查询块下标，便于审计“为什么命中”。

**空间 oracle**（`verify_pipeline.py`）绕开 SQLite，直接枚举源 STAC 的真实
矩形部件做同样的精确判定；60 次随机查询 + 三个固定场景下，计划结果必须与
oracle 完全一致。这直接复现并修复了基线中“日期线 Item 误命中地球中部”的
缺陷：中部 bbox 查询里，日期线 Item 连候选都不是。

## 3. 时间语义

仅支持闭区间：`datetime` 非空则退化为 `[t,t]` 瞬时区间；否则用
`start_datetime/end_datetime`（`center` 即此情形）。端点相交算命中，因此
`center` 在其结束时刻仍命中，结束后 1 秒即不命中（测试 `t-endpoint`、
`t-after`、`t-before` 覆盖）。

## 4. 资产身份与副本

* 身份四元组 `(collection_id, item_id, asset_key, content_sha256)`。同名
  `asset_key` 在不同 Item 下身份不同，**绝不跨 Item 合并**（测试
  “same asset_key across items is NOT merged”）。
* 每个副本带 `url, sha256, size, logical_version, range_capable`。
  `range_capable=False` 的整文件型副本被要求范围读取时，只能整文件读，
  绝不假装只读了请求字节（访问日志里 `start=null`、返回字节=整文件大小）。
* 块摘要是每 4096 字节一个 sha256，范围取数按块验真；未读块不宣称已校验，
  包清单显式写出 `unverified_claim`。

## 5. 副本选择 / 范围缓存策略（含成本决策）

每个资产在计划执行时记录一次 `selection_decisions`，决策依据请求覆盖、
是否存在可范围副本以及实际读取成本：

1. 整资产请求 → **全文件预验**（一次整读 + 整内容 sha256）。
2. 范围请求且无任何当前可用的可范围副本 → **全文件回退**：整读、整验后只
   打包请求切片；访问日志如实记录整文件返回字节。
3. 范围请求、可范围副本可用，但覆盖比例
   `Σ块字节/文件大小 ≥ full_file_threshold`（默认 0.5）→ 全文件更划算，
   选全文件。
4. 否则 → **按块验真**：把请求范围外扩到整块，相邻块合并成尽量少的连续
   range GET，每个返回块对照目录块摘要验真。

**缓存**：整对象按内容 sha256 寻址，块按块摘要寻址。两个查询共享某块时，
第二个查询该块命中缓存、副本返回字节为 0。基准 `range-benchmark.json` 的两个
重叠查询（`[100,200)` 与 `[150,250)`，都落在块 0）实测：共享计划总返回
4096 字节，远小于两次全文件的 24576 字节基线（测试
“shared-block plan beats full-file baseline”）。策略并不保证每次都省——
高覆盖或无 range 副本时主动选全文件——但保证至少存在一种合法计划更省。

## 6. 真实检查：HEAD 与 GET 都验

适配器只做真实本地文件读取，故障也是真实发生：

* **HEAD 元数据撒谎**（`head_lie`）→ size 不符即跳过该副本。
* **GET 字节损坏**（`corrupt_transport` 在适配器出口翻字节，磁盘文件不动）
  → 块摘要或整内容 sha256 失败，回退下一副本；无回退则资产失败。
* **首副本不可用**（`make_unavailable` 真重命名文件）→ HEAD/GET 报错回退。
* **同 URL 换内容**（`swap_content` 真覆写文件）→ 验真发现内容 sha 与计划
  固定身份不符，拒绝并回退；计划内容最终仍等于原始字节 oracle。
* **陈旧逻辑版本**：副本自身 sha 自洽但不等于资产 `content_sha256` 时，
  build 阶段记 `stale_replica_content`，fetch 阶段整读校验后跳过。

所有 HEAD/GET 都进入 `access_log`，含方法、URL、起止、请求/返回字节、状态与
备注；`access_totals` 汇总调用数与字节数，`cache_stats` 记录命中、发布、
丢弃临时文件与缓存占用。

## 7. 并发与半写缓存

缓存只允许“已验真”字节被共享：

* 写入先进私有临时文件（`tmp/` 下 UUID 唯一名，`0600`），验真通过后在
  **每键 RLock** 内单次 `os.replace` 原子发布。
* 控制点 `before_get` / `after_get_before_verify` / `before_publish` 都是
  可注入回调；`before_publish` 在持锁状态触发，测试用 `threading.Event`
  让线程 A 停在发布前，断言此刻缓存中既看不到该对象、`get()` 返回 None，
  线程 B 只能阻塞或在 A 发布后命中，绝不会读到 A 的临时文件。
* 发布竞态的两种合法结局都被接受：B 输掉发布则丢弃自己的临时文件
  （`discarded_temps`），或 B 观察到已发布对象走缓存命中；任一情况下最终
  只有一个内容对象，两个计划都持有引用。

## 8. 目录版本固定与缓存生命周期

* `build_id` 是对全部摄入内容（Item 部件、时间、资产身份、副本、块摘要、
  清单来源）规范化哈希得到；新快照（新增副本、bump `logical_version`）产生
  不同 build。计划序列化后自带 `build_id`，新版发布不改变旧计划，旧计划仍
  保留旧副本集合。
* 缓存对象记录全部来源 `(build_id, identity, replica_url)`；同内容跨快照
  可复用一个对象，但不同快照来源都保留（测试 twin 共享对象的双来源）。
* `release_plan(plan_id)` 删除该计划的全部引用；仅当某对象再无任何计划引用
  时才回收文件。共享块在第一个计划释放后仍保留，最后一个计划释放后 footprint
  归零。

## 9. 不完整包语义

任一资产所有副本都失败时，包仍写出，但 `complete=false`、
`failed_identities` 明确列出失败身份与原因，已成功资产照常交付，CLI 以
退出码 2 区分。部分成功绝不被当作全部成功。

## 10. 范围边界（明确不做）

不做栅格解码、复杂几何（仅轴对齐矩形 Polygon/MultiPolygon）、远端云认证与
前端；上限 10000 Item / 30000 资产 / 每资产 3 副本，超限或身份冲突、重复
Item、未知 collection、非矩形几何、倒置时间区间均在 build 阶段拒绝。

## 11. 复现方式

```bash
python make_fixtures.py                       # 生成确定性本地夹具
python verify_pipeline.py                     # 82 项断言 + 两个 oracle
python baseline_demo.py                       # 基线缺陷仍可复现（保持兼容）
# CLI 四命令见 README.md
```
