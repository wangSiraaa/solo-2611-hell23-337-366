# 设计说明：版本化序列资料加工（apply-revision / extract / trace）

## 1. 问题与基线缺陷

`baseline_demo.py` 复现的缺陷是：`sequence_store.apply` 只拼接新序列，
`features.extract` 仍按**旧坐标**在新序列上切片。一次替换删除了 f2 的全部原碱基、
又在 f1 前方插入了 2 个碱基后，f1 被静默读成 `TNTT`（存活原碱基实际应为 `GTGG`），
f2 被误读为 `GG`（实际已空）。根因是坐标与来源没有随编辑联动，且不区分
“存活原碱基”与“编辑新增碱基”。

本实现不做任何生物预测、比对或蛋白翻译，只做**坐标与来源的保真更新**。

## 2. 总体方案：piece-table + 每修订增量 + 派生坐标

### 2.1 序列层（sequence_store.py）

采用 **piece-table**：

* 原件 `origin.fa` 只保留一份，永不重写；
* 每个版本的序列表示为 piece 列表，piece 只有两类：
  * 原碱基 `("O", origin_start, origin_end)`，指向原件区间；
  * 新增 `("A", revision, edit_id, adds_offset, length)`，指向该修订 adds blob 的区间。
* 一次修订只产生：
  * `rev-N.adds.bin`：本修订所有 insert 拼接的新增字节（编辑来源）；
  * `rev-N.seq.json`：编辑记录（坐标、id、order、在 blob 内的偏移/长度）与校验 sha。
* piece-table 不整体落盘：重开时从原件单 piece 出发，按各修订 seq.json **重放**
  拼接得到任意版本。因为最多 20 版、每版编辑数有限，重放是 O(版本数 × 编辑数 +
  piece 数)，在给定规模下是亚毫秒到毫秒级，无需每版本复制序列。

### 2.2 feature 层（features.py）

每个 feature 的每段维护一条**溯源原子链**，原子与 piece 同源：

* `["O", start, end)`：来自原 FASTA 的连续原碱基；
* `["A", revision, edit_id, adds_offset, length]`：来自某次编辑插入的连续新碱基。

关键决策：**段坐标不落地，永远由当前版本 piece-table 的绝对区间反查**。
因此“编辑发生在段前方导致整体平移”不会产生陈旧坐标，也不占任何增量；
feat.json 只持久化原子链真正发生变化的段。空段（整段删除）原子链为 `[]`
并永久保留 `empty`，不会被后续插入复活。

### 2.3 粘附与归属语义（严格按 contract）

对段 `[ps, px)` 与编辑 `replace(s,t,ins)`：

1. 先删除旧坐标 `[s,t)` 的原碱基——删除范围内的原映射一律移除；
2. 新增 `ins` 锚定于删除前的 `s`；
3. `ins` 归属该段，当且仅当：
   * `ps < s < px`（锚严格在段内部），或
   * `s == ps` 且 `left == include`，或
   * `s == px` 且 `right == include`。
4. 锚在段外则**绝不**纳入，即使删除范围跨入该段；左右 `exclude` 都不纳入端点插入。
5. 不把替换后包络内的新字节自动纳入；部分替换的新碱基来源标 `edit`，
   存活原碱基来源仍是 `origin`，不虚报。
6. 负向 feature：输出段序反转、每块来源方向标记为 `-`、碱基做反向互补
   （A↔T、C↔G、N→N），顺序与方向一起反转。

实现上 `_splice_segment` 用段内相对坐标单遍扫描，`walk` 输出存活原子、
`erase` 移除被删映射、再按粘附规则决定是否挂新增原子，天然精确表达
“段穿越替换”时存活来源与新增来源的交错。

## 3. 方案取舍：长编辑链 / 反向多段 / 回查成本

| 维度 | 本方案选择 | 代价 | 为什么在本题规模下合适 |
|---|---|---|---|
| 序列组织 | piece-table，不存每版全量 | 读取旧版本需从 v0 重放拼接 | 最多 20 版、总新增 ≤2MiB，重放只需扫 piece 与小的 seq.json，实测 1MiB×20 版重建 <10ms |
| 坐标 | 由 piece-table 派生，不落地 | 每次 locate/trace 做二分定位 | 省掉“仅平移”类增量，避免陈旧坐标；piece 数有界，二分 O(log piece) |
| feature 索引 | 只存原子链变化段 | 读旧版本需回放 feat 增量链 | 增量通常很小；10000 feature 中绝大多数版本不变，零写入 |
| 反向多段 | 原子链保持正向存储，输出时反转 | 反转在查询期做 | 同一份原子链同时服务 + / - 语义，不重复存储；段 ≤20，反转成本可忽略 |
| 回查（trace） | 每个输出块带 origin/edit 来源与新坐标 | 块元数据有少量 JSON 开销 | 满足“逐字节可由原 FASTA 片段/编辑插入片段重建”，无需逐碱基文件 |
| 替代方案：每版全量快照 | 读取 O(1) | 20×1MiB 起，远超预算 | 直接违反“新增持久字节 < 源长度”，否决 |
| 替代方案：rope/平衡树 | 任意位置编辑 O(log n) | 结构复杂、持久化与恢复成本高 | 本题编辑按修订批量、版本数少，piece-table 的线性拼接已足够且更易审计 |
| 替代方案：纯版本差量坐标映射（无 piece） | 更省空间 | 反向多段、跨段替换的来源交错难表达 | piece-table 天然同时承载序列与来源，语义单一可靠 |

## 4. 磁盘布局与原子发布

```
store/
  origin.fa              唯一一份原件（FASTA 原字节）
  feature-defs.json      不可变 feature 规范（含每段 left/right 粘附）
  rev-N.adds.bin         该修订新增字节（编辑来源片段）
  rev-N.seq.json         编辑记录、长度、adds/seq sha256
  rev-N.feat.json        feature 原子链增量 + content_changed/coordinate_moved
  manifest.json          已发布版本清单（最后原子 rename）
```

发布顺序（`Store.apply_revision`）：

1. `_validate` 全量校验；任何错误抛 `RevisionRejected`，**在写任何文件之前**拒绝；
2. 原子写 adds.bin、seq.json（tmp + `os.replace`）；
3. （可注入 `after_sequence` 崩溃）；
4. 计算并原子写 feat.json；
5. （可注入 `before_commit` 崩溃）；
6. 最后原子替换 manifest.json。

manifest 是唯一的“可见性”来源：未登记的文件在 `Store.open` 时被当作孤儿清理，
因此崩溃后旧版本仍可读、新版本对 `extract` 不可见，也不会被误认为完整。
串行修改，不处理多写者。

## 5. 校验与失败语义

* base 不匹配、target ≠ base+1、越界、非 ACGTN、edit id 重复、旧坐标重叠、
  同点多项缺唯一显式 order、同点含非零长区间——整批 `RevisionRejected`，磁盘无变化；
* 版本 ≤20、原件 ≤1MiB、累计新增 ≤2MiB、feature ≤10000、每 feature ≤20 段；
* 小写输入归一为大写；N 不做任何互补猜测之外的推断（N 的互补仍是 N）。

## 6. 验证策略（verify.py）

* 独立 oracle：用“字符 + 来源标签”列表逐字编辑，**不调用生产内部函数**；
  每段独立维护标签在完整序列中的位置，重复段、同点插入、删除均无歧义；
* 覆盖：端点粘附四种组合、同点插入 order、跨两段替换、整段删除保留 empty、
  负向 join 反转、N、重复区段、部分替换来源不虚报、原子发布两种崩溃点、
  全部上限拒绝、1MiB 预算，以及固定种子的随机属性测试（数百个被接受修订
  逐版本对照序列/碱基/坐标）；
* 每个版本都从磁盘**重新打开** store 核对，验证真实可重读，而非只比内存对象。
