# 十进制列仓的分片在线语义迁移

在旧基线（`shards.py` / `migration.py` / `demo.py`，保持可运行）之上，本仓库新增一套
带迁移计划、逐分片转换、版本查询、读者协议、磁盘配额与崩溃恢复的在线语义迁移系统：

| 文件 | 作用 |
| --- | --- |
| `warehouse.py` | 核心库：目录(catalog)、逐行转换、迁移、对账、读者、配额、恢复 |
| `cli.py` | 命令行入口：init / plan / migrate-shard / begin-read / end-read / query / scan / status / gc / recover |
| `demo_upgrade.py` | 用 fixtures 关键场景走一遍完整新能力 |
| `experiment.py` | 可复现实验（工程取舍的真实测量） |
| `tests/test_invariants.py` | 46 个针对不变量的 pytest |

旧基线问题：`migration.py` 用 `safe=False` 缩 scale 直接截断，未迁移分片的 target 查询又
`round(sum(old))`，两条路径金额不同。新系统统一为**先逐行 convert 再聚合**：
`sum(convert(each))`，禁止 `round(sum(old))`。fixture a 上旧口径得 `3.03`，新口径得 `3.04`。

## 快速开始

```bash
pip install -r requirements.txt
python demo.py                 # 旧基线（保留）
python demo_upgrade.py         # 新能力端到端演示（fixtures 关键场景）
python -m pytest tests/ -q     # 不变量测试
python experiment.py           # 取舍实验（真实测量）

# CLI 一例
python cli.py init /tmp/wh --src fixtures/a.parquet --src fixtures/b.parquet --src fixtures/safe.parquet
python cli.py plan /tmp/wh --precision 6 --scale 2 --rounding half_even
R=$(python cli.py begin-read /tmp/wh --schema 1 | python -c "import json,sys;print(json.load(sys.stdin)['reader'])")
python cli.py migrate-shard /tmp/wh a        # → published，摘要含逐行损失恒等式
python cli.py migrate-shard /tmp/wh b        # → rejected（溢出，退出码 0：这是正确结果）
python cli.py query /tmp/wh --schema 2       # → MigrationFailed（退出码 4），不给部分成功金额
python cli.py query /tmp/wh --reader $R      # → 老读者仍看原值 10003.0000
python cli.py end-read /tmp/wh $R && python cli.py gc /tmp/wh
python cli.py status /tmp/wh                 # data_bytes / quota_bytes / meta_bytes 分列
```

退出码：`0` 成功（含 overflow 拒绝）；`2` 计划非法；`3` 配额背压（可重试）；
`4` 迁移失败/schema 已退役；`5` 读者超限；`6` 迁移冲突；`42` 模拟崩溃。

## 语义定义

- **逐行转换**：每个非 NULL 原值 `x` → `c = quantize(x, scale, rounding)`，
  `rounding ∈ {half_even, toward_zero}`；`loss = x - c` 为精确 Decimal（可正可负）。
  `len(c.digits) > p` 即溢出。NULL 原样保留。迁移路径与读时转换路径共用同一函数
  （`warehouse.convert_value` / `_convert_values`），因此两条路径金额必然一致。
- **溢出**：整片拒绝发布（不少一行、不改行数），失败明细（行 id、原值、转换值、原因）
  落 `meta/<shard>__v<n>.failed.json`；旧片保持可读；目标 schema 查询对该片明确抛
  `MigrationFailed`，绝不混入部分成功金额。放宽 plan 后可重试。
- **对账**（发布前强制）：总行数、非 NULL 数、行 id 序列、逐行 `old - new == loss`
  （正负抵消也逐行核对）、`sum(old) == sum(new) + sum(loss)`、文件聚合 == 逐行转换聚合。
  全部通过才允许发布；逐行明细存 `meta/<shard>__v<n>.recon.json`。
- **查询**：`begin-read --schema N` 绑定 (逻辑 schema 版本, 目录快照) 并钉住所用文件；
  聚合为 `sum/count/min/max/rows`（NULL 不参与）。已发布分片用校验过的预计算摘要
  （`--no-summary` 可回读物理文件），未迁移分片读时逐行转换，失败分片明确报错。
- **配额**：`quota = 启动时已有分片总字节 + 2 × 最大单片新文件字节`，全部按
  `os.stat` 实测物理字节；目录元数据（catalog.json + meta/）单列统计不计入。
  发布新片前检查：先回收无读者引用的旧片，仍不足则删除 staging 并返回可重试
  背压（退出码 3），绝不删除活读者依赖。

## 工程取舍（采用 vs 放弃）

**1) 查询时转换 vs 预计算目标摘要 —— 采用混合边界。**
已发布分片：迁移时一次性逐行转换，摘要（rows/non_null/sum/min/max/loss_sum/source_sum）
经“重读物理文件再聚合”校验与逐行语义严格一致后入目录，查询直接合并摘要；
未迁移分片：查询时逐行转换（无摘要可用）。放弃“全量预计算后切换”：需要约 2 倍全量
空间，违反配额且失败代价整批作废；放弃“永远查询时转换”：每次查询 O(全量行数) 且
没有可校验的持久产物。实验 A（8 片 × 1 万行）实测：读时转换 91.8ms/次、
回读物理文件 28.7ms/次、预计算摘要 1.2ms/次，三者金额完全一致。

**2) 读者引用 × 受限空间 → 迁移次序与旧片回收时点。**
一次只迁一片（目录迁移锁），逐片“转换→对账→发布”，旧片回收只发生在三个时点：
发布时配额压力、`end-read` 之后的显式 `gc`、背压前的尝试性回收；被任何读者引用的
文件永不删除。放弃“迁移完成前不回收”：峰值空间不可控；放弃“发布即回收”：
无压力时旧 schema 查询会提前死掉（旧片本可继续服务）。实验 B 实测：老读者钉住全部
旧片时第 3 片迁移返回可重试背压（缺 104307B），`end-read` 后重试即成功；
无读者对照组峰值 1120841B ≤ 配额 1121056B，全程无全量双份。

**崩溃恢复**：staging 写完（`after_output_before_manifest`）→ 对账 → 配额 →
原子 rename → 原子目录提交（`after_manifest_before_old_reader_release`）。
目录只引用 rename 后的完整文件；重开时回收 staging 孤儿与目录未引用的碎片、
校验引用完整、清迁移锁（`recover`）。两个命名中断点均有测试覆盖。

## 真实运行结果（本机，可复现）

- `python -m pytest tests/ -q` → **46 passed**（half_even 边界、NULL、溢出、
  正负损失抵消、a 片先/后转换一致、读者协议、配额背压、两个崩溃点、
  随机合法输入族变体、单片 1 万行、CLI 端到端）。
- `python demo.py` → `{"sum": "3.00"}`（基线保留）。
- `python experiment.py` 关键数字：见上两段的实测值；
  同一数据 schema-1 老读者看到 `-2982792.9781`，schema-2 新语义为 `-2982793.45`。
- `python demo_upgrade.py`：fixture a 迁移前后 schema-2 查询均为 `3.04`
  （旧错误口径 `3.03`）；fixture b `9999.9950→10000.00` 超 p=6 整片拒绝；
  两个命名中断点恢复后目录无半文件、旧片按读者引用保留/回收。

## 仍未覆盖的边界（如实说明）

- 单写者顺序模型：flock 只串行化目录读-改-写；多进程并发迁移/恢复的争抢未做压力测试。
- 读者崩溃不释放会永久钉住旧片，需人工 `end-read`；无租约/TTL。
- plan 变更若发生在旧片已回收之后，读时转换的源是该片当前活动版本（可能经多步
  舍入，与“从最初原值直接转换”不同）；首次迁移波次内不存在此问题。
- 目录 JSON 无校验和/版本链；Parquet 压缩编码未调优；行 id 唯一性不强制（对账按位置）。
- 性能未做基准优化（逐行 Python Decimal 转换，1 万行/片量级足够）。
