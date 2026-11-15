# 序列修订驱动的特征坐标与来源重建

版本化序列资料加工：对已有 feature 资料做**坐标与来源保真更新**，
不做生物预测、比对或蛋白翻译。提供 `apply-revision`、`extract`、`trace` 三类能力。

## 模块

| 文件 | 职责 |
|---|---|
| `sequence_store.py` | piece-table 版本化存储；校验、原子发布、序列重放、存储审计 |
| `features.py` | feature 溯源原子链；粘附语义、坐标派生、extract/trace/受影响分类 |
| `pipeline.py` | 串联层 `RevisionPipeline`；保留旧 `run()` 以复现基线缺陷 |
| `cli.py` | 命令行入口 |
| `verify.py` | 独立字符标签 oracle + 17 项端到端测试（真实重读每版） |
| `DESIGN.md` | 方案与取舍（长编辑链 / 反向多段 / 回查成本） |
| `AUDIT.md` | 按实际落盘字节的存储与查询审计 |

## 快速开始（Python 3.11，仓库根目录）

```bash
PYTHONDONTWRITEBYTECODE=1 python3 baseline_demo.py     # 复现旧实现的坐标漂移
PYTHONDONTWRITEBYTECODE=1 python3 verify.py            # 全部验证（17 项）
```

CLI：

```bash
python3 cli.py init --store .work/d --fasta inputs/reference.fa --features inputs/features.json
python3 cli.py apply-revision --store .work/d --base 0 --target 1 --edits inputs/edits.json
python3 cli.py extract --store .work/d --version 1 --id f1   # GTGG（旧实现误报 TNTT）
python3 cli.py trace   --store .work/d --version 1 --id f1   # 每字节 origin/edit 来源
python3 cli.py affected --store .work/d --version 1          # 内容改变 vs 仅坐标移动
python3 cli.py audit   --store .work/d
```

## 语义要点

* 编辑 `replace(start,end,insert,id)` 为绑定 base 版本的 0 基半开区间；先删 `[start,end)`，
  新增锚定于删除前的 `start`；同点零长插入按显式 `order`。
* 新增片段归属某段，仅当锚严格在段内部，或恰为端点且对应粘附为 `include`；
  锚在段外绝不纳入，即使删除跨入该段。
* 原碱基被删则移除其映射；整段删除保留 `empty`；部分替换的新碱基来源标 `edit`，
  存活原碱基仍标 `origin`，不虚报。
* 负向 feature 输出段序与来源方向一起反转并反向互补；N 不猜测。
* base 不匹配 / 非法重叠整批拒绝且不落盘；发布期崩溃后旧版本可读、新版本不可见。

## 预算结论

1MiB 源、20 次各插入 1 字节：新增持久字节 **13,253 < 1,048,576（源长）**，
详见 `AUDIT.md`。不做任何每版本全量序列/feature 文本复制。
