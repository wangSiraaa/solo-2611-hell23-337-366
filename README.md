# 统计结构演进与挂接属性的双版本数据发布

SDMX 2.1 子集（固定 `COUNTRY/MEASURE/TIME` 三维、本地 codelist、GenericData）下的真实
SQLite 统计仓，支持属性挂接在 dataset / group / series / observation 间迁移、带前置条件
的观测修订、迁移计划绑定与原子发布、旧 DSD 兼容导出与逐修订事实回查。完整语义与设计取舍
见 [design.md](design.md)。

## 模块

| 文件 | 职责 |
| --- | --- |
| `structure.py` | DSD 编译器：维度/组/codelist/属性挂接层/assignmentStatus，含结构拒绝 |
| `data_reader.py` | GenericData 解析：四层原始声明、显式空文本与缺失区分；保留 legacy `read()` |
| `warehouse.py` | SQLite 仓：声明边 + 有效属性事实表、修订、迁移 plan/apply、兼容视图、审计 |
| `oracle.py` | 独立 oracle：不依赖仓裁决代码，按完整键重算每版有效属性 |
| `cli.py` | 命令行：init / plan-migration / apply-migration / ingest-revision / query-version / export-compat / audit |
| `make_fixtures.py` | 生成真实 XML 夹具（多挂接层场景） |
| `validate.py` | 66 项端到端检查，逐观测与 oracle 比对 |
| `scale_test.py` | 100 000 观测 + 50 修订规模实测 |
| `queries.sql` | 10 条真实 SQLite 读回查询 |
| `make_readback_log.py` | 用 legacy 输入构建交付仓并生成读回日志 |

## 运行

```bash
python -m pip install -r requirements.txt   # 仅标准库，无第三方依赖

# 1. 基线缺陷复现 + 修复后行为对照
PYTHONDONTWRITEBYTECODE=1 python baseline_demo.py

# 2. 完整验证（66 项，日志 logs/validation_log.json）
PYTHONDONTWRITEBYTECODE=1 python validate.py

# 3. 10 万观测规模实测（日志 logs/scale_log.json）
PYTHONDONTWRITEBYTECODE=1 python scale_test.py

# 4. 交付仓 + 真实 SQL 读回日志（logs/query_readback.log）
PYTHONDONTWRITEBYTECODE=1 python make_readback_log.py
```

## CLI 工作流

```bash
python cli.py init wh.db legacy/structure.xml legacy/data.xml
python cli.py plan-migration wh.db inputs/structure-v2.xml --plan-id p1
python cli.py apply-migration wh.db p1
python cli.py ingest-revision wh.db inputs/revision-example.json
python cli.py query-version wh.db 1 --out rev1.json
python cli.py export-compat wh.db 2 0      # 不可表达时返回带证据的明确失败
python cli.py audit wh.db --source legacy/data.xml
```

## 关键语义保证

* 迁移以每个旧观测的**有效属性**为基准：细化展开保留来源；粗化仅在新 scope 内旧值全同且
  无 required 缺失时无损，否则计划列出全部冲突观测与来源边，拒绝「最后值覆盖」。
* 0 / missing(NaN) / 显式空文本三态互斥；absent 属性与显式空 `NOTE` 不同。
* 新增必需属性无来源时隔离观测，不构造假默认；迁移器不改单位、不换算、不做代码语义映射。
* 修订由序号 + 完整观测键定身份：同序同内容幂等、同序异内容冲突；前置条件不满足即拒绝。
* 计划绑定 DSD 版本、修订号与内容哈希；期间新观测使计划陈旧则拒绝应用，必须重新计划。
* 发布在单事务内完成，并发读连接只能观察完整修订；中断整体回滚。
* 所有修订事实保留可回查，`current_*` 只是最大修订的视图；旧 DSD 不可表达时导出明确失败
  并给证据，不静默回退旧数据。
