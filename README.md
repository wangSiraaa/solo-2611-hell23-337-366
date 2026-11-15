# 在线保留策略与外键删除计划的可验证提交

面向本地 SQLite 业务库，维护“可人工审阅、且真实执行结果与批准写集逐键一致”的
删除计划。主需求以 `contract.md` 为准，设计取舍见 `DESIGN.md`。

## 运行环境

Python 3.11，仅用标准库（`requirements.txt` 为空）。所有命令在仓库根目录执行。

```bash
python -m pip install -r requirements.txt

# 1) 旧基线：复现“计划后幻影级联被未审阅删除”，并展示新实现拦截为 STALE_PLAN
PYTHONDONTWRITEBYTECODE=1 python baseline_demo.py

# 2) 完整验收（58 项检查，含真实两连接日程与复制库 oracle）
PYTHONDONTWRITEBYTECODE=1 python run_tests.py
```

## 模块

| 文件 | 作用 |
| --- | --- |
| `database.py` | 外键常启连接；12 表业务库 schema/种子（CASCADE/SET NULL/延期 NO ACTION、环、多路径、NOT NULL 置空） |
| `retention.py` | `plan`（只读静态推演+指纹+计划文档）、`apply`（锁内三层重验+单事务真实 FK 动作+逐表核对） |
| `verifier.py` | 隔离复制库 FK 引擎 oracle，与保留规则、批准写集三方联合判断；提交后读回 |
| `auditor.py` | JSONL 审计（事务边界/受影响行/拒绝原因/读写行/锁重试）、逐表读回 |
| `schedules.py` | 另一连接在 before_lock/after_revalidation/before_commit 三点写入、占锁线程 |
| `retention_config.json` | 表角色（root/retained/ordinary）、root cutoff 谓词、retained 保护列与 expires_at 列 |
| `inputs/schedules.json` | 可复跑的真实两连接日程 |
| `run_tests.py` | 验收套件 |
| `DESIGN.md` | 计划格式、锁协议与取舍、oracle 联合判断、审计与复跑说明 |
| `legacy/` | 旧 TEXT-PK 材料，原样保留，仅历史参考，不被新模块依赖 |

## 计划/执行一句话流程

```python
from retention import plan, apply, load_config, save_plan
from auditor import AuditTrail
cfg = load_config("retention_config.json")
doc = plan(db_path, cfg,
           explicit_roots=[{"table": "account", "pk": 3}],
           cutoff=None, plan_clock=1000)      # 只读；BLOCKED 计划含完整阻断解释
save_plan(doc, "plan.json")                   # 人工审批；plan_id 覆盖整份文档
audit = AuditTrail("run.audit.jsonl", logical_clock=1000)
result = apply(db_path, doc, exec_clock=1000, audit=audit,
               hooks={...})                   # COMMITTED / STALE_PLAN / BUSY / ...
```

apply 只在一个 `BEGIN IMMEDIATE` 事务内让 SQLite 引擎触发真实外键动作，并在提交
前逐表核对预测删除集与置空集；任何计划后影响写集/裁决的并发修改返回
`STALE_PLAN` 并保留原库，可重新 plan。外键全程开启，不存在禁用外键的执行路径。
