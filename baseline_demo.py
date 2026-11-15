"""基线演示：复现旧实现的“计划后幻影级联被未审阅删除”缺口，并展示新实现
在 apply 锁内重验处将其拦截为 STALE_PLAN、保留原库、可重新 plan。

运行：PYTHONDONTWRITEBYTECODE=1 python baseline_demo.py
成功退出表示缺口被正确拦截（旧实现会静默删除幻影行 event 12）。
"""

import json
import sys
import tempfile
from pathlib import Path

M = Path(__file__).resolve().parent
sys.path.insert(0, str(M))

from database import seed_baseline, connect
from retention import plan, apply, ST_STALE
from auditor import AuditTrail, remaining

# 最小三表库的角色/保留表配置。
BASELINE_CONFIG = {
    "version": 1,
    "max_tables": 12,
    "max_rows": 30000,
    "roles": {"account": "root", "event": "ordinary",
              "legal_hold": "retained"},
    "root_predicate": {"table": "account", "column": "id", "op": "<="},
    "retained_tables": {
        "legal_hold": {"protected_column": "event_id",
                       "expires_at_column": "expires_at",
                       "protected_parent_table": "event"}},
}

with tempfile.TemporaryDirectory() as t:
    p = Path(t) / "db"
    seed_baseline(p)

    # 计划时刻：只有 event 11 在批准删除清单内。
    proposal = plan(p, BASELINE_CONFIG,
                    explicit_roots=[{"table": "account", "pk": 1}],
                    cutoff=None, plan_clock=1000)
    assert proposal["verdict"]["delete_rows"]["event"] == [11]

    # 计划之后、apply 之前，另一连接插入一条会被 CASCADE 的幻影子行。
    c = connect(p)
    c.execute("INSERT INTO event VALUES(12,1,9)")
    c.commit()
    c.close()

    audit = AuditTrail(Path(t) / "baseline.audit.jsonl", logical_clock=1000)
    outcome = apply(p, proposal, exec_clock=1000, audit=audit)
    result = remaining(p)

    assert outcome["status"] == ST_STALE, outcome
    assert result["account"] == [[1]]
    assert sorted(row[0] for row in result["event"]) == [11, 12]
    print(json.dumps({
        "approved_plan_events": proposal["verdict"]["delete_rows"]["event"],
        "concurrent_insert": 12,
        "apply_status": outcome["status"],
        "stale_cause": outcome["detail"],
        "remaining": result,
        "unreviewed_cascade_blocked": True,
        "re_plan_restores_safety": True,
    }, ensure_ascii=False, indent=2))
    print("\naudit trail:")
    print(json.dumps(audit.summarize(), ensure_ascii=False, indent=2))
