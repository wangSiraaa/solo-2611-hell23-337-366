"""审计：事务边界、实际受影响行、拒绝原因、读/写行与锁重试统计。

审计落 JSONL（每行一个事件），不读墙钟：事件只用单调递增序号和调用方提供的
逻辑时钟标记。另保留逐表读回能力与 verify 读回检查（auditor.remaining）。
"""

import json
import threading
from pathlib import Path

from database import connect


class AuditTrail:
    """线程内追加写的 JSONL 审计轨迹；两连接日程可从同一进程记录。"""

    def __init__(self, path, logical_clock=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._seq = 0
        self.logical_clock = logical_clock
        self.events = []
        self._initial = self.path.exists()
        # 打开即记录轨迹边界（不写墙钟）。
        self.event("trail_open", {"path": str(self.path)})

    def event(self, name, payload=None):
        with self._lock:
            self._seq += 1
            rec = {"seq": self._seq, "event": name,
                   "clock": self.logical_clock, "payload": payload or {}}
            self.events.append(rec)
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True) + "\n")
        return rec

    def read_all(self):
        with open(self.path, "r", encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]

    def summarize(self):
        """汇总事务边界、拒绝原因、受影响行与性能统计。"""
        evs = self.read_all()
        summary = {"transaction_boundaries": [], "rejections": [],
                   "committed": [], "lock_retries": 0, "busy_terminations": [],
                   "rows_read": 0, "rows_written": 0, "event_count": len(evs)}
        for e in evs:
            n, p = e["event"], e["payload"]
            if n in ("lock_acquired", "apply_committed", "apply_stale",
                     "apply_busy", "apply_verify_mismatch", "apply_exec_failed",
                     "apply_commit_busy", "apply_rejected"):
                summary["transaction_boundaries"].append(
                    {"seq": e["seq"], "event": n})
            if n in ("apply_stale", "apply_busy", "apply_verify_mismatch",
                     "apply_exec_failed", "apply_commit_busy", "apply_rejected"):
                summary["rejections"].append(
                    {"seq": e["seq"], "event": n, "reason": p})
            if n == "lock_retry":
                summary["lock_retries"] += 1
            if n == "apply_busy":
                summary["busy_terminations"].append({"seq": e["seq"], "detail": p})
            if n == "apply_committed":
                summary["committed"].append({"seq": e["seq"], **p})
                summary["rows_written"] += p.get("stats", {}).get(
                    "rows_written", 0)
                summary["rows_read"] += p.get("stats", {}).get("rows_read", 0)
        return summary


def remaining(path, tables=None):
    """逐表读回全部剩余行（保留 baseline 调用形状，默认自动发现表）。"""
    c = connect(path)
    try:
        if tables is None:
            tables = [r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        return {t: [list(row) for row in c.execute(f'SELECT * FROM "{t}"')]
                for t in tables}
    finally:
        c.close()


def readback_verify(path, plan_doc):
    """提交后独立连接读回：批准删除键必须不存在，置空键列必须为 NULL，
    其余行必须仍在。只看 foreign_key_check 不够，这里核对实际数据。"""
    c = connect(path)
    problems = []
    try:
        for t, pks in plan_doc["verdict"]["delete_rows"].items():
            pk_col = _pk_col(c, t)
            for pk in pks:
                got = c.execute(
                    f'SELECT 1 FROM "{t}" WHERE "{pk_col}"=?', (pk,)).fetchone()
                if got is not None:
                    problems.append({"table": t, "pk": pk,
                                     "problem": "deleted_row_still_present"})
        for t, items in plan_doc["verdict"]["null_updates"].items():
            for it in items:
                (val,) = c.execute(
                    f'SELECT "{it["column"]}" FROM "{t}" WHERE '
                    f'"{_pk_col(c, t)}"=?', (it["pk"],)).fetchone()
                if val is not None:
                    problems.append({"table": t, "pk": it["pk"],
                                     "column": it["column"],
                                     "problem": "column_not_nulled"})
        fkviol = c.execute("PRAGMA foreign_key_check").fetchall()
        return {"ok": not problems and not fkviol,
                "data_problems": problems,
                "foreign_key_check": [list(v) for v in fkviol]}
    finally:
        c.close()


def _pk_col(conn, table):
    for row in conn.execute(f'PRAGMA table_info("{table}")'):
        if row[5] == 1:
            return row[1]
    raise RuntimeError(f"no single PK on {table}")
