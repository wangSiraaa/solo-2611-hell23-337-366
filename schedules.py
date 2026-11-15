"""真实两连接日程：另一连接在 before_lock / after_revalidation /
before_commit 三个控制点尝试写入，检验 apply 的锁与重验协议。

日程来自 inputs/schedules.json，每条含 name、控制点 hook 与（可选）期望结果。
hook 始终自行开关连接，绝不复用 apply 连接；写入失败（SQLITE_BUSY/locked）按
真实异常如实记录，吞掉异常以免打断 apply 主流程。
"""

import json
import sqlite3


def _run_sql(db_path, sql):
    """ rival 连接：打开 -> 执行 -> 提交/回滚 -> 关闭。"""
    conn = sqlite3.connect(str(db_path), timeout=0)
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.executescript(sql)
        conn.commit()
        return {"attempted": sql, "result": "committed"}
    except Exception as exc:                      # noqa: BLE001
        conn.rollback()
        return {"attempted": sql, "result": "blocked",
                "error": f"{type(exc).__name__}: {exc}"}
    finally:
        conn.close()


def build_hooks(db_path, schedule, log):
    """构造三个控制点的 hook；schedule 形如
    {"point": "after_revalidation", "sql": "..."} 或多控制点列表。"""
    entries = schedule if isinstance(schedule, list) else [schedule]
    by_point = {}
    for e in entries:
        by_point.setdefault(e["point"], []).append(e)

    def make(point):
        def hook(ctx):
            for e in by_point.get(point, ()):
                outcome = _run_sql(db_path, e["sql"])
                log.append({"point": point, **outcome})
        return hook

    return {pt: make(pt) for pt in ("before_lock", "after_revalidation",
                                    "before_commit")}


def load_schedules(path="inputs/schedules.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def rival_holds_lock(db_path, hold_event, release_event):
    """线程化 rival：占着写锁直到 release_event 被 set，用于锁竞争/BUSY 用例。

    返回 (thread, info_dict)。info['locked'] 在拿到锁后置位。
    """
    import threading
    info = {"locked": threading.Event(), "errors": []}

    def worker():
        conn = sqlite3.connect(str(db_path), timeout=0)
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("INSERT INTO audit_log(id, account_id, note) "
                         "VALUES (9001, NULL, 'rival-lock')")
            info["locked"].set()
            hold_event.wait(10)
            conn.rollback()
        except Exception as exc:                  # noqa: BLE001
            info["errors"].append(str(exc))
        finally:
            conn.close()

    th = threading.Thread(target=worker, daemon=True)
    th.start()
    return th, info
