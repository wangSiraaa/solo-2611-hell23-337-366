"""独立验证：复制库 FK oracle + 保留规则/批准写集联合裁决。

foreign_key_check 通过只证明“无孤立引用”，不能证明删除集被批准过。这里在一份
与业务库隔离的复制库上真实执行原始根删除，让 SQLite 引擎给出 FK 动作 oracle，
再把 oracle 写集与（a）静态保留裁决、（b）批准写集三方对齐。

oracle 必须在 apply 之前的库镜像（pre-image）上运行：apply 提交后的库已无被删
行，无法复放。调用方负责在 plan 后、apply 前复制一份文件。
"""

import shutil
import sqlite3
from pathlib import Path

from database import connect
from retention import ST_BLOCKED, ST_READY, canonical, load_catalog
from auditor import readback_verify

ENGINE_BLOCK_KINDS = {"SURVIVING_NO_ACTION", "SET_NULL_NOT_NULL"}
HOLD_KINDS = {"HOLD_ROW_DELETED", "HOLD_PROTECTED_DELETED"}


def _snapshot(conn, tables):
    out = {}
    for tname, t in tables.items():
        cols = ", ".join(f'"{c[0]}"' for c in t.columns)
        pk_idx = t.col_index[t.pk_col]
        out[tname] = {row[pk_idx]: list(row)
                      for row in conn.execute(f'SELECT {cols} FROM "{tname}"')}
    return out


def oracle_delete(pre_image_path, plan_doc, config=None, work_dir=None):
    """在复制库上执行原始根删除，返回引擎真实 FK 结果（oracle）。

    全程外键开启、单事务；引擎 IntegrityError 即延期 NO ACTION / NOT NULL 冲突，
    复制库回滚，错误如实带回。
    """
    pre_image_path = Path(pre_image_path)
    work_dir = Path(work_dir or pre_image_path.parent)
    work_dir.mkdir(parents=True, exist_ok=True)
    replica = work_dir / (pre_image_path.stem + ".oracle.sqlite")
    if replica.exists():
        replica.unlink()
    shutil.copy2(pre_image_path, replica)

    cfg = config or plan_doc["embedded_config"]
    conn = connect(replica)
    catalog = load_catalog(conn, cfg)
    before = _snapshot(conn, catalog.tables)
    root_pks = [r["pk"] for r in plan_doc["verdict"]["candidate_roots"]
                if r["table"] == catalog.root_table]
    outcome = {"replica": str(replica), "engine_status": None,
               "engine_error": None, "deletes": {}, "null_updates": {},
               "foreign_key_check": None}
    try:
        conn.execute("BEGIN IMMEDIATE")
        marks = ",".join("?" for _ in root_pks)
        if root_pks:
            conn.execute(
                f'DELETE FROM "{catalog.root_table}" WHERE '
                f'"{catalog.tables[catalog.root_table].pk_col}" IN ({marks})',
                root_pks)
        conn.commit()
        outcome["engine_status"] = "committed"
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        outcome["engine_status"] = "integrity_error"
        outcome["engine_error"] = str(exc)
    except sqlite3.OperationalError as exc:
        conn.rollback()
        outcome["engine_status"] = "operational_error"
        outcome["engine_error"] = str(exc)

    if outcome["engine_status"] == "committed":
        after = _snapshot(conn, catalog.tables)
        for tname, t in catalog.tables.items():
            old, new = before[tname], after[tname]
            outcome["deletes"][tname] = sorted(set(old) - set(new))
            nulls = []
            for pk, new_row in new.items():
                if pk not in old:
                    continue
                for ci, (ov, nv) in enumerate(zip(old[pk], new_row)):
                    if ov is not None and nv is None:
                        nulls.append({"pk": pk, "column": t.columns[ci][0]})
            outcome["null_updates"][tname] = sorted(
                nulls, key=lambda x: (x["pk"], x["column"]))
        outcome["foreign_key_check"] = [
            list(v) for v in conn.execute("PRAGMA foreign_key_check")]
    conn.close()
    return outcome


def joint_judgment(plan_doc, oracle):
    """oracle 引擎结果 × 保留规则 × 批准写集三方联合判断。"""
    verdict = plan_doc["verdict"]
    checks = []

    def add(name, ok, detail=None):
        checks.append({"check": name, "ok": bool(ok), "detail": detail})

    kinds = {b["kind"] for b in verdict["blockers"]}
    engine_blocks = kinds & ENGINE_BLOCK_KINDS
    hold_blocks = kinds & HOLD_KINDS

    if verdict["status"] == ST_READY:
        all_tables = sorted(plan_doc["embedded_config"]["roles"])

        def full(tables_map, empty):
            return {t: tables_map.get(t, empty) for t in all_tables}

        def null_shape(tables_map):
            # 只比对 (pk,column) 写集；reasons 是解释材料，不参与引擎一致性。
            return {t: sorted((i["pk"], i["column"]) for i in items)
                    for t, items in tables_map.items()}

        approved_del = full(verdict["delete_rows"], [])
        approved_null = null_shape(full(verdict["null_updates"], []))
        oracle_null = null_shape(oracle["null_updates"])
        add("oracle_commits_for_ready_plan",
            oracle["engine_status"] == "committed",
            {"engine_status": oracle["engine_status"],
             "engine_error": oracle["engine_error"]})
        if oracle["engine_status"] == "committed":
            add("delete_set_equals_oracle",
                canonical(approved_del) == canonical(oracle["deletes"]),
                {"approved": approved_del, "oracle": oracle["deletes"]})
            add("null_set_equals_oracle",
                canonical(approved_null) == canonical(oracle_null),
                {"approved": approved_null, "oracle": oracle_null})
            add("oracle_foreign_key_check_clean",
                not oracle["foreign_key_check"], oracle["foreign_key_check"])
    elif verdict["status"] == ST_BLOCKED:
        if engine_blocks:
            add("engine_rejects_when_integrity_blocker_predicted",
                oracle["engine_status"] == "integrity_error",
                {"blockers": sorted(engine_blocks),
                 "engine_status": oracle["engine_status"],
                 "engine_error": oracle["engine_error"]})
        if hold_blocks and not engine_blocks:
            # 纯 hold 阻断：引擎本身会提交，正是“只看 FK 检查不够”的场景。
            add("oracle_would_commit_but_policy_holds_block",
                oracle["engine_status"] == "committed",
                {"hold_blockers": sorted(hold_blocks),
                 "engine_status": oracle["engine_status"]})
            for b in verdict["blockers"]:
                if b["kind"] == "HOLD_ROW_DELETED":
                    add(f"oracle_deletes_hold_row:{b['table']}:{b['pk']}",
                        b["pk"] in oracle["deletes"].get(b["table"], []))
                if b["kind"] == "HOLD_PROTECTED_DELETED":
                    add(f"oracle_deletes_protected:"
                        f"{b['protected_table']}:{b['protected_pk']}",
                        b["protected_pk"] in
                        oracle["deletes"].get(b["protected_table"], []))
        # 阻断计划的批准写集里，任何 hold/受保护行都必须能在静态闭包中解释。
        for b in verdict["blockers"]:
            if b["kind"] == "HOLD_ROW_DELETED":
                add(f"static_closure_contains_hold:{b['table']}:{b['pk']}",
                    b["pk"] in verdict["delete_rows"].get(b["table"], []))
            if b["kind"] == "HOLD_PROTECTED_DELETED":
                add(f"static_closure_contains_protected:"
                    f"{b['protected_table']}:{b['protected_pk']}",
                    b["protected_pk"] in
                    verdict["delete_rows"].get(b["protected_table"], []))
    else:
        add("known_verdict_status", False, {"status": verdict["status"]})

    return {"ok": all(c["ok"] for c in checks), "checks": checks,
            "blocker_kinds": sorted(kinds)}


def verify(db_path, pre_image_path, plan_doc, apply_result, config=None):
    """端到端 verify：oracle 联合判断 + 提交后独立连接读回。

    仅当 apply 已 COMMITTED 时读回才有意义；其余状态只做 oracle 联合判断并把
    apply 结果原样纳入报告。
    """
    oracle = oracle_delete(pre_image_path, plan_doc, config=config)
    judgment = joint_judgment(plan_doc, oracle)
    report = {"plan_id": plan_doc["plan_id"],
              "apply_status": apply_result["status"],
              "oracle": {k: v for k, v in oracle.items() if k != "replica"},
              "joint_judgment": judgment}
    if apply_result["status"] == "COMMITTED":
        report["readback"] = readback_verify(db_path, plan_doc)
        report["ok"] = judgment["ok"] and report["readback"]["ok"]
    else:
        report["readback"] = None
        # 未提交路径：库必须保持 pre-image 内容（保留原库）。
        rb_now = _fingerprint_db(db_path, plan_doc)
        rb_pre = _fingerprint_db(pre_image_path, plan_doc)
        report["database_unchanged"] = rb_now == rb_pre
        report["ok"] = judgment["ok"] and report["database_unchanged"]
    return report


def _fingerprint_db(path, plan_doc):
    from retention import _load_rows
    cfg = plan_doc["embedded_config"]
    conn = connect(path)
    try:
        catalog = load_catalog(conn, cfg)
        snap = {t: {pk: list(row) for pk, row in
                    _load_rows(conn, catalog.tables[t]).items()}
                for t in catalog.tables}
    finally:
        conn.close()
    return canonical(snap)
