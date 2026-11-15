"""可复跑验收：contract.md 全部关键路径 + 真实两连接日程 + oracle 联合判断。

运行：PYTHONDONTWRITEBYTECODE=1 python run_tests.py
退出码 0 表示全部用例通过；每个用例独立临时库，互不污染。
"""

import json
import shutil
import sys
import tempfile
import threading
from pathlib import Path

from database import connect, seed, seed_baseline
from retention import (plan, apply, load_config, save_plan, load_plan,
                       ST_COMMITTED, ST_STALE, ST_BUSY, ST_BLOCKED, ST_READY,
                       ST_REJECTED)
from auditor import AuditTrail, remaining
from verifier import verify, oracle_delete, joint_judgment
from schedules import build_hooks, load_schedules, rival_holds_lock

CONFIG_PATH = "retention_config.json"
RESULTS = []


def check(name, cond, detail=None):
    RESULTS.append({"case": name, "ok": bool(cond), "detail": detail})
    print(("  ok  " if cond else "FAIL ") + name +
          (f"  {json.dumps(detail, ensure_ascii=False)[:220]}"
           if detail and not cond else ""))
    return bool(cond)


def fresh_biz(tmp, name):
    p = Path(tmp) / f"{name}.sqlite"
    seed(p)
    return p


def run_apply(p, doc, clock, tmp, name, hooks=None, delay=0.02):
    audit = AuditTrail(Path(tmp) / f"{name}.audit.jsonl", logical_clock=clock)
    return audit, apply(p, doc, clock, audit, hooks=hooks,
                        busy_retry_delay=delay)


def case_happy_path(tmp):
    p = fresh_biz(tmp, "happy")
    cfg = load_config(CONFIG_PATH)
    doc = plan(p, cfg, [{"table": "account", "pk": 3}], None, 1000)
    check("happy: plan READY", doc["verdict"]["status"] == ST_READY)
    pre = Path(tmp) / "happy.pre.sqlite"
    shutil.copy2(p, pre)
    audit, res = run_apply(p, doc, 1000, tmp, "happy")
    check("happy: COMMITTED", res["status"] == ST_COMMITTED, res.get("detail"))
    rep = verify(p, pre, doc, res, cfg)
    check("happy: oracle+readback joint ok", rep["ok"],
          rep["joint_judgment"]["checks"])
    check("happy: audit has txn boundaries",
          {e["event"] for e in audit.read_all()} >=
          {"apply_begin", "lock_acquired", "revalidation", "apply_committed"})
    check("happy: stats rows_written=2",
          res["stats"]["rows_written"] == 2, res["stats"])


def case_set_null_and_cascade_priority(tmp):
    # 多路径：binder1 同时被 account CASCADE 与 event SET NULL 触及 -> 删除优先。
    p = fresh_biz(tmp, "multi")
    cfg = load_config(CONFIG_PATH)
    doc = plan(p, cfg, [{"table": "account", "pk": 2}], None, 1000)
    # a2 闭包还会撞 pinned NOT NULL / hold2(已过期)/tag? tag 挂 event11 不在闭包。
    kinds = {b["kind"] for b in doc["verdict"]["blockers"]}
    check("multi: NOT NULL conflict blocked",
          "SET_NULL_NOT_NULL" in kinds, doc["verdict"]["blockers"])
    check("multi: binder deleted not nulled (cascade wins)",
          1 in doc["verdict"]["delete_rows"].get("binder", [])
          and not any(i["pk"] == 1 for i in
                      doc["verdict"]["null_updates"].get("binder", [])))
    check("multi: subscription nullable set-null predicted",
          any(i["pk"] == 1 for i in
              doc["verdict"]["null_updates"].get("subscription", [])))
    # 构造一个干净的多路径库（去掉 pinned 冲突与 hold2 的幸存 FK）验证真实引擎。
    p2 = fresh_biz(tmp, "multi2")
    c = connect(p2)
    c.execute("DELETE FROM pinned_event")
    # hold2 已过期但 FK 仍在；干净路径需要移除该 hold 行（删除 hold 本身并不能
    # 绕过“有效 hold”，这里它在 clock=1000 本就无效）。
    c.execute("DELETE FROM legal_hold WHERE id=2")
    c.commit()
    c.close()
    doc2 = plan(p2, cfg, [{"table": "account", "pk": 2}], None, 1000)
    check("multi2: READY after removing not-null row",
          doc2["verdict"]["status"] == ST_READY, doc2["verdict"]["blockers"])
    pre = Path(tmp) / "multi2.pre.sqlite"
    shutil.copy2(p2, pre)
    _, res = run_apply(p2, doc2, 1000, tmp, "multi2")
    rep = verify(p2, pre, doc2, res, cfg)
    check("multi2: engine matches cascade-priority prediction",
          res["status"] == ST_COMMITTED and rep["ok"],
          [x for x in rep["joint_judgment"]["checks"] if not x["ok"]])


def case_deferred_no_action_same_batch(tmp):
    # alpha/beta 两表 CASCADE 环同批解除；tag 幸存 NO ACTION 阻断。
    p = fresh_biz(tmp, "noact")
    cfg = load_config(CONFIG_PATH)
    # 直接用 alpha1 根（环内两表同批删，延期约束不报错）。
    c = connect(p)
    c.execute("DELETE FROM tag")
    c.commit()
    c.close()
    doc = plan(p, cfg, [], None, 1000)  # 无根 -> 空计划
    check("noact: empty plan READY", doc["verdict"]["status"] == ST_READY
          and doc["verdict"]["delete_rows"] == {})
    # 环：把 node 自引用环也通过业务根间接不可达，直接造一张小库验证环同批。
    pc = Path(tmp) / "cycle.sqlite"
    c = connect(pc)
    c.executescript(
        "CREATE TABLE n(id INTEGER PRIMARY KEY, p INTEGER REFERENCES n(id) "
        "ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED);"
        "CREATE TABLE k(id INTEGER PRIMARY KEY, n INTEGER REFERENCES n(id) "
        "ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED);")
    c.execute("BEGIN")
    c.execute("INSERT INTO n VALUES(1,NULL),(2,NULL),(3,NULL)")
    c.execute("UPDATE n SET p=2 WHERE id=1")
    c.execute("UPDATE n SET p=1 WHERE id=2")
    c.execute("UPDATE n SET p=2 WHERE id=3")
    c.commit()
    c.close()
    ccfg = {
        "version": 1, "max_tables": 12, "max_rows": 30000,
        "roles": {"n": "root", "k": "ordinary"},
        "root_predicate": {"table": "n", "column": "id", "op": "<="},
        "retained_tables": {}}
    docc = plan(pc, ccfg, [{"table": "n", "pk": 1}], None, 0)
    check("cycle: cascade fixed point removes whole ring (n1,n2,n3)",
          sorted(docc["verdict"]["delete_rows"].get("n", [])) == [1, 2, 3],
          docc["verdict"]["delete_rows"])
    pre = Path(tmp) / "cycle.pre.sqlite"
    shutil.copy2(pc, pre)
    _, resc = run_apply(pc, docc, 0, tmp, "cycle")
    repc = verify(pc, pre, docc, resc, ccfg)
    check("cycle: engine deferred-batch delete matches", resc["status"]
          == ST_COMMITTED and repc["ok"], resc.get("detail"))

    # 幸存 NO ACTION：在独立库中让 k 引用 n2 且 k 不进删除闭包 -> 静态阻断，
    # 复制库 oracle 真实执行时引擎在提交点报 IntegrityError。
    pn = Path(tmp) / "noaction.sqlite"
    shutil.copy2(pre, pn)
    c = connect(pn)
    c.execute("BEGIN")
    c.execute("INSERT INTO k VALUES(1,2)")
    c.commit()
    c.close()
    docb = plan(pn, ccfg, [{"table": "n", "pk": 1}], None, 0)
    check("noaction: surviving reference blocks",
          any(b["kind"] == "SURVIVING_NO_ACTION"
              for b in docb["verdict"]["blockers"]))
    pre2 = Path(tmp) / "noaction.pre.sqlite"
    shutil.copy2(pn, pre2)
    orc = oracle_delete(pre2, docb, config=ccfg, work_dir=Path(tmp))
    judg = joint_judgment(docb, orc)
    check("noaction: oracle engine integrity_error agrees",
          orc["engine_status"] == "integrity_error" and judg["ok"],
          [x for x in judg["checks"] if not x["ok"]])
    _, resb = run_apply(pn, docb, 0, tmp, "noactb")
    check("noaction: blocked plan cannot apply",
          resb["status"] == ST_REJECTED)


def case_hold_boundaries(tmp):
    p = fresh_biz(tmp, "hold")
    cfg = load_config(CONFIG_PATH)
    # hold2 expires_at=500：clock=499 有效（保护 event20）；clock=500 恰等于已过期。
    doc_valid = plan(p, cfg, [{"table": "account", "pk": 2}], None, 499)
    check("hold: active at clock 499 blocks",
          any(b["kind"] == "HOLD_PROTECTED_DELETED" and b["pk"] == 2
              for b in doc_valid["verdict"]["blockers"]))
    doc_eq = plan(p, cfg, [{"table": "account", "pk": 2}], None, 500)
    check("hold: expires_at==clock means expired (no hold blocker)",
          not any(b["kind"] in ("HOLD_PROTECTED_DELETED", "HOLD_ROW_DELETED")
                  and b["pk"] == 2 for b in doc_eq["verdict"]["blockers"]))
    # 永久 hold（expires_at NULL）：a1 永远被 hold1 保护。
    doc_perm = plan(p, cfg, [{"table": "account", "pk": 1}], None, 10**9)
    check("hold: NULL expires_at is permanent",
          any(b["kind"] == "HOLD_PROTECTED_DELETED" and b["pk"] == 1
              for b in doc_perm["verdict"]["blockers"]))
    # 不能直接删 hold 解锁：把 hold1 本身也放进根同批（通过其表不可为根，
    # 改为在闭包中验证 HOLD_ROW_DELETED 路径：构造 hold 行位于级联闭包的库）。
    pc = Path(tmp) / "holdrow.sqlite"
    c = connect(pc)
    # hold 表通过一条非空 CASCADE 边挂在根上（hold 行随闭包被级联），
    # 受保护列另设且为 NULL，使唯一阻断点是“hold 行自身要被删”。
    c.executescript(
        "CREATE TABLE r(id INTEGER PRIMARY KEY);"
        "CREATE TABLE h(id INTEGER PRIMARY KEY, owner INTEGER NOT NULL "
        "REFERENCES r(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED, "
        "r_id INTEGER, x INTEGER);")
    c.execute("INSERT INTO r VALUES(1)")
    c.execute("INSERT INTO h VALUES(1,1,NULL,NULL)")
    c.commit()
    c.close()
    hcfg = {"version": 1, "max_tables": 12, "max_rows": 30000,
            "roles": {"r": "root", "h": "retained"},
            "root_predicate": {"table": "r", "column": "id", "op": "<="},
            "retained_tables": {"h": {"protected_column": "r_id",
                                      "expires_at_column": "x",
                                      "protected_parent_table": "r"}}}
    doch = plan(pc, hcfg, [{"table": "r", "pk": 1}], None, 10)
    check("hold: deleting hold row itself in closure is blocked",
          any(b["kind"] == "HOLD_ROW_DELETED" for b in doch["verdict"]["blockers"]),
          doch["verdict"]["blockers"])


def case_phantom_and_concurrent(tmp):
    cfg = load_config(CONFIG_PATH)
    for sch in load_schedules("inputs/schedules.json"):
        name = sch["name"]
        p = fresh_biz(tmp, "sch_" + name)
        # 所有日程基于 a3 根（event30 级联）的 READY 计划。
        doc = plan(p, cfg, [{"table": "account", "pk": 3}], None, 1000)
        assert doc["verdict"]["status"] == ST_READY
        pre = Path(tmp) / f"sch_{name}.pre.sqlite"
        shutil.copy2(p, pre)
        hook_log = []
        hooks = build_hooks(p, sch, hook_log)
        audit = AuditTrail(Path(tmp) / f"sch_{name}.audit.jsonl", 1000)
        res = apply(p, doc, 1000, audit, hooks=hooks)
        expect = sch["expect_apply"]
        ok = res["status"] == expect
        if sch.get("expect_rival") == "blocked":
            ok = ok and all(h["result"] == "blocked" for h in hook_log)
        check(f"schedule {name} -> {expect}", ok,
              {"got": res["status"], "detail": res.get("detail"),
               "rival": hook_log})
        if expect == "STALE_PLAN":
            # 原库保留：a3 与 event30 必须仍在。
            rem = remaining(p, ["account", "event"])
            check(f"schedule {name}: original db preserved",
                  3 in [r[0] for r in rem["account"]]
                  and 30 in [r[0] for r in rem["event"]])
            # 可重新 plan 且新计划包含幻影/新 hold 的裁决。
            redoc = plan(p, cfg, [{"table": "account", "pk": 3}], None, 1000)
            check(f"schedule {name}: re-plan possible",
                  redoc["plan_id"] != doc["plan_id"])
        if expect == "COMMITTED":
            rep = verify(p, pre, doc, res, cfg)
            check(f"schedule {name}: verify ok", rep["ok"],
                  [x for x in rep["joint_judgment"]["checks"] if not x["ok"]])


def case_fk_catalog_change(tmp):
    p = fresh_biz(tmp, "cat")
    cfg = load_config(CONFIG_PATH)
    doc = plan(p, cfg, [{"table": "account", "pk": 3}], None, 1000)
    # 修改外键动作：新建一个指向 account 的 SET NULL 子表（原库 account 的
    # ordinary 子表都是 CASCADE）。先移除本用例不需要的两表环 alpha/beta，
    # 把表数控制在 12 以内；这是真实的 FK 目录语义变化，catalog 指纹必须变。
    c = connect(p)
    c.execute("DROP TABLE beta")
    c.execute("DROP TABLE alpha")
    c.execute("CREATE TABLE cat_probe(id INTEGER PRIMARY KEY, "
              "account_id INTEGER REFERENCES account(id) ON DELETE SET NULL)")
    c.commit()
    c.close()
    _, res = run_apply(p, doc, 1000, tmp, "cat")
    check("catalog: FK catalog change -> STALE_PLAN",
          res["status"] == ST_STALE and res["detail"]["cause"]
          == "catalog_changed", res.get("detail"))
    rem = remaining(p, ["account"])
    check("catalog: db preserved on stale", 3 in [r[0] for r in rem["account"]])

    # 反向验证：仅新增索引（无语义分量）不得触发 catalog 失效。
    p2 = fresh_biz(tmp, "catidx")
    doc2 = plan(p2, cfg, [{"table": "account", "pk": 3}], None, 1000)
    c = connect(p2)
    c.execute("CREATE INDEX event_account ON event(account_id)")
    c.commit()
    c.close()
    _, res2 = run_apply(p2, doc2, 1000, tmp, "catidx")
    check("catalog: unrelated index does not stale the plan",
          res2["status"] == ST_COMMITTED, res2.get("detail"))


def case_lock_contention(tmp):
    p = fresh_biz(tmp, "lock")
    cfg = load_config(CONFIG_PATH)
    doc = plan(p, cfg, [{"table": "account", "pk": 3}], None, 1000)
    hold = threading.Event()
    th, info = rival_holds_lock(p, hold, threading.Event())
    assert info["locked"].wait(5), "rival failed to acquire lock"
    audit = AuditTrail(Path(tmp) / "lock.audit.jsonl", logical_clock=1000)
    # rival 全程不释放：一次显式 BUSY 重试后终止。
    res = apply(p, doc, 1000, audit, busy_retry_delay=0.05)
    hold.set()
    th.join(2)
    check("lock: BUSY after one explicit retry",
          res["status"] == ST_BUSY and res["lock_retries"] == 1
          and res["detail"]["attempts"] == 2,
          {"status": res["status"], "retries": res["lock_retries"]})
    check("lock: BUSY recorded in audit",
          any(e["event"] == "apply_busy" for e in audit.read_all()))
    rem = remaining(p, ["account"])
    check("lock: db preserved after BUSY", 3 in [r[0] for r in rem["account"]])

    # 第二场景：rival 短占后释放，apply 的一次重试成功提交。
    p2 = fresh_biz(tmp, "lock2")
    doc2 = plan(p2, cfg, [{"table": "account", "pk": 3}], None, 1000)
    release = threading.Event()
    th2, info2 = rival_holds_lock(p2, release, threading.Event())
    assert info2["locked"].wait(5)

    def release_after_delay():
        threading.Timer(0.08, release.set).start()

    audit2 = AuditTrail(Path(tmp) / "lock2.audit.jsonl", logical_clock=1000)
    release_after_delay()
    res2 = apply(p2, doc2, 1000, audit2, busy_retry_delay=0.15)
    th2.join(2)
    check("lock: retry succeeds after rival release -> COMMITTED",
          res2["status"] == ST_COMMITTED and res2["lock_retries"] == 1,
          {"status": res2["status"], "retries": res2["lock_retries"]})


def case_plan_tamper_and_approve_roundtrip(tmp):
    p = fresh_biz(tmp, "tamper")
    cfg = load_config(CONFIG_PATH)
    doc = plan(p, cfg, [{"table": "account", "pk": 3}], None, 1000)
    fp = Path(tmp) / "plan.json"
    save_plan(doc, fp)
    back = load_plan(fp)
    check("plan: save/load roundtrip keeps plan_id",
          back["plan_id"] == doc["plan_id"])
    back["verdict"]["delete_rows"]["account"] = [3, 1]  # 静默扩大删除集
    _, res = run_apply(p, back, 1000, tmp, "tamper")
    check("plan: tampered document rejected",
          res["status"] == ST_REJECTED
          and res["detail"]["cause"] == "plan_id_mismatch", res.get("detail"))
    rem = remaining(p, ["account"])
    check("plan: nothing deleted after tamper reject",
          sorted(r[0] for r in rem["account"]) == [1, 2, 3])


def case_baseline_gap_now_closed(tmp):
    # 复现 baseline_demo 的幻影级联：新实现必须 STALE，而不是未审阅删除。
    p = Path(tmp) / "baseline.sqlite"
    seed_baseline(p)
    cfg = {
        "version": 1, "max_tables": 12, "max_rows": 30000,
        "roles": {"account": "root", "event": "ordinary",
                  "legal_hold": "retained"},
        "root_predicate": {"table": "account", "column": "id", "op": "<="},
        "retained_tables": {"legal_hold": {
            "protected_column": "event_id", "expires_at_column": "expires_at",
            "protected_parent_table": "event"}}}
    doc = plan(p, cfg, [{"table": "account", "pk": 1}], None, 1000)
    check("baseline: original plan contains event 11",
          doc["verdict"]["delete_rows"].get("event") == [11])
    c = connect(p)
    c.execute("INSERT INTO event VALUES(12,1,9)")
    c.commit()
    c.close()
    _, res = run_apply(p, doc, 1000, tmp, "baseline")
    check("baseline: phantom child now yields STALE_PLAN (gap closed)",
          res["status"] == ST_STALE, res.get("detail"))
    rem = remaining(p, ["account", "event"])
    check("baseline: all rows preserved",
          [r[0] for r in rem["account"]] == [1]
          and sorted(r[0] for r in rem["event"]) == [11, 12])


def case_null_is_not_reference(tmp):
    pc = Path(tmp) / "nullref.sqlite"
    c = connect(pc)
    c.executescript(
        "CREATE TABLE r(id INTEGER PRIMARY KEY);"
        "CREATE TABLE s(id INTEGER PRIMARY KEY, r_id INTEGER REFERENCES r(id) "
        "ON DELETE SET NULL);"
        "CREATE TABLE n(id INTEGER PRIMARY KEY, r_id INTEGER NOT NULL "
        "REFERENCES r(id) ON DELETE SET NULL);")
    c.execute("INSERT INTO r VALUES(1),(2)")
    c.execute("INSERT INTO s VALUES(1,NULL),(2,1)")
    c.execute("INSERT INTO n VALUES(1,2)")
    c.commit()
    c.close()
    cfg = {"version": 1, "max_tables": 12, "max_rows": 30000,
           "roles": {"r": "root", "s": "ordinary", "n": "ordinary"},
           "root_predicate": {"table": "r", "column": "id", "op": "<="},
           "retained_tables": {}}
    doc = plan(pc, cfg, [{"table": "r", "pk": 1}], None, 0)
    check("nullref: NULL FK row untouched, only referencing row nulled",
          doc["verdict"]["null_updates"].get("s")
          == [{"pk": 2, "column": "r_id",
               "reasons": doc["verdict"]["null_updates"]["s"][0]["reasons"]}],
          doc["verdict"]["null_updates"])
    # r2 不删 -> n1 不受影响；另测 NOT NULL 置空阻断解释。
    doc2 = plan(pc, cfg, [{"table": "r", "pk": 2}], None, 0)
    check("nullref: NOT NULL SET NULL explained as blocker",
          any(b["kind"] == "SET_NULL_NOT_NULL" and b["table"] == "n"
              for b in doc2["verdict"]["blockers"]),
          doc2["verdict"]["blockers"])


def case_clock_flip(tmp):
    # 数据完全不变，仅执行时钟不同 -> hold 有效性翻转 -> 裁决变化必须 STALE。
    pc = Path(tmp) / "clockflip.sqlite"
    c = connect(pc)
    c.executescript(
        "CREATE TABLE r(id INTEGER PRIMARY KEY);"
        "CREATE TABLE e(id INTEGER PRIMARY KEY, r_id INTEGER NOT NULL "
        "REFERENCES r(id) ON DELETE CASCADE);"
        "CREATE TABLE h(id INTEGER PRIMARY KEY, e_id INTEGER, x INTEGER);")
    c.execute("INSERT INTO r VALUES(1)")
    c.execute("INSERT INTO e VALUES(1,1)")
    c.execute("INSERT INTO h VALUES(1,1,500)")
    c.commit()
    c.close()
    cfg = {"version": 1, "max_tables": 12, "max_rows": 30000,
           "roles": {"r": "root", "e": "ordinary", "h": "retained"},
           "root_predicate": {"table": "r", "column": "id", "op": "<="},
           "retained_tables": {"h": {"protected_column": "e_id",
                                     "expires_at_column": "x",
                                     "protected_parent_table": "e"}}}
    d_block = plan(pc, cfg, [{"table": "r", "pk": 1}], None, 400)
    check("clock: hold active at 400 blocks",
          d_block["verdict"]["status"] == ST_BLOCKED)
    d_ready = plan(pc, cfg, [{"table": "r", "pk": 1}], None, 600)
    check("clock: same data, hold expired at 600 -> READY",
          d_ready["verdict"]["status"] == ST_READY)
    _, res = run_apply(pc, d_ready, 400, tmp, "clockflip")
    check("clock: exec-clock verdict flip -> STALE_PLAN (verdict_changed)",
          res["status"] == ST_STALE
          and res["detail"]["cause"] == "verdict_changed", res.get("detail"))


def case_pure_set_null_oracle(tmp):
    cfg = {"version": 1, "max_tables": 12, "max_rows": 30000,
           "roles": {"r": "root", "s": "ordinary"},
           "root_predicate": {"table": "r", "column": "id", "op": "<="},
           "retained_tables": {}}
    pc = Path(tmp) / "puresetnull.sqlite"
    c = connect(pc)
    c.executescript(
        "CREATE TABLE r(id INTEGER PRIMARY KEY);"
        "CREATE TABLE s(id INTEGER PRIMARY KEY, r_id INTEGER REFERENCES r(id) "
        "ON DELETE SET NULL, name TEXT);")
    c.execute("INSERT INTO r VALUES(1),(2)")
    c.execute("INSERT INTO s VALUES(1,1,'a'),(2,1,'b'),(3,NULL,'c'),(4,2,'d')")
    c.commit()
    c.close()
    doc = plan(pc, cfg, [{"table": "r", "pk": 1}], None, 0)
    nulled = {(i["pk"], i["column"])
              for i in doc["verdict"]["null_updates"].get("s", [])}
    check("setnull: exactly the two referencing rows predicted",
          nulled == {(1, "r_id"), (2, "r_id")}, doc["verdict"]["null_updates"])
    pre = Path(tmp) / "puresetnull.pre.sqlite"
    shutil.copy2(pc, pre)
    _, res = run_apply(pc, doc, 0, tmp, "puresetnull")
    rep = verify(pc, pre, doc, res, cfg)
    check("setnull: oracle null set + readback column NULL",
          res["status"] == ST_COMMITTED and rep["ok"] and rep["readback"]["ok"],
          [x for x in rep["joint_judgment"]["checks"] if not x["ok"]])
    c = connect(pc)
    rows = dict((pk, fk) for pk, fk, _n in
                c.execute("SELECT id,r_id,name FROM s ORDER BY id"))
    c.close()
    check("setnull: only refs to deleted parent nulled, others intact",
          rows == {1: None, 2: None, 3: None, 4: 2}, rows)


def case_cutoff_roots(tmp):
    p = fresh_biz(tmp, "cutoff")
    cfg = load_config(CONFIG_PATH)
    # cutoff=100 选中 a1,a2（created_at<=100），a3=200 不选。
    doc = plan(p, cfg, [], cutoff=100, plan_clock=1000)
    root_pks = [r["pk"] for r in doc["verdict"]["candidate_roots"]]
    check("cutoff: predicate selects a1,a2 only", sorted(root_pks) == [1, 2],
          root_pks)
    check("cutoff: a3 never in closure",
          3 not in doc["verdict"]["delete_rows"].get("account", []))
    check("cutoff: plan blocked (holds/not-null/no-action on a1,a2)",
          doc["verdict"]["status"] == ST_BLOCKED)


def main():
    with tempfile.TemporaryDirectory() as tmp:
        case_happy_path(tmp)
        case_set_null_and_cascade_priority(tmp)
        case_deferred_no_action_same_batch(tmp)
        case_hold_boundaries(tmp)
        case_phantom_and_concurrent(tmp)
        case_fk_catalog_change(tmp)
        case_lock_contention(tmp)
        case_plan_tamper_and_approve_roundtrip(tmp)
        case_baseline_gap_now_closed(tmp)
        case_null_is_not_reference(tmp)
        case_clock_flip(tmp)
        case_pure_set_null_oracle(tmp)
        case_cutoff_roots(tmp)
    passed = sum(1 for r in RESULTS if r["ok"])
    print(f"\n{passed}/{len(RESULTS)} checks passed")
    fails = [r for r in RESULTS if not r["ok"]]
    if fails:
        print("FAILURES:")
        for r in fails:
            print(" -", r["case"], json.dumps(r["detail"], ensure_ascii=False))
        sys.exit(1)
    print("ALL ACCEPTANCE CHECKS PASSED")


if __name__ == "__main__":
    main()
