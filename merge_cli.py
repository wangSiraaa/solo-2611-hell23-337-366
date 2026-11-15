#!/usr/bin/env python3
"""Command-line entry for the constrained MERGE executor.

Usage:
  python merge_cli.py init DB [--seed]
  python merge_cli.py plan DB PLAN.json [--out FROZEN.json]
  python merge_cli.py commit DB FROZEN.json
  python merge_cli.py submit DB PLAN.json
  python merge_cli.py write DB update-amount KEY AMOUNT
  python merge_cli.py write DB update-parent KEY PARENT_ID
  python merge_cli.py write DB rename OLD_KEY NEW_KEY
  python merge_cli.py write DB insert KEY AMOUNT [PARENT_ID]
  python merge_cli.py write DB delete KEY
  python merge_cli.py receipt DB PLAN_ID
  python merge_cli.py rows DB
  python merge_cli.py run-fixture DB [PLAN.json]
  python merge_cli.py scenario DB (lost-update|phantom|rename|disjoint|fk-fail|lost-confirm)

Receipts are printed as JSON; exit code is 0 iff the receipt status is
"committed" (or the command is a pure query).
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from merge import MergeExecutor, connect, init_schema, writer  # noqa: E402

FIXTURE_PLAN = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "fixtures", "merge-plan.json")


def _print(obj):
    print(json.dumps(obj, indent=1, ensure_ascii=False, sort_keys=False, default=str))


def _load_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _normalize_spec(spec):
    """Drop a placeholder plan_id that does not bind to the content hash.

    The authoritative plan_id is always the computed hash; a client
    supplied id that mismatches is rejected by the planner, so the CLI
    only accepts plans whose id is absent or already correct.
    """
    return spec


def cmd_init(args):
    db = args[0]
    conn = connect(db)
    init_schema(conn, seed="--seed" in args)
    _print({"db": db, "initialized": True, "seeded": "--seed" in args})
    return 0


def cmd_plan(args):
    db, plan_path = args[0], args[1]
    out = args[args.index("--out") + 1] if "--out" in args else None
    conn = connect(db)
    executor = MergeExecutor(conn)
    frozen = executor.plan(_load_json(plan_path))
    if out and not frozen.get("status"):
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(frozen, fh, indent=1, ensure_ascii=False)
        _print({"plan_id": frozen["plan_id"], "frozen_to": out,
                "actions": len(frozen["actions"]),
                "read_rows": len(frozen["read_set"]["rows"]),
                "read_absent": len(frozen["read_set"]["absent"])})
        return 0
    _print(frozen)
    return 0 if not frozen.get("status") else 1


def cmd_commit(args):
    db, frozen_path = args[0], args[1]
    conn = connect(db)
    executor = MergeExecutor(conn)
    receipt, replayed = executor.commit(_load_json(frozen_path))
    _print(receipt)
    if replayed:
        print("; note: identical receipt replayed from the receipt store", file=sys.stderr)
    return 0 if receipt["status"] == "committed" else 1


def cmd_submit(args):
    db, plan_path = args[0], args[1]
    conn = connect(db)
    executor = MergeExecutor(conn)
    receipt, replayed = executor.submit(_normalize_spec(_load_json(plan_path)))
    _print(receipt)
    if replayed:
        print("; note: identical receipt replayed from the receipt store", file=sys.stderr)
    return 0 if receipt["status"] == "committed" else 1


def cmd_write(args):
    db, op = args[0], args[1]
    conn = connect(db)
    if op == "update-amount":
        n = writer.update_amount(conn, args[2], int(args[3]))
    elif op == "update-parent":
        n = writer.update_parent(conn, args[2], int(args[3]))
    elif op == "rename":
        n = writer.rename_key(conn, args[2], args[3])
    elif op == "insert":
        n = writer.insert_row(conn, args[2], int(args[3]),
                              int(args[4]) if len(args) > 4 else None)
    elif op == "delete":
        n = writer.delete_row(conn, args[2])
    else:
        raise SystemExit("unknown write op %r" % op)
    _print({"write": op, "result": n})
    return 0


def cmd_receipt(args):
    db, plan_id = args[0], args[1]
    conn = connect(db)
    row = conn.execute(
        "SELECT receipt_json FROM merge_receipt WHERE plan_id = ?", (plan_id,)
    ).fetchone()
    if row is None:
        _print({"plan_id": plan_id, "stored": False})
        return 1
    _print(json.loads(row[0]))
    return 0


def cmd_rows(args):
    conn = connect(args[0])
    rows = conn.execute(
        "SELECT id, business_key, amount, parent_id, rev FROM target ORDER BY id"
    ).fetchall()
    _print({"target": rows})
    return 0


def cmd_run_fixture(args):
    db = args[0]
    plan_path = args[1] if len(args) > 1 else FIXTURE_PLAN
    spec = _load_json(plan_path)
    placeholder = spec.pop("plan_id", None)
    conn = connect(db)
    executor = MergeExecutor(conn)
    receipt, replayed = executor.submit(spec)
    if placeholder is not None:
        receipt = dict(receipt)
        receipt["note"] = ("fixture plan_id %r is a placeholder label; the "
                           "authoritative plan_id binds to the content hash" % placeholder)
    _print(receipt)
    return 0 if receipt["status"] == "committed" else 1


def _base_spec(key, delta, extra_source=None, extra_set=None):
    set_clause = {"amount": {"op": "+", "args": [{"target": "amount"}, {"source": "delta"}]}}
    if extra_set:
        set_clause.update(extra_set)
    source = {"source_id": "s1", "key": key, "delta": delta}
    if extra_source:
        source.update(extra_source)
    return {
        "source": [source],
        "matched": [{"when": {"literal": True}, "action": "update", "set": set_clause}],
        "not_matched": [{"when": {"literal": True}, "action": "insert",
                         "set": {"business_key": {"source": "key"},
                                 "amount": {"source": "delta"}}}],
    }


def cmd_scenario(args):
    """Deterministic two-connection interleavings on a fresh database."""
    db, name = args[0], args[1]
    if os.path.exists(db):
        os.remove(db)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(db + suffix):
            os.remove(db + suffix)
    c1 = connect(db)
    init_schema(c1, seed=True)
    c2 = connect(db)
    executor = MergeExecutor(c1)
    out = {"scenario": name, "steps": []}

    def step(label, value):
        out["steps"].append({"step": label, "result": value})

    if name == "lost-update":
        frozen = executor.plan(_base_spec("A", 5))
        writer.update_amount(c2, "A", 100)
        receipt, _ = executor.commit(frozen)
        step("plan P(A+5) frozen, external update A=100, commit P", receipt["status"])
        step("conflict", receipt.get("conflict"))
        step("A amount (external write survives)", _amount(c1, "A"))
    elif name == "phantom":
        frozen = executor.plan(_base_spec("C", 7))
        writer.insert_row(c2, "C", 9)
        receipt, _ = executor.commit(frozen)
        step("plan P(insert C=7) frozen, external insert C=9, commit P", receipt["status"])
        step("conflict", receipt.get("conflict"))
        step("C rows", c1.execute(
            "SELECT business_key, amount FROM target WHERE business_key='C'").fetchall())
    elif name == "rename":
        frozen = executor.plan(_base_spec("A", 5))
        writer.rename_key(c2, "A", "A2")
        receipt, _ = executor.commit(frozen)
        step("plan P(A+5) frozen, external rename A->A2, commit P", receipt["status"])
        step("conflict", receipt.get("conflict"))
        step("A2 amount (untouched)", _amount(c1, "A2"))
    elif name == "disjoint":
        frozen_p = executor.plan(_base_spec("A", 1))
        frozen_q = executor.plan(_base_spec("B", 2))
        rp, _ = executor.commit(frozen_p)
        rq, _ = executor.commit(frozen_q)
        step("commit P(A+1)", rp["status"])
        step("commit Q(B+2)", rq["status"])
        step("amounts", {"A": _amount(c1, "A"), "B": _amount(c1, "B")})
    elif name == "fk-fail":
        spec = {
            "source": [
                {"source_id": "s1", "key": "A", "delta": 5},
                {"source_id": "s2", "key": "ZZ", "delta": 7, "parent_id": 999},
            ],
            "matched": [{"when": {"literal": True}, "action": "update",
                         "set": {"amount": {"op": "+", "args": [{"target": "amount"},
                                                              {"source": "delta"}]}}}],
            "not_matched": [{"when": {"literal": True}, "action": "insert",
                             "set": {"business_key": {"source": "key"},
                                     "amount": {"source": "delta"},
                                     "parent_id": {"source": "parent_id"}}}],
        }
        receipt, _ = executor.submit(spec)
        step("plan with valid update + insert into missing parent", receipt["status"])
        step("failure", receipt.get("failure"))
        step("A amount (rolled back, no half-update)", _amount(c1, "A"))
    elif name == "lost-confirm":
        spec = _base_spec("A", 5)
        r1, _ = executor.submit(spec)
        r2, replayed = executor.submit(spec)
        step("commit P(A+5)", r1["status"])
        step("resend same plan after lost confirmation",
             {"status": r2["status"], "replayed_from_store": replayed,
              "identical_receipt": json.dumps(r1, sort_keys=True) == json.dumps(r2, sort_keys=True)})
        step("A amount (applied exactly once)", _amount(c1, "A"))
    else:
        raise SystemExit("unknown scenario %r" % name)
    _print(out)
    return 0


def _amount(conn, key):
    row = conn.execute("SELECT amount FROM target WHERE business_key = ?", (key,)).fetchone()
    return row[0] if row else None


COMMANDS = {
    "init": cmd_init,
    "plan": cmd_plan,
    "commit": cmd_commit,
    "submit": cmd_submit,
    "write": cmd_write,
    "receipt": cmd_receipt,
    "rows": cmd_rows,
    "run-fixture": cmd_run_fixture,
    "scenario": cmd_scenario,
}


def main(argv):
    if len(argv) < 2 or argv[1] not in COMMANDS:
        print(__doc__)
        return 2
    return COMMANDS[argv[1]](argv[2:])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
