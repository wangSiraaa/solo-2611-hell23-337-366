"""Planner for the constrained MERGE executor.

plan() runs inside a short read transaction (a consistent snapshot) and
freezes, for every source row:

  * the match decision (SQL equality on business_key; NULL never matches),
  * the first branch whose `when` evaluates to TRUE (three-valued logic),
  * the fully evaluated assignment values,

plus the read set that the commit phase must re-verify:

  * rows[]   - matched rows: key, id and the exact target column values
               read by match/branch/assignment evaluation,
  * absent[] - keys observed absent: unmatched non-NULL source keys and
               non-NULL keys the plan will produce that did not exist in
               the snapshot (phantom assertions).

All matches are frozen before any action exists, and planning performs no
writes at all, so an action can never cause a later source to re-match.
Deterministic intra-plan conflicts (two sources matching one target, two
actions producing the same new unique key) are detected here, before any
execution, so they never depend on execution order.
"""

import hashlib
import json

from . import ast_eval
from .errors import PlanConflict, PlanInvalid

MAX_SOURCES = 20
PLAN_FORMAT = "frozen-merge-plan/1"

_MATCHED_ACTIONS = ("update", "delete")
_NOT_MATCHED_ACTIONS = ("insert",)


def compute_plan_id(spec):
    """Authoritative plan_id: hash of the frozen plan content."""
    payload = {
        "source": spec["source"],
        "matched": spec["matched"],
        "not_matched": spec["not_matched"],
    }
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "P-" + hashlib.sha256(canon.encode("utf-8")).hexdigest()[:32]


def _validate_source_row(row, index):
    if not isinstance(row, dict):
        raise PlanInvalid("source row %d is not an object" % index)
    allowed = {"source_id", "key", "delta", "new_key", "parent_id"}
    unknown = set(row) - allowed
    if unknown:
        raise PlanInvalid("source row %d has unknown fields %s" % (index, sorted(unknown)))
    sid = row.get("source_id")
    if not isinstance(sid, str) or not sid:
        raise PlanInvalid("source row %d needs a non-empty text source_id" % index)
    key = row.get("key", None)
    if "key" not in row:
        raise PlanInvalid("source row %s misses required field 'key'" % sid)
    if key is not None and not isinstance(key, str):
        raise PlanInvalid("source row %s: key must be text or null" % sid)
    if "delta" not in row or not ast_eval.is_int64(row["delta"]):
        raise PlanInvalid("source row %s: delta must be an int64" % sid)
    if "new_key" in row and row["new_key"] is not None and not isinstance(row["new_key"], str):
        raise PlanInvalid("source row %s: new_key must be text or null" % sid)
    if "parent_id" in row and row["parent_id"] is not None and not ast_eval.is_int64(row["parent_id"]):
        raise PlanInvalid("source row %s: parent_id must be int64 or null" % sid)


def _validate_branch(branch, actions, allow_target, require_insert_set):
    if not isinstance(branch, dict) or not set(branch) <= {"when", "action", "set"}:
        raise PlanInvalid("invalid branch object", branch)
    if "when" not in branch or "action" not in branch:
        raise PlanInvalid("branch needs 'when' and 'action'", branch)
    if branch["action"] not in actions:
        raise PlanInvalid(
            "action %r not allowed in %s branches"
            % (branch["action"], "matched" if allow_target else "not_matched"),
            branch,
        )
    ast_eval.validate_bool_expr(branch["when"], allow_target=allow_target)
    action = branch["action"]
    set_spec = branch.get("set")
    if action == "delete":
        if set_spec is not None:
            raise PlanInvalid("delete branch may not have a set clause", branch)
        return
    if not isinstance(set_spec, dict) or not set_spec:
        raise PlanInvalid("%s branch needs a non-empty set object" % action, branch)
    unknown = set(set_spec) - set(ast_eval.SETTABLE_COLUMNS)
    if unknown:
        raise PlanInvalid("set may only target business_key/amount/parent_id, got %s" % sorted(unknown))
    if require_insert_set:
        missing = {"business_key", "amount"} - set(set_spec)
        if missing:
            raise PlanInvalid("insert branch must set %s" % sorted(missing), branch)
    for expr in set_spec.values():
        ast_eval.validate_value_expr(expr, allow_target=allow_target)


def validate_spec(spec):
    """Structural validation of the whole plan spec. Returns plan_id."""
    if not isinstance(spec, dict) or not set(spec) <= {"plan_id", "source", "matched", "not_matched"}:
        raise PlanInvalid("plan spec has unknown or missing top-level keys")
    for field in ("source", "matched", "not_matched"):
        if not isinstance(spec.get(field), list):
            raise PlanInvalid("plan spec needs a list field %r" % field)
    if not 1 <= len(spec["source"]) <= MAX_SOURCES:
        raise PlanInvalid("plan must carry 1..%d source rows" % MAX_SOURCES)
    seen = set()
    for i, row in enumerate(spec["source"]):
        _validate_source_row(row, i)
        if row["source_id"] in seen:
            raise PlanInvalid("duplicate source_id %r" % row["source_id"])
        seen.add(row["source_id"])
    for branch in spec["matched"]:
        _validate_branch(branch, _MATCHED_ACTIONS, allow_target=True, require_insert_set=False)
    for branch in spec["not_matched"]:
        _validate_branch(branch, _NOT_MATCHED_ACTIONS, allow_target=False, require_insert_set=True)
    plan_id = compute_plan_id(spec)
    if "plan_id" in spec and spec["plan_id"] != plan_id:
        raise PlanInvalid(
            "plan_id %r does not match the hash of the plan content %r"
            % (spec["plan_id"], plan_id)
        )
    return plan_id


def _check_set_value_types(action, set_values, source_id):
    """Deterministic type/null checks on frozen assignment values."""
    for col, val in set_values.items():
        if col == "amount":
            if val is None:
                raise PlanInvalid("source %s: amount would be set to NULL (NOT NULL column)" % source_id)
            if not ast_eval.is_int64(val):
                raise PlanInvalid("source %s: amount must be int64, got %r" % (source_id, val))
        elif col == "business_key":
            if val is not None and not isinstance(val, str):
                raise PlanInvalid("source %s: business_key must be text or null" % source_id)
        elif col == "parent_id":
            if val is not None and not ast_eval.is_int64(val):
                raise PlanInvalid("source %s: parent_id must be int64 or null" % source_id)


def _select_branch(branches, source_row, target_row, read_columns):
    """First branch whose `when` is TRUE wins; FALSE/NULL fall through."""
    for index, branch in enumerate(branches):
        value, cols = ast_eval.eval_condition(branch["when"], source_row, target_row)
        read_columns.update(cols)
        if value is True:
            return index, branch
    return None, None


def _fetch_by_key(conn, key):
    return conn.execute(
        "SELECT id, business_key, amount, parent_id, rev FROM target WHERE business_key = ?",
        (key,),
    ).fetchone()


def _row_to_dict(row):
    return {"id": row[0], "business_key": row[1], "amount": row[2], "parent_id": row[3], "rev": row[4]}


def plan(conn, spec, validation_mode="column"):
    """Freeze a plan against the caller's snapshot read transaction.

    The caller owns the transaction: plan() only reads, and the read
    transaction must be released by the caller right after planning.
    """
    if validation_mode not in ("column", "row"):
        raise PlanInvalid("unknown validation mode %r" % validation_mode)
    plan_id = validate_spec(spec)

    target_owner = {}      # target id -> source_id (deterministic same-target conflict)
    produced_keys = {}     # new non-NULL business_key -> source_id
    kept_keys = {}         # matched rows whose key stays -> source_id
    read_rows = []         # matched-row read set entries
    read_absent = []       # absent-key assertions
    absent_seen = set()
    sources_out = []
    actions = []

    def note_absent(key):
        if key is not None and key not in absent_seen:
            absent_seen.add(key)
            read_absent.append(key)

    for source_row in spec["source"]:
        sid = source_row["source_id"]
        key = source_row["key"]
        read_columns = set()
        record = {"source_id": sid, "key": key, "matched": False}

        row = _fetch_by_key(conn, key) if key is not None else None
        if row is not None:
            target = _row_to_dict(row)
            record.update(matched=True, target_id=target["id"], old=target)
            if target["id"] in target_owner:
                raise PlanConflict(
                    "intra_plan_target",
                    {
                        "target_id": target["id"],
                        "business_key": key,
                        "sources": sorted([target_owner[target["id"]], sid]),
                    },
                )
            target_owner[target["id"]] = sid
            read_columns.add("business_key")  # the match itself reads the key
            branch_index, branch = _select_branch(spec["matched"], source_row, target, read_columns)
            if branch is None:
                record.update(action="noop", branch_index=None)
                kept_keys.setdefault(key, sid)
            else:
                action = branch["action"]
                record.update(action=action, branch_index=branch_index)
                if action == "update":
                    set_values = {}
                    for col, expr in branch["set"].items():
                        value, cols = ast_eval.eval_assignment(expr, source_row, target)
                        read_columns.update(cols)
                        set_values[col] = value
                    _check_set_value_types(action, set_values, sid)
                    record["set"] = set_values
                    actions.append({
                        "source_id": sid, "action": "update",
                        "target_id": target["id"], "set": set_values,
                    })
                    new_key = set_values.get("business_key")
                    if new_key is not None:
                        _register_produced(produced_keys, new_key, sid)
                    if "business_key" not in set_values or new_key == key:
                        kept_keys.setdefault(key, sid)  # key effectively stays
                else:  # delete
                    actions.append({"source_id": sid, "action": "delete", "target_id": target["id"]})
            if validation_mode == "row":
                cols_snapshot = {"business_key": target["business_key"], "rev": target["rev"]}
            else:
                cols_snapshot = {c: target[c] for c in sorted(read_columns)}
            read_rows.append({"key": key, "id": target["id"], "cols": cols_snapshot})
        else:
            # NULL keys never match; unmatched non-NULL keys are phantom-asserted.
            note_absent(key)
            branch_index, branch = _select_branch(spec["not_matched"], source_row, None, read_columns)
            if branch is None:
                record.update(action="noop", branch_index=None)
            else:
                record.update(action="insert", branch_index=branch_index)
                set_values = {}
                for col, expr in branch["set"].items():
                    value, cols = ast_eval.eval_assignment(expr, source_row, None)
                    read_columns.update(cols)
                    set_values[col] = value
                set_values.setdefault("parent_id", None)
                _check_set_value_types("insert", set_values, sid)
                record["set"] = set_values
                actions.append({"source_id": sid, "action": "insert", "set": set_values})
                if set_values["business_key"] is not None:
                    _register_produced(produced_keys, set_values["business_key"], sid)
        sources_out.append(record)

    # A produced key that collides with a key this plan keeps unchanged is
    # a deterministic final-state UNIQUE collision: report it now rather
    # than depending on action order at apply time.
    for new_key, producer in produced_keys.items():
        keeper = kept_keys.get(new_key)
        if keeper is not None and keeper != producer:
            raise PlanConflict(
                "intra_plan_key",
                {"business_key": new_key, "sources": sorted([producer, keeper])},
            )

    # Keys the plan will produce must have been absent in the snapshot, or
    # their presence is the plan's own business (a matched row it renames
    # away). Absent produced keys become phantom assertions; present ones
    # fall through to the UNIQUE constraint backstop at commit time.
    for new_key in produced_keys:
        if new_key not in absent_seen and _fetch_by_key(conn, new_key) is None:
            note_absent(new_key)

    return {
        "format": PLAN_FORMAT,
        "plan_id": plan_id,
        "spec": {"source": spec["source"], "matched": spec["matched"], "not_matched": spec["not_matched"]},
        "validation_mode": validation_mode,
        "read_set": {"rows": read_rows, "absent": read_absent},
        "actions": actions,
        "sources": sources_out,
    }


def _register_produced(produced_keys, new_key, source_id):
    if new_key in produced_keys:
        raise PlanConflict(
            "intra_plan_key",
            {
                "business_key": new_key,
                "sources": sorted([produced_keys[new_key], source_id]),
            },
        )
    produced_keys[new_key] = source_id
