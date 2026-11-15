"""Commit-phase executor for frozen MERGE plans.

Protocol (two short transactions, never a plan-lifetime lock):

  1. plan(): open a read transaction, freeze matches/actions via
     planner.plan(), release the read transaction immediately.
  2. commit(): open a short BEGIN IMMEDIATE write transaction, re-verify
     the frozen read set with key-indexed probes (never a table scan),
     apply the frozen actions in source order, and insert the receipt -
     all atomically. Any validation mismatch, or a UNIQUE/FK/NOT NULL/
     rev-guard failure from the database backstop, rolls the whole plan
     back: no half-applied merges, ever.

Receipts: every call ends in a receipt dict with a status:
  committed - applied and durably stored (idempotent under replay),
  conflict  - deterministic intra-plan conflict or read-set validation
              failure; carries conflict kind + identity,
  invalid   - deterministic content problem (AST, source row, overflow,
              plan_id binding),
  failed    - database constraint backstop fired during apply.
Only committed receipts are stored; rejected plans may be re-planned and
retried (a conflict-free plan is always committable).
"""

import json
import sqlite3
from datetime import datetime, timezone

from . import planner
from .errors import PlanConflict, PlanInvalid
from .planner import PLAN_FORMAT


def _utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class _RowCounter:
    """Counts rows actually read from the target table during validation."""

    def __init__(self):
        self.rows_read = 0
        self.probes = 0


class MergeExecutor:
    def __init__(self, conn, validation_mode="column"):
        if validation_mode not in ("column", "row"):
            raise ValueError("validation_mode must be 'column' or 'row'")
        self.conn = conn
        self.validation_mode = validation_mode

    # ---------------------------------------------------------- plan phase
    def plan(self, spec):
        """Freeze a plan. The read transaction is released before returning."""
        self.conn.execute("BEGIN")
        try:
            frozen = planner.plan(self.conn, spec, validation_mode=self.validation_mode)
        except PlanConflict as exc:
            return self._content_receipt(spec, "conflict",
                                         conflict={"kind": exc.kind, "identity": exc.identity})
        except PlanInvalid as exc:
            return self._content_receipt(spec, "invalid", reason=exc.reason, detail=exc.detail)
        finally:
            # Release the read snapshot no matter how we leave.
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
        frozen["planned_utc"] = _utcnow()
        return frozen

    # -------------------------------------------------------- commit phase
    def commit(self, frozen):
        """Validate the read set and apply the frozen plan atomically."""
        try:
            self._check_frozen(frozen)
        except PlanInvalid as exc:
            return self._content_receipt(
                frozen.get("spec") if isinstance(frozen, dict) else None,
                "invalid", reason=exc.reason, detail=exc.detail)
        plan_id = frozen["plan_id"]
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            stored = self._stored_receipt(plan_id)
            if stored is not None:
                self.conn.execute("COMMIT")
                return stored, True  # identical replay of a committed plan

            counter = _RowCounter()
            conflict = self._validate_read_set(frozen, counter)
            if conflict is not None:
                self.conn.execute("ROLLBACK")
                kind, identity = conflict
                receipt = self._base_receipt(frozen, "conflict")
                receipt["conflict"] = {"kind": kind, "identity": identity}
                receipt["validation"] = self._validation_report(frozen, counter)
                return receipt, False

            new_values = self._apply_actions(frozen)
            receipt = self._base_receipt(frozen, "committed")
            receipt["validation"] = self._validation_report(frozen, counter)
            for source in receipt["sources"]:
                sid = source["source_id"]
                if sid in new_values:
                    source["new"] = new_values[sid]
            self._store_receipt(plan_id, receipt)
            self.conn.execute("COMMIT")
            return receipt, False
        except sqlite3.IntegrityError as exc:
            self.conn.execute("ROLLBACK")
            receipt = self._base_receipt(frozen, "failed")
            receipt["failure"] = {"kind": "constraint", "message": str(exc)}
            return receipt, False
        except Exception:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def submit(self, spec):
        """Plan and commit in one call. Returns (receipt, replayed).

        A plan_id that is already committed replays its stored receipt
        before any planning happens, so a client retry after a lost
        confirmation never re-plans against a changed world.
        """
        try:
            plan_id = planner.compute_plan_id(spec)
        except Exception:
            plan_id = None
        if plan_id is not None:
            stored = self._stored_receipt(plan_id)
            if stored is not None:
                return stored, True
        planned = self.plan(spec)
        if planned.get("status"):  # planning already produced a receipt
            return planned, False
        return self.commit(planned)

    # --------------------------------------------------------- internals
    def _check_frozen(self, frozen):
        if not isinstance(frozen, dict) or frozen.get("format") != PLAN_FORMAT:
            raise PlanInvalid("not a frozen plan produced by this planner")
        # plan_id must bind the frozen source content; a tampered spec or
        # mismatched id is rejected deterministically.
        recomputed = planner.compute_plan_id(frozen["spec"])
        if recomputed != frozen.get("plan_id"):
            raise PlanInvalid("frozen plan_id does not bind to its source content")

    def _stored_receipt(self, plan_id):
        row = self.conn.execute(
            "SELECT receipt_json FROM merge_receipt WHERE plan_id = ?", (plan_id,)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def _store_receipt(self, plan_id, receipt):
        self.conn.execute(
            "INSERT INTO merge_receipt(plan_id, status, receipt_json, created_utc)"
            " VALUES (?, ?, ?, ?)",
            (plan_id, receipt["status"], json.dumps(receipt, sort_keys=True), _utcnow()),
        )

    def _content_receipt(self, spec, status, reason=None, detail=None, conflict=None):
        plan_id = None
        if isinstance(spec, dict):
            try:
                plan_id = planner.compute_plan_id(spec)
            except Exception:
                plan_id = spec.get("plan_id") if isinstance(spec.get("plan_id"), str) else None
        receipt = {
            "plan_id": plan_id,
            "status": status,
            "receipt_utc": _utcnow(),
            "sources": [],
        }
        if reason is not None:
            receipt["reason"] = reason
            if detail is not None:
                receipt["detail"] = detail
        if conflict is not None:
            receipt["conflict"] = conflict
        return receipt

    def _base_receipt(self, frozen, status):
        return {
            "plan_id": frozen["plan_id"],
            "status": status,
            "receipt_utc": _utcnow(),
            "sources": json.loads(json.dumps(frozen["sources"])),
        }

    def _validation_report(self, frozen, counter):
        return {
            "mode": frozen.get("validation_mode", self.validation_mode),
            "rows_read": counter.rows_read,
            "probes": counter.probes,
            "read_rows": len(frozen["read_set"]["rows"]),
            "read_absent": len(frozen["read_set"]["absent"]),
        }

    def _validate_read_set(self, frozen, counter):
        """Re-verify the frozen read set. Returns (kind, identity) or None.

        Every probe is a key-indexed lookup on the business_key UNIQUE
        index; the number of rows touched is bounded by the read set size,
        never by the target table size.
        """
        mode = frozen.get("validation_mode", self.validation_mode)
        for entry in frozen["read_set"]["rows"]:
            key, expected_id, expected_cols = entry["key"], entry["id"], entry["cols"]
            counter.probes += 1
            cur = self.conn.execute(
                "SELECT id, business_key, amount, parent_id, rev FROM target"
                " WHERE business_key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row is None:
                return "target_deleted", {"business_key": key, "target_id": expected_id}
            counter.rows_read += 1
            actual = {"id": row[0], "business_key": row[1], "amount": row[2],
                      "parent_id": row[3], "rev": row[4]}
            if actual["id"] != expected_id:
                return "target_replaced", {
                    "business_key": key,
                    "expected_id": expected_id,
                    "actual_id": actual["id"],
                }
            compared = ("rev",) if mode == "row" else tuple(sorted(expected_cols))
            for col in compared:
                if actual[col] != expected_cols[col]:
                    return "target_modified", {
                        "business_key": key,
                        "target_id": expected_id,
                        "column": col,
                        "expected": expected_cols[col],
                        "actual": actual[col],
                    }
        for key in frozen["read_set"]["absent"]:
            counter.probes += 1
            cur = self.conn.execute(
                "SELECT 1 FROM target WHERE business_key = ? LIMIT 1", (key,)
            )
            if cur.fetchone() is not None:
                counter.rows_read += 1
                return "phantom", {"business_key": key}
        return None

    def _apply_actions(self, frozen):
        """Apply frozen actions in source order; returns source_id -> new row."""
        new_values = {}
        for action in frozen["actions"]:
            kind = action["action"]
            if kind == "update":
                set_values = action["set"]
                assignments = ", ".join("%s = ?" % col for col in sorted(set_values))
                params = [set_values[col] for col in sorted(set_values)]
                params.append(action["target_id"])
                self.conn.execute(
                    "UPDATE target SET %s, rev = rev + 1 WHERE id = ?" % assignments,
                    params,
                )
                row = self.conn.execute(
                    "SELECT id, business_key, amount, parent_id, rev FROM target WHERE id = ?",
                    (action["target_id"],),
                ).fetchone()
                new_values[action["source_id"]] = dict(
                    zip(("id", "business_key", "amount", "parent_id", "rev"), row)
                )
            elif kind == "delete":
                self.conn.execute("DELETE FROM target WHERE id = ?", (action["target_id"],))
            elif kind == "insert":
                set_values = action["set"]
                cur = self.conn.execute(
                    "INSERT INTO target(business_key, amount, parent_id, rev)"
                    " VALUES (?, ?, ?, 1)",
                    (
                        set_values["business_key"],
                        set_values["amount"],
                        set_values.get("parent_id"),
                    ),
                )
                new_values[action["source_id"]] = {
                    "id": cur.lastrowid,
                    "business_key": set_values["business_key"],
                    "amount": set_values["amount"],
                    "parent_id": set_values.get("parent_id"),
                    "rev": 1,
                }
            else:  # pragma: no cover - planner never emits other kinds
                raise PlanInvalid("unknown frozen action %r" % kind)
        return new_values


def connect(path, busy_timeout_ms=5000):
    """Open a connection with the settings this system relies on."""
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = %d" % busy_timeout_ms)
    if path != ":memory:":
        conn.execute("PRAGMA journal_mode = WAL")
    return conn
