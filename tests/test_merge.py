"""Invariant tests for the constrained MERGE executor.

Covers, with deterministic two-connection interleavings on a shared WAL
database: lost update, missing-key phantom, key rename after a frozen
match, branch three-valued logic, foreign-key failure atomicity, lost
confirmation after commit, disjoint plans, deterministic intra-plan
conflicts, int64 overflow, read-set validation cost bounded by the read
set (never the table), and idempotent replay.

Run: python -m unittest discover -s tests -v
"""

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from merge import MergeExecutor, connect, init_schema, writer  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fixtures")

UPDATE_ADD_DELTA = {
    "when": {"literal": True},
    "action": "update",
    "set": {"amount": {"op": "+", "args": [{"target": "amount"}, {"source": "delta"}]}},
}
INSERT_SELF = {
    "when": {"literal": True},
    "action": "insert",
    "set": {"business_key": {"source": "key"}, "amount": {"source": "delta"}},
}


def merge_spec(rows, matched=None, not_matched=None):
    """Build a new-style spec from (source_id, key, delta) tuples."""
    return {
        "source": [
            dict({"source_id": sid, "key": key, "delta": delta}, **extra)
            for sid, key, delta, extra in rows
        ],
        "matched": matched if matched is not None else [UPDATE_ADD_DELTA],
        "not_matched": not_matched if not_matched is not None else [INSERT_SELF],
    }


def row(sid, key, delta, **extra):
    return (sid, key, delta, extra)


class MergeTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="merge-test-")
        self.db = os.path.join(self.dir, "test.db")
        self.c1 = connect(self.db)
        init_schema(self.c1, seed=True)
        self.c2 = connect(self.db)
        self.ex = MergeExecutor(self.c1)

    def tearDown(self):
        self.c1.close()
        self.c2.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def amount(self, key):
        row = self.c1.execute(
            "SELECT amount FROM target WHERE business_key = ?", (key,)).fetchone()
        return row[0] if row else None

    def key_of_id(self, target_id):
        row = self.c1.execute(
            "SELECT business_key FROM target WHERE id = ?", (target_id,)).fetchone()
        return row[0] if row else None

    def keys(self):
        return sorted(r[0] for r in self.c1.execute(
            "SELECT business_key FROM target WHERE business_key IS NOT NULL"))


class TestInterleavings(MergeTestBase):
    """Deterministic two-connection interleavings (conn1 plans/commits,
    conn2 is the external writer going through the revision entry)."""

    def test_lost_update_rejected(self):
        frozen = self.ex.plan(merge_spec([row("s1", "A", 5)]))
        writer.update_amount(self.c2, "A", 100)
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("target_modified", receipt["conflict"]["kind"])
        self.assertEqual({"business_key": "A", "target_id": 1, "column": "amount",
                          "expected": 10, "actual": 100},
                         receipt["conflict"]["identity"])
        self.assertEqual(100, self.amount("A"))  # external write survives
        self.assertEqual(2, self.c1.execute(
            "SELECT rev FROM target WHERE business_key='A'").fetchone()[0])

    def test_lost_update_variant_delete(self):
        frozen = self.ex.plan(merge_spec([row("s1", "B", 3)]))
        writer.delete_row(self.c2, "B")
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("target_deleted", receipt["conflict"]["kind"])
        self.assertIsNone(self.amount("B"))

    def test_missing_key_phantom_rejected(self):
        frozen = self.ex.plan(merge_spec([row("s1", "C", 7)]))
        writer.insert_row(self.c2, "C", 9)
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("phantom", receipt["conflict"]["kind"])
        self.assertEqual({"business_key": "C"}, receipt["conflict"]["identity"])
        self.assertEqual(9, self.amount("C"))  # only the external row exists

    def test_phantom_on_produced_rename_key(self):
        # Plan renames A -> Z; an external insert of Z must also conflict.
        spec = merge_spec(
            [row("s1", "A", 5, new_key="Z")],
            matched=[{"when": {"literal": True}, "action": "update",
                      "set": {"business_key": {"source": "new_key"}}}],
        )
        frozen = self.ex.plan(spec)
        writer.insert_row(self.c2, "Z", 1)
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("phantom", receipt["conflict"]["kind"])
        self.assertEqual("Z", receipt["conflict"]["identity"]["business_key"])
        self.assertEqual("A", self.key_of_id(1))

    def test_key_rename_after_frozen_match(self):
        frozen = self.ex.plan(merge_spec([row("s1", "A", 5)]))
        writer.rename_key(self.c2, "A", "A2")
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("target_deleted", receipt["conflict"]["kind"])
        self.assertEqual(10, self.amount("A2"))  # untouched
        self.assertIsNone(self.amount("A"))

    def test_disjoint_plans_both_commit(self):
        spec_p = merge_spec([row("s1", "A", 1)])
        spec_q = merge_spec([row("s1", "B", 2)])
        frozen_p = self.ex.plan(spec_p)
        frozen_q = self.ex.plan(spec_q)  # both frozen before either commits
        rp, _ = self.ex.commit(frozen_p)
        rq, _ = self.ex.commit(frozen_q)
        self.assertEqual("committed", rp["status"])
        self.assertEqual("committed", rq["status"])
        self.assertEqual(11, self.amount("A"))
        self.assertEqual(22, self.amount("B"))

    def test_unrelated_key_update_does_not_fail_plan(self):
        writer.insert_row(self.c2, "Z", 50)
        frozen = self.ex.plan(merge_spec([row("s1", "A", 5)]))
        writer.update_amount(self.c2, "Z", 999)  # key not involved in the plan
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("committed", receipt["status"])
        self.assertEqual(15, self.amount("A"))
        self.assertEqual(999, self.amount("Z"))

    def test_unrelated_column_update_commits_in_column_mode(self):
        # The plan reads only amount; an external parent_id update to the
        # same row must not falsely reject it.
        self.c2.execute("INSERT OR IGNORE INTO parent(id) VALUES (2)")
        frozen = self.ex.plan(merge_spec([row("s1", "A", 5)]))
        writer.update_parent(self.c2, "A", 2)
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("committed", receipt["status"])
        self.assertEqual(15, self.amount("A"))

    def test_same_column_update_in_row_mode_would_reject(self):
        # Coarse row/rev validation rejects the same legal interleaving;
        # this is the trade-off quantified by experiments/granularity_experiment.py.
        ex_row = MergeExecutor(self.c1, validation_mode="row")
        self.c2.execute("INSERT OR IGNORE INTO parent(id) VALUES (2)")
        frozen = ex_row.plan(merge_spec([row("s1", "A", 5)]))
        writer.update_parent(self.c2, "A", 2)
        receipt, _ = ex_row.commit(frozen)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("target_modified", receipt["conflict"]["kind"])
        self.assertEqual("rev", receipt["conflict"]["identity"]["column"])

    def test_read_transaction_released_after_plan(self):
        frozen = self.ex.plan(merge_spec([row("s1", "A", 5)]))
        self.assertFalse(self.c1.in_transaction)
        # The other writer can commit immediately: no plan-lifetime lock.
        writer.update_amount(self.c2, "B", 77)
        self.assertEqual(77, self.amount("B"))
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("committed", receipt["status"])

    def test_lost_confirmation_replay_is_idempotent(self):
        spec = merge_spec([row("s1", "A", 5)])
        r1, replayed1 = self.ex.submit(spec)
        self.assertEqual("committed", r1["status"])
        self.assertFalse(replayed1)
        # Client never saw the receipt and resends the identical plan.
        r2, replayed2 = self.ex.submit(spec)
        self.assertEqual("committed", r2["status"])
        self.assertTrue(replayed2)
        self.assertEqual(json.dumps(r1, sort_keys=True), json.dumps(r2, sort_keys=True))
        self.assertEqual(15, self.amount("A"))  # not 20: applied exactly once
        # The stored receipt is queryable by plan_id.
        stored = self.c1.execute(
            "SELECT receipt_json FROM merge_receipt WHERE plan_id = ?",
            (r1["plan_id"],)).fetchone()
        self.assertIsNotNone(stored)
        self.assertEqual(json.dumps(r1, sort_keys=True),
                         json.dumps(json.loads(stored[0]), sort_keys=True))

    def test_rejected_plan_not_stored_and_retriable(self):
        spec = merge_spec([row("s1", "A", 5)])
        frozen = self.ex.plan(spec)
        writer.update_amount(self.c2, "A", 100)
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("conflict", receipt["status"])
        self.assertIsNone(self.c1.execute(
            "SELECT 1 FROM merge_receipt WHERE plan_id = ?",
            (receipt["plan_id"],)).fetchone())
        # Re-planning against the new state commits cleanly.
        r2, _ = self.ex.submit(spec)
        self.assertEqual("committed", r2["status"])
        self.assertEqual(105, self.amount("A"))


class TestFixtureInterleavings(MergeTestBase):
    """Drives fixtures/interleavings.json step by step."""

    def _legacy_spec(self, name, source_rows):
        # Old-format fixture rows ({key, delta}) adapted to the new
        # interface: stable source_ids + default MERGE branches.
        return merge_spec(
            [row("%s-%d" % (name, i), r["key"], r["delta"])
             for i, r in enumerate(source_rows)]
        )

    def _run(self, steps):
        # Each scenario runs against a fresh database: the fixture
        # scenarios are independent of one another.
        self.tearDown()
        self.setUp()
        frozen, receipts = {}, {}
        for step in steps:
            if "plan" in step:
                frozen[step["plan"]] = self.ex.plan(self._legacy_spec(step["plan"], step["source"]))
            elif "external_update" in step:
                u = step["external_update"]
                writer.update_amount(self.c2, u["key"], u["amount"])
            elif "external_insert" in step:
                u = step["external_insert"]
                writer.insert_row(self.c2, u["key"], u["amount"])
            elif "commit" in step:
                receipts[step["commit"]], _ = self.ex.commit(frozen[step["commit"]])
            else:  # pragma: no cover
                raise AssertionError("unknown step %r" % step)
        return receipts

    def test_fixture_interleavings(self):
        with open(os.path.join(FIXTURES, "interleavings.json")) as fh:
            scenarios = json.load(fh)

        receipts = self._run(scenarios["lost_update"])
        self.assertEqual("conflict", receipts["P"]["status"])
        self.assertEqual("target_modified", receipts["P"]["conflict"]["kind"])
        self.assertEqual(100, self.amount("A"))

        receipts = self._run(scenarios["absent_insert"])
        self.assertEqual("conflict", receipts["P"]["status"])
        self.assertEqual("phantom", receipts["P"]["conflict"]["kind"])
        self.assertEqual(9, self.amount("C"))

        receipts = self._run(scenarios["disjoint"])
        self.assertEqual("committed", receipts["P"]["status"])
        self.assertEqual("committed", receipts["Q"]["status"])
        self.assertEqual(11, self.amount("A"))
        self.assertEqual(22, self.amount("B"))


class TestFixtureMergePlan(MergeTestBase):
    def test_fixture_plan_end_to_end(self):
        with open(os.path.join(FIXTURES, "merge-plan.json")) as fh:
            spec = json.load(fh)
        placeholder = spec.pop("plan_id")
        self.assertEqual("example-P", placeholder)
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("committed", receipt["status"])
        by_sid = {s["source_id"]: s for s in receipt["sources"]}
        # s1: matched A, first branch (delta 5 > 0) -> update amount and key.
        self.assertEqual("update", by_sid["s1"]["action"])
        self.assertEqual(0, by_sid["s1"]["branch_index"])
        self.assertEqual({"amount": 15, "business_key": "A2"}, by_sid["s1"]["set"])
        # s2: not matched -> insert branch.
        self.assertEqual("insert", by_sid["s2"]["action"])
        self.assertEqual({"business_key": "C", "amount": 7, "parent_id": 1},
                         by_sid["s2"]["set"])
        self.assertEqual(["A2", "B", "C"], self.keys())
        self.assertEqual(15, self.amount("A2"))
        self.assertEqual(7, self.amount("C"))
        self.assertEqual(2, self.c1.execute(
            "SELECT rev FROM target WHERE business_key='A2'").fetchone()[0])
        # Validation touched only the read set, not the whole table.
        self.assertLessEqual(receipt["validation"]["rows_read"], 2)


class TestBranchLogic(MergeTestBase):
    def test_null_condition_falls_through_to_delete(self):
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 5}],
            "matched": [
                # NULL > 0 is NULL -> not TRUE
                {"when": {"op": ">", "args": [{"literal": None}, {"literal": 0}]},
                 "action": "update", "set": {"amount": {"literal": 1}}},
                # NOT(NULL) is NULL -> not TRUE
                {"when": {"op": "not", "args": [
                    {"op": ">", "args": [{"literal": None}, {"literal": 0}]}]},
                 "action": "update", "set": {"amount": {"literal": 2}}},
                {"when": {"literal": True}, "action": "delete"},
            ],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("committed", receipt["status"])
        self.assertEqual("delete", receipt["sources"][0]["action"])
        self.assertEqual(2, receipt["sources"][0]["branch_index"])
        self.assertEqual(["B"], self.keys())

    def test_three_valued_and_or(self):
        null_gt_0 = {"op": ">", "args": [{"literal": None}, {"literal": 0}]}
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 5}],
            "matched": [
                # FALSE OR NULL -> NULL -> not TRUE
                {"when": {"op": "or", "args": [{"literal": False}, null_gt_0]},
                 "action": "update", "set": {"amount": {"literal": 1}}},
                # TRUE AND NULL -> NULL -> not TRUE
                {"when": {"op": "and", "args": [{"literal": True}, null_gt_0]},
                 "action": "update", "set": {"amount": {"literal": 2}}},
                # TRUE OR NULL -> TRUE -> taken
                {"when": {"op": "or", "args": [{"literal": True}, null_gt_0]},
                 "action": "update", "set": {"amount": {"literal": 42}}},
            ],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("committed", receipt["status"])
        self.assertEqual(2, receipt["sources"][0]["branch_index"])
        self.assertEqual(42, self.amount("A"))

    def test_first_true_branch_wins(self):
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 5}],
            "matched": [
                {"when": {"literal": True}, "action": "update",
                 "set": {"amount": {"literal": 111}}},
                {"when": {"literal": True}, "action": "delete"},
            ],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("update", receipt["sources"][0]["action"])
        self.assertEqual(111, self.amount("A"))

    def test_no_true_branch_is_noop(self):
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 5}],
            "matched": [{"when": {"literal": False}, "action": "delete"}],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("committed", receipt["status"])
        self.assertEqual("noop", receipt["sources"][0]["action"])
        self.assertEqual(10, self.amount("A"))

    def test_is_null_and_text_binary_comparison(self):
        spec = {
            "source": [{"source_id": "s1", "key": "B", "delta": 1}],
            "matched": [
                # parent_id of B is NULL
                {"when": {"op": "is_null", "args": [{"target": "parent_id"}]},
                 "action": "update",
                 "set": {"business_key": {"literal": "b- renamed"}}},
            ],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("committed", receipt["status"])
        self.assertEqual("b- renamed", self.key_of_id(2))
        # SQLite BINARY text comparison: 'Z' < 'a'.
        spec2 = {
            "source": [{"source_id": "s1", "key": "A", "delta": 1}],
            "matched": [
                {"when": {"op": "<", "args": [{"literal": "Z"}, {"literal": "a"}]},
                 "action": "update", "set": {"amount": {"literal": 66}}},
            ],
            "not_matched": [],
        }
        receipt2, _ = self.ex.submit(spec2)
        self.assertEqual("committed", receipt2["status"])
        self.assertEqual(66, self.amount("A"))

    def test_delete_branch(self):
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 5}],
            "matched": [{"when": {"op": ">", "args": [{"source": "delta"}, {"literal": 3}]},
                         "action": "delete"}],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("committed", receipt["status"])
        self.assertEqual("delete", receipt["sources"][0]["action"])
        self.assertEqual(["B"], self.keys())

    def test_null_key_never_matches_and_inserts(self):
        writer.insert_row(self.c2, None, 1)  # existing NULL-key row
        spec = merge_spec([row("s1", None, 8)])
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("committed", receipt["status"])
        self.assertFalse(receipt["sources"][0]["matched"])
        self.assertEqual("insert", receipt["sources"][0]["action"])
        count = self.c1.execute(
            "SELECT COUNT(*) FROM target WHERE business_key IS NULL").fetchone()[0]
        self.assertEqual(2, count)  # NULL keys coexist; no phantom assertion


class TestIntraPlanConflicts(MergeTestBase):
    def test_two_sources_same_target(self):
        spec = merge_spec([row("s1", "A", 1), row("s2", "A", 2)])
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("intra_plan_target", receipt["conflict"]["kind"])
        self.assertEqual(["s1", "s2"], receipt["conflict"]["identity"]["sources"])
        self.assertEqual(10, self.amount("A"))

    def test_noop_branch_does_not_evade_same_target_conflict(self):
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 1},
                       {"source_id": "s2", "key": "A", "delta": 2}],
            "matched": [
                {"when": {"literal": True}, "action": "update",
                 "set": {"amount": {"literal": 1}}},
                {"when": {"literal": False}, "action": "delete"},  # s2 no-ops
            ],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("intra_plan_target", receipt["conflict"]["kind"])

    def test_two_inserts_same_new_key(self):
        spec = {
            "source": [{"source_id": "s1", "key": "X", "delta": 1},
                       {"source_id": "s2", "key": "Y", "delta": 2}],
            "matched": [],
            "not_matched": [{"when": {"literal": True}, "action": "insert",
                             "set": {"business_key": {"literal": "C"},
                                     "amount": {"source": "delta"}}}],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("intra_plan_key", receipt["conflict"]["kind"])
        self.assertEqual("C", receipt["conflict"]["identity"]["business_key"])
        self.assertEqual(["A", "B"], self.keys())

    def test_rename_and_insert_produce_same_key(self):
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 1, "new_key": "N"},
                       {"source_id": "s2", "key": "Y", "delta": 2}],
            "matched": [{"when": {"literal": True}, "action": "update",
                         "set": {"business_key": {"source": "new_key"}}}],
            "not_matched": [{"when": {"literal": True}, "action": "insert",
                             "set": {"business_key": {"literal": "N"},
                                     "amount": {"source": "delta"}}}],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("intra_plan_key", receipt["conflict"]["kind"])

    def test_rename_onto_key_kept_by_same_plan(self):
        # s1 renames A->B while s2 keeps B: a deterministic final-state
        # UNIQUE collision, reported at plan time (not via apply order).
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 1, "new_key": "B"},
                       {"source_id": "s2", "key": "B", "delta": 2, "new_key": None}],
            "matched": [
                {"when": {"op": "is_not_null", "args": [{"source": "new_key"}]},
                 "action": "update",
                 "set": {"business_key": {"source": "new_key"}}},
                {"when": {"literal": True}, "action": "update",
                 "set": {"amount": {"op": "+", "args": [{"target": "amount"},
                                                       {"source": "delta"}]}}},
            ],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("intra_plan_key", receipt["conflict"]["kind"])
        self.assertEqual("B", receipt["conflict"]["identity"]["business_key"])
        self.assertEqual(["s1", "s2"], receipt["conflict"]["identity"]["sources"])
        self.assertEqual(10, self.amount("A"))
        self.assertEqual(20, self.amount("B"))

    def test_frozen_matching_no_rematch_after_own_insert(self):
        # s1 inserts B2; s2's key B2 must NOT match s1's pending insert:
        # matches are frozen on the pre-plan snapshot, so both go to
        # not_matched and collide deterministically on the produced key.
        spec = {
            "source": [{"source_id": "s1", "key": "B2", "delta": 1},
                       {"source_id": "s2", "key": "B2", "delta": 2}],
            "matched": [],
            "not_matched": [INSERT_SELF],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("intra_plan_key", receipt["conflict"]["kind"])
        self.assertEqual(["A", "B"], self.keys())


class TestConstraintBackstop(MergeTestBase):
    def test_fk_failure_rolls_back_everything(self):
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 5},
                       {"source_id": "s2", "key": "ZZ", "delta": 7, "parent_id": 999}],
            "matched": [UPDATE_ADD_DELTA],
            "not_matched": [{"when": {"literal": True}, "action": "insert",
                             "set": {"business_key": {"source": "key"},
                                     "amount": {"source": "delta"},
                                     "parent_id": {"source": "parent_id"}}}],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("failed", receipt["status"])
        self.assertIn("FOREIGN KEY", receipt["failure"]["message"])
        # No half-update: the valid update of A is rolled back too.
        self.assertEqual(10, self.amount("A"))
        self.assertEqual(["A", "B"], self.keys())
        self.assertIsNone(self.c1.execute(
            "SELECT 1 FROM merge_receipt WHERE plan_id = ?",
            (receipt["plan_id"],)).fetchone())

    def test_fk_failure_then_success_after_parent_appears(self):
        spec = {
            "source": [{"source_id": "s1", "key": "ZZ", "delta": 7, "parent_id": 999}],
            "matched": [],
            "not_matched": [{"when": {"literal": True}, "action": "insert",
                             "set": {"business_key": {"source": "key"},
                                     "amount": {"source": "delta"},
                                     "parent_id": {"source": "parent_id"}}}],
        }
        r1, _ = self.ex.submit(spec)
        self.assertEqual("failed", r1["status"])
        self.c2.execute("INSERT INTO parent(id) VALUES (999)")
        r2, _ = self.ex.submit(spec)
        self.assertEqual("committed", r2["status"])
        self.assertEqual(7, self.amount("ZZ"))

    def test_unique_backstop_against_existing_key(self):
        # Rename A -> B collides with existing B: no plan-level rule fires
        # (B exists and stays), so the UNIQUE constraint backstop applies
        # and rolls the whole plan back atomically.
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 1, "new_key": "B"}],
            "matched": [{"when": {"literal": True}, "action": "update",
                         "set": {"business_key": {"source": "new_key"}}}],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("failed", receipt["status"])
        self.assertIn("UNIQUE", receipt["failure"]["message"])
        self.assertEqual("A", self.key_of_id(1))
        self.assertEqual(10, self.amount("A"))

    def test_rev_guard_trigger_rejects_unmaintained_write(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.c2.execute("UPDATE target SET amount = 1 WHERE business_key = 'A'")
        with self.assertRaises(sqlite3.IntegrityError):
            self.c2.execute(
                "INSERT INTO target(business_key, amount, rev) VALUES ('Q', 1, 7)")


class TestDeterministicRejection(MergeTestBase):
    def test_overflow_in_arithmetic_rejects_plan(self):
        big = 2 ** 63 - 1
        spec = merge_spec([row("s1", "A", big)])
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("overflow", receipt["reason"])
        self.assertEqual(10, self.amount("A"))

    def test_overflow_in_source_delta_rejected(self):
        spec = merge_spec([row("s1", "A", 2 ** 63)])
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("int64", receipt["reason"])

    def test_null_delta_rejected(self):
        # SourceRow.delta is int64 per the spec; null is not legal input.
        spec = {"source": [{"source_id": "s1", "key": "A", "delta": None}],
                "matched": [], "not_matched": []}
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("int64", receipt["reason"])

    def test_non_integer_arithmetic_rejected(self):
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 1}],
            "matched": [{"when": {"literal": True}, "action": "update",
                         "set": {"amount": {"op": "+", "args": [
                             {"target": "business_key"}, {"literal": 1}]}}}],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("non-integer", receipt["reason"])

    def test_plan_id_must_bind_content_hash(self):
        spec = merge_spec([row("s1", "A", 5)])
        spec["plan_id"] = "forged"
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("plan_id", receipt["reason"])

    def test_ast_depth_limit(self):
        expr = {"literal": True}
        for _ in range(8):  # depth 9 > 8
            expr = {"op": "not", "args": [expr]}
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 1}],
            "matched": [{"when": expr, "action": "delete"}],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("depth", receipt["reason"])

    def test_not_matched_target_reference_is_illegal(self):
        spec = {
            "source": [{"source_id": "s1", "key": "ZZ", "delta": 1}],
            "matched": [],
            "not_matched": [{"when": {"op": ">", "args": [{"target": "amount"},
                                                          {"literal": 0}]},
                             "action": "insert",
                             "set": {"business_key": {"source": "key"},
                                     "amount": {"source": "delta"}}}],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("target", receipt["reason"])

    def test_omitted_optional_field_reference_rejected(self):
        # new_key omitted is not the same as explicit null.
        spec = {
            "source": [{"source_id": "s1", "key": "A", "delta": 1}],
            "matched": [{"when": {"literal": True}, "action": "update",
                         "set": {"business_key": {"source": "new_key"}}}],
            "not_matched": [],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("omitted", receipt["reason"])

    def test_max_sources_enforced(self):
        spec = merge_spec([row("s%d" % i, "K%d" % i, 1) for i in range(21)])
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("source", receipt["reason"])

    def test_insert_requires_key_and_amount(self):
        spec = {
            "source": [{"source_id": "s1", "key": "ZZ", "delta": 1}],
            "matched": [],
            "not_matched": [{"when": {"literal": True}, "action": "insert",
                             "set": {"amount": {"source": "delta"}}}],
        }
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("invalid", receipt["status"])
        self.assertIn("business_key", receipt["reason"])


class TestValidationCost(MergeTestBase):
    def test_rows_read_bounded_by_read_set_not_table_size(self):
        # 10k target rows; the plan touches 20 keys.
        self.c1.execute("BEGIN")
        for i in range(10000):
            self.c1.execute(
                "INSERT INTO target(business_key, amount, parent_id, rev)"
                " VALUES (?, ?, 1, 1)", ("K%05d" % i, i))
        self.c1.execute("COMMIT")
        sources = [row("m%d" % i, "K%05d" % i, 1) for i in range(10)]
        sources += [row("n%d" % i, "N%05d" % i, 1) for i in range(10)]
        receipt, _ = self.ex.submit(merge_spec(sources))
        self.assertEqual("committed", receipt["status"])
        validation = receipt["validation"]
        # 10 matched rows read; absent probes read 0 rows. Nowhere near 10k.
        self.assertEqual(10, validation["rows_read"])
        self.assertLessEqual(validation["probes"], 3 * 20)
        # The probe uses the UNIQUE index, never a scan.
        plan_rows = list(self.c1.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM target WHERE business_key = ?",
            ("K00000",)))
        self.assertIn("SEARCH", plan_rows[0][-1])
        self.assertIn("business_key", plan_rows[0][-1])

    def test_rejection_reports_rows_read_and_reason(self):
        for i in range(100):
            self.c2.execute(
                "INSERT INTO target(business_key, amount, rev) VALUES (?, ?, 1)",
                ("R%03d" % i, i))
        frozen = self.ex.plan(merge_spec([row("s1", "R050", 1)]))
        writer.update_amount(self.c2, "R050", 12345)
        receipt, _ = self.ex.commit(frozen)
        self.assertEqual("conflict", receipt["status"])
        self.assertEqual("target_modified", receipt["conflict"]["kind"])
        self.assertEqual(1, receipt["validation"]["rows_read"])
        self.assertEqual("amount", receipt["conflict"]["identity"]["column"])


class TestRenameAndDelete(MergeTestBase):
    def test_update_renames_key(self):
        spec = merge_spec(
            [row("s1", "A", 5, new_key="A2")],
            matched=[{"when": {"literal": True}, "action": "update",
                      "set": {"business_key": {"source": "new_key"},
                              "amount": {"op": "+", "args": [{"target": "amount"},
                                                             {"source": "delta"}]}}}],
        )
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("committed", receipt["status"])
        self.assertEqual("A2", self.key_of_id(1))
        self.assertEqual(15, self.amount("A2"))
        self.assertIsNone(self.amount("A"))

    def test_explicit_null_new_key_sets_null(self):
        spec = merge_spec(
            [row("s1", "A", 5, new_key=None)],
            matched=[{"when": {"literal": True}, "action": "update",
                      "set": {"business_key": {"source": "new_key"}}}],
        )
        receipt, _ = self.ex.submit(spec)
        self.assertEqual("committed", receipt["status"])
        self.assertIsNone(self.key_of_id(1))


if __name__ == "__main__":
    unittest.main()
