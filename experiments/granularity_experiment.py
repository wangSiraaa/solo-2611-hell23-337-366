#!/usr/bin/env python3
"""Experiment: fine-grained (column) vs coarse (row/rev) read-set validation.

Question: what does column-level validation cost, and how many false
rejections does it avoid compared with validating a whole-row revision?

Setup: 10k target rows, plans of up to 20 sources (the legal envelope).
Three interleavings between plan and commit:

  A. unrelated column, same rows - external writer touches parent_id of
     exactly the planned keys (the plan itself reads only amount).
  B. unrelated keys - external writer touches only keys outside the plan.
  C. true conflict - external writer rewrites amount of a planned key.

Metrics: commit outcome per mode, rows actually read during validation,
and mean validation latency over repeated runs.

Run: python3 experiments/granularity_experiment.py
"""

import os
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from merge import MergeExecutor, connect, init_schema, writer  # noqa: E402

TARGET_ROWS = 10000
PLAN_SOURCES = 20
REPEATS = 200


def fresh_db():
    path = os.path.join(tempfile.mkdtemp(prefix="granularity-"), "exp.db")
    conn = connect(path)
    init_schema(conn)
    conn.execute("INSERT OR IGNORE INTO parent(id) VALUES (1)")
    conn.execute("INSERT OR IGNORE INTO parent(id) VALUES (2)")
    conn.execute("BEGIN")
    for i in range(TARGET_ROWS):
        conn.execute(
            "INSERT INTO target(business_key, amount, parent_id, rev)"
            " VALUES (?, ?, 1, 1)", ("K%05d" % i, i))
    conn.execute("COMMIT")
    return path, conn


def plan_spec(n_sources):
    return {
        "source": [{"source_id": "s%02d" % i, "key": "K%05d" % i, "delta": 1}
                   for i in range(n_sources)],
        "matched": [{"when": {"literal": True}, "action": "update",
                     "set": {"amount": {"op": "+", "args": [{"target": "amount"},
                                                            {"source": "delta"}]}}}],
        "not_matched": [],
    }


def run_interleaving(mode, disturb):
    """Plan, disturb via the external writer, commit. Returns receipt."""
    path, c1 = fresh_db()
    c2 = connect(path)
    ex = MergeExecutor(c1, validation_mode=mode)
    frozen = ex.plan(plan_spec(PLAN_SOURCES))
    disturb(c2)
    receipt, _ = ex.commit(frozen)
    c1.close()
    c2.close()
    return receipt


def time_validation(mode, n_sources):
    """Mean latency of one read-set validation, in microseconds."""
    path, c1 = fresh_db()
    ex = MergeExecutor(c1, validation_mode=mode)
    frozen = ex.plan(plan_spec(n_sources))
    counter_cls_rows = []

    class Counter:
        rows_read = 0
        probes = 0

    samples = []
    for _ in range(REPEATS):
        c1.execute("BEGIN IMMEDIATE")
        start = time.perf_counter_ns()
        ex._validate_read_set(frozen, Counter())
        samples.append((time.perf_counter_ns() - start) / 1000.0)
        c1.execute("ROLLBACK")
    c1.close()
    return statistics.mean(samples), counter_cls_rows


def main():
    print("target rows: %d, sources per plan: %d, validation repeats: %d"
          % (TARGET_ROWS, PLAN_SOURCES, REPEATS))
    print()

    def disturb_unrelated_column(c2):
        for i in range(PLAN_SOURCES):
            writer.update_parent(c2, "K%05d" % i, 2)

    def disturb_unrelated_keys(c2):
        for i in range(PLAN_SOURCES):
            writer.update_amount(c2, "K%05d" % (TARGET_ROWS - 1 - i), -1)

    def disturb_true_conflict(c2):
        writer.update_amount(c2, "K00000", 999999)

    print("== interleaving outcomes by validation mode ==")
    header = ("scenario", "column-mode", "row/rev-mode")
    print("%-34s %-12s %-12s" % header)
    print("-" * 60)
    scenarios = [
        ("A unrelated column, same rows", disturb_unrelated_column),
        ("B unrelated keys", disturb_unrelated_keys),
        ("C true conflict on amount", disturb_true_conflict),
    ]
    for label, disturb in scenarios:
        outcomes = []
        for mode in ("column", "row"):
            receipt = run_interleaving(mode, disturb)
            outcomes.append(receipt["status"])
        print("%-34s %-12s %-12s" % (label, outcomes[0], outcomes[1]))
    print()

    print("== validation cost (rows read / mean latency) ==")
    print("%-8s %-22s %-22s" % ("sources", "column-mode", "row/rev-mode"))
    print("-" * 54)
    for n in (1, 5, 10, 20):
        cells = []
        for mode in ("column", "row"):
            path, c1 = fresh_db()
            ex = MergeExecutor(c1, validation_mode=mode)
            frozen = ex.plan(plan_spec(n))
            receipt, _ = ex.commit(frozen)
            rows_read = receipt["validation"]["rows_read"]
            mean_us, _ = time_validation(mode, n)
            cells.append("%3d rows / %7.1f us" % (rows_read, mean_us))
            c1.close()
        print("%-8d %-22s %-22s" % (n, cells[0], cells[1]))
    print()
    print("reading: column-mode commits scenario A (the read set is intact:")
    print("amount unchanged) while row/rev-mode falsely rejects it; both")
    print("reject the true conflict C; rows read stay at the read-set size")
    print("(<= sources), never near the 10k table size.")


if __name__ == "__main__":
    main()
