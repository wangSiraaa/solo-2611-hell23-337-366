"""Invariant tests for the exmem engine.

Covers: occurrence identity, quota consumption order, NULL/codepoint equality,
AST parenthesisation, deep/duplicate-source plans, randomized differential
testing vs the in-memory reference (operators.py), the 4096 resident-payload
limit, temp-disk recycling, count conservation, per-occurrence spot checks,
and the real 100k-row hot-key benchmark with its 6x temp-disk budget.

Run:  python3 -m unittest discover -s tests -v
"""
import json
import os
import random
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import operators  # in-memory reference implementation
from exmem.engine import run_plan, PlanError, DataError
from exmem.meter import Budget, BudgetExceeded

FIX = REPO / "fixtures"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def write_sources(folder, sources):
    paths = {}
    for name, rows in sources.items():
        p = Path(folder) / f"{name}.jsonl"
        with open(p, "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        paths[name] = str(p)
    return paths


def run_engine(plan, sources, folder, budget=None, payload_limit=None,
               audit_groups=False):
    paths = write_sources(folder, sources)
    out = Path(folder) / "out.jsonl"
    kw = {}
    if audit_groups:
        kw["audit_groups_path"] = str(Path(folder) / "groups.jsonl")
    rep = run_plan(plan, paths, str(out), Path(folder) / "tmp",
                   budget=budget, payload_limit=payload_limit, **kw)
    rows = []
    with open(out, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            rows.append((rec["row"], rec["witness"]))
    return rows, rep


def run_reference(plan, sources):
    return [(x["row"], x["witness"]) for x in operators.evaluate(
        plan, {k: [json.loads(json.dumps(v)) for v in rows]
               for k, rows in sources.items()})]


def tiny_budget():
    """Force many spill runs and multi-pass merges on small data."""
    return Budget(run_rows=8, merge_fanin=3, merge_buf_rows=2,
                  reader_buf=4, writer_buf=4, counts_buf=4)


class EngineTestBase(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.mkdtemp(prefix="exmem-test-")

    def tearDown(self):
        shutil.rmtree(self.td, ignore_errors=True)

    def assert_matches_reference(self, plan, sources, budget=None):
        got, rep = run_engine(plan, sources, self.td, budget=budget)
        want = run_reference(plan, sources)
        self.assertEqual(want, got, f"plan={json.dumps(plan)}")
        return rep


# ---------------------------------------------------------------------------
# semantics
# ---------------------------------------------------------------------------

class TestSemantics(EngineTestBase):
    def test_fixture_plan(self):
        plan = json.loads((FIX / "plan.json").read_text())
        sources = {"a": [json.loads(l) for l in
                         (FIX / "a.jsonl").read_text().splitlines()],
                   "b": [json.loads(l) for l in
                         (FIX / "b.jsonl").read_text().splitlines()]}
        self.assert_matches_reference(plan, sources)

    def test_deep_skew_fixture(self):
        plan = json.loads((FIX / "deep-skew-plan.json").read_text())
        sources = {"a": [json.loads(l) for l in
                         (FIX / "a.jsonl").read_text().splitlines()],
                   "b": [json.loads(l) for l in
                         (FIX / "b.jsonl").read_text().splitlines()]}
        self.assert_matches_reference(plan, sources)

    def test_except_consumes_earliest(self):
        plan = {"op": "except_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "b"}}
        sources = {"a": [[1], [1], [1]], "b": [[1]]}
        got, _ = run_engine(plan, sources, self.td)
        self.assertEqual([([1], "root.L:a:1"), ([1], "root.L:a:2")], got)

    def test_intersect_keeps_earliest(self):
        plan = {"op": "intersect_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "b"}}
        sources = {"a": [[1], [1], [1]], "b": [[1], [1]]}
        got, _ = run_engine(plan, sources, self.td)
        self.assertEqual([([1], "root.L:a:0"), ([1], "root.L:a:1")], got)

    def test_occurrence_identity_double_scan(self):
        """One source scanned twice = two independent occurrence families;
        equal-valued rows never share an identity."""
        plan = {"op": "union_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "a"}}
        sources = {"a": [[7, "v"], [7, "v"]]}
        got, _ = run_engine(plan, sources, self.td)
        witnesses = [w for _, w in got]
        self.assertEqual(4, len(witnesses))
        self.assertEqual(4, len(set(witnesses)))  # all identities distinct
        self.assertEqual(["root.L:a:0", "root.L:a:1",
                          "root.R:a:0", "root.R:a:1"], witnesses)

    def test_except_self_is_empty_per_occurrence(self):
        """a EXCEPT a: identical values but distinct occurrences -> all consumed."""
        plan = {"op": "except_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "a"}}
        got, _ = run_engine(plan, {"a": [[5], [5], [6]]}, self.td)
        self.assertEqual([], got)

    def test_null_equals_null(self):
        plan = {"op": "except_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "b"}}
        got, _ = run_engine(plan, {"a": [[None], [None]], "b": [[None]]}, self.td)
        self.assertEqual([([None], "root.L:a:1")], got)
        plan_i = {"op": "intersect_all",
                  "left": {"op": "scan", "source": "a"},
                  "right": {"op": "scan", "source": "b"}}
        got, _ = run_engine(plan_i, {"a": [[None], [None]], "b": [[None]]}, self.td)
        self.assertEqual([([None], "root.L:a:0")], got)

    def test_text_exact_codepoints(self):
        """Composed vs decomposed unicode are different keys."""
        composed = "é"          # U+00E9
        decomposed = "é"      # U+0065 U+0301
        plan = {"op": "except_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "b"}}
        got, _ = run_engine(plan, {"a": [[composed]], "b": [[decomposed]]}, self.td)
        self.assertEqual([([composed], "root.L:a:0")], got)
        plan_i = {"op": "intersect_all",
                  "left": {"op": "scan", "source": "a"},
                  "right": {"op": "scan", "source": "b"}}
        got, _ = run_engine(plan_i, {"a": [[composed]], "b": [[decomposed]]}, self.td)
        self.assertEqual([], got)

    def test_int_text_null_distinct(self):
        plan = {"op": "intersect_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "b"}}
        got, _ = run_engine(plan, {"a": [[1], ["1"], [None]], "b": [[1]]}, self.td)
        self.assertEqual([([1], "root.L:a:0")], got)

    def test_parentheses_preserved(self):
        """(a EXCEPT b) EXCEPT c  !=  a EXCEPT (b EXCEPT c); engine must match
        the reference for both, proving no rewriting."""
        sources = {"a": [[1], [1], [2]], "b": [[1]], "c": [[2]]}
        p1 = {"op": "except_all",
              "left": {"op": "except_all",
                       "left": {"op": "scan", "source": "a"},
                       "right": {"op": "scan", "source": "b"}},
              "right": {"op": "scan", "source": "c"}}
        p2 = {"op": "except_all",
              "left": {"op": "scan", "source": "a"},
              "right": {"op": "except_all",
                       "left": {"op": "scan", "source": "b"},
                       "right": {"op": "scan", "source": "c"}}}
        r1 = self.assert_matches_reference(p1, sources)
        r2 = self.assert_matches_reference(p2, sources)
        got1, _ = run_engine(p1, sources, self.td)
        got2, _ = run_engine(p2, sources, self.td)
        self.assertNotEqual(got1, got2)  # semantics genuinely differ

    def test_union_order_and_empty_sides(self):
        plan = {"op": "union_all",
                "left": {"op": "union_all",
                         "left": {"op": "scan", "source": "a"},
                         "right": {"op": "scan", "source": "b"}},
                "right": {"op": "scan", "source": "a"}}
        sources = {"a": [[1]], "b": [[2], [3]]}
        got, _ = run_engine(plan, sources, self.td)
        self.assertEqual(["root.L.L:a:0", "root.L.R:b:0",
                          "root.L.R:b:1", "root.R:a:0"], [w for _, w in got])
        # empty source
        got, _ = run_engine(plan, {"a": [], "b": []}, self.td)
        self.assertEqual([], got)

    def test_tiny_budget_multipass(self):
        """Same semantics when every buffer is tiny (multi-pass merging)."""
        rng = random.Random(7)
        sources = {"a": [[rng.randrange(4), rng.choice(["x", None])]
                         for _ in range(50)],
                   "b": [[rng.randrange(4), rng.choice(["x", "y"])]
                         for _ in range(40)]}
        plan = json.loads((FIX / "plan.json").read_text())
        self.assert_matches_reference(plan, sources, budget=tiny_budget())
        deep = json.loads((FIX / "deep-skew-plan.json").read_text())
        self.assert_matches_reference(deep, sources, budget=tiny_budget())


# ---------------------------------------------------------------------------
# randomized differential testing (legal input family + variants)
# ---------------------------------------------------------------------------

def random_plan(rng, sources, max_depth):
    if max_depth == 1 or rng.random() < 0.35:
        return {"op": "scan", "source": rng.choice(sorted(sources))}
    op = rng.choice(["union_all", "intersect_all", "except_all"])
    return {"op": op,
            "left": random_plan(rng, sources, max_depth - 1),
            "right": random_plan(rng, sources, max_depth - 1)}


def plan_leaves(plan):
    if plan["op"] == "scan":
        return 1
    return plan_leaves(plan["left"]) + plan_leaves(plan["right"])


def random_sources(rng, names, max_rows, hot=0.0):
    pool = [None, 0, 1, 2, 3, 7, "x", "y", "", "é", "é", "热", -5]
    sources = {}
    for name in names:
        rows = []
        for _ in range(rng.randrange(max_rows + 1)):
            if rng.random() < hot:
                rows.append([0, "hot"])
            else:
                rows.append([rng.choice(pool), rng.choice(pool)])
        sources[name] = rows
    return sources


class TestDifferential(EngineTestBase):
    def test_random_plans_small(self):
        rng = random.Random(20261115)
        cases = 0
        while cases < 60:
            plan = random_plan(rng, {"s0", "s1", "s2"}, max_depth=6)
            if plan_leaves(plan) > 16:
                continue
            sources = random_sources(rng, ["s0", "s1", "s2"], 40, hot=0.4)
            self.assert_matches_reference(plan, sources)
            cases += 1

    def test_random_plans_tiny_budget(self):
        rng = random.Random(99)
        cases = 0
        while cases < 25:
            plan = random_plan(rng, {"s0", "s1"}, max_depth=6)
            if plan_leaves(plan) > 16:
                continue
            sources = random_sources(rng, ["s0", "s1"], 60, hot=0.6)
            self.assert_matches_reference(plan, sources, budget=tiny_budget())
            cases += 1

    def test_deep_skew_variants(self):
        """Variants of the deep-skew family: depth-6 plans reusing one source,
        skewed data with NULLs and unicode."""
        rng = random.Random(4242)
        for _ in range(20):
            plan = random_plan(rng, {"a", "b"}, max_depth=6)
            if plan_leaves(plan) > 16:
                continue
            sources = random_sources(rng, ["a", "b"], 80, hot=0.7)
            self.assert_matches_reference(plan, sources)


# ---------------------------------------------------------------------------
# resource invariants
# ---------------------------------------------------------------------------

class TestResources(EngineTestBase):
    def test_payload_budget_enforced(self):
        """A tiny payload limit must make the run fail loudly."""
        plan = {"op": "except_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "b"}}
        sources = {"a": [[i] for i in range(200)], "b": [[0]]}
        with self.assertRaises(BudgetExceeded):
            run_engine(plan, sources, self.td, payload_limit=16)

    def test_payload_peak_within_4096(self):
        rng = random.Random(5)
        sources = {"a": [[rng.randrange(50), "t"] for _ in range(3000)],
                   "b": [[rng.randrange(50), "t"] for _ in range(3000)]}
        for plan_file in ("plan.json", "deep-skew-plan.json"):
            plan = json.loads((FIX / plan_file).read_text())
            _, rep = run_engine(plan, sources, self.td)
            self.assertLessEqual(rep["meter"]["payload_peak"], 4096)
            self.assertLessEqual(rep["meter"]["payload_peak"],
                                 rep["meter"]["payload_limit"])

    def test_temp_recycled_and_workdir_empty(self):
        plan = json.loads((FIX / "deep-skew-plan.json").read_text())
        sources = {"a": [[i % 3, "x"] for i in range(500)],
                   "b": [[i % 3, "x"] for i in range(400)]}
        _, rep = run_engine(plan, sources, self.td)
        self.assertEqual(0, rep["meter"]["temp_bytes_current"])
        self.assertEqual([], rep["meter"]["temp_files_open"])
        leftovers = list((Path(self.td) / "tmp").iterdir())
        self.assertEqual([], leftovers, f"temp files left behind: {leftovers}")

    def test_count_conservation_and_summaries(self):
        rng = random.Random(11)
        sources = random_sources(rng, ["a", "b"], 200, hot=0.5)
        plan = json.loads((FIX / "deep-skew-plan.json").read_text())
        _, rep = run_engine(plan, sources, self.td)
        setops = [s for s in rep["operators"] if s["op"] in
                  ("except_all", "intersect_all")]
        self.assertTrue(setops)
        for s in setops:
            self.assertTrue(s["conservation_ok"])
            self.assertIsNotNone(s["left_in"])
            self.assertIsNotNone(s["emitted"])
            if s["op"] == "except_all":
                self.assertEqual(s["emitted"], s["left_in"] - s["matched"])
            else:
                self.assertEqual(s["emitted"], s["matched"])
            self.assertLessEqual(s["matched"], s["right_in"])

    def test_empty_right_side(self):
        """EXCEPT with empty right passes left through (with real counts);
        INTERSECT with empty right emits nothing."""
        ex = {"op": "except_all",
              "left": {"op": "scan", "source": "a"},
              "right": {"op": "scan", "source": "b"}}
        got, rep = run_engine(ex, {"a": [[1], [2]], "b": []}, self.td)
        self.assertEqual([([1], "root.L:a:0"), ([2], "root.L:a:1")], got)
        s = [x for x in rep["operators"] if x["path"] == "root"][0]
        self.assertEqual((2, 0, 2), (s["left_in"], s["matched"], s["emitted"]))
        ix = {"op": "intersect_all",
              "left": {"op": "scan", "source": "a"},
              "right": {"op": "scan", "source": "b"}}
        got, rep = run_engine(ix, {"a": [[1], [2]], "b": []}, self.td)
        self.assertEqual([], got)
        s = [x for x in rep["operators"] if x["path"] == "root"][0]
        self.assertEqual((0, 0, 0), (s["left_in"], s["matched"], s["emitted"]))

    def test_group_audit_log_spotcheck(self):
        """Per-group quota log must agree with an independent recomputation."""
        sources = {"a": [[i % 5, "g"] for i in range(120)],
                   "b": [[i % 7, "g"] for i in range(90)]}
        plan = {"op": "except_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "b"}}
        rows, rep = run_engine(plan, sources, self.td, audit_groups=True)
        groups = [json.loads(l) for l in
                  open(Path(self.td) / "groups.jsonl", encoding="utf-8")]
        self.assertTrue(groups)
        rng = random.Random(0)
        for g in rng.sample(groups, min(10, len(groups))):
            n = sum(1 for r in sources["a"] if r == g["key"])
            m = sum(1 for r in sources["b"] if r == g["key"])
            self.assertEqual(n, g["left"], g)
            self.assertEqual(m, g["right"], g)
            self.assertEqual(min(n, m), g["consumed"], g)
            self.assertEqual(max(0, n - m), g["emitted"], g)

    def test_per_occurrence_spotcheck(self):
        """Sampled output witnesses must point at the claimed source row."""
        rng = random.Random(31)
        sources = random_sources(rng, ["a", "b"], 150, hot=0.5)
        plan = json.loads((FIX / "deep-skew-plan.json").read_text())
        rows, rep = run_engine(plan, sources, self.td)
        self.assertTrue(rows)
        for row, witness in rng.sample(rows, min(25, len(rows))):
            path, source, rownum = witness.rsplit(":", 2)
            self.assertEqual(row, sources[source][int(rownum)], witness)
        # witnesses unique across the whole output
        self.assertEqual(len(rows), len({w for _, w in rows}))


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

class TestValidation(EngineTestBase):
    def test_depth_limit(self):
        plan = {"op": "scan", "source": "a"}
        for _ in range(6):
            plan = {"op": "union_all", "left": plan,
                    "right": {"op": "scan", "source": "a"}}
        with self.assertRaises(PlanError):
            run_engine(plan, {"a": [[1]]}, self.td)

    def test_leaf_limit(self):
        # balanced 32-leaf tree has depth 6 -> the leaf limit must fire
        def balanced(d):
            if d == 0:
                return {"op": "scan", "source": "a"}
            return {"op": "union_all", "left": balanced(d - 1),
                    "right": balanced(d - 1)}
        with self.assertRaises(PlanError):
            run_engine(balanced(5), {"a": [[1]]}, self.td)  # 32 leaves, depth 6

    def test_source_count_limit(self):
        plan = {"op": "scan", "source": "s4"}
        sources = {f"s{i}": [[1]] for i in range(5)}
        with self.assertRaises(PlanError):
            run_engine(plan, sources, self.td)

    def test_unknown_source_and_op(self):
        with self.assertRaises(PlanError):
            run_engine({"op": "scan", "source": "zzz"}, {"a": [[1]]}, self.td)
        with self.assertRaises(PlanError):
            run_engine({"op": "except", "left": {"op": "scan", "source": "a"},
                        "right": {"op": "scan", "source": "a"}},
                       {"a": [[1]]}, self.td)

    def test_row_validation(self):
        plan = {"op": "scan", "source": "a"}
        with self.assertRaises(DataError):
            run_engine(plan, {"a": [[1], [1, 2]]}, self.td)  # ragged
        with self.assertRaises(DataError):
            run_engine(plan, {"a": [[1, 2, 3, 4, 5]]}, self.td)  # >4 cols
        with self.assertRaises(DataError):
            run_engine(plan, {"a": [[2 ** 63]]}, self.td)  # int64 overflow
        with self.assertRaises(DataError):
            run_engine(plan, {"a": [[1.5]]}, self.td)  # float not in schema
        with self.assertRaises(DataError):
            run_engine(plan, {"a": [[True]]}, self.td)  # bool not allowed
        with self.assertRaises(DataError):
            run_engine(plan, {"a": [["x" * 2000]]}, self.td)  # >1KiB row

    def test_schema_validation(self):
        from exmem.engine import run_plan as rp
        plan = {"op": "scan", "source": "a"}
        paths = write_sources(self.td, {"a": [[None]]})
        schema = {"columns": [{"name": "c", "type": "int64", "nullable": False}]}
        with self.assertRaises(DataError):
            rp(plan, paths, str(Path(self.td) / "o"), Path(self.td) / "w",
               schema=schema)


# ---------------------------------------------------------------------------
# the real hot-key benchmark: 100k rows/source, 90% one key
# ---------------------------------------------------------------------------

class TestBenchmark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.td = tempfile.mkdtemp(prefix="exmem-bench-")
        sys.path.insert(0, str(REPO))
        import make_large
        gen = Path(cls.td) / "generated"
        make_large.generate(str(gen), n=100_000)
        cls.sources = {"a": str(gen / "a.jsonl"), "b": str(gen / "b.jsonl")}
        cls.plan = json.loads((FIX / "resource-benchmark-plan.json").read_text())
        cls.out = Path(cls.td) / "out.jsonl"
        cls.rep = run_plan(cls.plan, cls.sources, str(cls.out),
                           Path(cls.td) / "tmp")
        cls.input_bytes = sum(os.path.getsize(p)
                              for p in cls.sources.values())

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.td, ignore_errors=True)

    def test_temp_disk_within_6x(self):
        peak = self.rep["meter"]["temp_bytes_peak"]
        self.assertLessEqual(peak, 6 * self.input_bytes,
                             f"temp peak {peak} > 6x input {6 * self.input_bytes}")

    def test_payload_within_4096(self):
        self.assertLessEqual(self.rep["meter"]["payload_peak"], 4096)

    def test_full_output_matches_reference(self):
        """Full per-occurrence diff against the in-memory reference --
        not just row counts."""
        sources = {k: [json.loads(l) for l in open(p)]
                   for k, p in self.sources.items()}
        want = [(x["row"], x["witness"]) for x in
                operators.evaluate(self.plan, sources)]
        got = []
        with open(self.out, encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                got.append((rec["row"], rec["witness"]))
        self.assertEqual(want, got)

    def test_conservation_and_spotcheck(self):
        rep = self.rep
        for s in rep["operators"]:
            if s["op"] in ("except_all", "intersect_all"):
                self.assertTrue(s["conservation_ok"])
        # hot group arithmetic: 90k hot rows on each side cancel exactly
        root = [s for s in rep["operators"] if s["path"] == "root"][0]
        self.assertEqual(100_000, root["left_in"])
        self.assertEqual(100_000, root["right_in"])
        self.assertEqual(rep["output_rows"], root["emitted"])
        # per-occurrence spot check on the real output
        rows = []
        with open(self.out, encoding="utf-8") as f:
            for line in f:
                rows.append(json.loads(line))
        src_cache = {}
        rng = random.Random(3)
        for rec in rng.sample(rows, min(50, len(rows))):
            path, source, rownum = rec["witness"].rsplit(":", 2)
            if source not in src_cache:
                src_cache[source] = [json.loads(l) for l in
                                     open(self.sources[source])]
            self.assertEqual(rec["row"], src_cache[source][int(rownum)])

    def test_temp_recycled(self):
        self.assertEqual(0, self.rep["meter"]["temp_bytes_current"])
        self.assertEqual([], list((Path(self.td) / "tmp").iterdir()))


class TestDeepSkewScale(EngineTestBase):
    def test_deep_skew_plan_at_scale(self):
        """Depth-6 plan with duplicated sources on skewed 20k-row sources,
        full differential check vs the reference."""
        rng = random.Random(77)
        n = 20_000
        sources = {}
        for name, off in (("a", 0), ("b", 1)):
            rows = []
            for i in range(n):
                if rng.random() < 0.9:
                    rows.append([0, "hot"])
                else:
                    rows.append([rng.randrange(200), rng.choice(["c", None])])
            sources[name] = rows
        plan = json.loads((FIX / "deep-skew-plan.json").read_text())
        rep = self.assert_matches_reference(plan, sources)
        self.assertLessEqual(rep["meter"]["payload_peak"], 4096)
        self.assertEqual(0, rep["meter"]["temp_bytes_current"])


if __name__ == "__main__":
    unittest.main()
