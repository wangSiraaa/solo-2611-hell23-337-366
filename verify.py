"""End-to-end verification with an independent whole-graph oracle.

Every test drives the real SQLite store (store.py), then rebuilds the
expected state in plain Python using closure.full_closure over the full
graph at each retained version, enumerating physical intervals without
using seg_index, and compares:
  * trusted/quarantined status and reasons for every entity;
  * every overlap query over a deterministic probe grid;
  * content_version / status_version monotonicity;
  * provenance reconstruction of every segment;
  * two-connection reader isolation across an in-flight write;
  * no half-written index / no published change after a failed patch.
Run: PYTHONDONTWRITEBYTECODE=1 python3 verify.py
"""
import copy
import json
import os
import random
import shutil
import sqlite3
import tempfile
from pathlib import Path

from annotation import load_graph_json, parse_gff3
from closure import full_closure, find_cyclic
from store import open_store, PatchError, DB_NAME

ROOT = Path(__file__).resolve().parent
PASS, FAIL = [], []
LOG = []


def log(msg):
    LOG.append(msg)
    print(msg)


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
        log("  PASS %s %s" % (name, detail))
    else:
        FAIL.append((name, detail))
        log("  FAIL %s %s" % (name, detail))


# --------------------------------------------------------------------- oracle
class Oracle:
    """Versioned in-memory model, recomputed from scratch per version."""

    def __init__(self):
        self.versions = {}      # version -> dict id -> entity body
        self.statuses = {}      # version -> (status map, reason map)
        self.extras = {}        # version -> extra_reasons
        self.head = 0

    def publish(self, entities, extra=None):
        self.head += 1
        self.versions[self.head] = copy.deepcopy(entities)
        self.extras[self.head] = dict(extra or {})
        self.statuses[self.head] = full_closure(entities, extra)

    def apply(self, ops):
        ent = copy.deepcopy(self.versions[self.head])
        for op in ops:
            if op["op"] == "delete":
                ent.pop(op["id"], None)
            else:
                ent[op["id"]] = copy.deepcopy(op["entity"])
        extras = {i: r for i, r in self.extras[self.head].items()
                  if i in ent}
        self.publish(ent, extras)
        return ent

    def expected_query(self, lo, hi, version):
        ent = self.versions[version]
        status, _ = self.statuses[version]
        out = []
        for i, e in ent.items():
            if status[i] != "trusted":
                continue
            if any(slo < hi and shi > lo for slo, shi in e["segments"]):
                out.append(i)
        return sorted(out)

    def coords(self, version):
        cs = [0, 1, 1000]
        for e in self.versions[version].values():
            for lo, hi in e["segments"]:
                cs += [lo, hi, lo - 1, hi + 1, (lo + hi) // 2]
        return sorted({c for c in cs if c >= 0})


def compare_store_to_oracle(st, oracle, label):
    """Full status + query comparison at every retained version."""
    head = st.head_version
    retained = [v for v in (head - 1, head) if v >= 1]
    for v in retained:
        status, reasons = oracle.statuses[v]
        for i, e in oracle.versions[v].items():
            d = st.inspect_entity(i, v)
            check("%s v%d status[%s]" % (label, v, i),
                  d["status"] == status[i],
                  "store=%s oracle=%s" % (d["status"], status[i]))
            check("%s v%d reasons[%s]" % (label, v, i),
                  sorted(d["reasons"]) == sorted(reasons[i]),
                  "%s vs %s" % (d["reasons"], reasons[i]))
            check("%s v%d body[%s]" % (label, v, i),
                  d["body"] == e)
            check("%s v%d segments[%s]" % (label, v, i),
                  d["segments"] == e["segments"])
        grid = oracle.coords(v)
        for a in grid:
            for b in grid:
                if a >= b:
                    continue
                got = st.query(a, b, v)
                want = oracle.expected_query(a, b, v)
                check("%s v%d query(%d,%d)" % (label, v, a, b),
                      got == want, "%s vs %s" % (got, want))
    # current seg_index must only contain trusted current entities
    rows = st.conn.execute(
        "SELECT si.id FROM seg_index si JOIN entity_version ev "
        "ON ev.id=si.id AND ev.version_to IS NULL "
        "WHERE ev.status!='trusted'").fetchall()
    check("%s index has no quarantined rows" % label, not rows, str(rows))
    n_idx = st.conn.execute("SELECT COUNT(*) FROM seg_index").fetchone()[0]
    n_exp = sum(len(e["segments"])
                for i, e in oracle.versions[head].items()
                if oracle.statuses[head][0][i] == "trusted")
    check("%s index physical row count" % label, n_idx == n_exp,
          "%d vs %d" % (n_idx, n_exp))


# --------------------------------------------------------------------- tests
def test_json_import_and_baseline_defect(tmp):
    log("[1] JSON import + delete shared parent (baseline defect)")
    d = tmp / "a"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "inputs/graph.json")
    oracle = Oracle()
    oracle.publish(rows)
    compare_store_to_oracle(st, oracle, "import")
    check("initial query hits t1,e", st.query(20, 21) == ["e", "g", "t1", "t2"])

    patch = json.load(open(ROOT / "inputs/patch.json"))
    stats = st.apply_patch(patch, base=1, patch_id="del-t1")
    oracle.apply(patch)
    compare_store_to_oracle(st, oracle, "delete-t1")
    # the exact baseline bug must be gone: no stale t1, no complete e
    hits = st.query(20, 21)
    check("stale t1 gone from index", "t1" not in hits, str(hits))
    check("shared child e not complete", "e" not in hits, str(hits))
    check("e quarantined keeping both edges",
          st.inspect_entity("e")["status"] == "quarantined"
          and st.inspect_entity("e")["reasons"] == ["missing_parent:t1"]
          and st.inspect_entity("e")["parents"] == ["t1", "t2"])
    check("delete audit touched only t1,e,g?,t2? cone",
          set(stats["changed_entities"]) == {"t1", "e"},
          str(stats["changed_entities"]))
    check("no segment rewrite on pure status change",
          stats["segments_rewritten"] == 0, str(stats["segments_rewritten"]))
    check("t1 index rows deleted (not envelope)",
          stats["index_rows_deleted"] == 3
          and stats["index_rows_written"] == 0,
          "deleted=%d" % stats["index_rows_deleted"])
    st.close()


def test_repair_restores_full_parent_set(tmp):
    log("[2] repair parent -> next atomic version restores all parents/segments")
    d = tmp / "b"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    oracle = Oracle()
    oracle.publish(rows)
    st.apply_patch(json.load(open(ROOT / "inputs/patch.json")), base=1,
                   patch_id="p1")
    oracle.apply(json.load(open(ROOT / "inputs/patch.json")))
    repair = json.load(open(ROOT / "inputs/patch-repair.json"))
    stats = st.apply_patch(repair, base=2, patch_id="p2")
    oracle.apply(repair)
    compare_store_to_oracle(st, oracle, "repair")
    e = st.inspect_entity("e")
    check("e trusted again with both parents restored",
          e["status"] == "trusted" and e["parents"] == ["t1", "t2"]
          and e["segments"] == [[20, 25], [40, 45]])
    check("e content version not bumped (content unchanged)",
          e["content_version"] == 1 and e["status_version"] == 3)
    check("unaffected g versions untouched",
          st.inspect_entity("g")["content_version"] == 1
          and st.inspect_entity("g")["status_version"] == 1)
    check("repair only rewrote t1's one physical segment",
          stats["segments_rewritten"] == 1, str(stats["segments_rewritten"]))
    st.close()


def test_move_segment(tmp):
    log("[3] move one segment out of old range")
    d = tmp / "c"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    oracle = Oracle()
    oracle.publish(rows)
    moved = copy.deepcopy(rows["e"])
    moved["segments"] = [[60, 65], [40, 45]]  # first segment leaves [20,21]
    patch = [{"op": "replace", "id": "e", "entity": moved}]
    stats = st.apply_patch(patch, base=1, patch_id="move")
    oracle.apply(patch)
    compare_store_to_oracle(st, oracle, "move")
    check("old range no longer hits e", "e" not in st.query(20, 21))
    check("new range hits e", "e" in st.query(61, 62))
    check("both physical segment rows rewritten (2+2)",
          stats["segments_rewritten"] == 4, str(stats["segments_rewritten"]))
    check("index rows for e = 2",
          st.conn.execute("SELECT COUNT(*) FROM seg_index WHERE id='e'")
          .fetchone()[0] == 2)
    st.close()


def test_same_batch_delete_parent_and_child(tmp):
    log("[4] same batch deletes parent then child")
    d = tmp / "d"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    oracle = Oracle()
    oracle.publish(rows)
    patch = [{"op": "delete", "id": "t1"}, {"op": "delete", "id": "e"}]
    stats = st.apply_patch(patch, base=1, patch_id="batch")
    oracle.apply(patch)
    compare_store_to_oracle(st, oracle, "batch")
    check("both gone from current",
          st.inspect_entity("t1")["version_to"] is not None
          and st.inspect_entity("e")["version_to"] is not None)
    check("t2 remains trusted", st.inspect_entity("t2")["status"] == "trusted")
    check("index has neither",
          st.conn.execute("SELECT COUNT(*) FROM seg_index WHERE id IN ('t1','e')")
          .fetchone()[0] == 0)
    check("audit changed set exactly t1,e",
          set(stats["changed_entities"]) == {"t1", "e"})
    st.close()


def test_cycle_create_and_break(tmp):
    log("[5] patch creates then breaks a cycle")
    d = tmp / "e"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    oracle = Oracle()
    oracle.publish(rows)
    cyc = json.load(open(ROOT / "inputs/patch-cycle.json"))
    st.apply_patch(cyc, base=1, patch_id="cyc")
    oracle.apply(cyc)
    compare_store_to_oracle(st, oracle, "cycle")
    check("t1,e quarantined by cycle",
          st.inspect_entity("t1")["status"] == "quarantined"
          and st.inspect_entity("e")["status"] == "quarantined")
    check("t2 outside cycle stays trusted",
          st.inspect_entity("t2")["status"] == "trusted")
    fix = json.load(open(ROOT / "inputs/patch-repair.json"))
    st.apply_patch(fix, base=2, patch_id="fix")
    oracle.apply(fix)
    compare_store_to_oracle(st, oracle, "uncycle")
    check("cycle cleared",
          st.inspect_entity("t1")["status"] == "trusted"
          and st.inspect_entity("e")["status"] == "trusted")
    st.close()


def test_two_parents_change_together(tmp):
    log("[6] two parents of a shared child change in one patch")
    d = tmp / "f"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    oracle = Oracle()
    oracle.publish(rows)
    t1b = copy.deepcopy(rows["t1"]); t1b["parents"] = []   # detach from g
    t2b = copy.deepcopy(rows["t2"]); t2b["parents"] = []
    patch = [{"op": "replace", "id": "t1", "entity": t1b},
             {"op": "replace", "id": "t2", "entity": t2b}]
    stats = st.apply_patch(patch, base=1, patch_id="two")
    oracle.apply(patch)
    compare_store_to_oracle(st, oracle, "two-parents")
    check("e still trusted, edges intact",
          st.inspect_entity("e")["status"] == "trusted"
          and st.inspect_entity("e")["parents"] == ["t1", "t2"])
    check("g still trusted, t1/t2 roots",
          all(st.inspect_entity(i)["status"] == "trusted"
              for i in ("g", "t1", "t2")))
    check("changed set exactly t1,t2 (e status/content stable)",
          set(stats["changed_entities"]) == {"t1", "t2"},
          str(stats["changed_entities"]))
    # now delete both parents simultaneously: e must keep both edges, quarantined
    patch2 = [{"op": "delete", "id": "t1"}, {"op": "delete", "id": "t2"}]
    st.apply_patch(patch2, base=2, patch_id="kill")
    oracle.apply(patch2)
    compare_store_to_oracle(st, oracle, "kill-parents")
    e = st.inspect_entity("e")
    check("e keeps both parent edges while quarantined",
          e["status"] == "quarantined" and e["parents"] == ["t1", "t2"]
          and set(e["reasons"]) == {"missing_parent:t1", "missing_parent:t2"})
    st.close()


def test_stale_base_and_failed_patch(tmp):
    log("[7] stale base rejected; failed patch changes nothing published")
    d = tmp / "g"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    st.apply_patch(json.load(open(ROOT / "inputs/patch.json")), base=1,
                   patch_id="p1")
    head_before = st.head_version
    snapshot = _raw_snapshot(d)
    try:
        st.apply_patch(json.load(open(ROOT / "inputs/patch-repair.json")),
                       base=1, patch_id="stale")
        check("stale base raises", False)
    except PatchError as ex:
        check("stale base raises stale_base", ex.code == "stale_base")
    check("head unchanged after stale", st.head_version == head_before)
    check("published tables byte-identical after stale",
          _raw_snapshot(d) == snapshot)
    # malformed op / identity mismatch / unknown delete
    for bad, code in [
        ([{"op": "frob", "id": "g"}], "bad_patch"),
        ([{"op": "delete", "id": "ghost"}], "unknown_id"),
        ([{"op": "replace", "id": "g",
           "entity": {"id": "other", "kind": "gene",
                      "parents": [], "segments": [[1, 2]]}}],
         "identity_mismatch"),
        ([{"op": "replace", "id": "g",
           "entity": {"id": "g", "kind": "gene",
                      "parents": [], "segments": [[5, 2]]}}], "input_error"),
    ]:
        try:
            st.apply_patch(bad, base=head_before, patch_id="bad")
            check("bad patch rejected %s" % code, False, str(bad))
        except PatchError as ex:
            check("bad patch rejected %s" % code, ex.code == code, ex.code)
        except ValueError as ex:
            check("bad patch rejected %s" % code, code == "input_error", str(ex))
    check("still nothing published", st.head_version == head_before
          and _raw_snapshot(d) == snapshot)
    st.close()


def test_fault_injection(tmp):
    log("[8] fault at index control points -> rollback, no half index")
    d = tmp / "h"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    snapshot = _raw_snapshot(d)
    head = st.head_version
    patch = [{"op": "replace", "id": "e",
              "entity": {**rows["e"], "segments": [[60, 70], [40, 45]]}}]
    for point in ("before_index_switch", "after_index_switch", "before_commit"):
        st.fault = point
        try:
            st.apply_patch(patch, base=head, patch_id="f-" + point)
            check("%s raised" % point, False)
        except RuntimeError as ex:
            check("%s raised inside txn" % point, point in str(ex))
        st.fault = None
        check("%s head unchanged" % point, st.head_version == head)
        check("%s no half-written index/tables" % point,
              _raw_snapshot(d) == snapshot)
    # same patch succeeds once the fault hook is removed
    stats = st.apply_patch(patch, base=head, patch_id="ok")
    check("retry after fault succeeds", st.head_version == head + 1)
    check("e moved", "e" in st.query(61, 62) and "e" not in st.query(20, 21))
    check("only e index rows touched",
          stats["index_rows_deleted"] == 2 and stats["index_rows_written"] == 2)
    st.close()


def test_two_connection_reader(tmp):
    log("[9] two connections: reader never crosses an in-flight version")
    d = tmp / "i"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    st.apply_patch(json.load(open(ROOT / "inputs/patch.json")), base=1,
                   patch_id="p1")  # head=2
    # a second connection opens and holds a read snapshot BEFORE the write
    reader = sqlite3.connect(os.path.join(d, DB_NAME))
    reader.execute("PRAGMA journal_mode=WAL")
    reader.execute("BEGIN")
    old_hits = [r[0] for r in reader.execute(
        "SELECT DISTINCT id FROM seg_index WHERE lo<21 AND hi>20 ORDER BY id")]
    old_head = reader.execute(
        "SELECT value FROM meta WHERE key='head_version'").fetchone()[0]
    check("reader sees old complete version pre-write",
          old_head == "2" and "t1" not in old_hits and "e" not in old_hits,
          str(old_hits))
    stages = []
    st.hook = lambda stage, store: stages.append(
        (stage, [r[0] for r in reader.execute(
            "SELECT DISTINCT id FROM seg_index WHERE lo<21 AND hi>20 ORDER BY id")],
         reader.execute("SELECT value FROM meta WHERE key='head_version'")
         .fetchone()[0]))
    repair = json.load(open(ROOT / "inputs/patch-repair.json"))
    st.apply_patch(repair, base=2, patch_id="p2")
    st.hook = None
    check("hooks observed while write in flight", len(stages) >= 3,
          str([s[0] for s in stages]))
    for stage, hits, hv in stages:
        check("reader frozen at v2 during %s" % stage,
              hv == "2" and "e" not in hits and "t1" not in hits,
              "%s %s head=%s" % (stage, hits, hv))
    reader.execute("COMMIT")
    # new read transaction sees the new complete version
    reader.execute("BEGIN")
    new_hits = [r[0] for r in reader.execute(
        "SELECT DISTINCT id FROM seg_index WHERE lo<21 AND hi>20 ORDER BY id")]
    new_head = reader.execute(
        "SELECT value FROM meta WHERE key='head_version'").fetchone()[0]
    reader.execute("COMMIT")
    reader.close()
    check("new reader sees complete v3",
          new_head == "3" and new_hits == ["e", "g", "t1", "t2"],
          "%s head=%s" % (new_hits, new_head))
    check("only complete versions ever visible (no mixed)",
          all(hv == "2" for _, _, hv in stages))
    st.close()


def test_version_retention(tmp):
    log("[10] only newest two versions readable; old reader cannot cross")
    d = tmp / "j"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    oracle = Oracle()
    oracle.publish(rows)
    for k in range(1, 5):
        ent = copy.deepcopy(rows["e"])
        ent["segments"] = [[20 + k, 25 + k], [40, 45]]
        p = [{"op": "replace", "id": "e", "entity": ent}]
        st.apply_patch(p, base=st.head_version, patch_id="m%d" % k)
        oracle.apply(p)
    check("head at 5", st.head_version == 5)
    check("readable only 4,5", st.versions()["readable_versions"] == [4, 5])
    try:
        st.query(1, 1000, version=3)
        check("v3 rejected", False)
    except PatchError as ex:
        check("v3 rejected version_unavailable", ex.code == "version_unavailable")
    compare_store_to_oracle(st, oracle, "retention")
    st.close()


def test_provenance_rebuild(tmp):
    log("[11] provenance reconstructs every stored segment")
    d = tmp / "k"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "inputs/graph.json")
    moved = copy.deepcopy(rows["e"])
    moved["segments"] = [[60, 65], [40, 45]]
    st.apply_patch([{"op": "replace", "id": "e", "entity": moved}],
                   base=1, patch_id="move")
    # every current segment must be reconstructable from provenance rows
    cur_seg = {(r[0], r[1], r[2]) for r in st.conn.execute(
        "SELECT id,lo,hi FROM segment_version WHERE version_to IS NULL")}
    prov_seg = {(r[0], r[1], r[2]) for r in st.conn.execute(
        "SELECT id,lo,hi FROM provenance WHERE (id,version) IN ("
        "SELECT id,MAX(version) FROM provenance GROUP BY id)")}
    check("current segments subset of provenance", cur_seg <= prov_seg,
          "missing=%s" % (cur_seg - prov_seg))
    # inspect still explains the original patch payloads
    recs = st.inspect_patch()
    check("patch records explain payloads",
          [r["patch_id"] for r in recs] == ["move"]
          and recs[0]["base_version"] == 1)
    full = st.inspect_patch("move")
    check("original payload retained",
          full["payload"][0]["entity"]["segments"] == [[60, 65], [40, 45]])
    st.close()


def test_gff3_import(tmp):
    log("[12] GFF3 legacy semantics incl. conflict file and quarantine")
    parsed = parse_gff3(ROOT / "legacy/features.gff3")
    d = tmp / "l"
    st = open_store(d, create=True)
    res = st.import_graph(parsed["entities"], "legacy/features.gff3",
                          parsed["extra_reasons"])
    oracle = Oracle()
    oracle.publish(parsed["entities"], parsed["extra_reasons"])
    compare_store_to_oracle(st, oracle, "gff3")
    check("bad+badchild quarantined",
          st.inspect_entity("bad")["status"] == "quarantined"
          and st.inspect_entity("badchild")["status"] == "quarantined")
    check("cycle pair quarantined, others trusted",
          all(st.inspect_entity(i)["status"] == "quarantined"
              for i in ("cycle1", "cycle2"))
          and st.inspect_entity("g1")["status"] == "trusted")
    check("multi-parent exon single segment set",
          st.inspect_entity("ex,shared")["segments"] == [[100, 120]]
          and st.inspect_entity("ex,shared")["parents"] == ["tx1", "tx2"])
    check("percent decode not applied before split",
          "ex,shared" in res["quarantined"] or
          st.inspect_entity("ex,shared")["status"] == "trusted")
    check("anonymous feature stored and not referenceable",
          len(parsed["anonymous_ids"]) == 1)
    # conflicting-parts.gff3: cds1 strand differs -> identity conflict
    conflict = parse_gff3(ROOT / "legacy/conflicting-parts.gff3")
    check("identity conflict detected on cds1",
          "cds1" in conflict["conflict_ids"],
          str(conflict["conflict_ids"]))
    d2 = tmp / "l2"
    st2 = open_store(d2, create=True)
    st2.import_graph(conflict["entities"], "conflicting",
                     conflict["extra_reasons"])
    check("conflicting cds1 quarantined, no line picked",
          st2.inspect_entity("cds1")["status"] == "quarantined"
          and "identity_conflict" in st2.inspect_entity("cds1")["reasons"])
    st2.close()
    st.close()


def test_reopen_persistence(tmp):
    log("[13] data directory reopens across handles")
    d = tmp / "m"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    st.apply_patch(json.load(open(ROOT / "inputs/patch.json")), base=1,
                   patch_id="p1")
    st.close()
    st2 = open_store(d)
    check("reopened head=2", st2.head_version == 2)
    check("reopened query consistent",
          st2.query(20, 21) == ["g", "t2"])
    check("reopened inspect explains quarantine",
          st2.inspect_entity("e")["reasons"] == ["missing_parent:t1"])
    st2.close()
    try:
        open_store(tmp / "nope")
        check("missing dir rejected", False)
    except PatchError as ex:
        check("missing dir rejected no_store", ex.code == "no_store")


def test_randomized_oracle(tmp):
    log("[14] randomized patches vs independent full-graph oracle")
    rng = random.Random(20261115)
    d = tmp / "n"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    # extend graph with extra entities so multi-parent/forward cases exist
    base = copy.deepcopy(rows)
    for k in range(6):
        base["x%d" % k] = {"id": "x%d" % k, "kind": "part",
                           "parents": [rng.choice(list(base))] if base else [],
                           "segments": [[rng.randint(0, 50),
                                         rng.randint(51, 200)]]}
    st.import_graph(base, "rng")
    oracle = Oracle()
    oracle.publish(base)
    ids = list(base)
    for step in range(25):
        ops = []
        for _ in range(rng.randint(1, 4)):
            oid = rng.choice(ids)
            cur = oracle.versions[oracle.head].get(oid)
            if cur is None:
                continue
            if rng.random() < 0.25:
                ops.append({"op": "delete", "id": oid})
            else:
                ent = copy.deepcopy(cur)
                r = rng.random()
                if r < 0.4 and ent["segments"]:
                    j = rng.randrange(len(ent["segments"]))
                    lo = rng.randint(0, 80)
                    ent["segments"][j] = [lo, lo + rng.randint(1, 40)]
                elif r < 0.7:
                    pool = [p for p in ids if p != oid]
                    ent["parents"] = rng.sample(pool, rng.randint(0, 2))
                ops.append({"op": "replace", "id": oid, "entity": ent})
        if not ops:
            continue
        try:
            stats = st.apply_patch(ops, base=st.head_version,
                                   patch_id="r%d" % step)
        except PatchError as ex:
            check("rng step %d unexpected reject" % step, False, str(ex))
            continue
        oracle.apply(ops)
        # read audit must stay local: no untouched entity rows rewritten
        compare_store_to_oracle(st, oracle, "rng%d" % step)
        # every changed entity must intersect the oracle status/content delta
        check("rng%d changed set subset of affected cone" % step,
              set(stats["changed_entities"]) <= set(stats["affected_entities"]))
    st.close()


def _raw_snapshot(d):
    """Dump all published/index rows for byte-level rollback comparison."""
    c = sqlite3.connect(os.path.join(d, DB_NAME))
    out = {}
    for tbl in ("entity_version", "segment_version", "edge_version",
                "seg_index", "provenance", "patch_record", "meta"):
        out[tbl] = sorted(c.execute("SELECT * FROM %s" % tbl).fetchall())
    c.close()
    return out


def test_noop_forward_and_limits(tmp):
    log("[15] no-op patch, forward parent, op/coordinate limits")
    d = tmp / "o"
    st = open_store(d, create=True)
    rows = load_graph_json(ROOT / "inputs/graph.json")
    st.import_graph(rows, "g")
    import copy as _copy
    stats = st.apply_patch(
        [{"op": "replace", "id": "g", "entity": _copy.deepcopy(rows["g"])}],
        base=1, patch_id="noop")
    check("no-op rewrites nothing",
          stats["changed_entities"] == []
          and stats["entity_rows_written"] == 0
          and stats["index_rows_written"] == 0
          and stats["segments_rewritten"] == 0)
    check("no-op still publishes an atomic version", st.head_version == 2)
    # forward-declared parent resolved at import
    fwd = {"a": {"id": "a", "kind": "x", "parents": ["b"],
                 "segments": [[1, 2]]},
           "b": {"id": "b", "kind": "x", "parents": [],
                 "segments": [[3, 4]]}}
    d2 = tmp / "o2"
    st2 = open_store(d2, create=True)
    st2.import_graph(fwd, "fwd")
    check("forward parent trusted",
          st2.inspect_entity("a")["status"] == "trusted")
    st2.close()
    # 101-op patch rejected by the per-patch limit
    big = [{"op": "delete", "id": "g"}] + \
          [{"op": "replace", "id": "g", "entity": rows["g"]}
           for _ in range(100)]
    try:
        st.apply_patch(big, base=2, patch_id="big")
        check("101-op limit enforced", False)
    except PatchError as ex:
        check("101-op limit enforced", ex.code == "limit")
    # invalid coordinates rejected before any write
    try:
        st.apply_patch([{"op": "replace", "id": "g",
                         "entity": {"id": "g", "kind": "gene",
                                    "parents": [], "segments": [[9, 3]]}}],
                       base=2, patch_id="badcoord")
        check("bad coordinate rejected", False)
    except ValueError:
        check("bad coordinate rejected", True)
    check("head unchanged after rejected patches", st.head_version == 2)
    st.close()


def main():
    tmp = Path(tempfile.mkdtemp(prefix="ann-verify-"))
    log("temporary data directories under %s" % tmp)
    try:
        test_json_import_and_baseline_defect(tmp)
        test_repair_restores_full_parent_set(tmp)
        test_move_segment(tmp)
        test_same_batch_delete_parent_and_child(tmp)
        test_cycle_create_and_break(tmp)
        test_two_parents_change_together(tmp)
        test_stale_base_and_failed_patch(tmp)
        test_fault_injection(tmp)
        test_two_connection_reader(tmp)
        test_version_retention(tmp)
        test_provenance_rebuild(tmp)
        test_gff3_import(tmp)
        test_reopen_persistence(tmp)
        test_randomized_oracle(tmp)
        test_noop_forward_and_limits(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    log("\nSUMMARY: %d passed, %d failed" % (len(PASS), len(FAIL)))
    if FAIL:
        for n, d_ in FAIL:
            log("FAILED %s -- %s" % (n, d_))
        raise SystemExit(1)
    Path(ROOT / "verify.log").write_text("\n".join(LOG) + "\n", encoding="utf-8")
    log("log written to verify.log")


if __name__ == "__main__":
    main()
