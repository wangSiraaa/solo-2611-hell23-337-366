#!/usr/bin/env python3
"""End-to-end verification of the incremental area catalog.

Generates real OSM XML + ordered diff files under out/, runs the catalog
through every contract scenario, and after EVERY committed sequence compares
against an independent oracle that fully rebuilds the graph and validates
geometry with Shapely:

  * geometry of every valid relation (normalized WKB equality),
  * the quarantine set,
  * search hit ids over a battery of query rectangles (true-area intersects),
  * the actually rebuilt relation-id set vs the independently computed
    reverse-dependency closure (no full-library rebuild disguised as
    incremental; cache entries outside the closure must keep their built_at).

Finally the persisted state is re-read from disk (Catalog.load), the journal
is replayed into a fresh catalog, and the offline oracle replays the journal
files themselves and compares every committed version's state file.

Exits nonzero on the first mismatch.  Human-readable linkage logs land in
out/<scenario>/apply.log; machine-readable comparisons in out/oracle_report.json.
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import shapely
from shapely.geometry import box

M = Path(__file__).resolve().parent
sys.path.insert(0, str(M))

import oracle
import osm_source
from catalog import Catalog, IndexBuildError, SequenceRejected
from osm_source import DiffRejected

OUT = M / "out"
CHECKS = {"n": 0}


def check(cond, msg):
    CHECKS["n"] += 1
    if not cond:
        raise AssertionError(msg)


class Log:
    def __init__(self, path):
        self._fh = open(path, "w")

    def line(self, s):
        print(s)
        self._fh.write(s + "\n")
        self._fh.flush()

    def close(self):
        self._fh.close()


# ------------------------------------------------------------- file writers

def write_xml(path, nodes, ways, relations):
    lines = ['<?xml version="1.0" encoding="UTF-8"?><osm version="0.6" generator="scenario">']
    for nid, (lon, lat) in sorted(nodes.items()):
        lines.append(f'<node id="{nid}" lat="{lat}" lon="{lon}"/>')
    for wid, refs in sorted(ways.items()):
        nds = "".join(f'<nd ref="{r}"/>' for r in refs)
        lines.append(f"<way id=\"{wid}\">{nds}</way>")
    for rid, members in sorted(relations.items()):
        ms = "".join(f'<member type="way" ref="{r}" role="{role}"/>' for r, role in members)
        lines.append(f'<relation id="{rid}">{ms}<tag k="type" v="multipolygon"/></relation>')
    lines.append("</osm>")
    path.write_text("\n".join(lines) + "\n")


def write_diff(path, sequence, base, nodes=(), ways=(), relations=()):
    payload = {"sequence": sequence, "base": base}
    if nodes:
        payload["nodes"] = list(nodes)
    if ways:
        payload["ways"] = list(ways)
    if relations:
        payload["relations"] = list(relations)
    path.write_text(json.dumps(payload, indent=1) + "\n")
    return path


# --------------------------------------------------------- oracle cross-cut

def oracle_step(cat, pre_graph, post_graph, changed, res, rects, pre_built_at, label, report):
    """Compare the incremental catalog against a full independent rebuild."""
    st = cat.debug_state()
    valid, invalid = oracle.build_all(post_graph)
    check(set(valid) == set(st["cache_ids"]),
          f"{label}: valid set mismatch oracle={sorted(valid)} catalog={st['cache_ids']}")
    check(set(invalid) == set(st["quarantine"].keys()),
          f"{label}: quarantine mismatch oracle={sorted(invalid)} catalog={sorted(st['quarantine'])}")
    for rid, g in valid.items():
        got = shapely.from_wkt(st["wkt"][rid])
        check(g.normalize().wkb_hex == got.normalize().wkb_hex,
              f"{label}: geometry mismatch rid={rid}")
    exp_live, exp_removed = oracle.expected_closure(pre_graph, post_graph, changed)
    check(set(res["rebuilt"]) == exp_live,
          f"{label}: rebuilt set catalog={res['rebuilt']} oracle={sorted(exp_live)}")
    check(set(res["removed"]) == exp_removed,
          f"{label}: removed set catalog={res['removed']} oracle={sorted(exp_removed)}")
    for rid, built_at in pre_built_at.items():
        if rid not in exp_live and rid in st["built_at"]:
            check(st["built_at"][rid] == built_at,
                  f"{label}: relation {rid} rebuilt outside the closure (fake incremental)")
    for rect in rects:
        hits = {h["id"] for h in cat.search(*rect)}
        exp = oracle.oracle_hits(valid, rect)
        check(hits == exp, f"{label}: search rect={rect} catalog={sorted(hits)} oracle={sorted(exp)}")
    report.append({"label": label, "sequence": cat.sequence,
                   "valid": sorted(valid), "quarantine": sorted(invalid),
                   "rebuilt": sorted(exp_live), "removed": sorted(exp_removed)})


# =========================================================== main scenario

def scenario_main(report):
    d = OUT / "main"
    (d / "diffs").mkdir(parents=True, exist_ok=True)
    log = Log(d / "apply.log")
    log.line("== main saga: shared nodes/ways, quarantine, recovery, dependency transfer ==")

    nodes = {
        1: (0, 0), 2: (4, 0), 3: (4, 4), 4: (0, 4),
        5: (8, 0), 6: (6, 3),
        7: (10, 0), 8: (14, 0), 9: (14, 4), 10: (10, 4),
        11: (11, 1), 12: (13, 1), 13: (13, 3), 14: (11, 3),
        15: (20, 0), 16: (24, 0), 17: (24, 4), 18: (20, 4), 19: (20, 0),  # 19==15 by coords only
        20: (30, 0), 21: (34, 0), 22: (34, 4), 23: (30, 4),
        24: (31, 1), 25: (33, 1), 26: (33, 3), 27: (31, 3),
        28: (100, 100), 29: (104, 100), 30: (104, 104), 31: (100, 104),
        32: (40, 0), 33: (44, 0), 34: (44, 4), 35: (40, 4),
        36: (40, 10), 37: (44, 10), 38: (44, 14), 39: (40, 14),
        40: (50, 0), 41: (54, 0), 42: (54, 4), 43: (50, 4),
        44: (60, 0), 45: (64, 0), 46: (64, 4), 47: (60, 4),
    }
    ways = {
        101: [1, 2, 3, 4, 1],
        102: [2, 5, 6, 2],
        103: [7, 8, 9],
        104: [7, 10, 9],        # stitched reversed
        105: [11, 12, 13],
        106: [11, 14, 13],      # stitched reversed
        107: [15, 16, 17],
        108: [17, 18, 19],      # gap: 19 != 15 by identity
        109: [20, 21, 22, 23, 20],
        110: [24, 25, 26, 27, 24],
        111: [28, 29, 30, 31, 28],
        112: [32, 33, 34, 35, 32],
        113: [36, 37, 38, 39, 36],
        114: [40, 41, 42, 43, 40],
        115: [44, 45, 46, 47, 44],
    }
    relations = {
        1001: [(101, "outer")],
        1002: [(102, "outer")],
        1003: [(104, "outer"), (106, "inner"), (103, "outer"), (105, "inner")],
        1004: [(107, "outer"), (108, "outer")],
        1005: [(109, "outer")],
        1006: [(109, "outer"), (110, "inner")],
        1007: [(111, "outer")],
        1008: [(112, "outer")],
        1009: [(114, "outer")],
        1010: [(115, "outer")],
    }
    base_xml = d / "base_main.osm"
    write_xml(base_xml, nodes, ways, relations)

    RECTS = [
        (3, -1, 5, 5), (-1, -1, 9, 9), (9, -1, 15, 5), (11.5, 1.5, 12.5, 2.5),
        (19, -1, 25, 5), (29, -1, 35, 5), (31.5, 1.5, 32.5, 2.5), (99, 99, 105, 105),
        (39, -1, 45, 15), (49, -1, 55, 5), (59, -1, 65, 6), (4, 0.5, 4, 0.5),
        (0, 0, 200, 200),
    ]

    cat = Catalog(workdir=d / "catalog")
    res = cat.import_osm(base_xml)
    log.line(f"[seq= 0] import rebuilt={res['rebuilt']} quarantined={res['quarantined']} index=9")
    check(res["quarantined"] == [1004], "1004 must be quarantined at import (coordinate gap)")
    check(cat.explain(1004)["reason"] == "open-ring", "1004 reason open-ring")
    # true-area search: the hole of 1003 and the hole of 1006 must NOT hit
    check([h["id"] for h in cat.search(11.5, 1.5, 12.5, 2.5)] == [], "1003 hole must not hit")
    check([h["id"] for h in cat.search(31.5, 1.5, 32.5, 2.5)] == [1005], "1006 hole must not hit, 1005 must")
    check([h["id"] for h in cat.search(19, -1, 25, 5)] == [], "quarantined 1004 invisible")
    # explain: opposite-direction ways recorded with direction
    expl = cat.explain(1003)
    dirs = {p["way_id"]: p["direction"] for r in expl["rings"] for p in r["parts"]}
    check(dirs == {103: 1, 104: -1, 105: 1, 106: -1}, f"1003 directions {dirs}")
    v = cat.verify_explain(1003)
    check(v["verifiable"] and v["geometry_matches"] and v["provenance_matches_current_source"],
          "explain(1003) must be reproducible from current source")

    og = osm_source.load_graph(base_xml)  # oracle graph, advanced per commit
    pre_built_at = cat.debug_state()["built_at"]
    all_changed = {"nodes": set(og.nodes), "ways": set(og.ways),
                   "relations": set(og.relations)}  # import == honest full rebuild
    oracle_step(cat, og, og, all_changed,
                {"rebuilt": res["rebuilt"], "removed": []}, RECTS, {}, "seq0-import", report["steps"])

    def commit(path, label, expect):
        nonlocal og, pre_built_at
        diff = osm_source.load_diff(path)
        r = cat.apply_diff(diff)
        og2, changed, _ = osm_source.apply_diff_to_graph(og, diff)
        log.line(f"[seq={r['sequence']:2d}] {label} changed={r['changed']} rebuilt={r['rebuilt']} "
                 f"quarantined+={r['quarantined']} recovered={r['recovered']} removed={r['removed']} "
                 f"index={r['index_size']}")
        for k, v in expect.items():
            check(r[k] == v, f"{label}: {k} expected {v} got {r[k]}")
        oracle_step(cat, og, og2, changed, r, RECTS, pre_built_at, label, report["steps"])
        og = og2
        pre_built_at = cat.debug_state()["built_at"]
        return r

    # d1: shared node move -> 1001 invalid, 1002 valid
    p = write_diff(d / "diffs" / "d01_move_shared_node.json", 1, 0,
                   nodes=[{"id": 2, "old_version": 1, "new_version": 2, "lon": 4, "lat": 5}])
    commit(p, "d01_move_shared_node", {"rebuilt": [1001, 1002], "quarantined": [1001], "recovered": []})
    check([h["id"] for h in cat.search(3, -1, 5, 5)] == [1002], "seq1: only 1002 near shared node")
    check(cat.explain(1001)["status"] == "quarantined", "1001 quarantined")

    # d2: delete a way shared by two relations -> both quarantined
    p = write_diff(d / "diffs" / "d02_delete_shared_way.json", 2, 1,
                   ways=[{"id": 109, "old_version": 1, "new_version": 2, "delete": True}])
    commit(p, "d02_delete_shared_way", {"rebuilt": [1005, 1006], "quarantined": [1005, 1006]})
    check(cat.explain(1005)["reason"] == "way-missing", "1005 way-missing")
    check([h["id"] for h in cat.search(29, -1, 35, 5)] == [], "seq2: shared-way region empty")

    # d3: break 1010 by moving a node; d4: repair it -> recovery
    p = write_diff(d / "diffs" / "d03_break_1010.json", 3, 2,
                   nodes=[{"id": 44, "old_version": 1, "new_version": 2, "lon": 63, "lat": 5}])
    commit(p, "d03_break_1010", {"rebuilt": [1010], "quarantined": [1010]})
    conn_old = cat.connect()  # pinned read connection at seq 3
    p = write_diff(d / "diffs" / "d04_repair_1010.json", 4, 3,
                   nodes=[{"id": 44, "old_version": 2, "new_version": 3, "lon": 60, "lat": 0}])
    commit(p, "d04_repair_1010", {"rebuilt": [1010], "recovered": [1010]})
    check(conn_old.sequence == 3 and [h["id"] for h in conn_old.search(59, -1, 65, 6)] == [],
          "old read connection keeps the old whole (1010 still quarantined there)")
    check([h["id"] for h in cat.connect().search(59, -1, 65, 6)] == [1010],
          "new read connection sees the new whole")

    # d5: delete relation 1009; d6: editing its old member way must NOT trigger it
    p = write_diff(d / "diffs" / "d05_delete_relation.json", 5, 4,
                   relations=[{"id": 1009, "old_version": 1, "new_version": 2, "delete": True}])
    commit(p, "d05_delete_relation", {"rebuilt": [], "removed": [1009]})
    check(cat.explain(1009)["status"] == "deleted", "1009 deleted")
    check([h["id"] for h in cat.search(49, -1, 55, 5)] == [], "seq5: 1009 gone from index")
    p = write_diff(d / "diffs" / "d06_edit_orphan_way.json", 6, 5,
                   ways=[{"id": 114, "old_version": 1, "new_version": 2,
                          "nodes": [40, 43, 42, 41, 40]}])
    commit(p, "d06_edit_orphan_way", {"rebuilt": [], "removed": []})

    # d7: dependency transfer 1008: way 112 -> way 113
    p = write_diff(d / "diffs" / "d07_transfer_members.json", 7, 6,
                   relations=[{"id": 1008, "old_version": 1, "new_version": 2,
                               "members": [{"type": "way", "ref": 113, "role": "outer"}]}])
    commit(p, "d07_transfer_members", {"rebuilt": [1008]})
    check(cat.explain(1008)["bbox"] == [40.0, 10.0, 44.0, 14.0], "1008 moved with way 113")
    # d8: old dependency way 112 no longer triggers 1008
    p = write_diff(d / "diffs" / "d08_edit_old_way.json", 8, 7,
                   ways=[{"id": 112, "old_version": 1, "new_version": 2,
                          "nodes": [32, 33, 35, 32]}])
    commit(p, "d08_edit_old_way", {"rebuilt": []})
    # d9: new dependency way 113 triggers 1008
    p = write_diff(d / "diffs" / "d09_edit_new_way.json", 9, 8,
                   ways=[{"id": 113, "old_version": 1, "new_version": 2,
                          "nodes": [36, 37, 38, 36]}])
    commit(p, "d09_edit_new_way", {"rebuilt": [1008]})

    # d10: repair shared node -> 1001 recovers; d11: undelete way -> 1005/1006 recover
    p = write_diff(d / "diffs" / "d10_repair_shared_node.json", 10, 9,
                   nodes=[{"id": 2, "old_version": 2, "new_version": 3, "lon": 4, "lat": 0}])
    commit(p, "d10_repair_shared_node", {"rebuilt": [1001, 1002], "recovered": [1001]})
    check([h["id"] for h in cat.search(3, -1, 5, 5)] == [1001, 1002], "seq10: both back")
    p = write_diff(d / "diffs" / "d11_undelete_way.json", 11, 10,
                   ways=[{"id": 109, "old_version": 2, "new_version": 3,
                          "nodes": [20, 21, 22, 23, 20]}])
    commit(p, "d11_undelete_way", {"rebuilt": [1005, 1006], "recovered": [1005, 1006]})

    # d12: byte-identical same-version object is an idempotent no-op
    p = write_diff(d / "diffs" / "d12_noop_object.json", 12, 11,
                   nodes=[{"id": 2, "old_version": 3, "new_version": 3, "lon": 4, "lat": 0}])
    commit(p, "d12_noop_object", {"rebuilt": []})

    # d13: close the coordinate gap by identity (way 108 ends at node 15) -> 1004 recovers
    p = write_diff(d / "diffs" / "d13_close_gap.json", 13, 12,
                   ways=[{"id": 108, "old_version": 1, "new_version": 2,
                          "nodes": [17, 18, 15]}])
    commit(p, "d13_close_gap", {"rebuilt": [1004], "recovered": [1004]})
    check(cat.debug_state()["quarantine"] == {}, "seq13: quarantine empty")
    check([h["id"] for h in cat.search(19, -1, 25, 5)] == [1004], "1004 searchable after repair")

    # d14: index-build failure -> whole diff rolls back; retry succeeds
    p = write_diff(d / "diffs" / "d14_move_node5.json", 14, 13,
                   nodes=[{"id": 5, "old_version": 1, "new_version": 2, "lon": 9, "lat": 0}])
    before = cat.debug_state()
    conn = cat.connect()
    try:
        cat.apply_diff(p, _inject_index_failure=True)
        check(False, "expected IndexBuildError")
    except IndexBuildError:
        log.line("[seq=13] d14_move_node5 INDEX BUILD FAILED -> diff rolled back")
    check(cat.sequence == 13 and cat.debug_state() == before, "rollback restores the old whole")
    check(conn.sequence == 13, "pinned connection unaffected by rolled-back diff")
    commit(p, "d14_move_node5_retry", {"rebuilt": [1002]})

    # d15: create nodes + way + relation in one atomic batch
    p = write_diff(d / "diffs" / "d15_create_relation.json", 15, 14,
                   nodes=[{"id": 48, "lon": 70, "lat": 0}, {"id": 49, "lon": 74, "lat": 0},
                          {"id": 50, "lon": 74, "lat": 4}, {"id": 51, "lon": 70, "lat": 4}],
                   ways=[{"id": 116, "nodes": [48, 49, 50, 51, 48]}],
                   relations=[{"id": 1011, "members": [{"type": "way", "ref": 116, "role": "outer"}]}])
    commit(p, "d15_create_relation", {"rebuilt": [1011]})
    check([h["id"] for h in cat.search(69, -1, 75, 5)] == [1011], "created relation searchable")
    check(cat.explain(1011)["relation_version"] == 1, "created relation version 1")

    # ---- idempotent replay and version-rule rejections (state must not move)
    res = cat.apply_diff(d / "diffs" / "d10_repair_shared_node.json")
    check(res["idempotent_replay"] and cat.sequence == 15, "identical replay is an idempotent no-op")
    log.line("[seq=15] identical replay of d10 -> idempotent no-op")

    tampered = json.loads((d / "diffs" / "d10_repair_shared_node.json").read_text())
    tampered["nodes"][0]["lat"] = 0.5
    rejections = [
        ("tampered replay (same versions, other content)", tampered, SequenceRejected),
        ("base version mismatch", {"sequence": 16, "base": 15, "nodes": [
            {"id": 2, "old_version": 2, "new_version": 4, "lon": 4, "lat": 0}]}, DiffRejected),
        ("version regression", {"sequence": 16, "base": 15, "nodes": [
            {"id": 2, "old_version": 3, "new_version": 2, "lon": 4, "lat": 0}]}, DiffRejected),
        ("same version, different content", {"sequence": 16, "base": 15, "nodes": [
            {"id": 2, "old_version": 3, "new_version": 3, "lon": 99, "lat": 0}]}, DiffRejected),
        ("way repeated inside one relation", {"sequence": 16, "base": 15, "relations": [
            {"id": 1001, "old_version": 1, "new_version": 2, "members": [
                {"type": "way", "ref": 101, "role": "outer"},
                {"type": "way", "ref": 101, "role": "outer"}]}]}, DiffRejected),
        ("bad role", {"sequence": 16, "base": 15, "relations": [
            {"id": 1001, "old_version": 1, "new_version": 2, "members": [
                {"type": "way", "ref": 101, "role": "side"}]}]}, DiffRejected),
        ("out-of-order sequence", {"sequence": 17, "base": 15, "nodes": [
            {"id": 6, "old_version": 1, "new_version": 2, "lon": 6, "lat": 3}]}, SequenceRejected),
    ]
    frozen = cat.debug_state()
    for label, payload, exc in rejections:
        try:
            cat.apply_diff(payload)
            check(False, f"{label}: must be rejected")
        except exc:
            log.line(f"[reject-ok] {label}")
        check(cat.debug_state() == frozen, f"{label}: state must not change on rejection")
    check(cat.sequence == 15, "rejections consume no sequence")

    # ---- persistence: reload from disk, replay the journal, compare
    cat2 = Catalog.load(d / "catalog")
    check(cat2.sequence == cat.sequence and cat2.debug_state() == cat.debug_state(),
          "reloaded catalog equals the live one")
    for rect in RECTS:
        check(cat2.search(*rect) == cat.search(*rect), "reloaded search identical")
    check(cat2.apply_diff(d / "diffs" / "d10_repair_shared_node.json")["idempotent_replay"],
          "idempotent replay also works after reload")

    cat3 = Catalog()
    cat3.import_osm(base_xml)
    for line in (d / "catalog" / "journal.jsonl").read_text().splitlines()[1:]:
        cat3.apply_diff(json.loads(line)["diff"])
    check(cat3.debug_state() == cat.debug_state(), "journal replay reproduces the final state")
    log.line("[persist] reload + journal replay reproduce the committed state")

    # ---- offline oracle: replay persisted journal files, compare every
    #      committed version's state file (geometry, quarantine, closure)
    series = oracle.replay_journal(base_xml, d / "catalog" / "journal.jsonl")
    check(len(series) == 16, "16 committed versions on disk")
    for seq, graph, entry in series:
        state = json.loads((d / "catalog" / "versions" / f"state-{seq:06d}.json").read_text())
        valid, invalid = oracle.build_all(graph)
        check(set(valid) == {int(r) for r in state["cache"]}, f"v{seq}: cache set")
        check(set(invalid) == {int(r) for r in state["quarantine"]}, f"v{seq}: quarantine set")
        for rid, g in valid.items():
            got = shapely.from_wkt(state["cache"][str(rid)]["wkt"])
            check(g.normalize().wkb_hex == got.normalize().wkb_hex, f"v{seq}: geometry {rid}")
        if seq:
            changed = {k: set(v) for k, v in entry["changed"].items()}
            exp_live, exp_removed = oracle.expected_closure(series[seq - 1][1], graph, changed)
            check(set(entry["rebuilt"]) == exp_live, f"v{seq}: journaled rebuilt set")
            check(set(entry["removed"]) == exp_removed, f"v{seq}: journaled removed set")
            for rid_s, e in state["cache"].items():
                if int(rid_s) not in exp_live:
                    check(e["built_at"] < seq, f"v{seq}: {rid_s} built outside closure")
    log.line("[oracle] every committed version matches an independent full rebuild")
    log.close()
    return cat, RECTS


# ========================================================= legacy scenario

def scenario_legacy(report):
    d = OUT / "legacy"
    d.mkdir(parents=True, exist_ok=True)
    log = Log(d / "apply.log")
    cat = Catalog(workdir=d / "catalog")
    res = cat.import_osm(M / "legacy" / "areas.osm")
    log.line(f"[seq= 0] import legacy/areas.osm rebuilt={res['rebuilt']} quarantined={res['quarantined']}")
    check(res["quarantined"] == [501], "501 has a coordinate gap -> quarantine")
    check(cat.explain(501)["reason"] == "open-ring", "501 open-ring (nodes not welded by coords)")

    expl = cat.explain(500)
    dirs = {p["way_id"]: p["direction"] for r in expl["rings"] for p in r["parts"]}
    check(dirs == {101: 1, 102: -1, 103: 1, 104: -1, 105: 1}, f"500 directions {dirs}")
    roles = sorted((r["role"], len(r["parts"])) for r in expl["rings"])
    check(roles == [("inner", 2), ("outer", 1), ("outer", 2)], "500: two outers, one inner")
    v = cat.verify_explain(500)
    check(v["verifiable"] and v["geometry_matches"] and v["provenance_matches_current_source"],
          "explain(500) reproducible from current source")
    log.line(f"[seq= 0] explain(500) directions={dirs} verifiable=True")

    check([h["id"] for h in cat.search(1.2, 1.2, 1.8, 1.8)] == [], "rect inside 500's hole misses")
    check([h["id"] for h in cat.search(0.5, 0.5, 0.9, 0.9)] == [500], "solid part of 500 hits")
    check([h["id"] for h in cat.search(10.5, 0.5, 11.5, 1.5)] == [500], "second outer of 500 hits")
    check([h["id"] for h in cat.search(20.5, -0.5, 24.5, 1.5)] == [], "quarantined 501 invisible")

    og = osm_source.load_graph(M / "legacy" / "areas.osm")
    valid, invalid = oracle.build_all(og)
    check(set(valid) == {500} and set(invalid) == {501}, "oracle agrees on legacy import")
    st = cat.debug_state()
    check(shapely.from_wkt(st["wkt"][500]).normalize().wkb_hex == valid[500].normalize().wkb_hex,
          "oracle geometry matches for 500")
    report["steps"].append({"label": "legacy-import", "valid": sorted(valid),
                            "quarantine": sorted(invalid)})
    log.close()


# ========================================================= inputs scenarios

def scenario_inputs(report):
    d = OUT / "inputs"
    d.mkdir(parents=True, exist_ok=True)
    log = Log(d / "apply.log")

    # given fixtures: base.osm -> diff.json -> delete-way.json
    cat = Catalog(workdir=d / "catalog")
    cat.import_osm(M / "inputs" / "base.osm")
    r1 = cat.apply_diff(M / "inputs" / "diff.json")
    log.line(f"[seq= 1] inputs/diff.json rebuilt={r1['rebuilt']}")
    check(r1["rebuilt"] == [21], "diff.json rebuilds 21")
    hits = cat.search(4, 1)
    check([h["id"] for h in hits] == [21] and hits[0]["relation_version"] == 1,
          "the baseline defect is fixed: search sees the moved nodes")
    r2 = cat.apply_diff(M / "inputs" / "delete-way.json")
    log.line(f"[seq= 2] inputs/delete-way.json rebuilt={r2['rebuilt']} quarantined={r2['quarantined']}")
    check(r2["quarantined"] == [21] and cat.search(4, 1) == [], "delete-way quarantines 21")
    check(cat.explain(21)["reason"] == "way-missing", "21 way-missing")
    report["steps"].append({"label": "inputs-delete-way", "quarantine": [21]})
    log.close()

    # given shared-node fixture with expected_validity
    d2 = OUT / "shared"
    (d2 / "diffs").mkdir(parents=True, exist_ok=True)
    log = Log(d2 / "apply.log")
    given = json.loads((M / "inputs" / "shared-node-move.json").read_text())
    p = write_diff(d2 / "diffs" / "d01_shared_node_move.json", 1, 0, nodes=given["nodes"])
    cat = Catalog(workdir=d2 / "catalog")
    cat.import_osm(M / "inputs" / "shared.osm")
    r = cat.apply_diff(p)
    exp = given["expected_validity"]
    got = {"21": 21 not in cat.debug_state()["quarantine"],
           "22": 22 not in cat.debug_state()["quarantine"]}
    log.line(f"[seq= 1] shared-node-move rebuilt={r['rebuilt']} validity={got} expected={exp}")
    check(got == {k: v for k, v in exp.items()}, "shared-node-move expected_validity honored")
    check(r["rebuilt"] == [21, 22], "shared node closure covers both relations")
    report["steps"].append({"label": "shared-node-move", "validity": got})
    log.close()


# ==================================================================== main

def main():
    shutil.rmtree(OUT, ignore_errors=True)
    OUT.mkdir()
    report = {"main": {"steps": []}, "legacy": {"steps": []}, "inputs": {"steps": []}}
    scenario_main(report["main"])
    scenario_legacy(report["legacy"])
    scenario_inputs(report["inputs"])
    report["checks_passed"] = CHECKS["n"]
    (OUT / "oracle_report.json").write_text(json.dumps(report, indent=1) + "\n")
    print(f"\nALL SCENARIOS PASSED: {CHECKS['n']} checks, report at out/oracle_report.json")


if __name__ == "__main__":
    main()
