"""Independent verification oracle.

For every committed sequence this module reconstructs the source graph from
the persisted journal (base XML + journaled diffs) and performs a FULL
geometry rebuild of every relation with its own ring stitcher, validating
with Shapely.  It is deliberately a separate code path from geometry.py so a
bug in the incremental engine cannot silently agree with itself.

Compared against the incremental catalog:
  * geometry of every valid relation (normalized WKB),
  * the quarantine set,
  * search hit ids over a battery of query rectangles,
  * the actually rebuilt relation set per diff (expected reverse closure),
  * built_at stability for relations outside the closure (no fake increment).
"""
from __future__ import annotations

import json
from pathlib import Path

from shapely.geometry import Polygon, box
from shapely.ops import unary_union

import osm_source


class _Invalid(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


# ------------------------------------------------- independent ring stitch

def stitch(parts):
    """Greedy adjacency walk over way node-lists; node identity only.
    Returns a list of closed rings, or None when a chain cannot close."""
    pool = [list(p) for p in parts]
    rings = []
    while pool:
        chain = pool.pop()
        while chain[0] != chain[-1]:
            nxt = None
            for i, p in enumerate(pool):
                if p and p[0] == chain[-1]:
                    nxt = (i, p)
                    break
                if p and p[-1] == chain[-1]:
                    nxt = (i, list(reversed(p)))
                    break
            if nxt is None:
                return None
            pool.pop(nxt[0])
            chain = chain + nxt[1][1:]
        rings.append(chain)
    return rings


# ---------------------------------------------------------- full recompute

def build_all(graph):
    """Rebuild EVERY live relation from scratch.
    Returns (valid: {rid: shapely geom}, invalid: {rid: reason})."""
    valid, invalid = {}, {}
    for rid, rel in sorted(graph.relations.items()):
        if rel.deleted:
            continue
        try:
            valid[rid] = _build_one(graph, rel)
        except _Invalid as e:
            invalid[rid] = e.reason
    return valid, invalid


def _build_one(graph, rel):
    if not rel.members:
        raise _Invalid("no-members")
    roles = {"outer": [], "inner": []}
    for wid, role in rel.members:
        way = graph.ways.get(wid)
        if way is None or way.deleted:
            raise _Invalid("way-missing")
        for nid in way.nodes:
            n = graph.nodes.get(nid)
            if n is None or n.deleted:
                raise _Invalid("node-missing")
        roles[role].append(list(way.nodes))
    if not roles["outer"]:
        raise _Invalid("no-outer-ring")
    outers = stitch(roles["outer"])
    inners = stitch(roles["inner"]) if roles["inner"] else []
    if outers is None or inners is None:
        raise _Invalid("open-ring")

    def coord(nid):
        n = graph.nodes[nid]
        return (n.lon, n.lat)

    opolys = []
    for ring in outers:
        if len(ring) < 4:
            raise _Invalid("ring-too-short")
        p = Polygon([coord(n) for n in ring])
        if not p.is_valid:
            raise _Invalid("outer-ring-invalid")
        opolys.append(p)
    assigned = [[] for _ in opolys]
    for ring in inners:
        if len(ring) < 4:
            raise _Invalid("ring-too-short")
        hp = Polygon([coord(n) for n in ring])
        if not hp.is_valid:
            raise _Invalid("inner-ring-invalid")
        containers = [i for i, op in enumerate(opolys) if op.contains(hp)]
        if not containers:
            raise _Invalid("inner-outside-outer")
        assigned[min(containers, key=lambda j: opolys[j].area)].append(hp)
    polys = []
    for i, op in enumerate(opolys):
        poly = Polygon(op.exterior.coords, [h.exterior.coords for h in assigned[i]])
        if not poly.is_valid:
            raise _Invalid("invalid-geometry")
        polys.append(poly)
    g = unary_union(polys) if len(polys) > 1 else polys[0]
    if g.is_empty or not g.is_valid:
        raise _Invalid("invalid-geometry")
    return g


# ------------------------------------------------- independent closure spec

def expected_closure(pre_graph, post_graph, changed):
    """The relation set a correct incremental engine must rebuild for a diff,
    computed from independently built reverse maps.  Returns (live, removed)."""
    def index(g):
        n2w, w2r = {}, {}
        for w in g.ways.values():
            if w.deleted:
                continue
            for n in w.nodes:
                n2w.setdefault(n, set()).add(w.id)
        for r in g.relations.values():
            if r.deleted:
                continue
            for wid, _role in r.members:
                w2r.setdefault(wid, set()).add(r.id)
        return n2w, w2r

    pn2w, pw2r = index(pre_graph)
    qn2w, qw2r = index(post_graph)
    ways = set(changed["ways"])
    for n in changed["nodes"]:
        ways |= pn2w.get(n, set()) | qn2w.get(n, set())
    rels = set(changed["relations"])
    for w in ways:
        rels |= pw2r.get(w, set()) | qw2r.get(w, set())
    live = {r for r in rels
            if r in post_graph.relations and not post_graph.relations[r].deleted}
    return live, rels - live


# ----------------------------------------------------------- journal replay

def replay_journal(base_xml, journal_path):
    """Reconstruct every committed graph purely from persisted files.
    Returns [(sequence, graph, journal_entry)]."""
    lines = [json.loads(l) for l in Path(journal_path).read_text().splitlines()
             if l.strip()]
    assert lines and lines[0]["sequence"] == 0 and lines[0]["op"] == "import"
    graph = osm_source.load_graph(base_xml)
    out = [(0, graph, lines[0])]
    for entry in lines[1:]:
        diff = osm_source.normalize_diff(entry["diff"])
        graph, _changed, _noop = osm_source.apply_diff_to_graph(graph, diff)
        out.append((entry["sequence"], graph, entry))
    return out


def oracle_hits(valid, rect):
    """Full-scan search over the oracle's own geometries."""
    return {rid for rid, g in valid.items() if g.intersects(box(*rect))}
