"""Persistent incremental area catalog with MVCC snapshots.

One published Snapshot per committed sequence carries, atomically:
  * the source graph (nodes/ways/relations with versions and tombstones),
  * the geometry cache  rid -> BuiltRelation        (valid relations only),
  * the quarantine map  rid -> {reason,detail,since} (invalid relations),
  * ring provenance (inside cache entries) for explain(),
  * the spatial index over cached geometries.

Geometry is recomputed ONLY for the reverse-dependency closure of a diff
(changed nodes -> their ways -> their relations, plus changed ways/relations,
expanded over both the pre- and post-diff reverse index).  The rebuilt
relation ids are recorded per commit; everything else keeps its cache entry
untouched.  Readers pin a Snapshot, so two read connections can only ever see
the old whole or the new whole, never a mix.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import shapely
from shapely.geometry import box
from shapely.strtree import STRtree

import geometry
import osm_source
from geometry import BuildError
from osm_source import LIMITS, DiffRejected, Relation, SourceError, SourceGraph, Node, Way


class IndexBuildError(SourceError):
    """Spatial index construction failed; the whole diff is rolled back."""


class SequenceRejected(DiffRejected):
    """Diff is out of order or conflicts with committed history."""


# ------------------------------------------------------------ spatial index

class SpatialIndex:
    """Immutable STRtree over the true geometries of valid relations."""

    def __init__(self, entries):
        # entries: list of (rid, BuiltRelation)
        self._rids = [rid for rid, _ in entries]
        self._geoms = [b.geometry for _, b in entries]
        self._tree = STRtree(self._geoms) if self._geoms else None

    def query(self, rect):
        """Rids whose true geometry intersects the query rectangle."""
        if self._tree is None:
            return []
        return [self._rids[i] for i in self._tree.query(rect, predicate="intersects")]

    def __len__(self):
        return len(self._rids)


# ---------------------------------------------------------------- snapshot

class Snapshot:
    __slots__ = ("sequence", "graph", "cache", "quarantine", "index")

    def __init__(self, sequence, graph, cache, quarantine, index):
        self.sequence = sequence
        self.graph = graph
        self.cache = cache            # rid -> BuiltRelation
        self.quarantine = quarantine  # rid -> {"reason","detail","since"}
        self.index = index


def _search(snap, minx, miny, maxx=None, maxy=None):
    if maxx is None:  # point query
        maxx, maxy = minx, miny
    rect = box(minx, miny, maxx, maxy)
    hits = []
    for rid in snap.index.query(rect):
        e = snap.cache[rid]
        hits.append({"id": rid, "relation_version": e.relation_version,
                     "built_at": e.built_at, "bbox": list(e.bbox)})
    hits.sort(key=lambda h: h["id"])
    return hits


def _explain(snap, rid):
    rel = snap.graph.relations.get(rid)
    if rid in snap.quarantine:
        q = snap.quarantine[rid]
        return {"id": rid, "status": "quarantined", "reason": q["reason"],
                "detail": q["detail"], "since": q["since"],
                "relation_version": rel.version if rel else None}
    if rid in snap.cache:
        e = snap.cache[rid]
        return {"id": rid, "status": "valid", "relation_version": e.relation_version,
                "built_at": e.built_at, "bbox": list(e.bbox),
                "area": e.geometry.area, "rings": e.rings}
    if rel is not None and rel.deleted:
        return {"id": rid, "status": "deleted", "relation_version": rel.version}
    return {"id": rid, "status": "unknown"}


class ReadConnection:
    """A read connection pinned to one published snapshot: it keeps seeing
    that whole committed version even while a writer commits newer ones."""

    def __init__(self, snap):
        self._snap = snap

    @property
    def sequence(self):
        return self._snap.sequence

    def search(self, minx, miny, maxx=None, maxy=None):
        return _search(self._snap, minx, miny, maxx, maxy)

    def explain(self, rid):
        return _explain(self._snap, rid)


# ------------------------------------------------------- reverse dependency

def build_reverse_index(graph):
    """node_id -> {way_id} and way_id -> {relation_id} over live objects."""
    node_to_ways, way_to_rels = {}, {}
    for wid, way in graph.ways.items():
        if way.deleted:
            continue
        for nid in way.nodes:
            node_to_ways.setdefault(nid, set()).add(wid)
    for rid, rel in graph.relations.items():
        if rel.deleted:
            continue
        for wid, _role in rel.members:
            way_to_rels.setdefault(wid, set()).add(rid)
    return {"node_to_ways": node_to_ways, "way_to_rels": way_to_rels}


def _reverse_closure(pre_idx, post_idx, changed, graph2):
    """Relations whose geometry may be affected: changed relations plus
    dependents of changed nodes/ways, expanded over both index generations
    (a deleted relation leaves the index, so it can never be re-triggered).
    Returns (live_closure, removed)."""
    ways = set(changed["ways"])
    for nid in changed["nodes"]:
        ways |= pre_idx["node_to_ways"].get(nid, set())
        ways |= post_idx["node_to_ways"].get(nid, set())
    rels = set(changed["relations"])
    for wid in ways:
        rels |= pre_idx["way_to_rels"].get(wid, set())
        rels |= post_idx["way_to_rels"].get(wid, set())
    live, removed = set(), set()
    for rid in rels:
        rel = graph2.relations.get(rid)
        if rel is None or rel.deleted:
            removed.add(rid)
        else:
            live.add(rid)
    return live, removed


# ----------------------------------------------------------------- catalog

class Catalog:
    def __init__(self, workdir=None):
        self._snap = Snapshot(0, SourceGraph({}, {}, {}), {}, {}, SpatialIndex([]))
        self._canon = []          # canonical diff per committed sequence
        self._journal = []        # rich journal entries (dicts)
        self._imported = False
        self._workdir = Path(workdir) if workdir else None
        if self._workdir:
            (self._workdir / "versions").mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------- import

    def import_osm(self, path):
        """Initial full import (this is the one honest full rebuild)."""
        if self._imported:
            raise SourceError("catalog already initialized")
        graph = osm_source.load_graph(path)
        cache, quarantine, rebuilt = {}, {}, []
        for rid in sorted(graph.relations):
            if graph.relations[rid].deleted:
                continue
            rebuilt.append(rid)
            try:
                cache[rid] = geometry.build_relation(graph, rid, built_at=0)
            except BuildError as e:
                quarantine[rid] = {"reason": e.reason, "detail": e.detail, "since": 0}
        self._snap = Snapshot(0, graph, cache, quarantine,
                              SpatialIndex(sorted(cache.items())))
        self._imported = True
        entry = {"sequence": 0, "op": "import", "source": str(path),
                 "rebuilt": rebuilt, "quarantined": sorted(quarantine)}
        self._canon = [json.dumps(entry, sort_keys=True)]
        self._journal = [entry]
        self._persist(0, entry, self._snap)
        return {"sequence": 0, "rebuilt": rebuilt, "quarantined": sorted(quarantine)}

    # -------------------------------------------------------------- diffs

    def apply_diff(self, diff, _inject_index_failure=False):
        """Validate and commit one ordered diff atomically.

        Geometry is recomputed only for the reverse-dependency closure; the
        actually rebuilt relation ids are returned and journaled.  Any
        rejection or an index-build failure leaves the previous snapshot
        fully intact (rollback)."""
        if not self._imported:
            raise SourceError("import_osm first")
        if isinstance(diff, (str, Path)):
            diff = osm_source.load_diff(diff)
        else:
            diff = osm_source.normalize_diff(diff)
        cur = self._snap
        canon = osm_source.canonical(diff)
        seq, base = diff["sequence"], diff["base"]

        if seq <= cur.sequence:
            if 1 <= seq < len(self._canon) and self._canon[seq] == canon:
                return {"sequence": cur.sequence, "idempotent_replay": True,
                        "rebuilt": [], "removed": [], "quarantined": [], "recovered": []}
            raise SequenceRejected(
                f"diff sequence {seq} conflicts with committed history")
        if seq != cur.sequence + 1 or base != cur.sequence:
            raise SequenceRejected(
                f"out-of-order diff sequence={seq} base={base}, current={cur.sequence}")
        if cur.sequence >= LIMITS["diffs"]:
            raise SequenceRejected("diff count limit exceeded")

        # 1) atomic source-graph update (metadata validation only)
        graph2, changed, _noop = osm_source.apply_diff_to_graph(cur.graph, diff)

        # 2) reverse-dependency closure (metadata, no geometry)
        pre_idx = build_reverse_index(cur.graph)
        post_idx = build_reverse_index(graph2)
        closure, removed = _reverse_closure(pre_idx, post_idx, changed, graph2)

        # 3) geometry rebuild strictly limited to the closure
        cache2 = dict(cur.cache)
        quarantine2 = dict(cur.quarantine)
        rebuilt, quarantined_now, recovered = [], [], []
        for rid in sorted(closure):
            cache2.pop(rid, None)
            try:
                cache2[rid] = geometry.build_relation(graph2, rid, built_at=seq)
            except BuildError as e:
                since = cur.quarantine.get(rid, {}).get("since", seq)
                quarantine2[rid] = {"reason": e.reason, "detail": e.detail, "since": since}
                quarantined_now.append(rid)
            else:
                if rid in quarantine2:
                    del quarantine2[rid]
                    recovered.append(rid)
            rebuilt.append(rid)
        for rid in sorted(removed):
            cache2.pop(rid, None)
            quarantine2.pop(rid, None)

        # 4) publish: build the new index; failure here rolls the whole diff back
        if _inject_index_failure:
            raise IndexBuildError("injected index-build failure; diff rolled back")
        index2 = SpatialIndex(sorted(cache2.items()))
        snap2 = Snapshot(seq, graph2, cache2, quarantine2, index2)

        entry = {"sequence": seq, "op": "diff", "diff": diff,
                 "changed": {k: sorted(v) for k, v in changed.items()},
                 "rebuilt": rebuilt, "removed": sorted(removed),
                 "quarantined": quarantined_now, "recovered": recovered}
        self._persist(seq, entry, snap2)  # may raise -> nothing is swapped
        self._canon.append(canon)
        self._journal.append(entry)
        self._snap = snap2  # single atomic publication
        return {"sequence": seq, "idempotent_replay": False,
                "changed": entry["changed"], "rebuilt": rebuilt,
                "removed": sorted(removed), "quarantined": quarantined_now,
                "recovered": recovered, "index_size": len(index2)}

    # -------------------------------------------------------------- reads

    @property
    def sequence(self):
        return self._snap.sequence

    def connect(self):
        return ReadConnection(self._snap)

    def search(self, minx, miny, maxx=None, maxy=None):
        """Relations whose true area intersects the query rectangle, with
        versions.  Quarantined relations are withdrawn from the index."""
        return _search(self._snap, minx, miny, maxx, maxy)

    def explain(self, rid):
        """Per-ring node/way versions and directions from provenance."""
        return _explain(self._snap, rid)

    def verify_explain(self, rid):
        """Rebuild from the CURRENT source and compare with the explanation:
        proves the recorded rings are reproducible from current source."""
        expl = self.explain(rid)
        if expl["status"] != "valid":
            return {"id": rid, "verifiable": False, "status": expl["status"]}
        rebuilt = geometry.build_relation(self._snap.graph, rid, built_at=expl["built_at"])
        return {"id": rid, "verifiable": True,
                "geometry_matches": (rebuilt.geometry.normalize().wkb_hex
                                     == self._snap.cache[rid].geometry.normalize().wkb_hex),
                "provenance_matches_current_source":
                    json.dumps(rebuilt.rings, sort_keys=True)
                    == json.dumps(expl["rings"], sort_keys=True)}

    def debug_state(self):
        """White-box view for the verification harness."""
        s = self._snap
        return {"sequence": s.sequence,
                "cache_ids": sorted(s.cache),
                "quarantine": {r: dict(q) for r, q in sorted(s.quarantine.items())},
                "wkt": {r: shapely.to_wkt(e.geometry, rounding_precision=-1)
                        for r, e in s.cache.items()},
                "built_at": {r: e.built_at for r, e in s.cache.items()},
                "relation_versions": {rid: rel.version
                                      for rid, rel in s.graph.relations.items()}}

    # --------------------------------------------------------- persistence

    def _state_dict(self, snap):
        s = snap
        return {
            "sequence": s.sequence,
            "graph": {
                "nodes": [{"id": n.id, "lon": n.lon, "lat": n.lat,
                           "version": n.version, "deleted": n.deleted}
                          for n in sorted(s.graph.nodes.values(), key=lambda o: o.id)],
                "ways": [{"id": w.id, "nodes": list(w.nodes),
                          "version": w.version, "deleted": w.deleted}
                         for w in sorted(s.graph.ways.values(), key=lambda o: o.id)],
                "relations": [{"id": r.id, "members": [[w, role] for w, role in r.members],
                               "version": r.version, "deleted": r.deleted}
                              for r in sorted(s.graph.relations.values(), key=lambda o: o.id)],
            },
            "cache": {str(rid): {"relation_version": e.relation_version,
                                 "built_at": e.built_at,
                                 "wkt": shapely.to_wkt(e.geometry, rounding_precision=-1),
                                 "bbox": list(e.bbox), "rings": e.rings}
                      for rid, e in sorted(s.cache.items())},
            "quarantine": {str(rid): dict(q) for rid, q in sorted(s.quarantine.items())},
        }

    def _persist(self, seq, journal_entry, snap):
        """Durable write of the NEW committed state; runs before the in-memory
        swap, so a persistence failure also rolls the diff back."""
        if not self._workdir:
            return
        state = json.dumps(self._state_dict(snap), sort_keys=True, indent=1)
        ver = self._workdir / "versions" / f"state-{seq:06d}.json"
        ver.write_text(state)
        tmp = self._workdir / "state.json.tmp"
        tmp.write_text(state)
        os.replace(tmp, self._workdir / "state.json")
        with open(self._workdir / "journal.jsonl", "a") as fh:
            fh.write(json.dumps(journal_entry, sort_keys=True) + "\n")

    def save(self, directory):
        """Persist the current committed state + full journal to `directory`."""
        directory = Path(directory)
        (directory / "versions").mkdir(parents=True, exist_ok=True)
        state = json.dumps(self._state_dict(self._snap), sort_keys=True, indent=1)
        (directory / "state.json").write_text(state)
        (directory / "versions" / f"state-{self.sequence:06d}.json").write_text(state)
        with open(directory / "journal.jsonl", "w") as fh:
            for entry in self._journal:
                fh.write(json.dumps(entry, sort_keys=True) + "\n")
        return directory

    @classmethod
    def load(cls, directory):
        """Reload a persisted catalog (source graph, quarantine, ring
        provenance, geometry cache and index all at the same sequence)."""
        directory = Path(directory)
        state = json.loads((directory / "state.json").read_text())
        cat = cls()
        g = state["graph"]
        graph = SourceGraph(
            {n["id"]: Node(n["id"], n["lon"], n["lat"], n["version"], n["deleted"])
             for n in g["nodes"]},
            {w["id"]: Way(w["id"], tuple(w["nodes"]), w["version"], w["deleted"])
             for w in g["ways"]},
            {r["id"]: Relation(r["id"], tuple((w, role) for w, role in r["members"]),
                               r["version"], r["deleted"]) for r in g["relations"]},
        )
        cache = {int(rid): geometry.BuiltRelation(
                     int(rid), e["relation_version"], e["built_at"],
                     shapely.from_wkt(e["wkt"]), e["rings"])
                 for rid, e in state["cache"].items()}
        quarantine = {int(rid): dict(q) for rid, q in state["quarantine"].items()}
        cat._snap = Snapshot(state["sequence"], graph, cache, quarantine,
                             SpatialIndex(sorted(cache.items())))
        cat._imported = True
        cat._canon, cat._journal = [], []
        journal_path = directory / "journal.jsonl"
        if journal_path.exists():
            for line in journal_path.read_text().splitlines():
                if not line.strip():
                    continue
                entry = json.loads(line)
                cat._journal.append(entry)
                if entry["op"] == "diff":
                    cat._canon.append(osm_source.canonical(
                        osm_source.normalize_diff(entry["diff"])))
                else:
                    cat._canon.append(json.dumps(entry, sort_keys=True))
        return cat
