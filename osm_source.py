"""OSM source graph: real OSM XML import and ordered, atomic, versioned diffs.

The graph is an immutable-by-convention snapshot of nodes / ways / relations
with explicit versions and tombstones.  Node identity is by id only; equal
coordinates never weld two nodes together.  A diff is validated as a whole
and applied atomically: any rejection discards the entire batch.
"""
from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

LIMITS = {"nodes": 20000, "ways": 5000, "relations": 1000, "diffs": 50}
ROLES = ("outer", "inner")


class SourceError(Exception):
    """Base class for source-graph failures."""


class DiffRejected(SourceError):
    """The whole diff batch is rejected; no object takes effect."""


@dataclass(frozen=True)
class Node:
    id: int
    lon: float
    lat: float
    version: int = 1
    deleted: bool = False


@dataclass(frozen=True)
class Way:
    id: int
    nodes: tuple = ()
    version: int = 1
    deleted: bool = False


@dataclass(frozen=True)
class Relation:
    id: int
    members: tuple = ()  # ((way_id, role), ...) in member order
    version: int = 1
    deleted: bool = False


@dataclass(frozen=True)
class SourceGraph:
    nodes: dict
    ways: dict
    relations: dict

    def live_counts(self):
        return {
            "nodes": sum(1 for o in self.nodes.values() if not o.deleted),
            "ways": sum(1 for o in self.ways.values() if not o.deleted),
            "relations": sum(1 for o in self.relations.values() if not o.deleted),
        }


# ---------------------------------------------------------------- XML import

def load_graph(path):
    """Parse a real OSM XML file into a SourceGraph (versions default to 1).

    Only type=multipolygon relations are imported; members are ways only,
    roles are outer/inner (an empty role is read as outer, per OSM custom).
    """
    root = ET.parse(str(path)).getroot()
    nodes, ways, relations = {}, {}, {}
    for el in root.findall("node"):
        oid = int(el.get("id"))
        nodes[oid] = Node(oid, float(el.get("lon")), float(el.get("lat")),
                          int(el.get("version") or 1))
    for el in root.findall("way"):
        oid = int(el.get("id"))
        ways[oid] = Way(oid, tuple(int(nd.get("ref")) for nd in el.findall("nd")),
                        int(el.get("version") or 1))
    for el in root.findall("relation"):
        tags = {t.get("k"): t.get("v") for t in el.findall("tag")}
        if tags.get("type") != "multipolygon":
            continue
        members = []
        for m in el.findall("member"):
            if m.get("type") != "way":
                continue  # this subset has way members only
            role = m.get("role") or "outer"
            members.append((int(m.get("ref")), role))
        oid = int(el.get("id"))
        relations[oid] = Relation(oid, tuple(members), int(el.get("version") or 1))
    graph = SourceGraph(nodes, ways, relations)
    _check_limits(graph)
    return graph


# ----------------------------------------------------------------- diff I/O

def load_diff(path):
    return normalize_diff(json.loads(Path(path).read_text()))


def normalize_diff(raw):
    """Coerce a diff payload to the canonical in-memory shape.

    old_version/new_version are optional for legacy fixtures: they default to
    the current object version / current+1 at apply time.
    """
    if "sequence" not in raw or "base" not in raw:
        raise DiffRejected("diff lacks sequence/base")
    out = {"sequence": int(raw["sequence"]), "base": int(raw["base"]),
           "nodes": [], "ways": [], "relations": []}
    for n in raw.get("nodes", []):
        obj = {"id": int(n["id"])}
        _versions(n, obj)
        if n.get("delete"):
            obj["delete"] = True
        else:
            obj["lon"] = float(n["lon"])
            obj["lat"] = float(n["lat"])
        out["nodes"].append(obj)
    for w in raw.get("ways", []):
        obj = {"id": int(w["id"])}
        _versions(w, obj)
        if w.get("delete"):
            obj["delete"] = True
        else:
            obj["nodes"] = [int(x) for x in w["nodes"]]
        out["ways"].append(obj)
    for r in raw.get("relations", []):
        obj = {"id": int(r["id"])}
        _versions(r, obj)
        if r.get("delete"):
            obj["delete"] = True
        else:
            obj["members"] = [{"type": m.get("type", "way"),
                               "ref": int(m["ref"]),
                               "role": m.get("role") or "outer"}
                              for m in r["members"]]
        out["relations"].append(obj)
    return out


def _versions(src, dst):
    if "old_version" in src:
        dst["old_version"] = int(src["old_version"])
    if "new_version" in src:
        dst["new_version"] = int(src["new_version"])


def canonical(diff):
    """Canonical byte form of a normalized diff (for idempotent-replay compare)."""
    return json.dumps(diff, sort_keys=True, separators=(",", ":"))


# ------------------------------------------------------------- atomic apply

def apply_diff_to_graph(graph, diff):
    """Validate the whole batch against `graph`, then return
    (new_graph, changed, noop) where changed/noop are {"nodes","ways","relations"}
    sets of ids.  Raises DiffRejected; on rejection nothing is applied."""
    seen = set()
    ops = []
    for kind in ("nodes", "ways", "relations"):
        for obj in diff.get(kind, []):
            oid = obj["id"]
            if (kind, oid) in seen:
                raise DiffRejected(f"duplicate object {kind}/{oid} in one diff")
            seen.add((kind, oid))
            ops.append((kind, obj))

    new_maps = {"nodes": dict(graph.nodes), "ways": dict(graph.ways),
                "relations": dict(graph.relations)}
    changed = {"nodes": set(), "ways": set(), "relations": set()}
    noop = {"nodes": set(), "ways": set(), "relations": set()}
    for kind, obj in ops:
        cur = new_maps[kind].get(obj["id"])
        new_obj = _apply_object(kind, obj, cur)
        if new_obj is None:
            noop[kind].add(obj["id"])
        else:
            new_maps[kind][obj["id"]] = new_obj
            changed[kind].add(obj["id"])
    graph2 = SourceGraph(new_maps["nodes"], new_maps["ways"], new_maps["relations"])
    _check_limits(graph2)
    return graph2, changed, noop


def _apply_object(kind, obj, cur):
    oid = obj["id"]
    delete = bool(obj.get("delete", False))
    old_v = obj.get("old_version")
    new_v = obj.get("new_version")
    if cur is None:
        if delete:
            raise DiffRejected(f"{kind}/{oid}: delete of nonexistent object")
        if old_v not in (None, 0):
            raise DiffRejected(f"{kind}/{oid}: create with nonzero old_version {old_v}")
        if new_v is None:
            new_v = 1
        if new_v < 1:
            raise DiffRejected(f"{kind}/{oid}: version regression on create")
        return _build_object(kind, oid, obj, new_v)
    # existing object (possibly a tombstone)
    if old_v is None:
        old_v = cur.version
    if new_v is None:
        new_v = cur.version + 1
    if new_v < cur.version:
        raise DiffRejected(
            f"{kind}/{oid}: version regression new_version {new_v} < current {cur.version}")
    if new_v == cur.version:
        # replay of the same object version: only byte-identical content is a no-op
        identical = (delete == cur.deleted) and (cur.deleted or _content_equal(kind, obj, cur))
        if not identical:
            raise DiffRejected(
                f"{kind}/{oid}: same version {new_v} with different content")
        return None
    # new_v > cur.version: the diff must be based on the current version
    if old_v != cur.version:
        raise DiffRejected(
            f"{kind}/{oid}: base version mismatch old_version {old_v} != current {cur.version}")
    if delete:
        if cur.deleted:
            raise DiffRejected(f"{kind}/{oid}: re-delete of a tombstone")
        return _tombstone(kind, oid, new_v)
    return _build_object(kind, oid, obj, new_v)


def _build_object(kind, oid, obj, version):
    if kind == "nodes":
        return Node(oid, float(obj["lon"]), float(obj["lat"]), version)
    if kind == "ways":
        return Way(oid, tuple(int(x) for x in obj["nodes"]), version)
    members = []
    refs = set()
    for m in obj["members"]:
        if m["type"] != "way":
            raise DiffRejected(f"relations/{oid}: member type {m['type']!r} is not 'way'")
        if m["role"] not in ROLES:
            raise DiffRejected(f"relations/{oid}: bad role {m['role']!r}")
        if m["ref"] in refs:
            raise DiffRejected(f"relations/{oid}: way {m['ref']} repeated in one relation")
        refs.add(m["ref"])
        members.append((m["ref"], m["role"]))
    return Relation(oid, tuple(members), version)


def _tombstone(kind, oid, version):
    if kind == "nodes":
        return Node(oid, 0.0, 0.0, version, deleted=True)
    if kind == "ways":
        return Way(oid, (), version, deleted=True)
    return Relation(oid, (), version, deleted=True)


def _content_equal(kind, obj, cur):
    if cur.deleted:
        return False  # a contentful object never equals a tombstone
    if kind == "nodes":
        return float(obj["lon"]) == cur.lon and float(obj["lat"]) == cur.lat
    if kind == "ways":
        return tuple(obj["nodes"]) == cur.nodes
    return tuple((m["ref"], m["role"]) for m in obj["members"]) == cur.members


def _check_limits(graph):
    counts = graph.live_counts()
    for kind, limit in LIMITS.items():
        if kind != "diffs" and counts[kind] > limit:
            raise DiffRejected(f"{kind} limit exceeded: {counts[kind]} > {limit}")
