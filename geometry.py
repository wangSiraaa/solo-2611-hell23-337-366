"""Ring assembly and multipolygon building under the legacy rules.

Legacy rules (exercised by legacy/areas.osm):
  * member ways of one role are stitched into closed rings by node identity;
    a way may be used in either direction, and the used direction is recorded;
  * node identity is never welded by coordinates: two nodes sharing lon/lat
    are still different endpoints, so a chain that only "closes" by equal
    coordinates is an open ring and the relation is invalid;
  * every member way must be used exactly once across the rings of its role;
  * each inner ring must lie strictly inside an outer ring (the smallest
    containing outer wins when outers are nested);
  * the resulting (multi)polygon must be valid: no self-intersection, holes
    inside their shell, holes not overlapping.

Invalid relations raise BuildError; the catalog quarantines them.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from shapely.geometry import Polygon
from shapely.ops import unary_union
from shapely.validation import explain_validity


class BuildError(Exception):
    def __init__(self, reason, detail=""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass
class BuiltRelation:
    rid: int
    relation_version: int
    built_at: int          # catalog sequence at which this entry was built
    geometry: object       # shapely Polygon or MultiPolygon
    rings: list = field(default_factory=list)  # per-ring provenance (see below)

    @property
    def bbox(self):
        b = self.geometry.bounds
        return [b[0], b[1], b[2], b[3]]


# ------------------------------------------------------------ ring assembly

def assemble_chains(parts):
    """Stitch way parts [(way_id, way_version, [node_id, ...])] into closed
    rings by node identity.  Returns [{"nodes": [...], "parts": [...]}] where
    each part records {"way_id", "way_version", "direction", "nodes"} with
    direction +1 (as stored) or -1 (reversed).  Raises BuildError("open-ring").
    """
    remaining = list(parts)
    rings = []
    while remaining:
        first = remaining.pop()
        if not first[2]:
            raise BuildError("empty-way", f"way {first[0]} has no nodes")
        out = _close_chain(first, remaining)
        if out is None:
            raise BuildError("open-ring", f"chain starting at way {first[0]} does not close")
        ring, remaining = out
        rings.append(ring)
    return rings


def _close_chain(first, pool):
    """Depth-first search for one closed chain through `first` and a subset of
    `pool`; returns (ring, rest_of_pool) or None.  Backtracking covers
    ambiguous endpoints; node ids are the only identity compared."""
    wid, wver, nodes = first
    start = [{"way_id": wid, "way_version": wver, "direction": 1, "nodes": list(nodes)}]

    def dfs(chain_nodes, acc, rest):
        if chain_nodes[0] == chain_nodes[-1]:
            return {"nodes": chain_nodes, "parts": acc}, rest
        for i, (wid2, wver2, nodes2) in enumerate(rest):
            n2 = list(nodes2)
            if n2 and n2[0] == chain_nodes[-1]:
                direction, used = 1, n2
            elif n2 and n2[-1] == chain_nodes[-1]:
                direction, used = -1, list(reversed(n2))
            else:
                continue
            rest2 = rest[:i] + rest[i + 1:]
            acc2 = acc + [{"way_id": wid2, "way_version": wver2,
                           "direction": direction, "nodes": used}]
            out = dfs(chain_nodes + used[1:], acc2, rest2)
            if out is not None:
                return out
        return None

    return dfs(list(nodes), start, list(pool))


# ---------------------------------------------------------- relation build

def build_relation(graph, rid, built_at=0):
    """Build one relation's geometry from the current source graph.
    Raises BuildError with a stable reason string when invalid."""
    rel = graph.relations.get(rid)
    if rel is None or rel.deleted:
        raise BuildError("relation-missing", str(rid))
    if not rel.members:
        raise BuildError("no-members", str(rid))

    by_role = {"outer": [], "inner": []}
    for way_id, role in rel.members:
        if role not in by_role:
            raise BuildError("bad-role", f"role {role!r} in relation {rid}")
        way = graph.ways.get(way_id)
        if way is None or way.deleted:
            raise BuildError("way-missing", f"way {way_id} referenced by relation {rid}")
        for nid in way.nodes:
            node = graph.nodes.get(nid)
            if node is None or node.deleted:
                raise BuildError("node-missing", f"node {nid} in way {way_id}")
        by_role[role].append((way_id, way.version, list(way.nodes)))
    if not by_role["outer"]:
        raise BuildError("no-outer-ring", str(rid))

    outer_rings = assemble_chains(by_role["outer"])
    inner_rings = assemble_chains(by_role["inner"]) if by_role["inner"] else []

    def coords(ring):
        return [(graph.nodes[n].lon, graph.nodes[n].lat) for n in ring["nodes"]]

    outers = []
    for ring in outer_rings:
        if len(ring["nodes"]) < 4:
            raise BuildError("ring-too-short", f"outer ring of relation {rid}")
        poly = Polygon(coords(ring))
        if not poly.is_valid:
            raise BuildError("outer-ring-invalid", explain_validity(poly))
        outers.append((ring, poly))

    inners = []
    for ring in inner_rings:
        if len(ring["nodes"]) < 4:
            raise BuildError("ring-too-short", f"inner ring of relation {rid}")
        poly = Polygon(coords(ring))
        if not poly.is_valid:
            raise BuildError("inner-ring-invalid", explain_validity(poly))
        inners.append((ring, poly))

    # hole condition: every inner ring strictly inside an outer ring
    assigned = {i: [] for i in range(len(outers))}
    for ring, hpoly in inners:
        containers = [i for i, (_, opoly) in enumerate(outers) if opoly.contains(hpoly)]
        if not containers:
            raise BuildError("inner-outside-outer",
                             f"inner ring starting at node {ring['nodes'][0]} of relation {rid}")
        containers.sort(key=lambda i: outers[i][1].area)
        assigned[containers[0]].append((ring, hpoly))

    polys = []
    for i, (oring, opoly) in enumerate(outers):
        holes = [coords(r) for r, _ in assigned[i]]
        poly = Polygon(coords(oring), holes)
        if not poly.is_valid:
            raise BuildError("invalid-geometry", explain_validity(poly))
        polys.append(poly)
    geom = unary_union(polys) if len(polys) > 1 else polys[0]
    if geom.is_empty or not geom.is_valid:
        raise BuildError("invalid-geometry", explain_validity(geom))

    rings_prov = []
    for role, ring_list in (("outer", outer_rings), ("inner", inner_rings)):
        for ring in ring_list:
            rings_prov.append({
                "role": role,
                "nodes": list(ring["nodes"]),
                "node_versions": [graph.nodes[n].version for n in ring["nodes"]],
                "parts": ring["parts"],
            })
    return BuiltRelation(rid, rel.version, built_at, geom, rings_prov)
