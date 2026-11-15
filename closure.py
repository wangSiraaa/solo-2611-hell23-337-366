"""Pure graph closure logic.

Entities form a directed graph edge child -> parent. Parents may be
forward-declared (entity appears later), multi-parent is normal, and an
entity whose parent id does not exist at all is "missing parent".

Status rules (no biology, only relational integrity):
  * direct quarantine reasons: missing parent, membership in a cycle,
    or externally supplied identity/coordinate reasons;
  * quarantine propagates along parent -> child edges until a fixed point;
  * every parent edge is retained: a quarantined parent never makes the
    edge disappear, it makes the child quarantined as well.

No I/O in this module so it can be reused both by the store and by the
independent verification oracle.
"""

from collections import deque


def find_cyclic(parents):
    """Return the set of node ids that belong to a cycle.

    Iterative Tarjan SCC (safe for the 10k-node limit). Edges to unknown
    parent ids are ignored here; they are handled as missing-parent errors
    by the closure functions.
    """
    known = set(parents)
    adj = {n: [p for p in parents.get(n, ()) if p in known] for n in parents}
    indices, low = {}, {}
    stack, on_stack = [], set()
    cyclic = set()
    counter = 0
    for root in list(parents):
        if root in indices:
            continue
        work = [(root, 0)]
        while work:
            v, pi = work[-1]
            if pi == 0:
                indices[v] = low[v] = counter
                counter += 1
                stack.append(v)
                on_stack.add(v)
            neigh = adj[v]
            if pi < len(neigh):
                w = neigh[pi]
                work[-1] = (v, pi + 1)
                if w not in indices:
                    work.append((w, 0))
                elif w in on_stack:
                    low[v] = min(low[v], indices[w])
            else:
                if low[v] == indices[v]:
                    comp = []
                    while True:
                        w = stack.pop()
                        on_stack.discard(w)
                        comp.append(w)
                        if w == v:
                            break
                    if len(comp) > 1 or v in adj[v]:
                        cyclic.update(comp)
                work.pop()
                if work:
                    pv = work[-1][0]
                    low[pv] = min(low[pv], low[v])
    return cyclic


def _children_map(parents):
    children = {}
    for node, ps in parents.items():
        for p in ps:
            children.setdefault(p, set()).add(node)
    return children


def full_closure(entities, extra_reasons=None):
    """Compute status/reason for every entity from scratch.

    Returns (status, reasons) where status maps id -> 'trusted'|'quarantined'
    and reasons maps id -> sorted list of deterministic reason strings.
    This is the authoritative whole-graph algorithm, also used as oracle.
    """
    extra_reasons = extra_reasons or {}
    parents = {i: list(e.get("parents", ())) for i, e in entities.items()}
    cyclic = find_cyclic(parents)
    children = _children_map(parents)
    reasons = {}
    for i, e in entities.items():
        rs = list(extra_reasons.get(i, ()))
        if i in cyclic:
            rs.append("cycle")
        for p in parents[i]:
            if p not in entities:
                rs.append("missing_parent:%s" % p)
        if rs:
            reasons[i] = sorted(set(rs))
    bad = set(reasons)
    dq = deque(bad)
    while dq:
        b = dq.popleft()
        for ch in children.get(b, ()):
            rs = reasons.setdefault(ch, [])
            tag = "bad_parent:%s" % b
            if tag not in rs:
                rs.append(tag)
            if ch not in bad:
                bad.add(ch)
                dq.append(ch)
    status = {i: ("quarantined" if i in bad else "trusted") for i in entities}
    return status, {i: sorted(reasons.get(i, ())) for i in entities}


def affected_set(targets, old_parents, new_parents):
    """Targets plus every entity reachable as a child under either graph.

    Union of old and new parent->child edges is required: deleting an edge
    must still re-evaluate the old children, and adding one must reach the
    new ones.
    """
    children = {}
    for node in set(old_parents) | set(new_parents):
        for p in old_parents.get(node, ()):
            children.setdefault(p, set()).add(node)
        for p in new_parents.get(node, ()):
            children.setdefault(p, set()).add(node)
    affected = set(targets)
    dq = deque(t for t in targets if t in children or True)
    while dq:
        x = dq.popleft()
        for ch in children.get(x, ()):
            if ch not in affected:
                affected.add(ch)
                dq.append(ch)
    return affected


def incremental_closure(affected, new_entities, new_extra, old_status):
    """Recompute status only for the affected subgraph.

    Nodes outside `affected` cannot change status: no changed edge is
    reachable from them by construction. Cycle detection runs on the
    induced subgraph only; any cycle touched by a changed edge lies wholly
    inside the affected set.
    """
    new_parents = {
        i: list(e.get("parents", ())) for i, e in new_entities.items()
    }
    sub = {
        i: [p for p in new_parents.get(i, ()) if p in affected]
        for i in affected
        if i in new_entities
    }
    cyclic = find_cyclic(sub)
    children = {}
    for i in affected:
        if i not in new_entities:
            continue
        for p in new_parents[i]:
            children.setdefault(p, set()).add(i)
    reasons = {}
    for i in affected:
        if i not in new_entities:
            continue
        rs = list(new_extra.get(i, ()))
        if i in cyclic:
            rs.append("cycle")
        for p in new_parents[i]:
            if p not in new_entities or old_status.get(p) == "__missing__":
                rs.append("missing_parent:%s" % p)
            elif p not in affected and old_status.get(p) == "quarantined":
                rs.append("bad_parent:%s" % p)
        if rs:
            reasons[i] = sorted(set(rs))
    bad = set(reasons)
    dq = deque(bad)
    while dq:
        b = dq.popleft()
        for ch in children.get(b, ()):
            rs = reasons.setdefault(ch, [])
            tag = "bad_parent:%s" % b
            if tag not in rs:
                rs.append(tag)
            if ch not in bad:
                bad.add(ch)
                dq.append(ch)
    out = {}
    for i in affected:
        if i in new_entities:
            out[i] = (
                "quarantined" if i in reasons else "trusted",
                sorted(reasons.get(i, ())),
            )
    return out
