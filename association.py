"""Association: legacy exact-match pairs (kept for baseline_demo.py) plus the
new bag-semantics pair engine with tolerance and windowed summaries.

Fixed association rule (new engine)
-----------------------------------
A-source and B-source events with the same ``key`` form all bag pairs with
``|taiA - taiB| <= tolerance_ns``.  Pair identity is ``(A_id, B_id)`` — two
events at the same instant with different ids are *not* deduplicated.
Window summaries bucket pairs by the A endpoint's TAI into half-open windows
``[origin + k*width, origin + (k+1)*width)`` and report the pair count plus
the A/B source-id sets.  ``tolerance_ns <= window_width_ns`` is enforced at
ingest.
"""
from bisect import bisect_left, bisect_right

# ---------------------------------------------------------------------------
# Legacy API (question baseline; unchanged semantics).
# ---------------------------------------------------------------------------

def pairs(events):
    return sorted([[a['id'], b['id']] for a in events for b in events
                   if a['source'] == 'A' and b['source'] == 'B' and a['key'] == b['key']
                   and abs(a['tai'] - b['tai']) <= a.get('tolerance', 0)])


# ---------------------------------------------------------------------------
# New engine
# ---------------------------------------------------------------------------

def window_index_of(tai_ns, origin_ns, width_ns):
    """Half-open window index on the TAI axis (floor division)."""
    return (tai_ns - origin_ns) // width_ns


def make_pair_record(a_rec, b_rec, config):
    return {
        "a": a_rec["id"], "b": b_rec["id"], "key": a_rec["key"],
        "tai_a": a_rec["tai_ns"], "tai_b": b_rec["tai_ns"],
        "window": window_index_of(a_rec["tai_ns"], config["window_origin_tai_ns"],
                                  config["window_width_ns"]),
    }


def build_match_index(canonical):
    """Pairing index: key -> {'A': [(tai_ns, id), ...sorted], 'B': [...]}
    over supported events that carry source+key."""
    idx = {}
    for rec in canonical:
        if rec["status"] != "supported" or rec.get("source") is None or rec.get("key") is None:
            continue
        per = idx.setdefault(rec["key"], {"A": [], "B": []})
        per[rec["source"]].append((rec["tai_ns"], rec["id"]))
    for per in idx.values():
        per["A"].sort()
        per["B"].sort()
    return idx


def _range(sorted_list, center, tol):
    lo = bisect_left(sorted_list, (center - tol,))
    hi = bisect_right(sorted_list, (center + tol, chr(0x10FFFF)))
    return sorted_list[lo:hi]


def _pair_view(rec):
    return {"tai_a": rec["tai_a"], "tai_b": rec["tai_b"], "window": rec["window"]}


def scoped_pair_delta(old_index, new_index, old_canon, new_canon, changed_ids, config):
    """Local pair recompute around changed events only.

    A pair can only appear, vanish or move if at least one endpoint's
    canonical time changed, so the recompute scope is the tolerance
    neighbourhood of every changed event at its old *and* new position.
    Unchanged events may be read as neighbours; candidate reads and actual
    result changes are metered separately.
    Returns (inserts, retracts, updates, stats)."""
    tol = config["tolerance_ns"]
    stats = {"pair_candidates_scanned": 0, "unchanged_neighbors_read": 0}
    scopes = []
    for canon, index in ((old_canon, old_index), (new_canon, new_index)):
        scope = {}
        for eid in changed_ids:
            rec = canon.get(eid)
            if (rec is None or rec["status"] != "supported"
                    or rec.get("source") is None or rec.get("key") is None):
                continue
            opp = "B" if rec["source"] == "A" else "A"
            partners = _range(index.get(rec["key"], {}).get(opp, []), rec["tai_ns"], tol)
            stats["pair_candidates_scanned"] += len(partners)
            for _tai, oid in partners:
                if oid not in changed_ids:
                    stats["unchanged_neighbors_read"] += 1
                a, b = (eid, oid) if rec["source"] == "A" else (oid, eid)
                scope[(a, b)] = make_pair_record(canon[a], canon[b], config)
        scopes.append(scope)
    old_scope, new_scope = scopes

    retracts = [old_scope[k] for k in sorted(old_scope.keys() - new_scope.keys())]
    inserts = [new_scope[k] for k in sorted(new_scope.keys() - old_scope.keys())]
    updates = []
    for k in sorted(old_scope.keys() & new_scope.keys()):
        old_v, new_v = _pair_view(old_scope[k]), _pair_view(new_scope[k])
        if old_v != new_v:
            updates.append({"a": k[0], "b": k[1], "old": old_v, "new": new_v})
    return inserts, retracts, updates, stats


def summarize_window(members):
    return {"pair_count": len(members),
            "a_ids": sorted({m["a"] for m in members}),
            "b_ids": sorted({m["b"] for m in members})}


def window_deltas(inserts, retracts, updates, new_pairs, old_windows,
                  origin_ns, width_ns):
    """Identity-aware window deltas.

    ``new_pairs`` is the fully applied pair map of the new version;
    ``old_windows`` the previous {index: summary}.  A pair that keeps its
    identity but crosses a window boundary appears as an explicit out+in of
    the same (a, b) — net count deltas never hide membership changes."""
    touched = {}

    def slot(w):
        return touched.setdefault(w, {"in": [], "out": []})

    for p in retracts:
        slot(p["window"])["out"].append((p["a"], p["b"]))
    for p in inserts:
        slot(p["window"])["in"].append((p["a"], p["b"]))
    for u in updates:
        if u["old"]["window"] != u["new"]["window"]:
            slot(u["old"]["window"])["out"].append((u["a"], u["b"]))
            slot(u["new"]["window"])["in"].append((u["a"], u["b"]))

    by_window = {}
    for rec in new_pairs.values():
        by_window.setdefault(rec["window"], []).append(rec)

    deltas = []
    for w in sorted(touched):
        members = by_window.get(w, [])
        after = summarize_window(members) if members else None
        before_count = old_windows.get(w, {}).get("pair_count", 0)
        deltas.append({
            "index": w,
            "pairs_in": sorted(touched[w]["in"]),
            "pairs_out": sorted(touched[w]["out"]),
            "count_delta": (after["pair_count"] if after else 0) - before_count,
            "after": after,
        })
    return deltas


def oracle_pairs(canonical, config):
    """Independent full recompute from canonical events (the oracle).
    Returns (pair_list, window_list) in exact snapshot format."""
    index = build_match_index(canonical)
    by_id = {rec["id"]: rec for rec in canonical}
    found = {}
    for key, per in index.items():
        for tai_a, aid in per["A"]:
            for _tai_b, bid in _range(per["B"], tai_a, config["tolerance_ns"]):
                rec = make_pair_record(by_id[aid], by_id[bid], config)
                found[(rec["a"], rec["b"])] = rec
    pair_list = [found[k] for k in sorted(found)]
    by_window = {}
    for rec in pair_list:
        by_window.setdefault(rec["window"], []).append(rec)
    origin, width = config["window_origin_tai_ns"], config["window_width_ns"]
    windows = [{"index": w, "start_ns": origin + w * width,
                "end_ns": origin + (w + 1) * width,
                **summarize_window(by_window[w])}
               for w in sorted(by_window)]
    return pair_list, windows
