"""Annotation input parsing.

`read` is the original compact GFF3 loader kept for the baseline demo.
`parse_gff3` implements legacy/semantics.md fully (identity conflicts,
anonymous features, forward parents, sequence-region coverage, FASTA cut).
`load_graph_json` normalizes the inputs/graph.json canonical representation.
Both produce the same normalized entity shape used by the store.
"""
from urllib.parse import unquote

ENTITY_FIELDS = ("id", "kind", "parents", "segments")
MAX_GFF_LINES = 500
MAX_GFF_ENTITIES = 100


def read(path):
    rows = {}
    for line in open(path):
        if line.startswith('#') or not line.strip():
            continue
        f = line.rstrip().split('\t')
        attrs = dict(x.split('=', 1) for x in f[8].split(';') if '=' in x)
        key = unquote(attrs.get('ID', 'line-' + str(len(rows))))
        r = rows.setdefault(key, {'id': key, 'kind': f[2],
                                  'parents': [unquote(p) for p in
                                              attrs.get('Parent', '').split(',')
                                              if p],
                                  'segments': []})
        r['segments'].append([int(f[3]) - 1, int(f[4])])
    return rows


def _parse_attrs(raw):
    """Split by ';' first, unquote each piece; Parent split after unquote."""
    attrs, dup_keys = {}, set()
    for piece in raw.split(';'):
        if not piece:
            continue
        if '=' not in piece:
            continue
        k, v = piece.split('=', 1)
        k = unquote(k)
        if k in attrs:
            dup_keys.add(k)
        if k == 'Parent':
            attrs.setdefault(k, [])
            seen = set(attrs[k])
            for p in v.split(','):
                pu = unquote(p)
                if pu and pu not in seen:
                    attrs[k].append(pu)
                    seen.add(pu)
        else:
            attrs[k] = unquote(v)
    return attrs, dup_keys


def _identity_key(ln):
    return (ln["seqid"], ln["kind"], ln["strand"], tuple(ln["parents"]))


def parse_gff3(path):
    """Parse a GFF3 file per legacy/semantics.md.

    Returns dict(entities, lines, conflict_ids, anonymous_ids, issues,
    regions, fasta_ignored). Entity quarantine (missing parents, cycles)
    is decided later by the graph closure; parser-level problems
    (identity conflict, bad phase/coordinates, region overflow) attach
    explicit reasons via `extra_reasons`.
    """
    lines, issues = [], []
    regions = {}
    fasta_ignored = False
    n_features = 0
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if fasta_ignored:
                continue
            if line.startswith("##FASTA"):
                fasta_ignored = True
                continue
            if not line.strip():
                continue
            if line.startswith("##sequence-region"):
                parts = line.split()
                if len(parts) >= 4:
                    seqid, lo1, hi1 = parts[1], int(parts[2]), int(parts[3])
                    if seqid in regions and regions[seqid] != (lo1, hi1):
                        issues.append("conflicting sequence-region %s" % seqid)
                    else:
                        regions[seqid] = (lo1, hi1)
                continue
            if line.startswith("#"):
                continue
            n_features += 1
            if n_features > MAX_GFF_LINES:
                raise ValueError("gff3 exceeds %d feature lines" % MAX_GFF_LINES)
            f = line.split("\t")
            if len(f) != 9:
                raise ValueError("gff3 line %d has %d columns" %
                                 (n_features, len(f)))
            seqid, _src, kind, s1, e1, score, strand, phase, attr_raw = f
            try:
                lo, hi = int(s1) - 1, int(e1)
            except ValueError as exc:
                raise ValueError("line %d: %s" % (n_features, exc))
            if not (0 <= lo < hi):
                raise ValueError("line %d: bad 0-based half-open [%d,%d)"
                                 % (n_features, lo, hi))
            if strand not in ("+", "-", "."):
                raise ValueError("line %d: bad strand %r" % (n_features, strand))
            if kind == "CDS":
                if phase not in ("0", "1", "2"):
                    raise ValueError("line %d: CDS phase must be 0/1/2"
                                     % n_features)
            elif phase != ".":
                raise ValueError("line %d: non-CDS phase must be '.'"
                                 % n_features)
            attrs, dup = _parse_attrs(attr_raw)
            if "ID" in dup or "Parent" in dup:
                raise ValueError("line %d: repeated ID/Parent attribute"
                                 % n_features)
            raw_id = attrs.get("ID")
            is_anon = raw_id is None
            eid = unquote(raw_id) if raw_id else "anon@%s:%d" % (path, n_features)
            parents = list(attrs.get("Parent", ()))
            rec = {"line_no": n_features, "seqid": seqid, "kind": kind,
                   "strand": strand, "phase": phase, "score": score,
                   "raw": line, "id": eid, "parents": parents,
                   "lo": lo, "hi": hi, "anonymous": is_anon,
                   "attrs": {k: v for k, v in attrs.items() if k != "Parent"}}
            lines.append(rec)
    if len(lines) > MAX_GFF_LINES:
        raise ValueError("gff3 exceeds %d feature lines" % MAX_GFF_LINES)

    entities, identity, conflict_ids = {}, {}, set()
    anonymous_ids = []
    extra_reasons = {}
    for ln in lines:
        eid = ln["id"]
        if ln["anonymous"]:
            anonymous_ids.append(eid)
        key = _identity_key(ln)
        if eid in identity and identity[eid] != key:
            # All lines carrying this ID lose, none is picked.
            conflict_ids.add(eid)
            extra_reasons.setdefault(eid, []).append("identity_conflict")
        else:
            identity.setdefault(eid, key)
        ent = entities.setdefault(eid, {
            "id": eid, "kind": ln["kind"], "parents": list(ln["parents"]),
            "segments": [], "seqid": ln["seqid"], "strand": ln["strand"],
            "line_nos": [], "gff3": True})
        # parent lists must be identical when merging; conflicts already
        # captured by the identity key, but keep a de-duplicated union.
        for p in ln["parents"]:
            if p not in ent["parents"]:
                ent["parents"].append(p)
        ent["segments"].append([ln["lo"], ln["hi"]])
        ent["line_nos"].append(ln["line_no"])
        reg = regions.get(ln["seqid"])
        if reg is not None and not (reg[0] - 1 <= ln["lo"] and ln["hi"] <= reg[1]):
            extra_reasons.setdefault(eid, []).append(
                "outside_sequence_region:%s" % ln["seqid"])

    # Parents pointing at anonymous identities cannot resolve.
    anon = set(anonymous_ids)
    for ln in lines:
        for p in ln["parents"]:
            if p in anon:
                extra_reasons.setdefault(ln["id"], []).append(
                    "anonymous_parent:%s" % p)

    if len(entities) > MAX_GFF_ENTITIES:
        raise ValueError("gff3 exceeds %d entities" % MAX_GFF_ENTITIES)
    if fasta_ignored:
        issues.append("sequence_payload_ignored")
    return {"entities": entities, "lines": lines,
            "conflict_ids": sorted(conflict_ids),
            "anonymous_ids": anonymous_ids, "issues": issues,
            "regions": regions, "fasta_ignored": fasta_ignored,
            "extra_reasons": extra_reasons}


def normalize_entity(e):
    """Validate and canonicalize one JSON entity body."""
    if not isinstance(e, dict) or not isinstance(e.get("id"), str) or not e["id"]:
        raise ValueError("entity needs non-empty string id")
    if not isinstance(e.get("kind"), str) or not e["kind"]:
        raise ValueError("entity %s needs kind" % e["id"])
    parents = e.get("parents", [])
    if not isinstance(parents, list) or not all(isinstance(p, str) and p
                                                for p in parents):
        raise ValueError("entity %s: parents must be a list of strings"
                         % e["id"])
    segs = e.get("segments", [])
    norm_segs = []
    if not isinstance(segs, list):
        raise ValueError("entity %s: segments must be a list" % e["id"])
    for s in segs:
        if (not isinstance(s, (list, tuple)) or len(s) != 2 or
                not all(isinstance(v, int) for v in s)):
            raise ValueError("entity %s: segment must be [lo,hi] ints"
                             % e["id"])
        lo, hi = s
        if not (0 <= lo < hi):
            raise ValueError("entity %s: bad segment [%d,%d)"
                             % (e["id"], lo, hi))
        norm_segs.append([lo, hi])
    out = {"id": e["id"], "kind": e["kind"],
           "parents": list(dict.fromkeys(parents)), "segments": norm_segs}
    for k, v in e.items():
        if k not in ENTITY_FIELDS:
            out[k] = v
    return out


def load_graph_json(path):
    """Load the canonical JSON graph (mapping id -> entity)."""
    import json
    data = json.load(open(path, encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("graph json must be an object id -> entity")
    entities = {}
    for k, e in data.items():
        ne = normalize_entity(e)
        if ne["id"] != k:
            raise ValueError("graph json key %r != entity id %r"
                             % (k, ne["id"]))
        entities[k] = ne
    return entities
