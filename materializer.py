"""Materializer: legacy Store (kept for baseline_demo.py) plus the versioned
Catalog — an event data processing catalog with ingest / install-rules /
query / explain, atomic rule installation and reviewable deltas.

Storage layout under the db directory::

    catalog.json        commit pointer: config, versions, current   (commit point)
    events/raw.json     ingested raw events (original representation) + rejections
    versions/vN.json    self-contained snapshot: rules used, canonical events,
                        materialized pairs, window summaries
    changes/delta_N.json  event net changes, pair insert/retract/update,
                          identity-aware window deltas, audit/metering stats

install-rules writes the new version and delta to temporary files, renames
them into place, and only then atomically replaces catalog.json.  A failed
install therefore never exposes half-new canonical times with half-old pairs.
"""
import json
import os
from pathlib import Path

from time_rules import (RuleTable, RuleError, parse_utc_text, parse_tai_text,
                        parse_gps_text, check_sealed_consistency)
from association import (build_match_index, scoped_pair_delta, window_deltas,
                         oracle_pairs)

# ---------------------------------------------------------------------------
# Legacy API (question baseline; unchanged semantics).
# ---------------------------------------------------------------------------
from time_rules import normalize as _legacy_normalize
from association import pairs as _legacy_pairs


class Store:
    def __init__(self, events, rules):
        self.raw = events
        self.rules = rules
        self.events = [dict(e, tai=_legacy_normalize(e, rules)) for e in events]
        self.pairs = _legacy_pairs(self.events)

    def update_rules(self, rules):
        self.rules = rules


# ---------------------------------------------------------------------------
# New catalog
# ---------------------------------------------------------------------------

MAX_EVENTS = 100_000
MAX_RULE_VERSIONS = 3
_EVENT_FIELDS = {"id", "source", "key", "scale", "value"}
_SCALES = {"UTC", "TAI", "GPS"}


class CatalogError(Exception):
    """Raised for ingest/install failures; the catalog is left untouched."""


def validate_config(cfg):
    if not isinstance(cfg, dict):
        raise CatalogError("config must be a JSON object")
    try:
        tol = cfg["tolerance_ns"]
        origin = cfg["window_origin_tai_ns"]
        width = cfg["window_width_ns"]
    except KeyError as exc:
        raise CatalogError(f"config missing key: {exc}")
    for name, v in (("tolerance_ns", tol), ("window_origin_tai_ns", origin),
                    ("window_width_ns", width)):
        if not isinstance(v, int) or isinstance(v, bool):
            raise CatalogError(f"config {name} must be an integer")
    if tol < 0:
        raise CatalogError("tolerance_ns must be >= 0")
    if width <= 0:
        raise CatalogError("window_width_ns must be > 0")
    if tol > width:
        raise CatalogError("tolerance_ns must be <= window_width_ns")
    return {"tolerance_ns": tol, "window_origin_tai_ns": origin,
            "window_width_ns": width}


def validate_event(ev, seen_ids):
    """-> (clean_event, None) or (None, reason).  Structural/lexical checks
    only; whether a UTC ``:60`` is usable is decided per rule version."""
    if not isinstance(ev, dict):
        return None, "event must be a JSON object"
    unknown = set(ev) - _EVENT_FIELDS
    if unknown:
        return None, f"unknown event fields: {sorted(unknown)}"
    eid = ev.get("id")
    if not isinstance(eid, str) or not eid:
        return None, "event id must be a non-empty string"
    if eid in seen_ids:
        return None, "duplicate event id"
    scale = ev.get("scale")
    if scale not in _SCALES:
        return None, f"scale must be one of {sorted(_SCALES)}"
    value = ev.get("value")
    if not isinstance(value, str):
        return None, "event value must be a string"
    source = ev.get("source")
    if source is not None and source not in ("A", "B"):
        return None, "source must be 'A' or 'B'"
    key = ev.get("key")
    if key is not None and not isinstance(key, str):
        return None, "key must be a string"
    try:
        if scale == "UTC":
            label, nanos, leap = parse_utc_text(value)
            parsed = {"utc_label_sec": label, "utc_nanos": nanos, "utc_leap60": leap}
        elif scale == "TAI":
            parsed = {"fixed_tai_ns": parse_tai_text(value)}
        else:
            parsed = {"fixed_tai_ns": parse_gps_text(value)}
    except RuleError as exc:
        return None, str(exc)
    clean = {"id": eid, "scale": scale, "value": value,
             "source": source, "key": key, **parsed}
    return clean, None


def _canonical_for(clean_event, table):
    """Canonical record for one event under one rule version."""
    base = {"id": clean_event["id"], "source": clean_event["source"],
            "key": clean_event["key"],
            "raw": {"scale": clean_event["scale"], "value": clean_event["value"]}}
    if clean_event["scale"] in ("TAI", "GPS"):
        # Absolute identity: never depends on the UTC rule table.
        return {**base, "status": "supported", "tai_ns": clean_event["fixed_tai_ns"],
                "reason": None}
    tai_ns, reason = table.normalize_utc(clean_event["utc_label_sec"],
                                         clean_event["utc_nanos"],
                                         clean_event["utc_leap60"])
    if tai_ns is None:
        return {**base, "status": "unsupported", "tai_ns": None, "reason": reason}
    return {**base, "status": "supported", "tai_ns": tai_ns, "reason": None}


def _atomic_write_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as fh:
        json.dump(obj, fh, indent=1, sort_keys=False)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


class Catalog:
    def __init__(self, db_dir):
        self.db = Path(db_dir)
        meta_path = self.db / "catalog.json"
        if not meta_path.exists():
            raise CatalogError(f"no catalog at {self.db} (run ingest first)")
        self.meta = json.loads(meta_path.read_text())
        raw = json.loads((self.db / "events" / "raw.json").read_text())
        self.events = raw["events"]
        self.rejected = raw["rejected"]

    # -- ingest -------------------------------------------------------------

    @classmethod
    def ingest(cls, db_dir, events_path, config):
        db = Path(db_dir)
        if db.exists() and any(db.iterdir()):
            raise CatalogError(f"db directory {db} is not empty")
        (db / "events").mkdir(parents=True, exist_ok=True)
        (db / "versions").mkdir(exist_ok=True)
        (db / "changes").mkdir(exist_ok=True)
        cfg = validate_config(config)

        raw_events = json.loads(Path(events_path).read_text())
        if not isinstance(raw_events, list):
            raise CatalogError("events file must contain a JSON array")
        if len(raw_events) > MAX_EVENTS:
            raise CatalogError(
                f"too many events: {len(raw_events)} > {MAX_EVENTS}")

        clean, rejected, seen = [], [], set()
        for ev in raw_events:
            rec, reason = validate_event(ev, seen)
            if reason:
                rejected.append({"id": ev.get("id") if isinstance(ev, dict) else None,
                                 "reason": reason})
            else:
                seen.add(rec["id"])
                clean.append(rec)
        _atomic_write_json(db / "events" / "raw.json",
                           {"events": clean, "rejected": rejected})
        meta = {"config": cfg, "current": 0, "versions": [],
                "event_count": len(clean), "rejected_count": len(rejected)}
        _atomic_write_json(db / "catalog.json", meta)
        return {"ingested": len(clean), "rejected": rejected}

    # -- install-rules --------------------------------------------------------

    def _current_snapshot(self):
        if self.meta["current"] == 0:
            return None
        snap = json.loads((self.db / "versions" /
                           f"v{self.meta['current']}.json").read_text())
        # JSON object keys round-trip as strings; window indices are ints.
        snap["windows_by_index"] = {int(w): s for w, s in
                                    snap.get("windows_by_index", {}).items()}
        return snap

    def install_rules(self, table_json, fault_at=None):
        """Validate, compute and atomically commit a new rule version.
        Any failure raises and leaves the catalog byte-identical."""
        try:
            new_table = RuleTable.from_json(table_json)
        except RuleError as exc:
            raise CatalogError(f"invalid rule table: {exc}")
        installed = [v["rule_id"] for v in self.meta["versions"]]
        if new_table.table_id in installed:
            raise CatalogError(f"rule table id {new_table.table_id!r} already installed")
        if len(installed) >= MAX_RULE_VERSIONS:
            raise CatalogError(f"rule version limit reached ({MAX_RULE_VERSIONS})")

        old_snap = self._current_snapshot()
        if old_snap is not None:
            old_table = RuleTable.from_json(old_snap["rules"])
            try:
                check_sealed_consistency(old_table, new_table)
            except RuleError as exc:
                raise CatalogError(f"sealed history violation: {exc}")
        old_canon = ({r["id"]: r for r in old_snap["events"]} if old_snap else {})

        # --- normalization audit: only UTC events can be affected ---------
        # TAI/GPS events have absolute identity and are never rescanned.
        # Below new_table.sealed_before the old and new tables are identical,
        # so only UTC events labelled at/after sealed_before are rescanned.
        stats = {"utc_events_total": 0, "utc_events_rescanned": 0,
                 "tai_gps_events_not_rescanned": 0, "events_changed": 0}
        new_canon = {}
        event_changes = []
        for ev in self.events:
            if ev["scale"] != "UTC":
                stats["tai_gps_events_not_rescanned"] += 1
                rec = _canonical_for(ev, new_table)
                new_canon[ev["id"]] = rec
                if old_snap is None:
                    event_changes.append({"id": ev["id"], "old": None, "new": _view(rec)})
                continue
            stats["utc_events_total"] += 1
            if old_snap is not None and ev["utc_label_sec"] < new_table.sealed_before:
                # Guaranteed identical by the sealed-history check: carry over.
                new_canon[ev["id"]] = old_canon[ev["id"]]
                continue
            stats["utc_events_rescanned"] += 1
            rec = _canonical_for(ev, new_table)
            new_canon[ev["id"]] = rec
            old = old_canon.get(ev["id"])
            old_view = _view(old) if old else None
            if old_view != _view(rec):
                event_changes.append({"id": ev["id"], "old": old_view, "new": _view(rec)})
        stats["events_changed"] = len(event_changes)
        changed_ids = {c["id"] for c in event_changes}

        self._fault(fault_at, "pre_association")   # test hook: anomaly before
        #                                          # association update

        # --- local pair recompute around changed events --------------------
        cfg = self.meta["config"]
        old_list = list(old_canon.values())
        new_list = [new_canon[ev["id"]] for ev in self.events]
        old_index = build_match_index(old_list)
        new_index = build_match_index(new_list)
        inserts, retracts, updates, pair_stats = scoped_pair_delta(
            old_index, new_index, old_canon, new_canon, changed_ids, cfg)
        stats.update(pair_stats)
        stats["pairs_inserted"] = len(inserts)
        stats["pairs_retracted"] = len(retracts)
        stats["pairs_updated"] = len(updates)

        old_pairs = {(p["a"], p["b"]): p for p in (old_snap["pairs"] if old_snap else [])}
        new_pairs = dict(old_pairs)
        for p in retracts:
            del new_pairs[(p["a"], p["b"])]
        for u in updates:
            rec = dict(new_pairs[(u["a"], u["b"])])
            rec.update(u["new"])
            new_pairs[(u["a"], u["b"])] = rec
        for p in inserts:
            new_pairs[(p["a"], p["b"])] = p
        pair_list = [new_pairs[k] for k in sorted(new_pairs)]

        old_windows = (old_snap["windows_by_index"] if old_snap else {})
        wd = window_deltas(inserts, retracts, updates, new_pairs, old_windows,
                           cfg["window_origin_tai_ns"], cfg["window_width_ns"])
        stats["windows_affected"] = len(wd)
        new_windows = dict(old_windows)
        for d in wd:
            if d["after"] is None:
                new_windows.pop(d["index"], None)
            else:
                new_windows[d["index"]] = d["after"]

        self._fault(fault_at, "pre_commit")

        # --- atomic commit --------------------------------------------------
        version = self.meta["current"] + 1
        snapshot = {
            "version": version,
            "rule_id": new_table.table_id,
            "rules": new_table.source,
            "config": cfg,
            "events": [new_canon[ev["id"]] for ev in self.events],
            "pairs": pair_list,
            "windows": [
                {"index": w, "start_ns": cfg["window_origin_tai_ns"] + w * cfg["window_width_ns"],
                 "end_ns": cfg["window_origin_tai_ns"] + (w + 1) * cfg["window_width_ns"],
                 **new_windows[w]}
                for w in sorted(new_windows)],
            "windows_by_index": new_windows,
        }
        delta = {
            "from_version": version - 1,
            "to_version": version,
            "rule_id": new_table.table_id,
            "event_changes": event_changes,
            "pair_inserts": inserts,
            "pair_retracts": retracts,
            "pair_updates": updates,
            "window_deltas": wd,
            "stats": stats,
        }
        _atomic_write_json(self.db / "versions" / f"v{version}.json", snapshot)
        _atomic_write_json(self.db / "changes" / f"delta_{version}.json", delta)
        self.meta["versions"].append({"version": version, "rule_id": new_table.table_id})
        self.meta["current"] = version
        _atomic_write_json(self.db / "catalog.json", self.meta)  # commit point
        return delta

    @staticmethod
    def _fault(fault_at, phase):
        wanted = fault_at or os.environ.get("CATALOG_FAULT_AT")
        if wanted == phase:
            raise CatalogError(f"injected fault at {phase}; catalog left untouched")

    # -- query ---------------------------------------------------------------

    def query(self, version=None):
        current = self.meta["current"]
        if current == 0:
            raise CatalogError("no rule version installed yet")
        if version is None or version == "latest":
            version = current
        elif version == "previous":
            version = current - 1
        version = int(version)
        if version < 1 or version > current:
            raise CatalogError(
                f"version {version} not available (have 1..{current}; "
                f"last two versions always queryable)")
        snap = json.loads((self.db / "versions" / f"v{version}.json").read_text())
        snap = dict(snap)
        snap.pop("windows_by_index", None)
        return snap

    # -- explain --------------------------------------------------------------

    def _deltas(self):
        out = []
        for v in self.meta["versions"]:
            out.append(json.loads((self.db / "changes" /
                                   f"delta_{v['version']}.json").read_text()))
        return out

    def explain_pair(self, a, b):
        timeline, transitions = [], []
        for v in self.meta["versions"]:
            snap = self.query(v["version"])
            rec = next((p for p in snap["pairs"] if p["a"] == a and p["b"] == b), None)
            timeline.append({
                "version": v["version"], "rule_id": snap["rule_id"],
                "present": rec is not None,
                **({"window": rec["window"], "tai_a": rec["tai_a"], "tai_b": rec["tai_b"]}
                   if rec else {}),
            })
        for d in self._deltas():
            key = [a, b]
            change, detail = "unchanged", None
            if any([p["a"], p["b"]] == key for p in d["pair_inserts"]):
                change, detail = "inserted", "pair entered tolerance neighbourhood"
            elif any([p["a"], p["b"]] == key for p in d["pair_retracts"]):
                change, detail = "retracted", "pair left tolerance neighbourhood"
            elif any([u["a"], u["b"]] == key for u in d["pair_updates"]):
                u = next(u for u in d["pair_updates"] if [u["a"], u["b"]] == key)
                change, detail = "updated", f"identity kept; moved {u['old']} -> {u['new']}"
            endpoints = [c for c in d["event_changes"] if c["id"] in (a, b)]
            transitions.append({"from": d["from_version"], "to": d["to_version"],
                                "rule_id": d["rule_id"], "change": change,
                                "detail": detail, "endpoint_changes": endpoints})
        return {"pair": [a, b], "timeline": timeline, "transitions": transitions}

    def explain_event(self, event_id):
        raw = next((e for e in self.events if e["id"] == event_id), None)
        if raw is None:
            raise CatalogError(f"unknown event id {event_id!r}")
        timeline = []
        for v in self.meta["versions"]:
            snap = self.query(v["version"])
            rec = next(e for e in snap["events"] if e["id"] == event_id)
            timeline.append({"version": v["version"], "rule_id": snap["rule_id"],
                             "status": rec["status"], "tai_ns": rec["tai_ns"],
                             "reason": rec["reason"]})
        transitions = []
        for d in self._deltas():
            c = next((c for c in d["event_changes"] if c["id"] == event_id), None)
            transitions.append({"from": d["from_version"], "to": d["to_version"],
                                "rule_id": d["rule_id"],
                                "changed": c is not None, "change": c})
        return {"event": {"id": event_id, "scale": raw["scale"], "value": raw["value"],
                          "source": raw["source"], "key": raw["key"]},
                "note": "TAI/GPS identity is absolute; only UTC events are "
                        "re-evaluated on rule install",
                "timeline": timeline, "transitions": transitions}


def _view(canon_rec):
    return {"status": canon_rec["status"], "tai_ns": canon_rec["tai_ns"],
            "reason": canon_rec["reason"]}


def oracle_snapshot(events, table_json, config):
    """Independent full recompute of a whole version from raw events — the
    oracle used by verification.  Shares no state with incremental updates."""
    table = RuleTable.from_json(table_json)
    canon = [_canonical_for(ev, table) for ev in events]
    pair_list, windows = oracle_pairs(canon, config)
    return {"events": canon, "pairs": pair_list, "windows": windows}
