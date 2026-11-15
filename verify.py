#!/usr/bin/env python3
"""Verification suite for the event data processing catalog.

Every check runs against real files on disk: raw events are re-read and an
independent brute-force oracle is rebuilt for each rule version; materialized
snapshots are compared pair-by-pair and window-by-window; change-log deltas
are replayed by an independent mini consumer that must reproduce ``query``
output exactly.

Run from the repository root:  python3 verify.py
Exit code 0 iff all checks pass.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from materializer import Catalog, CatalogError          # noqa: E402
from time_rules import (table_from_legacy, parse_utc_text, parse_tai_text,  # noqa: E402
                        parse_gps_text)

NS = 1_000_000_000
ORIGIN = 1_483_228_800 * NS          # 2017-01-01T00:00:00Z posix label, on the TAI axis
WIDTH = 10 * NS
CONFIG = {"tolerance_ns": 0, "window_origin_tai_ns": ORIGIN, "window_width_ns": WIDTH}

RESULTS = []


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def test(fn):
    RESULTS.append(fn)
    return fn


def run_cli(*args, env_extra=None, expect=0):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run([sys.executable, str(ROOT / "catalog.py"), *args],
                          capture_output=True, text=True, cwd=ROOT, env=env)
    if expect == 0 and proc.returncode != 0:
        raise AssertionError(f"CLI {args} failed unexpectedly: {proc.stderr}")
    if expect != 0 and proc.returncode == 0:
        raise AssertionError(f"CLI {args} unexpectedly succeeded: {proc.stdout}")
    return proc


def write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj))
    return path


def fresh_catalog(tmp, events, config=CONFIG):
    db = Path(tmp) / "db"
    ep = write_json(Path(tmp) / "events.json", events)
    res = Catalog.ingest(db, ep, config)
    return db, res


def install(db, table, expect_ok=True):
    try:
        return Catalog(db).install_rules(table)
    except CatalogError as exc:
        if expect_ok:
            raise AssertionError(f"install of {table.get('id')!r} rejected: {exc}")
        return str(exc)


# ---------------------------------------------------------------------------
# Independent oracle: re-reads raw event files, brute-force pair matching,
# no state shared with the incremental engine.
# ---------------------------------------------------------------------------

def oracle_tai(ev, table):
    scale, value = ev["scale"], ev["value"]
    if scale == "TAI":
        return parse_tai_text(value), None
    if scale == "GPS":
        return parse_gps_text(value), None
    label, nanos, leap = parse_utc_text(value)
    c0 = parse_utc_text(table["coverage"][0])[0]
    c1 = parse_utc_text(table["coverage"][1])[0]

    def offset_at(sec):
        off = table["initial_offset"]
        for ch in table["changes"]:
            if sec >= parse_utc_text(ch["effective"])[0]:
                off = ch["offset"]
        return off

    if leap:
        effs = [parse_utc_text(ch["effective"])[0] for ch in table["changes"]]
        if label not in effs:
            return None, "leap_second_not_in_table"
        if not (c0 <= label - 1 < c1):
            return None, "outside_coverage"
        off = offset_at(label) - 1
    else:
        if not (c0 <= label < c1):
            return None, "outside_coverage"
        off = offset_at(label)
    return (label + off) * NS + nanos, None


def oracle_full(events, table, config):
    canon = {}
    for ev in events:
        tai, reason = oracle_tai(ev, table)
        canon[ev["id"]] = {"status": "supported" if tai is not None else "unsupported",
                           "tai_ns": tai, "reason": reason}
    pairs = []
    for a in events:
        if a.get("source") != "A" or canon[a["id"]]["tai_ns"] is None or a.get("key") is None:
            continue
        for b in events:
            if b.get("source") != "B" or b.get("key") != a.get("key"):
                continue
            tb = canon[b["id"]]["tai_ns"]
            if tb is None:
                continue
            ta = canon[a["id"]]["tai_ns"]
            if abs(ta - tb) <= config["tolerance_ns"]:
                pairs.append({"a": a["id"], "b": b["id"], "key": a["key"],
                              "tai_a": ta, "tai_b": tb,
                              "window": (ta - config["window_origin_tai_ns"])
                                        // config["window_width_ns"]})
    pairs.sort(key=lambda p: (p["a"], p["b"]))
    by_w = {}
    for p in pairs:
        by_w.setdefault(p["window"], []).append(p)
    windows = [{"index": w,
                "start_ns": config["window_origin_tai_ns"] + w * config["window_width_ns"],
                "end_ns": config["window_origin_tai_ns"] + (w + 1) * config["window_width_ns"],
                "pair_count": len(ps),
                "a_ids": sorted({p["a"] for p in ps}),
                "b_ids": sorted({p["b"] for p in ps})}
               for w, ps in sorted(by_w.items())]
    return canon, pairs, windows


def assert_version_matches_oracle(db, events, table, version, config=CONFIG):
    snap = Catalog(db).query(version)
    check(snap["rule_id"] == table["id"], f"v{version}: rule id mismatch")
    got = {e["id"]: e for e in snap["events"]}
    # events rejected at ingest (e.g. too-precise GPS) are not in the catalog
    events = [e for e in events if e["id"] in got]
    canon, pairs, windows = oracle_full(events, table, config)
    check(set(got) == set(canon), f"v{version}: event id set mismatch")
    for eid, want in canon.items():
        g = got[eid]
        check(g["status"] == want["status"] and g["tai_ns"] == want["tai_ns"]
              and g["reason"] == want["reason"],
              f"v{version} event {eid}: got {g['status']}/{g['tai_ns']}/{g['reason']} "
              f"want {want['status']}/{want['tai_ns']}/{want['reason']}")
    check(snap["pairs"] == pairs,
          f"v{version}: pairs differ\n got {snap['pairs']}\nwant {pairs}")
    check(snap["windows"] == windows,
          f"v{version}: windows differ\n got {snap['windows']}\nwant {windows}")
    return snap


class ReplayConsumer:
    """Independent mini consumer: applies change-log deltas to an initially
    empty state; final state must equal `query` output exactly."""

    def __init__(self, config):
        self.config = config
        self.events = {}
        self.pairs = {}

    def apply(self, delta):
        for c in delta["event_changes"]:
            self.events[c["id"]] = c["new"]
        for p in delta["pair_retracts"]:
            key = (p["a"], p["b"])
            check(key in self.pairs, f"retract of unknown pair {key}")
            del self.pairs[key]
        for u in delta["pair_updates"]:
            rec = self.pairs[(u["a"], u["b"])]
            check({k: rec[k] for k in ("tai_a", "tai_b", "window")} == u["old"],
                  f"pair update old view mismatch for {(u['a'], u['b'])}")
            rec.update(u["new"])
        for p in delta["pair_inserts"]:
            key = (p["a"], p["b"])
            check(key not in self.pairs, f"insert of existing pair {key}")
            self.pairs[key] = dict(p)
        # logged window deltas must match the state we can derive ourselves
        derived = self._windows()
        for wd in delta["window_deltas"]:
            after = derived.get(wd["index"])
            got = None if after is None else {k: after[k] for k in
                                              ("pair_count", "a_ids", "b_ids")}
            check(got == wd["after"],
                  f"window {wd['index']}: logged after={wd['after']} != derived {got}")
        return self

    def _windows(self):
        by_w = {}
        for p in self.pairs.values():
            by_w.setdefault(p["window"], []).append(p)
        return {w: {"pair_count": len(ps),
                    "a_ids": sorted({p["a"] for p in ps}),
                    "b_ids": sorted({p["b"] for p in ps})}
                for w, ps in by_w.items()}

    def assert_equals_query(self, db, version):
        snap = Catalog(db).query(version)
        want_events = {e["id"]: {"status": e["status"], "tai_ns": e["tai_ns"],
                                 "reason": e["reason"]} for e in snap["events"]}
        check(self.events == want_events,
              f"replayed events != query v{version}")
        want_pairs = {(p["a"], p["b"]): p for p in snap["pairs"]}
        check(self.pairs == want_pairs, f"replayed pairs != query v{version}")
        want_w = {w["index"]: {k: w[k] for k in ("pair_count", "a_ids", "b_ids")}
                  for w in snap["windows"]}
        check(self._windows() == want_w, f"replayed windows != query v{version}")


def replay_all(db, config=CONFIG):
    consumer = ReplayConsumer(config)
    n = Catalog(db).meta["current"]
    for v in range(1, n + 1):
        delta = json.loads((Path(db) / "changes" / f"delta_{v}.json").read_text())
        consumer.apply(delta)
        consumer.assert_equals_query(db, v)
    return consumer


def legacy_table(table_id="legacy-leaps"):
    leaps = json.loads((ROOT / "legacy" / "leaps.json").read_text())
    return table_from_legacy(leaps, table_id)


def legacy_events():
    return json.loads((ROOT / "legacy" / "events.json").read_text())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@test
def test_leap_second_boundary_ns():
    """Positive leap second: before/during/after nanoseconds."""
    with tempfile.TemporaryDirectory() as tmp:
        events = legacy_events()
        table = legacy_table()
        db, res = fresh_catalog(tmp, events)
        install(db, table)
        snap = Catalog(db).query()
        tai = {e["id"]: e["tai_ns"] for e in snap["events"]}
        check(tai["before"] == 1_483_228_835 * NS, f"before={tai['before']}")
        check(tai["leap"] == 1_483_228_836 * NS, f"leap={tai['leap']}")
        check(tai["after"] == 1_483_228_837 * NS, f"after={tai['after']}")
        check(tai["nano"] == 1_483_228_836 * NS + 1, f"nano={tai['nano']}")
        check(tai["leap"] - tai["before"] == NS and tai["after"] - tai["leap"] == NS,
              "leap second must be exactly 1s between :59 and :00 of the next minute")
        assert_version_matches_oracle(db, events, table, 1)


@test
def test_tai_gps_equivalence_and_absolute_identity():
    """Equivalent TAI/GPS/UTC instants coincide; TAI/GPS identity is absolute."""
    with tempfile.TemporaryDirectory() as tmp:
        events = legacy_events()
        t1 = legacy_table("L1")
        t2 = dict(legacy_table("L2"), coverage=["2015-01-01T00:00:00Z",
                                                "2021-01-01T00:00:00Z"])
        db, _ = fresh_catalog(tmp, events)
        install(db, t1)
        d2 = install(db, t2)
        snap = Catalog(db).query()
        tai = {e["id"]: e["tai_ns"] for e in snap["events"]}
        check(tai["leap"] == tai["tai-leap"] == tai["gps-leap"] == 1_483_228_836 * NS,
              f"leap/tai-leap/gps-leap must coincide: {tai}")
        changed = {c["id"] for c in d2["event_changes"]}
        check("tai-leap" not in changed and "gps-leap" not in changed,
              "TAI/GPS events must never be marked changed by a UTC rule revision")
        check(d2["stats"]["tai_gps_events_not_rescanned"] == 2,
              "audit must not rescan TAI/GPS events")
        v1 = {e["id"]: e["tai_ns"] for e in Catalog(db).query(1)["events"]}
        check(v1["tai-leap"] == tai["tai-leap"] and v1["gps-leap"] == tai["gps-leap"],
              "TAI/GPS tai must be identical across rule versions")
        assert_version_matches_oracle(db, events, t2, 2)


@test
def test_ingest_rejections():
    """Structurally invalid events are rejected with reasons; valid ones kept."""
    with tempfile.TemporaryDirectory() as tmp:
        events = [
            {"id": "ok-tai", "scale": "TAI", "value": "2017-01-01T00:00:36"},
            {"id": "ok-utc", "scale": "UTC", "value": "2017-01-01T00:00:00Z"},
            {"id": "ok-gps", "scale": "GPS", "value": "1167264017.5"},
            {"id": "gps-10dp", "scale": "GPS", "value": "1167264017.0000000001"},
            {"id": "gps-neg", "scale": "GPS", "value": "-5"},
            {"id": "utc-mon13", "scale": "UTC", "value": "2017-13-01T00:00:00Z"},
            {"id": "utc-s61", "scale": "UTC", "value": "2017-01-01T00:00:61Z"},
            {"id": "utc-10dp", "scale": "UTC", "value": "2017-01-01T00:00:00.0000000001Z"},
            {"id": "tai-z", "scale": "TAI", "value": "2017-01-01T00:00:36Z"},
            {"id": "tai-60", "scale": "TAI", "value": "2017-01-01T00:00:60"},
            {"id": "ok-tai", "scale": "TAI", "value": "2017-01-01T00:00:37"},
            {"scale": "TAI", "value": "2017-01-01T00:00:38"},
            {"id": "bad-scale", "scale": "UT1", "value": "2017-01-01T00:00:00Z"},
            {"id": "extra", "scale": "TAI", "value": "2017-01-01T00:00:39", "foo": 1},
            {"id": "bad-src", "scale": "TAI", "value": "2017-01-01T00:00:40",
             "source": "C", "key": "k"},
        ]
        db, res = fresh_catalog(tmp, events)
        check(res["ingested"] == 3, f"ingested={res['ingested']}")
        rejected = {r["id"]: r["reason"] for r in res["rejected"]}
        check(len(rejected) == 12, f"rejected={rejected}")
        check("too precise" in rejected["gps-10dp"], rejected["gps-10dp"])
        check("duplicate" in rejected[None] or "duplicate" in rejected.get("ok-tai", ""),
              "duplicate id must be rejected")
        # legacy file: too-precise GPS rejected, the other 8 events ingested
        db2, res2 = fresh_catalog(Path(tmp) / "x", legacy_events())
        check(res2["ingested"] == 8 and [r["id"] for r in res2["rejected"]] == ["too-precise"],
              f"legacy ingest: {res2['rejected']}")


@test
def test_bad_leap_and_out_of_coverage_unsupported():
    """':60' on a non-leap day and instants outside coverage are unsupported,
    not ingest errors; raw representation is preserved."""
    with tempfile.TemporaryDirectory() as tmp:
        events = legacy_events()
        table = legacy_table()
        db, _ = fresh_catalog(tmp, events)
        install(db, table)
        snap = Catalog(db).query()
        by_id = {e["id"]: e for e in snap["events"]}
        check(by_id["bad-leap"]["status"] == "unsupported"
              and by_id["bad-leap"]["reason"] == "leap_second_not_in_table",
              by_id["bad-leap"])
        check(by_id["future"]["status"] == "unsupported"
              and by_id["future"]["reason"] == "outside_coverage", by_id["future"])
        check(by_id["bad-leap"]["raw"] == {"scale": "UTC", "value": "2016-12-30T23:59:60Z"},
              "raw representation must be preserved")
        exp = Catalog(db).explain_event("future")
        check(exp["event"]["value"] == "2020-01-01T00:00:00Z", exp["event"])


@test
def test_legacy_durations_contract():
    """legacy/durations.json semantics rebuilt from legacy/leaps.json."""
    with tempfile.TemporaryDirectory() as tmp:
        events = legacy_events()
        table = legacy_table()
        db, _ = fresh_catalog(tmp, events)
        install(db, table)
        tai = {e["id"]: e["tai_ns"] for e in Catalog(db).query()["events"]}
        durations = json.loads((ROOT / "legacy" / "durations.json").read_text())
        want = {"cross-leap": 2 * NS, "same": 0, "one-ns": 1, "bad": None}
        for d in durations:
            s, e = tai.get(d["start"]), tai.get(d["end"])
            got = None if s is None or e is None else e - s
            check(got == want[d["id"]], f"duration {d['id']}: got {got} want {want[d['id']]}")


@test
def test_inputs_scenario_association_0_to_1():
    """The baseline gap scenario via CLI: r1 -> r2 flips pair (u,t) 0 -> 1,
    jointly with canonical time and window deltas."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "db"
        cfg = {"tolerance_ns": 0, "window_origin_tai_ns": ORIGIN,
               "window_width_ns": 60 * NS}
        run_cli("ingest", "--db", str(db), "--events", "inputs/events.json",
                "--config", "inputs/config.json")
        run_cli("install-rules", "--db", str(db), "--rules", "inputs/r1.json")
        out = run_cli("install-rules", "--db", str(db), "--rules", "inputs/r2.json")
        delta = json.loads(out.stdout)
        check(delta["event_changes"] == 1 and delta["pair_inserts"] == 1
              and delta["pair_retracts"] == 0, delta)
        check(delta["stats"]["utc_events_rescanned"] == 1
              and delta["stats"]["tai_gps_events_not_rescanned"] == 1,
              "audit must rescan only rule-affected UTC events")
        events = json.loads((ROOT / "inputs" / "events.json").read_text())
        r1 = json.loads((ROOT / "inputs" / "r1.json").read_text())
        r2 = json.loads((ROOT / "inputs" / "r2.json").read_text())
        s1 = assert_version_matches_oracle(db, events, r1, 1, cfg)
        s2 = assert_version_matches_oracle(db, events, r2, 2, cfg)
        check(s1["pairs"] == [] and [(p["a"], p["b"]) for p in s2["pairs"]] == [("u", "t")],
              "association must go 0 -> 1 exactly like a full recompute")
        replay_all(db, cfg)
        prev = json.loads(run_cli("query", "--db", str(db), "--version", "previous").stdout)
        check(prev["version"] == 1 and prev["pairs"] == [], "previous version queryable")


@test
def test_reverse_correction_1_to_0():
    """Tentative coverage may be corrected: removing the leap retracts the pair."""
    with tempfile.TemporaryDirectory() as tmp:
        events = json.loads((ROOT / "inputs" / "events.json").read_text())
        r2 = json.loads((ROOT / "inputs" / "r2.json").read_text())
        r3 = {"id": "r3", "initial_offset": 36, "changes": [],
              "sealed_before": "2016-12-01T00:00:00Z",
              "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        cfg = dict(CONFIG, window_width_ns=60 * NS)
        db, _ = fresh_catalog(tmp, events, cfg)
        install(db, r2)
        d = install(db, r3)
        check(len(d["pair_retracts"]) == 1 and len(d["pair_inserts"]) == 0, d)
        tai = {e["id"]: e["tai_ns"] for e in Catalog(db).query()["events"]}
        check(tai["u"] == 1_483_228_836 * NS, "u must move back; old tai is not eternal")
        assert_version_matches_oracle(db, events, r3, 3 - 1, cfg)
        replay_all(db, cfg)


@test
def test_coverage_extension_releases_event():
    """Extending tentative coverage turns unsupported events usable."""
    with tempfile.TemporaryDirectory() as tmp:
        events = legacy_events()
        t1 = legacy_table("L1")
        t2 = dict(legacy_table("L2"), coverage=["2015-01-01T00:00:00Z",
                                                "2021-01-01T00:00:00Z"])
        db, _ = fresh_catalog(tmp, events)
        install(db, t1)
        d2 = install(db, t2)
        fut = next(c for c in d2["event_changes"] if c["id"] == "future")
        check(fut["old"]["status"] == "unsupported" and fut["new"]["status"] == "supported",
              fut)
        check(fut["new"]["tai_ns"] == (1_577_836_800 + 37) * NS, fut)
        assert_version_matches_oracle(db, events, t2, 2)
        replay_all(db)


@test
def test_illegal_tables_rejected():
    """Illegal rule tables are rejected and leave the catalog untouched."""
    with tempfile.TemporaryDirectory() as tmp:
        events = [{"id": "e", "scale": "TAI", "value": "2017-01-01T00:00:00"}]
        db, _ = fresh_catalog(tmp, events)
        good = {"id": "g1", "initial_offset": 36, "changes": [],
                "sealed_before": "2016-12-01T00:00:00Z",
                "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        install(db, good)

        def bad(**kw):
            t = dict(good, **kw)
            t["id"] = kw.get("id", "x")
            return t

        leap = {"effective": "2017-01-01T00:00:00Z", "offset": 37}
        cases = [
            bad(coverage=None),                                  # missing-ish field
            bad(initial_offset="36"),                            # wrong type
            bad(initial_offset=-1),                              # negative offset
            bad(changes=[{"effective": "2017-01-01T00:00:00Z", "offset": 38}]),  # +2 jump
            bad(changes=[{"effective": "2017-01-01T00:00:00Z", "offset": 35}]),  # -1 step
            bad(changes=[{"effective": "2017-06-01T00:00:00Z", "offset": 37},
                         leap]),                                 # unsorted
            bad(changes=[{"effective": "2017-01-01T00:00:01Z", "offset": 37}]),  # not midnight
            bad(changes=[{"effective": "2016-12-31T23:59:60Z", "offset": 37}]),  # :60 effective
            bad(coverage=["2017-02-01T00:00:00Z", "2016-12-01T00:00:00Z"]),      # inverted
            bad(sealed_before="2016-11-01T00:00:00Z"),           # sealed outside coverage
            bad(id="g1"),                                        # duplicate id
        ]
        cases.append({"id": "x2", "initial_offset": 36, "changes": [],
                      "sealed_before": "2016-12-01T00:00:00Z"})   # missing coverage
        cases.append(dict(good, id="x3", extra=1))                # unknown field
        for i, t in enumerate(cases):
            msg = install(db, t, expect_ok=False)
            check(isinstance(msg, str) and msg, f"case {i} must be rejected with a reason")
        check(Catalog(db).meta["current"] == 1, "failed installs must not advance the version")
        check(len(list((db / "versions").glob("v*.json"))) == 1, "no partial snapshots")


@test
def test_sealed_history_change_rejected():
    """Changes at/after sealed_before are tentative; before it they are sealed."""
    with tempfile.TemporaryDirectory() as tmp:
        events = [{"id": "e", "scale": "UTC", "value": "2017-01-01T00:00:00Z"}]
        db, _ = fresh_catalog(tmp, events)
        base = {"id": "s1", "initial_offset": 36,
                "changes": [{"effective": "2017-01-01T00:00:00Z", "offset": 37}],
                "sealed_before": "2016-12-01T00:00:00Z",
                "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        install(db, base)
        violations = [
            dict(base, id="v1", initial_offset=35),                       # sealed offset
            dict(base, id="v2", changes=[{"effective": "2016-06-01T00:00:00Z", "offset": 37},
                                         {"effective": "2017-01-01T00:00:00Z", "offset": 38}]),
            dict(base, id="v3", sealed_before="2016-10-01T00:00:00Z"),    # sealed regression
            dict(base, id="v4", coverage=["2016-11-15T00:00:00Z", "2017-02-01T00:00:00Z"]),
        ]
        for i, t in enumerate(violations):
            msg = install(db, t, expect_ok=False)
            check(isinstance(msg, str), f"sealed violation {i} must be rejected")
        # ...while a correction of the tentative 2017 leap is accepted
        ok = dict(base, id="v5", changes=[])
        install(db, ok)
        check(Catalog(db).meta["current"] == 2, "tentative correction must install")


@test
def test_a_event_crosses_window_boundary():
    """A moves across a window edge; pair identity kept; window delta explicit."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = {"tolerance_ns": 2 * NS, "window_origin_tai_ns": ORIGIN,
               "window_width_ns": WIDTH}
        events = [
            {"id": "a", "source": "A", "key": "k", "scale": "UTC",
             "value": "2017-01-01T00:00:03Z"},
            {"id": "b", "source": "B", "key": "k", "scale": "TAI",
             "value": "2017-01-01T00:00:39.5"},
        ]
        t1 = {"id": "w1", "initial_offset": 36,
              "changes": [{"effective": "2017-01-01T00:00:00Z", "offset": 37}],
              "sealed_before": "2016-12-01T00:00:00Z",
              "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        t2 = dict(t1, id="w2", changes=[])
        db, _ = fresh_catalog(tmp, events, cfg)
        install(db, t1)
        d = install(db, t2)
        check(len(d["pair_inserts"]) == 0 and len(d["pair_retracts"]) == 0
              and len(d["pair_updates"]) == 1,
              "pair identity must survive a window crossing")
        u = d["pair_updates"][0]
        check(u["old"]["window"] == 4 and u["new"]["window"] == 3, u)
        wd = {w["index"]: w for w in d["window_deltas"]}
        check(wd[4]["pairs_out"] == [("a", "b")] and wd[3]["pairs_in"] == [("a", "b")],
              "window move must be logged as explicit out+in of the same identity")
        assert_version_matches_oracle(db, events, t2, 2, cfg)
        replay_all(db, cfg)


@test
def test_two_events_move_pair_identity_unchanged():
    """A and B move together; pair identity unchanged; window membership moves."""
    with tempfile.TemporaryDirectory() as tmp:
        events = [
            {"id": "a", "source": "A", "key": "k", "scale": "UTC",
             "value": "2017-01-01T00:00:03Z"},
            {"id": "b", "source": "B", "key": "k", "scale": "UTC",
             "value": "2017-01-01T00:00:03Z"},
        ]
        t1 = {"id": "m1", "initial_offset": 36,
              "changes": [{"effective": "2017-01-01T00:00:00Z", "offset": 37}],
              "sealed_before": "2016-12-01T00:00:00Z",
              "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        t2 = dict(t1, id="m2", changes=[])
        db, _ = fresh_catalog(tmp, events)
        install(db, t1)
        d = install(db, t2)
        check(len(d["pair_updates"]) == 1 and not d["pair_inserts"]
              and not d["pair_retracts"], d)
        exp = Catalog(db).explain_pair("a", "b")
        check(all(t["present"] for t in exp["timeline"]), exp["timeline"])
        check(exp["timeline"][0]["window"] == 4 and exp["timeline"][1]["window"] == 3,
              exp["timeline"])
        assert_version_matches_oracle(db, events, t2, 2)
        replay_all(db)


@test
def test_bag_semantics_no_dedup():
    """Different ids at the same instant produce distinct pairs (bag)."""
    with tempfile.TemporaryDirectory() as tmp:
        events = [
            {"id": "a1", "source": "A", "key": "k", "scale": "TAI",
             "value": "2017-01-01T00:00:37"},
            {"id": "a2", "source": "A", "key": "k", "scale": "TAI",
             "value": "2017-01-01T00:00:37"},
            {"id": "b1", "source": "B", "key": "k", "scale": "TAI",
             "value": "2017-01-01T00:00:37"},
        ]
        db, _ = fresh_catalog(tmp, events)
        install(db, {"id": "t1", "initial_offset": 36, "changes": [],
                     "sealed_before": "2016-12-01T00:00:00Z",
                     "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]})
        snap = Catalog(db).query()
        check([(p["a"], p["b"]) for p in snap["pairs"]] == [("a1", "b1"), ("a2", "b1")],
              snap["pairs"])
        w = snap["windows"][0]
        check(w["pair_count"] == 2 and w["a_ids"] == ["a1", "a2"], w)


@test
def test_fault_injection_is_atomic():
    """An anomaly injected before the association update (or before commit)
    exposes neither half-new canonical times nor half-old pairs."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "db"
        run_cli("ingest", "--db", str(db), "--events", "inputs/events.json",
                "--config", "inputs/config.json")
        run_cli("install-rules", "--db", str(db), "--rules", "inputs/r1.json")
        before = run_cli("query", "--db", str(db)).stdout
        for phase in ("pre_association", "pre_commit"):
            proc = run_cli("install-rules", "--db", str(db), "--rules", "inputs/r2.json",
                           env_extra={"CATALOG_FAULT_AT": phase}, expect=2)
            check("injected fault" in proc.stderr, proc.stderr)
            after = run_cli("query", "--db", str(db)).stdout
            check(after == before, f"fault at {phase} leaked partial state")
            check(not (db / "versions" / "v2.json").exists()
                  and not (db / "changes" / "delta_2.json").exists(),
                  f"fault at {phase} left partial files")
        run_cli("install-rules", "--db", str(db), "--rules", "inputs/r2.json")
        check(json.loads(run_cli("query", "--db", str(db)).stdout)["version"] == 2,
              "install must succeed once the fault is cleared")


@test
def test_retract_then_reinsert_is_explainable():
    """Same pair retracted in v2 and re-inserted in v3: fully explainable,
    never netted away."""
    with tempfile.TemporaryDirectory() as tmp:
        events = [
            {"id": "a", "source": "A", "key": "k", "scale": "UTC",
             "value": "2017-01-01T00:00:00Z"},
            {"id": "b", "source": "B", "key": "k", "scale": "TAI",
             "value": "2017-01-01T00:00:37"},
        ]
        mk = lambda rid, changes: {"id": rid, "initial_offset": 36, "changes": changes,
                                   "sealed_before": "2016-12-01T00:00:00Z",
                                   "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        leap = [{"effective": "2017-01-01T00:00:00Z", "offset": 37}]
        db, _ = fresh_catalog(tmp, events)
        install(db, mk("x1", leap))
        d2 = install(db, mk("x2", []))
        d3 = install(db, mk("x3", leap))
        check(len(d2["pair_retracts"]) == 1 and len(d3["pair_inserts"]) == 1,
              "retract then re-insert must both be logged")
        exp = Catalog(db).explain_pair("a", "b")
        check([t["present"] for t in exp["timeline"]] == [True, False, True],
              exp["timeline"])
        changes = [t["change"] for t in exp["transitions"]]
        check(changes == ["inserted", "retracted", "inserted"], changes)
        moved = exp["transitions"][1]["endpoint_changes"]
        check(moved and moved[0]["old"]["tai_ns"] - moved[0]["new"]["tai_ns"] == NS,
              "retraction must be explained by the endpoint's 1s move")
        replay_all(db)


@test
def test_window_delta_not_netted():
    """A window losing one pair and gaining another in the same revision keeps
    both identities explicit even though the net count delta is 0."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = {"tolerance_ns": 500_000_000, "window_origin_tai_ns": ORIGIN,
               "window_width_ns": WIDTH}
        events = [
            {"id": "a1", "source": "A", "key": "k1", "scale": "UTC",
             "value": "2017-01-01T00:00:05Z"},
            {"id": "b1", "source": "B", "key": "k1", "scale": "TAI",
             "value": "2017-01-01T00:00:42"},
            {"id": "a2", "source": "A", "key": "k2", "scale": "UTC",
             "value": "2017-01-01T00:00:04Z"},
            {"id": "b2", "source": "B", "key": "k2", "scale": "TAI",
             "value": "2017-01-01T00:00:40.4"},
        ]
        t1 = {"id": "n1", "initial_offset": 36,
              "changes": [{"effective": "2017-01-01T00:00:00Z", "offset": 37}],
              "sealed_before": "2016-12-01T00:00:00Z",
              "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        t2 = dict(t1, id="n2", changes=[])
        db, _ = fresh_catalog(tmp, events, cfg)
        install(db, t1)
        d = install(db, t2)
        wd = [w for w in d["window_deltas"] if w["index"] == 4]
        check(len(wd) == 1 and wd[0]["count_delta"] == 0, d["window_deltas"])
        check(wd[0]["pairs_out"] == [("a1", "b1")] and wd[0]["pairs_in"] == [("a2", "b2")],
              "net-zero window delta must still expose both identities")
        assert_version_matches_oracle(db, events, t2, 2, cfg)
        replay_all(db, cfg)


@test
def test_query_last_two_versions_and_version_cap():
    """At most 3 rule versions; the last two are always queryable."""
    with tempfile.TemporaryDirectory() as tmp:
        events = [{"id": "e", "scale": "UTC", "value": "2017-01-01T00:00:00Z"}]
        db = Path(tmp) / "db"
        run_cli("ingest", "--db", str(db), "--events",
                str(write_json(Path(tmp) / "e.json", events)), "--config",
                str(write_json(Path(tmp) / "c.json", CONFIG)))
        mk = lambda rid, off: {"id": rid, "initial_offset": 36, "changes":
                               ([{"effective": "2017-01-01T00:00:00Z", "offset": 37}]
                                if off == 37 else []),
                               "sealed_before": "2016-12-01T00:00:00Z",
                               "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        for rid, off in (("q1", 36), ("q2", 37), ("q3", 36)):
            run_cli("install-rules", "--db", str(db), "--rules",
                    str(write_json(Path(tmp) / f"{rid}.json", mk(rid, off))))
        latest = json.loads(run_cli("query", "--db", str(db), "--version", "latest").stdout)
        prev = json.loads(run_cli("query", "--db", str(db), "--version", "previous").stdout)
        check(latest["version"] == 3 and prev["version"] == 2, "last two versions queryable")
        v1 = json.loads(run_cli("query", "--db", str(db), "--version", "1").stdout)
        check(v1["rule_id"] == "q1", v1["rule_id"])
        run_cli("query", "--db", str(db), "--version", "4", expect=2)
        proc = run_cli("install-rules", "--db", str(db), "--rules",
                       str(write_json(Path(tmp) / "q4.json", mk("q4", 37))), expect=2)
        check("limit" in proc.stderr, proc.stderr)


@test
def test_event_and_rule_limits():
    """At most 100000 events per catalog."""
    with tempfile.TemporaryDirectory() as tmp:
        too_many = [{"id": f"e{i}", "scale": "TAI", "value": "2017-01-01T00:00:00"}
                    for i in range(100_001)]
        ep = write_json(Path(tmp) / "many.json", too_many)
        proc = run_cli("ingest", "--db", str(Path(tmp) / "db1"), "--events", str(ep),
                       "--config", str(write_json(Path(tmp) / "c.json", CONFIG)), expect=2)
        check("too many events" in proc.stderr, proc.stderr)
        exactly = too_many[:100_000]
        for i, e in enumerate(exactly):      # distinct keys -> no pair blowup
            e["key"] = f"k{i}"
            e["source"] = "A" if i % 2 == 0 else "B"
        ep2 = write_json(Path(tmp) / "exact.json", exactly)
        run_cli("ingest", "--db", str(Path(tmp) / "db2"), "--events", str(ep2),
                "--config", str(Path(tmp) / "c.json"))


@test
def test_reads_metered_separately_from_changes():
    """Neighbor reads and result changes are metered separately in the audit."""
    with tempfile.TemporaryDirectory() as tmp:
        events = [
            {"id": "a", "source": "A", "key": "k", "scale": "UTC",
             "value": "2017-01-01T00:00:00Z"},
            {"id": "b-near", "source": "B", "key": "k", "scale": "TAI",
             "value": "2017-01-01T00:00:36.4"},   # read as neighbor, never paired
            {"id": "b-far", "source": "B", "key": "k", "scale": "TAI",
             "value": "2017-01-01T00:10:00"},     # never even read
            {"id": "old", "source": "A", "key": "z", "scale": "UTC",
             "value": "2016-12-05T00:00:00Z"},    # before sealed_before: not rescanned
        ]
        cfg = {"tolerance_ns": 400_000_000, "window_origin_tai_ns": ORIGIN,
               "window_width_ns": WIDTH}
        t1 = {"id": "t1", "initial_offset": 36, "changes": [],
              "sealed_before": "2016-12-10T00:00:00Z",
              "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        t2 = {"id": "t2", "initial_offset": 36,
              "changes": [{"effective": "2017-01-01T00:00:00Z", "offset": 37}],
              "sealed_before": "2016-12-10T00:00:00Z",
              "coverage": ["2016-12-01T00:00:00Z", "2017-02-01T00:00:00Z"]}
        db, _ = fresh_catalog(tmp, events, cfg)
        install(db, t1)
        d = install(db, t2)
        s = d["stats"]
        check(s["utc_events_total"] == 2 and s["utc_events_rescanned"] == 1,
              f"only the post-sealed UTC event may be rescanned: {s}")
        check(s["unchanged_neighbors_read"] >= 1 and s["pairs_inserted"] == 0
              and s["events_changed"] == 1,
              f"reads and changes must be separate counters: {s}")
        check(s["pair_candidates_scanned"] >= s["unchanged_neighbors_read"], s)


@test
def test_baseline_gap_artifact_still_reproduces():
    """baseline_demo.py keeps reproducing the legacy staleness gap."""
    proc = subprocess.run([sys.executable, str(ROOT / "baseline_demo.py")],
                          capture_output=True, text=True, cwd=ROOT)
    check(proc.returncode == 0, proc.stderr)
    out = json.loads(proc.stdout)
    check(out["cross_module_staleness_reproduced"] is True
          and out["stale_pairs"] == [] and out["new_rule_recompute"] == [["u", "t"]], out)


def main():
    failures = 0
    for fn in RESULTS:
        try:
            fn()
            print(f"PASS {fn.__name__}")
        except Exception as exc:  # noqa: BLE001 - report and continue
            failures += 1
            print(f"FAIL {fn.__name__}: {exc}")
    print(f"\n{len(RESULTS) - failures}/{len(RESULTS)} checks passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
