#!/usr/bin/env python3
"""Event data processing catalog CLI: ingest / install-rules / query / explain.

Usage (all paths relative to the repository root):

    python3 catalog.py ingest        --db DB --events FILE --config FILE
    python3 catalog.py install-rules --db DB --rules FILE
    python3 catalog.py query         --db DB [--version N|latest|previous]
    python3 catalog.py explain       --db DB (--pair A_ID B_ID | --event ID)

Exit code 0 on success, 2 on a declared rejection/failure (message on stderr).
Set CATALOG_FAULT_AT=pre_association|pre_commit to exercise failure atomicity.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from materializer import Catalog, CatalogError
from time_rules import RuleError


def _load_json(path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalogError(f"cannot read {path}: {exc}")


def cmd_ingest(args):
    config = _load_json(args.config)
    result = Catalog.ingest(args.db, args.events, config)
    print(json.dumps({"ingested": result["ingested"], "rejected": result["rejected"]},
                     indent=1))
    return 0


def cmd_install_rules(args):
    cat = Catalog(args.db)
    table = _load_json(args.rules)
    delta = cat.install_rules(table)
    print(json.dumps({"installed_version": delta["to_version"],
                      "rule_id": delta["rule_id"],
                      "event_changes": len(delta["event_changes"]),
                      "pair_inserts": len(delta["pair_inserts"]),
                      "pair_retracts": len(delta["pair_retracts"]),
                      "pair_updates": len(delta["pair_updates"]),
                      "window_deltas": len(delta["window_deltas"]),
                      "stats": delta["stats"]}, indent=1))
    return 0


def cmd_query(args):
    cat = Catalog(args.db)
    print(json.dumps(cat.query(args.version), indent=1))
    return 0


def cmd_explain(args):
    cat = Catalog(args.db)
    if args.pair:
        out = cat.explain_pair(args.pair[0], args.pair[1])
    else:
        out = cat.explain_event(args.event)
    print(json.dumps(out, indent=1))
    return 0


def cmd_verify(args):
    """Full independent recompute of one version vs the materialized snapshot."""
    from materializer import oracle_snapshot
    cat = Catalog(args.db)
    snap = cat.query(args.version)
    oracle = oracle_snapshot(cat.events, snap["rules"], snap["config"])
    checks = {
        "events": _events_view(snap) == _events_view(oracle),
        "pairs": snap["pairs"] == oracle["pairs"],
        "windows": snap["windows"] == oracle["windows"],
    }
    ok = all(checks.values())
    print(json.dumps({"version": snap["version"], "rule_id": snap["rule_id"],
                      "oracle_matches": checks, "ok": ok}, indent=1))
    return 0 if ok else 1


def _events_view(snap_or_oracle):
    return [{k: e[k] for k in ("id", "status", "tai_ns", "reason")}
            for e in snap_or_oracle["events"]]


def main(argv=None):
    parser = argparse.ArgumentParser(prog="catalog.py",
                                     description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("ingest", help="create a catalog from raw events + config")
    p.add_argument("--db", required=True)
    p.add_argument("--events", required=True)
    p.add_argument("--config", required=True)
    p.set_defaults(fn=cmd_ingest)

    p = sub.add_parser("install-rules", help="validate and atomically install a rule table")
    p.add_argument("--db", required=True)
    p.add_argument("--rules", required=True)
    p.set_defaults(fn=cmd_install_rules)

    p = sub.add_parser("query", help="dump a version snapshot (default: latest)")
    p.add_argument("--db", required=True)
    p.add_argument("--version", default=None,
                   help="version number, 'latest' or 'previous'")
    p.set_defaults(fn=cmd_query)

    p = sub.add_parser("explain", help="explain a pair or an event across versions")
    p.add_argument("--db", required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--pair", nargs=2, metavar=("A_ID", "B_ID"))
    g.add_argument("--event", metavar="ID")
    p.set_defaults(fn=cmd_explain)

    p = sub.add_parser("verify", help="full-recompute a version and compare to the snapshot")
    p.add_argument("--db", required=True)
    p.add_argument("--version", default=None)
    p.set_defaults(fn=cmd_verify)

    args = parser.parse_args(argv)
    try:
        return args.fn(args)
    except (CatalogError, RuleError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
