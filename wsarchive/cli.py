"""Command-line interface for building indexes, querying, and verifying.

Subcommands
-----------
``build``   parse an archive and write ``index.bin`` + ``manifest.json``
``query``   decode message ids (arbitrary order, duplicates allowed)
``verify``  sequential decode compared against the index digests
``info``    print index structure and size-budget accounting
``bench``   run the 100x4096 workload experiment across checkpoint densities

All commands read the archive and negotiation JSON from disk; nothing is
imported from the demo baseline.
"""

import argparse
import json
import sys
from pathlib import Path

from .build import (
    METADATA_BUDGET,
    QUERY_BUDGET,
    STATE_BUDGET,
    build_from_files,
)
from .errors import ArchiveError
from .index import canonical_negotiation
from .model import Negotiation
from .reader import Reader


def _load_neg(path):
    return Negotiation.from_json(json.loads(Path(path).read_text(encoding="utf-8")))


def cmd_build(args):
    index, path = build_from_files(
        args.archive,
        args.negotiation,
        args.index_dir,
        density=args.density,
        state_budget=args.state_budget,
        metadata_budget=args.metadata_budget,
        query_budget=args.query_budget,
    )
    print(
        json.dumps(
            {
                "index": str(path),
                "frames": len(index.frames),
                "messages": len(index.messages),
                "checkpoints": len(index.checkpoints),
                "density": index.density,
                "sizes": index.size_report(),
                "build_stats": getattr(index, "build_stats", {}),
            },
            indent=2,
        )
    )


def _open_reader(args):
    neg = _load_neg(args.negotiation) if args.negotiation else None
    return Reader.open(args.archive, args.index_dir, neg)


def cmd_query(args):
    reader = _open_reader(args)
    ids = [int(x) for x in args.ids.split(",") if x.strip() != ""]
    result = reader.query(ids, mode=args.mode)
    report = result.report()
    if args.json:
        print(
            json.dumps(
                {
                    "stats": report,
                    "messages": [
                        {
                            "id": mid,
                            "size": len(result.messages[i]),
                            "sha256": report["message_digests"][i],
                        }
                        for i, mid in enumerate(ids)
                    ],
                },
                indent=2,
            )
        )
    else:
        for i, mid in enumerate(ids):
            print(f"{mid}\t{len(result.messages[i])}\t{report['message_digests'][i]}")
        print("# " + json.dumps(report, sort_keys=True), file=sys.stderr)


def cmd_verify(args):
    reader = _open_reader(args)
    plaintexts = reader.decode_sequential()
    ok = len(plaintexts) == len(reader.index.messages)
    print(
        json.dumps(
            {
                "messages": len(plaintexts),
                "ok": ok,
                "total_plaintext_bytes": sum(map(len, plaintexts)),
            },
            indent=2,
        )
    )
    return 0 if ok else 1


def cmd_info(args):
    reader = _open_reader(args)
    idx = reader.index
    payload = {
        "archive_sha256": idx.archive_sha256.hex(),
        "archive_size": idx.archive_size,
        "negotiation": json.loads(canonical_negotiation(idx.negotiation).decode()),
        "frames": len(idx.frames),
        "messages": len(idx.messages),
        "checkpoints": [
            {
                "message": cp.message_index,
                "dict_bytes": cp.dict_len,
                "mode": cp.resume_mode,
            }
            for cp in idx.checkpoints
        ],
        "sizes": idx.size_report(),
        "budgets": {
            "dictionary_state_limit": STATE_BUDGET,
            "metadata_limit": METADATA_BUDGET,
            "cold_query_limit": QUERY_BUDGET,
            "dictionary_state_ok": idx.dictionary_state_bytes <= STATE_BUDGET,
            "metadata_ok": idx.metadata_bytes <= METADATA_BUDGET,
        },
    }
    print(json.dumps(payload, indent=2))


def cmd_bench(args):
    from .experiments import run_density_experiment, run_workload_experiment

    if args.density_sweep:
        payload = run_density_experiment(
            args.archive, args.negotiation, densities=args.densities or None
        )
    else:
        payload = run_workload_experiment(args.archive, args.negotiation)
    print(json.dumps(payload, indent=2))


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="wsarchive",
        description="Random access into compressed WebSocket message archives",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--archive", required=True, help="path to .frames file")
    common.add_argument(
        "--negotiation", required=True, help="path to negotiation JSON"
    )

    p_build = sub.add_parser("build", parents=[common], help="build index")
    p_build.add_argument("--index-dir", required=True)
    p_build.add_argument("--density", type=int, default=None)
    p_build.add_argument("--state-budget", type=int, default=STATE_BUDGET)
    p_build.add_argument("--metadata-budget", type=int, default=METADATA_BUDGET)
    p_build.add_argument("--query-budget", type=int, default=QUERY_BUDGET)
    p_build.set_defaults(func=cmd_build)

    p_query = sub.add_parser("query", parents=[common], help="decode message ids")
    p_query.add_argument("--index-dir", required=True)
    p_query.add_argument("--ids", required=True, help="comma separated ids")
    p_query.add_argument("--mode", default="auto", choices=["auto", "cold", "batch"])
    p_query.add_argument("--json", action="store_true")
    p_query.set_defaults(func=cmd_query)

    p_verify = sub.add_parser("verify", parents=[common], help="verify all messages")
    p_verify.add_argument("--index-dir", required=True)
    p_verify.set_defaults(func=cmd_verify)

    p_info = sub.add_parser("info", parents=[common], help="index information")
    p_info.add_argument("--index-dir", required=True)
    p_info.set_defaults(func=cmd_info)

    p_bench = sub.add_parser("bench", parents=[common], help="run experiments")
    p_bench.add_argument("--density-sweep", action="store_true")
    p_bench.add_argument("--densities", type=int, nargs="*")
    p_bench.set_defaults(func=cmd_bench)

    args = parser.parse_args(argv)
    try:
        rc = args.func(args)
    except ArchiveError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return rc or 0


if __name__ == "__main__":
    raise SystemExit(main())
