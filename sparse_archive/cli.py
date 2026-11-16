"""Command line interface: ingest-base / apply-delta / read-range / compact / verify.

All paths are relative to the current repository root.  Every command
prints one JSON document; failures print an error document to stderr and
exit non-zero.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .archive import Archive
from .errors import ArchiveError
from .model import decl_from_json


def _open(args) -> Archive:
    return Archive.open(args.archive)


def _cmd_ingest_base(args) -> dict:
    arch = Archive.create(args.archive)
    return arch.ingest_base(args.source)


def _cmd_apply_delta(args) -> dict:
    arch = _open(args)
    obj = json.loads(Path(args.decl).read_text())
    payload = Path(args.payload).read_bytes() if args.payload else None
    decl = decl_from_json(obj, payload)
    return arch.apply_delta(decl)


def _cmd_read_range(args) -> dict:
    arch = _open(args)
    res = arch.read_range(args.generation, args.start, args.end)
    res["data_hex"] = res.pop("data").hex()
    return res


def _cmd_compact(args) -> dict:
    return _open(args).compact()


def _cmd_verify(args) -> dict:
    return _open(args).verify(generation=args.generation)


def _cmd_stats(args) -> dict:
    return _open(args).storage_stats()


def _cmd_status(args) -> dict:
    return _open(args).status()


def _cmd_restore(args) -> dict:
    return _open(args).restore(args.generation, args.out)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sparse-archive",
        description="sparse multi-generation image archive")
    sub = p.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("ingest-base", help="ingest frozen file as generation 1")
    q.add_argument("archive")
    q.add_argument("source")
    q.set_defaults(fn=_cmd_ingest_base)

    q = sub.add_parser("apply-delta", help="apply one delta declaration")
    q.add_argument("archive")
    q.add_argument("decl", help="delta declaration JSON")
    q.add_argument("--payload", help="file with concatenated DATA payloads")
    q.set_defaults(fn=_cmd_apply_delta)

    q = sub.add_parser("read-range", help="read [start,end) of a generation")
    q.add_argument("archive")
    q.add_argument("generation", type=int)
    q.add_argument("start", type=int)
    q.add_argument("end", type=int)
    q.set_defaults(fn=_cmd_read_range)

    q = sub.add_parser("compact", help="rebase oldest needed generation, reclaim")
    q.add_argument("archive")
    q.set_defaults(fn=_cmd_compact)

    q = sub.add_parser("verify", help="full integrity verification")
    q.add_argument("archive")
    q.add_argument("--generation", type=int, default=None)
    q.set_defaults(fn=_cmd_verify)

    q = sub.add_parser("stats", help="persistent byte accounting")
    q.add_argument("archive")
    q.set_defaults(fn=_cmd_stats)

    q = sub.add_parser("status", help="generations, pins, pending reclaim")
    q.add_argument("archive")
    q.set_defaults(fn=_cmd_status)

    q = sub.add_parser("restore", help="materialize a generation to a file")
    q.add_argument("archive")
    q.add_argument("generation", type=int)
    q.add_argument("out")
    q.set_defaults(fn=_cmd_restore)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = args.fn(args)
    except ArchiveError as exc:
        print(json.dumps({"ok": False, "error": str(exc),
                          "error_type": type(exc).__name__}),
              file=sys.stderr)
        return 2
    print(json.dumps(result, indent=1, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
