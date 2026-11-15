"""Command-line entry point.

    python -m streamlex restore  SOURCE --target DB --manifest M [--crash-at POINT]
    python -m streamlex audit-source SOURCE --target DB --manifest M
    python -m streamlex status   --target DB

``restore`` streams from the last safe lexical/transaction boundary, applying
one source transaction at a time.  ``--crash-at`` names a control point at
which the current process aborts abruptly (simulated crash); the database file
is left exactly as the crash would leave it.

Exit codes
    0  success
    2  source/input rejected (unknown SQL, bad COPY row, truncation, tamper)
    3  audit found a mismatch (target marked untrusted)
    4  source changed in a way that is not a pure append
"""

import argparse
import json
import os
import sys

from .engine import Engine
from .errors import (
    AuditFailed,
    InputFormatError,
    SourceChanged,
    StreamLexError,
    TxnRejected,
)

EXIT_OK = 0
EXIT_INPUT = 2
EXIT_AUDIT = 3
EXIT_SOURCE = 4


class SimulatedCrash(Exception):
    pass


CRASH_POINTS = (
    "after_copy_end",
    "after_source_commit_before_target_commit",
    "before_target_commit",
    "after_target_commit",
    "after_checkpoint",
    "txn_discarded",
)


# Points at which the independent reader is asked what it can see.  These are
# the ones that matter for the half-transaction claim.
OBSERVE_POINTS = (
    "after_copy_end",
    "after_source_commit_before_target_commit",
    "after_target_commit",
)


def _crash_hook(point, reader_db=None):
    """Return a control callback that aborts the process at ``point``.

    Exiting without cleanup is the point: the SQLite connection's uncommitted
    work is dropped by the OS, and nothing after the control point runs.  If
    ``reader_db`` is given, a *separate* connection reads the target at the
    observation points — this is how we prove a reader never observes a half
    transaction.
    """

    observed = []

    def hook(name, engine, **kw):
        if reader_db and name in OBSERVE_POINTS:
            observed.append((name, getattr(kw.get("unit"), "seq", None),
                             _reader_view(reader_db)))
        if name == point:
            sys.stdout.write(
                "[crash] at control point %s (unit=%r)\n"
                % (name, getattr(kw.get("unit"), "seq", None))
            )
            for obs_name, seq, view in observed:
                sys.stdout.write(
                    "  reader@%s (unit=%r): %s\n"
                    % (obs_name, seq, json.dumps(view, sort_keys=True))
                )
            sys.stdout.flush()
            raise SimulatedCrash(name)

    hook.observed = observed
    return hook


def _reader_view(dbpath):
    """Open a fresh reader connection and report committed row counts."""
    import sqlite3

    c = sqlite3.connect("file:%s?mode=ro" % dbpath, uri=True)
    try:
        tables = [
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
                " AND name NOT LIKE '__streamlex_%' ORDER BY name"
            )
        ]
        return {
            t: c.execute('SELECT COUNT(*) FROM "%s"' % t).fetchone()[0]
            for t in tables
        }
    finally:
        c.close()


def _engine_from_args(args):
    return Engine(
        args.source,
        args.target,
        args.manifest,
        on_control=_crash_hook(args.crash_at, args.observe_target)
        if getattr(args, "crash_at", None)
        else None,
        chunk_size=getattr(args, "chunk_size", 65536),
    )


def cmd_restore(args):
    if args.crash_at and args.crash_at not in CRASH_POINTS:
        sys.stderr.write("unknown crash point %r\n" % args.crash_at)
        return EXIT_INPUT
    eng = _engine_from_args(args)
    try:
        result = eng.run()
    except SimulatedCrash:
        return 130
    except AuditFailed as exc:
        sys.stderr.write("REFUSED (untrusted): %s\n" % exc)
        return EXIT_AUDIT
    except SourceChanged as exc:
        sys.stderr.write("REFUSED (source changed): %s\n" % exc)
        return EXIT_SOURCE
    except (InputFormatError, TxnRejected) as exc:
        sys.stderr.write("REJECTED: %s\n" % exc)
        return EXIT_INPUT
    finally:
        ops = eng.target.ops_executed if hasattr(eng, "target") else None
        eng.close()
    result["ops_executed_this_run"] = ops
    json.dump(result, sys.stdout, indent=2, sort_keys=True, default=str)
    sys.stdout.write("\n")
    return EXIT_OK


def cmd_audit(args):
    eng = _engine_from_args(args)
    try:
        result = eng.audit()
    except AuditFailed as exc:
        sys.stderr.write("AUDIT FAILED: %s\n" % exc)
        return EXIT_AUDIT
    except SourceChanged as exc:
        sys.stderr.write("REFUSED (source changed): %s\n" % exc)
        return EXIT_SOURCE
    finally:
        eng.close()
    result["ok"] = True
    # Report audit reads separately from resume reads; the two budgets are not
    # interchangeable.
    result["accounting"] = {
        "audit_bytes_read": result.pop("bytes_read"),
        "audit_read_calls": result.pop("read_calls"),
    }
    json.dump(result, sys.stdout, indent=2, sort_keys=True, default=str)
    sys.stdout.write("\n")
    return EXIT_OK


def cmd_status(args):
    from .target import Target

    t = Target(args.target)
    try:
        t.ensure_schema()
        cp = t.checkpoint()
        out = {
            "checkpoint": cp,
            "tables": t.counts(),
            "untrusted": t.untrusted_reason(),
            "foreign_key_violations": t.foreign_key_violations(),
        }
    finally:
        t.close()
    json.dump(out, sys.stdout, indent=2, sort_keys=True, default=str)
    sys.stdout.write("\n")
    return EXIT_OK


def build_parser():
    p = argparse.ArgumentParser(prog="streamlex")
    sub = p.add_subparsers(dest="command", required=True)

    def common(sp):
        sp.add_argument("source")
        sp.add_argument("--target", required=True)
        sp.add_argument("--manifest", required=True)

    r = sub.add_parser("restore", help="stream-restore the export into SQLite")
    common(r)
    r.add_argument("--crash-at", choices=CRASH_POINTS + ("none",), default="none")
    r.add_argument(
        "--observe-target",
        metavar="DB",
        help="read-only connection that observes committed state at each "
        "control point (for the half-transaction demonstration)",
    )
    r.add_argument(
        "--chunk-size", type=int, default=65536, help="source read chunk size"
    )
    r.set_defaults(func=cmd_restore)

    a = sub.add_parser("audit-source", help="full re-verification of the source prefix")
    common(a)
    a.set_defaults(func=cmd_audit)

    s = sub.add_parser("status", help="show the target checkpoint and row counts")
    s.add_argument("--target", required=True)
    s.set_defaults(func=cmd_status)
    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "crash_at", None) == "none":
        args.crash_at = None
    try:
        return args.func(args)
    except BrokenPipeError:
        return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
