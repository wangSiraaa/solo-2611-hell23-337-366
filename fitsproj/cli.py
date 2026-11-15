"""Command line entry point for the version-consistent projection engine.

Usage
-----
Start a local source and run a query from a JSON spec or inline arguments::

    python3 -m fitsproj.cli query --file fixtures/a.fits \
        --rows 7,83 --slice 0:8

    python3 -m fitsproj.cli query --file fixtures/a.fits \
        --spec fixtures/queries.json --all --out result.json

    python3 -m fitsproj.cli plan --file fixtures/a.fits --rows 7,83 --slice 0:8

    # version switch after descriptors, before heap
    python3 -m fitsproj.cli query --file fixtures/a.fits --file fixtures/b.fits \
        --switch-on-heap --rows 7,83 --slice 0:8

    # twice-switch demonstration (must fail explicitly)
    python3 -m fitsproj.cli query --file fixtures/a.fits --file fixtures/b.fits \
        --file fixtures/a.fits --switch-on-heap --rows 7,83 --slice 0:8

``--file`` may be repeated to serve equal-length versions from one object URL.
"""

from __future__ import annotations

import argparse
import json
import sys

from .engine import ProjectionEngine, RowSpec
from .errors import ProjectionError, VersionConflictError
from .plan import PlanConfig
from .spec import parse_request


def _parse_rows(text):
    return [int(x) for x in text.split(",") if x.strip()]


def _parse_slice(text):
    if ":" in text:
        start, stop = text.split(":", 1)
    elif "-" in text:
        start, stop = text.split("-", 1)
    else:
        raise argparse.ArgumentTypeError("slice must look like START:STOP")
    return int(start), int(stop)


def _parse_intervals(text):
    """Parse ``0:8,16:24`` or ``0:8;4:12,20:24`` (semicolon per row)."""

    groups = []
    for group in text.split(";"):
        group = group.strip()
        if not group:
            groups.append([])
            continue
        intervals = []
        for part in group.split(","):
            part = part.strip()
            if not part:
                continue
            start, stop = re_split_interval(part)
            intervals.append((start, stop))
        groups.append(intervals)
    return groups


def re_split_interval(part):
    if ":" in part:
        start, stop = part.split(":", 1)
    elif "-" in part:
        start, stop = part.split("-", 1)
    else:
        raise ValueError(f"bad interval {part!r}")
    return int(start), int(stop)


def _row_specs(args):
    specs = []
    if args.spec:
        with open(args.spec, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        items = data if isinstance(data, list) else [data]
        for item in items:
            specs.extend(parse_request(item))
        return specs

    rows = _parse_rows(args.rows) if args.rows else []
    if args.intervals:
        groups = _parse_intervals(args.intervals)
        if len(groups) == 1:
            groups = groups * len(rows)
        if len(groups) != len(rows):
            raise SystemExit(
                "--intervals groups must match the number of --rows"
            )
        return [RowSpec(row, tuple(group)) for row, group in zip(rows, groups)]

    interval = ()
    if args.slice:
        interval = (_parse_slice(args.slice),)
    return [RowSpec(row, interval) for row in rows]


def _make_source(args):
    from range_server import Source

    switch_when = None
    switch_at = None
    if args.switch_on_heap:
        threshold = args.heap_start_threshold
        if threshold is None:
            # Probe the layout without heap reads so the switch fires on the
            # first heap request, i.e. after all descriptors, before any heap.
            probe = Source(args.file)
            probe_url = probe.start()
            try:
                layout = ProjectionEngine(probe_url).plan(_row_specs(args))["layout"]
                threshold = layout["heap_range"][0]
            finally:
                probe.close()
        switch_when = lambda index, start, end: start >= threshold
    if args.switch_at is not None:
        switch_at = [int(x) for x in args.switch_at.split(",")]
    source = Source(
        args.file,
        switch_after=args.switch_after,
        switch_when=switch_when,
        switch_at=switch_at,
    )
    return source


def _config_from_args(args):
    return PlanConfig(
        page_size=args.page_size,
        cache_capacity=args.cache_capacity,
        descriptor_merge_gap=args.descriptor_gap,
        heap_merge_gap=args.heap_gap,
    )


def _run_one(specs, args):
    source = _make_source(args)
    url = source.start()
    try:
        result = ProjectionEngine(url, config=_config_from_args(args)).query(specs)
        return {
            "url": url,
            "requests_spec": [
                {"row": s.row, "intervals": [list(iv) for iv in s.intervals]}
                for s in specs
            ],
            "etag": result["etag"],
            "rows": result["rows"],
            "response_body_bytes": result["response_body_bytes"],
            "header_bytes": result["header_bytes"],
            "cache": result["cache"],
            "attempts": result["attempts"],
            "trace": result["trace"],
            "server": {
                "sent": source.sent,
                "switches": source.switches,
                "requests": source.log(),
            },
        }
    finally:
        source.close()


def cmd_query(args):
    config = _config_from_args(args)

    if args.spec and args.all:
        # Each spec is a separate cold-start query on the same stable source
        # files.  Report per-query body bytes against the 16 KiB budget.
        with open(args.spec, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        items = data if isinstance(data, list) else [data]
        reports = []
        for index, item in enumerate(items):
            specs = parse_request(item)
            payload = _run_one(specs, args)
            budget = item.get("body_budget", 16384)
            reports.append(
                {
                    "query_index": index,
                    "requests_spec": payload["requests_spec"],
                    "etag": payload["etag"],
                    "rows": payload["rows"],
                    "response_body_bytes": payload["response_body_bytes"],
                    "body_budget": budget,
                    "within_budget": payload["response_body_bytes"] <= budget,
                    "header_bytes": payload["header_bytes"],
                    "cache": payload["cache"],
                    "attempts": payload["attempts"],
                    "trace": payload["trace"],
                    "server": payload["server"],
                }
            )
        _emit(
            {
                "source": args.file,
                "cold_start": True,
                "queries": reports,
                "all_within_budget": all(r["within_budget"] for r in reports),
            },
            args.out,
        )
        return 0

    specs = _row_specs(args)
    source = _make_source(args)
    url = source.start()
    try:
        result = ProjectionEngine(url, config=config).query(specs)
        payload = {
            "url": url,
            "requests_spec": [
                {"row": s.row, "intervals": [list(iv) for iv in s.intervals]}
                for s in specs
            ],
            "etag": result["etag"],
            "rows": result["rows"],
            "response_body_bytes": result["response_body_bytes"],
            "header_bytes": result["header_bytes"],
            "cache": result["cache"],
            "attempts": result["attempts"],
            "trace": result["trace"],
            "server": {
                "sent": source.sent,
                "switches": source.switches,
                "requests": source.log(),
            },
        }
        _emit(payload, args.out)
        return 0
    finally:
        source.close()


def cmd_plan(args):
    specs = _row_specs(args)
    config = PlanConfig(
        page_size=args.page_size,
        cache_capacity=args.cache_capacity,
        descriptor_merge_gap=args.descriptor_gap,
        heap_merge_gap=args.heap_gap,
    )
    source = _make_source(args)
    url = source.start()
    try:
        engine = ProjectionEngine(url, config=config)
        plan = engine.plan(specs)
        _emit(
            {
                "url": url,
                "plan": plan,
                "server": {"sent": source.sent, "requests": source.log()},
            },
            args.out,
        )
        return 0
    finally:
        source.close()


def _emit(payload, out):
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    if out:
        with open(out, "w", encoding="utf-8") as handle:
            handle.write(text + "\n")
        print(
            json.dumps(
                {
                    "out": out,
                    "response_body_bytes": payload.get("response_body_bytes"),
                    "etag": payload.get("etag"),
                }
            )
        )
    else:
        print(text)


def build_parser():
    parser = argparse.ArgumentParser(
        prog="fitsproj",
        description="Version-consistent projection engine for remote BINTABLE PJ() columns",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p):
        p.add_argument(
            "--file",
            action="append",
            required=True,
            help="FITS version; repeat for equal-length switchable versions",
        )
        p.add_argument("--rows", help="comma-separated row indices, order preserved")
        p.add_argument(
            "--slice",
            help="element interval START:STOP applied to every row",
        )
        p.add_argument(
            "--intervals",
            help="per-row intervals, 'START:STOP,START:STOP;...' (semicolon per row)",
        )
        p.add_argument("--spec", help="JSON query spec file")
        p.add_argument("--all", action="store_true", help="process every spec in --spec")
        p.add_argument("--page-size", type=int, default=0, help="remote read alignment (0=exact)")
        p.add_argument("--cache-capacity", type=int, default=8192)
        p.add_argument("--descriptor-gap", type=int, default=8)
        p.add_argument("--heap-gap", type=int, default=0)
        p.add_argument("--switch-after", type=int, default=None,
                       help="legacy: serve requests with index >= N from version 2")
        p.add_argument("--switch-at", default=None,
                       help="comma-separated global request indices at which to switch")
        p.add_argument("--switch-on-heap", action="store_true",
                       help="switch before the first heap-range request")
        p.add_argument("--heap-start-threshold", type=int, default=None,
                       help="range start that counts as 'after descriptors' "
                            "(default: probe the layout's heap start)")
        p.add_argument("--out", help="write JSON result to this path")

    p_query = sub.add_parser("query", help="run a heap projection under one ETag")
    add_common(p_query)
    p_query.set_defaults(func=cmd_query)

    p_plan = sub.add_parser("plan", help="only build the metadata/descriptor plan")
    add_common(p_plan)
    p_plan.set_defaults(func=cmd_plan)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except VersionConflictError as exc:
        print(
            json.dumps(
                {
                    "error": "version_conflict",
                    "message": str(exc),
                    "attempts": exc.attempts,
                    "response_body_bytes": exc.response_body_bytes,
                    "rows": None,
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        return 3
    except ProjectionError as exc:
        print(
            json.dumps({"error": type(exc).__name__, "message": str(exc)}, indent=2),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
