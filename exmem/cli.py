"""Command-line entry point.

    python -m exmem run --plan PLAN.json --source a=A.jsonl [--source b=B.jsonl]
                        --out OUT.jsonl --workdir DIR [--schema SCHEMA.json]
                        [--report REPORT.json] [--audit-groups GROUPS.jsonl]
    python -m exmem bench [--rows N] [--workdir DIR]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def cmd_run(args):
    from .engine import run_plan
    plan = _load_json(args.plan)
    schema = _load_json(args.schema) if args.schema else None
    sources = {}
    for spec in args.source:
        name, sep, path = spec.partition("=")
        if not sep:
            raise SystemExit(f"--source must be name=path, got {spec!r}")
        sources[name] = path
    report = run_plan(plan, sources, args.out, args.workdir,
                      schema=schema, audit_groups_path=args.audit_groups)
    text = json.dumps(report, ensure_ascii=False, indent=1)
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            f.write(text + "\n")
    print(text)
    return 0


def cmd_bench(args):
    sys.path.insert(0, str(REPO_ROOT))
    import make_large
    from .engine import run_plan

    workdir = Path(args.workdir)
    if workdir.exists():
        shutil.rmtree(workdir)
    gen = workdir / "generated"
    make_large.generate(str(gen), n=args.rows)
    sources = {"a": str(gen / "a.jsonl"), "b": str(gen / "b.jsonl")}
    plan_path = REPO_ROOT / "fixtures" / "resource-benchmark-plan.json"
    plan = _load_json(plan_path)
    out = workdir / "out.jsonl"
    report = run_plan(plan, sources, str(out), workdir / "tmp")

    input_bytes = sum(os.path.getsize(p) for p in sources.values())
    temp_peak = report["meter"]["temp_bytes_peak"]
    payload_peak = report["meter"]["payload_peak"]
    budget_bytes = 6 * input_bytes
    checks = {
        "input_bytes_two_sources": input_bytes,
        "temp_disk_budget_bytes(6x_input)": budget_bytes,
        "temp_bytes_peak": temp_peak,
        "temp_disk_ok": temp_peak <= budget_bytes,
        "payload_peak": payload_peak,
        "payload_limit": report["meter"]["payload_limit"],
        "payload_ok": payload_peak <= report["meter"]["payload_limit"],
        "output_rows": report["output_rows"],
    }
    report["benchmark_checks"] = checks
    text = json.dumps(report, ensure_ascii=False, indent=1)
    report_path = workdir / "bench-report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    print(text)
    print(f"\nbench: temp_peak={temp_peak}B <= {budget_bytes}B "
          f"({temp_peak / input_bytes:.2f}x of 6.00x budget), "
          f"payload_peak={payload_peak} <= 4096, report -> {report_path}")
    return 0 if (checks["temp_disk_ok"] and checks["payload_ok"]) else 1


def main(argv=None):
    ap = argparse.ArgumentParser(prog="exmem",
                                 description="external-memory bag-semantics set executor")
    sub = ap.add_subparsers(dest="cmd", required=True)

    pr = sub.add_parser("run", help="run a plan over JSONL sources")
    pr.add_argument("--plan", required=True)
    pr.add_argument("--source", action="append", required=True,
                    help="name=path, repeatable (max 4)")
    pr.add_argument("--out", required=True)
    pr.add_argument("--workdir", required=True)
    pr.add_argument("--schema")
    pr.add_argument("--report")
    pr.add_argument("--audit-groups", help="write per-group quota log here")
    pr.set_defaults(fn=cmd_run)

    pb = sub.add_parser("bench", help="hot-key resource benchmark "
                        "(make_large data + fixtures/resource-benchmark-plan.json)")
    pb.add_argument("--rows", type=int, default=100_000)
    pb.add_argument("--workdir", default=".exmem-bench")
    pb.set_defaults(fn=cmd_bench)

    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
