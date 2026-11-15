"""Reproducible planner tradeoff experiment.

The engine's planner has two knobs per read:

* ``page_size`` -- round a remote read outward to a byte boundary;
* ``*_merge_gap`` -- merge logically separate intervals when the gap is small.

Both reduce the *number of HTTP requests* at the cost of *response body bytes*
(over-read).  The interval cache bounds resident bytes at ``cache_capacity``
(8 KiB by default); descriptor and heap data share it.

This script runs a fixed set of legal queries against a stable source and prints
measured bytes / requests / peak cache for several planner settings, including a
case where an aggressive page size over-reads past the 16 KiB per-query body
budget.  It does not claim optimality; it makes the actual tradeoff visible and
reproducible.

Run::

    python3 -m fitsproj.experiment
"""

from __future__ import annotations

import json
from pathlib import Path

from range_server import Source

from .engine import ProjectionEngine, RowSpec
from .plan import PlanConfig

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"

SCENARIOS = {
    "two_rows_window_0_8": ([RowSpec(7, ((0, 8),)), RowSpec(83, ((0, 8),))], 16384),
    "duplicate_row_window_1000_1024": (
        [RowSpec(4, ((1000, 1024),)), RowSpec(5, ((1000, 1024),)), RowSpec(4, ((1000, 1024),))],
        16384,
    ),
    "two_full_arrays": ([RowSpec(7, ()), RowSpec(83, ())], None),
    "scattered_small": (
        [RowSpec(r, ((0, 4),)) for r in (2, 17, 40, 63, 88)],
        16384,
    ),
    "disjoint_windows_same_row": (
        [RowSpec(9, ((0, 16),)), RowSpec(9, ((900, 1024),))],
        16384,
    ),
}

CONFIGS = {
    "exact/no-merge": PlanConfig(page_size=0, descriptor_merge_gap=0, heap_merge_gap=0),
    "exact/desc-gap8": PlanConfig(page_size=0, descriptor_merge_gap=8, heap_merge_gap=0),
    "exact/desc+heap-gap64": PlanConfig(
        page_size=0, descriptor_merge_gap=8, heap_merge_gap=64
    ),
    "page512/desc-gap8": PlanConfig(
        page_size=512, descriptor_merge_gap=8, heap_merge_gap=0
    ),
    "page4096/desc-gap8": PlanConfig(
        page_size=4096, descriptor_merge_gap=8, heap_merge_gap=0
    ),
}


def run(files, specs, config):
    source = Source(files)
    url = source.start()
    try:
        result = ProjectionEngine(url, config=config).query(specs)
        return {
            "body_bytes": result["response_body_bytes"],
            "requests": len(result["trace"]),
            "peak_cache_bytes": result["cache"]["peak_cache_bytes"],
            "header_bytes": result["header_bytes"],
        }
    finally:
        source.close()


def main():
    files = [str(FIXTURES / "a.fits")]
    print("== planner tradeoff on fixtures/a.fits (one stable source) ==")
    rows = []
    for scenario, (specs, budget) in SCENARIOS.items():
        for name, config in CONFIGS.items():
            m = run(files, specs, config)
            rows.append(
                {
                    "scenario": scenario,
                    "config": name,
                    "body_bytes": m["body_bytes"],
                    "requests": m["requests"],
                    "peak_cache_bytes": m["peak_cache_bytes"],
                    "budget": budget,
                    "within_budget": None if budget is None else m["body_bytes"] <= budget,
                }
            )
    width = max(len(r["scenario"]) for r in rows)
    print(f"{'scenario'.ljust(width)}  {'config':22} {'body':>7} {'reqs':>5} {'peak':>6} {'<=16KiB':>8}")
    for r in rows:
        flag = "-" if r["within_budget"] is None else ("yes" if r["within_budget"] else "NO")
        print(
            f"{r['scenario'].ljust(width)}  {r['config']:22} "
            f"{r['body_bytes']:>7} {r['requests']:>5} {r['peak_cache_bytes']:>6} {flag:>8}"
        )
    print(json.dumps({"results": rows}, indent=2)[:0])  # keep stdout tabular
    return rows


if __name__ == "__main__":
    main()
