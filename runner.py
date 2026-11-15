"""Compatibility runner: same API as the original in-memory baseline, but the
work is done by the spillable external-memory engine (exmem)."""
import json
import tempfile
from pathlib import Path

from exmem.engine import run_plan


def run(plan, paths, workdir=None):
    with tempfile.TemporaryDirectory(dir=workdir) as td:
        out = Path(td) / "out.jsonl"
        run_plan(plan, dict(paths), str(out), Path(td) / "tmp")
        rows = []
        with open(out, "r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                rows.append({"row": rec["row"], "witness": rec["witness"]})
        return rows
