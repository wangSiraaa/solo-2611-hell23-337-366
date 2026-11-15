#!/usr/bin/env python3
"""Demo: the inputs/ scenario that exposed the baseline staleness gap, run
through the new catalog.  Produces real, inspectable artifacts under
out/demo_db/ (per-version snapshots + change logs) and shows the gap is
closed: installing r2 jointly updates canonical times, pairs and windows.
"""
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / "out" / "demo_db"


def run(*args):
    proc = subprocess.run([sys.executable, str(ROOT / "catalog.py"), *args],
                          capture_output=True, text=True, cwd=ROOT)
    if proc.returncode != 0:
        raise SystemExit(f"command failed: {args}\n{proc.stderr}")
    return json.loads(proc.stdout)


def main():
    shutil.rmtree(DB, ignore_errors=True)
    DB.parent.mkdir(parents=True, exist_ok=True)

    print("== ingest inputs/events.json ==")
    print(json.dumps(run("ingest", "--db", str(DB), "--events", "inputs/events.json",
                         "--config", "inputs/config.json"), indent=1))

    print("== install-rules inputs/r1.json ==")
    print(json.dumps(run("install-rules", "--db", str(DB), "--rules", "inputs/r1.json"),
                     indent=1))

    print("== install-rules inputs/r2.json (the revision baseline_demo loses) ==")
    print(json.dumps(run("install-rules", "--db", str(DB), "--rules", "inputs/r2.json"),
                     indent=1))

    print("== query --version latest: pairs ==")
    latest = run("query", "--db", str(DB), "--version", "latest")
    print(json.dumps(latest["pairs"], indent=1))

    print("== query --version previous: pairs (last two versions queryable) ==")
    previous = run("query", "--db", str(DB), "--version", "previous")
    print(json.dumps(previous["pairs"], indent=1))

    print("== explain --pair u t ==")
    print(json.dumps(run("explain", "--db", str(DB), "--pair", "u", "t"), indent=1))

    print("== verify both versions against full recompute ==")
    for v in ("1", "2"):
        print(json.dumps(run("verify", "--db", str(DB), "--version", v), indent=1))

    print(f"real change logs written under {DB}/changes/ "
          f"(delta_1.json, delta_2.json); snapshots under {DB}/versions/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
