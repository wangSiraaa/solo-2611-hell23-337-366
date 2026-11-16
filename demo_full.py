#!/usr/bin/env python3
"""End-to-end demo on the repository fixtures (inputs/g1.img, g2.img, delta.json).

Shows the baseline defect (delta.py ignores HOLE ops), then runs the full
archive protocol on the same data: ingest-base, apply-delta, cross-generation
read-range, compact, verify, restore -- plus explicit rejection results.
All output is JSON-lines; the archive is left on disk under demo_out/ for
re-reading.
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from sparse_archive import (Archive, Oracle, DeltaDecl, Op, DATA, HOLE,
                            ArchiveError)
from sparse_archive.model import sha256_hex
from delta import apply as baseline_apply          # baseline modules
from range_reader import fetch as baseline_fetch
from source_image import read as source_read

OUT = ROOT / "demo_out"
LOG: list[dict] = []


def emit(step: str, **info) -> None:
    rec = {"step": step, **info}
    LOG.append(rec)
    print(json.dumps(rec, sort_keys=True, default=str))


def main() -> int:
    fixture = json.loads((ROOT / "inputs/delta.json").read_text())
    g1 = (ROOT / "inputs/g1.img").read_bytes()
    g2 = (ROOT / "inputs/g2.img").read_bytes()

    # ---- 0. reproduce the baseline defect on the same fixtures -------------
    with tempfile.TemporaryDirectory() as t:
        target = Path(t) / "mirror.img"
        shutil.copy2(ROOT / "inputs/g1.img", target)
        baseline_apply(ROOT / "inputs/g2.img", target,
                       fixture["ops"], fixture["new_size"])
        got = baseline_fetch(target, 0, 4)["data"]
    emit("baseline_defect", range=[0, 4], baseline_returned=got,
         correct="00000000",
         note="delta.py copies DATA ops but silently drops HOLE ops")

    # ---- 1. ingest-base ------------------------------------------------------
    if OUT.exists():
        shutil.rmtree(OUT)
    arch_dir = OUT / "archive"
    arch = Archive.create(arch_dir)
    info = arch.ingest_base(ROOT / "inputs/g1.img")
    emit("ingest-base", **info,
         matches_fixture_sha256=info["source_sha256"] == fixture["base_sha256"])

    oracle = Oracle()
    oracle.ingest_base(g1)

    # ---- 2. apply-delta (the fixture change log, made authoritative) --------
    # The fixture declares the ops; the DATA payload is taken from the frozen
    # new image (the replication source), and the declared per-block digests
    # come from the fixture's logical_blocks.
    ops = []
    for o in fixture["ops"]:
        if o["kind"] == "data":
            ops.append(Op(DATA, o["start"], o["end"], g2[o["start"]:o["end"]]))
        else:
            ops.append(Op(HOLE, o["start"], o["end"]))
    oracle.apply_delta(fixture["new_size"], ops)
    digests = [b["sha256"] for b in fixture["logical_blocks"]]
    assert digests == oracle.block_digests(), "fixture digests must match oracle"
    decl = DeltaDecl(1, 2, fixture["new_size"], ops, digests)
    emit("apply-delta", **arch.apply_delta(decl))
    emit("apply-delta-idempotent", **arch.apply_delta(decl))

    # ---- 3. read-range: the range the baseline got wrong --------------------
    r = arch.read_range(2, 0, 4)
    emit("read-range", generation=2, range=[0, 4], data=r["data"].hex(),
         meter={k: v for k, v in r["meter"].items() if isinstance(v, int)},
         fixed=got != r["data"].hex())
    r = arch.read_range(1, 0, 4)
    emit("read-range-cross-generation", generation=1, range=[0, 4],
         data=r["data"].hex())
    r = arch.read_range(2, 8192, 8196)
    emit("read-range", generation=2, range=[8192, 8196], data=r["data"].hex())
    r = arch.read_range(2, 4095, 8193)   # crosses blocks 0-2
    emit("read-range-cross-block", generation=2, range=[4095, 8193],
         blocks=r["blocks"], data_len=len(r["data"]),
         meter={k: v for k, v in r["meter"].items() if isinstance(v, int)})

    # ---- 4. a third generation: explicit all-zero DATA (stays data) ---------
    ops3 = [Op(DATA, 4096, 8192, bytes(4096))]
    oracle.apply_delta(16384, ops3)
    decl3 = DeltaDecl(2, 3, 16384, ops3, oracle.block_digests())
    emit("apply-delta", **arch.apply_delta(decl3))

    # ---- 5. reader pin + compact ---------------------------------------------
    with arch.open_reader(generation=1) as pinned:
        res = arch.compact()
        emit("compact-with-pin", **res,
             note="pin on gen 1 keeps every ancestor alive")
        emit("pinned-read", generation=1,
             data=pinned.read_range(0, 4)["data"].hex())
    res = arch.compact()
    emit("compact-after-pin-release", **res)
    emit("status", **arch.status())

    # ---- 6. verify + restore, byte-for-byte ----------------------------------
    report = arch.verify()
    emit("verify", ok=report["ok"], generations=report["generations"])
    for gen, expect in ((2, g2), (3, oracle.snapshot())):
        out = OUT / f"restored-g{gen}.img"
        arch.restore(gen, out)
        actual = out.read_bytes()
        emit("restore", generation=gen, path=str(out),
             byte_identical=actual == expect,
             sha256=sha256_hex(actual))
    emit("restore-target-matches-fixture-sha256",
         ok=sha256_hex(g2) == fixture["target_sha256"])

    # ---- 7. explicit failure results ------------------------------------------
    failures = {}

    def expect_reject(name, fn):
        try:
            fn()
        except ArchiveError as exc:
            failures[name] = {"rejected": True,
                              "error_type": type(exc).__name__}
        else:
            failures[name] = {"rejected": False}

    expect_reject("out_of_bounds_read",
                  lambda: arch.read_range(3, 0, 99999))
    expect_reject("reclaimed_ancestor_read",
                  lambda: arch.read_range(1, 0, 4))
    bad = DeltaDecl(2, 3, 16384, [Op(DATA, 0, 4, b"EVIL")],
                    oracle.block_digests())
    expect_reject("same_target_different_content",
                  lambda: arch.apply_delta(bad))
    evil = Oracle()
    evil.ingest_base(b"X" * 16384)
    evil.apply_delta(16384, [])
    expect_reject("wrong_ancestor_digests",
                  lambda: arch.apply_delta(
                      DeltaDecl(3, 4, 16384, [], evil.block_digests())))
    expect_reject("unpinned_third_reader", lambda: _third_reader(arch))
    emit("explicit-failures", **failures)

    # ---- 8. storage accounting --------------------------------------------------
    stats = arch.storage_stats()
    emit("storage-stats", **{k: v for k, v in stats.items()
                             if k != "generations"})
    emit("storage-per-generation", generations=stats["generations"])
    emit("done", archive=str(arch_dir),
         note="archive left on disk for re-reading")
    return 0


def _third_reader(arch):
    r1 = arch.open_reader()
    r2 = arch.open_reader()
    try:
        arch.open_reader()
    finally:
        r1.close()
        r2.close()


if __name__ == "__main__":
    raise SystemExit(main())
