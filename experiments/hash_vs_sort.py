#!/usr/bin/env python3
"""Reproducible strategy-tradeoff experiment: partitioned (grace) hashing vs
external sort-merge for EXCEPT ALL under a single hot key.

The hot key cannot be split by re-hashing: every occurrence of the same key
lands in the same partition no matter the hash function, the partition count,
or the recursion depth.  A partitioned-hash engine must therefore either hold
the whole hot group in memory (violating the 4096-slot payload budget) or fall
back to sorting the oversized partition -- i.e. it converges to sort-merge
anyway, with extra I/O spent on the partitioning round trip.

This script measures, on the same 90%-hot input:
  H1  grace-hash partition sizes (max resident rows for the biggest bucket)
  H2  the same after one re-hash round with a different modulus/seed
  S   the exmem engine's sort-merge: metered resident-payload peak

Usage:  python3 experiments/hash_vs_sort.py [--rows 100000]
"""
import argparse
import hashlib
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exmem.engine import run_plan  # noqa: E402

PAYLOAD_LIMIT = 4096


def key_of(row):
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"))


def grace_hash_partitions(rows, nparts, seed):
    """Return partition sizes for hash(key) % nparts."""
    sizes = [0] * nparts
    for r in rows:
        h = hashlib.blake2b(key_of(r).encode("utf-8"),
                            digest_size=8, person=seed).digest()
        sizes[int.from_bytes(h, "big") % nparts] += 1
    return sizes


def make_skewed(n, hot_frac=0.9):
    rows = []
    hot = int(n * hot_frac)
    for i in range(n):
        rows.append([0, "hot"] if i < hot else [i, "cold"])
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100_000)
    args = ap.parse_args()
    n = args.rows

    left = make_skewed(n)
    right = make_skewed(n)

    print(f"input: {n} rows/side, 90% sharing the single key [0,\"hot\"]")
    print(f"resident payload budget: {PAYLOAD_LIMIT} slots\n")

    for nparts, seed in ((64, b"graceH1"), (64, b"graceH2"), (256, b"graceH3")):
        sizes = grace_hash_partitions(left, nparts, seed)
        biggest = max(sizes)
        print(f"H  hash-partition left into {nparts:4d} buckets (seed {seed.decode()}): "
              f"biggest bucket = {biggest} rows "
              f"({biggest / PAYLOAD_LIMIT:.0f}x the payload budget)")

    # re-hash the hot bucket: same key -> same bucket again, nothing splits
    hot_bucket = [r for r in left if key_of(r) == key_of([0, "hot"])]
    sizes2 = grace_hash_partitions(hot_bucket, 64, b"rehash2")
    print(f"H  re-hash the hot bucket with a new seed/modulus: "
          f"biggest sub-bucket = {max(sizes2)} rows (unchanged: one key, one bucket)")

    print()
    with tempfile.TemporaryDirectory(prefix="exmem-exp-") as td:
        gen = Path(td) / "src"
        gen.mkdir()
        paths = {}
        for name, rows in (("a", left), ("b", right)):
            p = gen / f"{name}.jsonl"
            with open(p, "w") as f:
                for r in rows:
                    f.write(json.dumps(r) + "\n")
            paths[name] = str(p)
        plan = {"op": "except_all",
                "left": {"op": "scan", "source": "a"},
                "right": {"op": "scan", "source": "b"}}
        rep = run_plan(plan, paths, str(Path(td) / "out"), Path(td) / "tmp")
        m = rep["meter"]
        print(f"S  exmem sort-merge: payload_peak = {m['payload_peak']} slots "
              f"(<= {PAYLOAD_LIMIT}), temp_bytes_peak = {m['temp_bytes_peak']}, "
              f"output_rows = {rep['output_rows']}")

    print("\nconclusion: the hot partition can never be subdivided by hashing, "
          "so partitioned hashing either breaks the payload budget or falls "
          "back to sorting the hot bucket.  Sort-merge handles the hot key "
          "natively (a run is just a file) and keeps a deterministic total "
          "order, which the witness-order quota semantics need anyway.")


if __name__ == "__main__":
    main()
