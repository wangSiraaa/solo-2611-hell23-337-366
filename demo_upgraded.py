#!/usr/bin/env python3
"""End-to-end demonstration of the upgraded archive reader.

Runs the real fixture, then a generated 100x4096 workload, and prints the
measured numbers.  This is a thin harness over the ``wsarchive`` package; it is
not a reimplementation of the baseline ``demo.py``.

Usage:  python demo_upgraded.py [--workload-dir DIR]
"""

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

from wsarchive import Negotiation, Reader, build_index
from wsarchive.corpus import Spec, generate, workload_messages
from wsarchive.index import serialize, write_index


def report_fixture():
    root = Path(__file__).resolve().parent
    data = (root / "fixtures" / "archive.frames").read_bytes()
    neg = Negotiation.from_json(
        json.loads((root / "fixtures" / "negotiation.json").read_text())
    )
    truth = {
        m["message"]: m["sha256"]
        for m in json.loads((root / "fixtures" / "source-truth.json").read_text())
    }
    index = build_index(data, neg)
    reader = Reader(data, index)
    cold = reader.query([99], mode="cold")
    got = hashlib.sha256(cold.messages[0]).hexdigest()
    print("== fixture archive ==")
    print(f"messages={len(index.messages)} frames={len(index.frames)} "
          f"checkpoints={len(index.checkpoints)} density={index.density}")
    print(f"message 99 sha256={got} matches_source_truth={got == truth[99]}")
    print(f"cold inflate bytes for message 99: {cold.stats.inflated_bytes} "
          f"(prefix {cold.stats.prefix_inflated_bytes}, "
          f"output {cold.stats.output_bytes})")
    print(f"permanent dictionary state: {index.dictionary_state_bytes} B")
    print(f"ordinary index metadata:    {index.metadata_bytes} B")
    return index


def report_workload(workload_dir=None):
    msgs = workload_messages(100, 4096)
    spec = Spec(msgs, [True] * 100, takeover=True, fragments=(1, 2, 3), control="mixed")
    data, manifest = generate(spec)
    neg = Negotiation.from_json(
        {
            "permessage_deflate": True,
            "server_no_context_takeover": False,
            "server_max_window_bits": 15,
        }
    )
    index = build_index(data, neg)
    reader = Reader(data, index)
    total = 0
    worst = 0
    body = 0
    for i in range(100):
        st = reader.query([i], mode="cold").stats
        total += st.inflated_bytes
        worst = max(worst, st.inflated_bytes)
        body += st.compressed_body_bytes
    seq = reader.decode_sequential()
    assert all(
        hashlib.sha256(seq[i]).hexdigest() == manifest[i]["sha256"]
        for i in range(100)
    )
    order = list(range(99, -1, -1))
    batch = reader.query(order, mode="batch")
    cold_all = reader.query(order, mode="cold")
    print("\n== 100 x 4096-byte workload ==")
    print(f"wire={len(data)} B  density={index.density}  "
          f"checkpoints={len(index.checkpoints)}")
    print(f"dictionary state={index.dictionary_state_bytes} B "
          f"(<=256KiB: {index.dictionary_state_bytes <= 256*1024})")
    print(f"index metadata={index.metadata_bytes} B "
          f"(<=64KiB: {index.metadata_bytes <= 64*1024})")
    print(f"worst single cold query inflated={worst} B "
          f"(<=64KiB: {worst <= 64*1024})")
    print(f"total inflated across all 100 cold queries={total} B")
    print(f"raw compressed body bytes read={body} B")
    print(f"batch inflated={batch.stats.inflated_bytes} B; "
          f"cold re-run inflated={cold_all.stats.inflated_bytes} B; "
          f"batch saved={cold_all.stats.inflated_bytes - batch.stats.inflated_bytes} B")
    print(f"batch output order preserved="
          f"{all(hashlib.sha256(batch.messages[i]).hexdigest() == manifest[order[i]]['sha256'] for i in range(100))}")

    if workload_dir:
        d = Path(workload_dir)
        d.mkdir(parents=True, exist_ok=True)
        (d / "workload.frames").write_bytes(data)
        (d / "workload.negotiation.json").write_text(json.dumps(spec and {
            "permessage_deflate": True,
            "server_no_context_takeover": False,
            "server_max_window_bits": 15,
        }))
        write_index(index, d / "index", d / "workload.frames")
        print(f"\nwrote workload corpus and index under {d}")
    return index


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload-dir", default=None)
    args = ap.parse_args()
    report_fixture()
    report_workload(args.workload_dir)


if __name__ == "__main__":
    main()
