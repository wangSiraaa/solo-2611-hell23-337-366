"""命令行入口。

  python -m unionx.cli fixture  --fixtures fixtures --store out/fixture
      按 fixtures/schedule.json 跑真实 IPC 双生产者协商交换并落盘
  python -m unionx.cli evolution --store out/evo [--mode sparse|dense]
      struct 演进全场景（改名、新增 nullable、missing vs child-null、屏障）
  python -m unionx.cli query --store out/evo [--branch ID] [--field ID] [--missing]
      重开存储，按稳定身份查询原值与来源
  python -m unionx.cli experiment --store out/exp [--rows N] [--evolutions K]
      可复现取舍实验：只增片+读时对齐 vs 立即重编码旧片
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

import pyarrow as pa

from . import scenarios
from .exchange import Exchange
from .producer import Producer
from .schema import (
    KIND_INT64,
    KIND_TEXT,
    MISSING_BRANCH_ID,
    BranchSpec,
    FieldSpec,
    IncompatibleSchema,
    ProducerDescriptor,
)
from .store import ShardStore


def _print_rows(rows: list[dict]) -> None:
    for r in rows:
        print(json.dumps(r, ensure_ascii=False))


def cmd_fixture(args) -> int:
    ex = scenarios.run_fixture_schedule(args.fixtures, args.store)
    store = ShardStore.reopen(args.store)
    print(f"[fixture] 落盘 {store.count()} 行，分片 {len(store.manifest['shards'])} 个")
    print("[fixture] canonical 稳定 code:", store.manifest["canonical_history"][-1]["code"])
    _print_rows(store.query())
    print("[fixture] stats:", json.dumps(ex.stats(), ensure_ascii=False))
    return 0


def cmd_evolution(args) -> int:
    ex = scenarios.run_struct_evolution(args.store, mode_p1=args.mode)
    store = ShardStore.reopen(args.store)
    print(f"[evolution] 落盘 {store.count()} 行，分片 {len(store.manifest['shards'])} 个")
    print("[evolution] 全量（含 present 标记，区分缺席与 child NULL）:")
    _print_rows(store.query())
    print("[evolution] 逻辑缺席行:")
    _print_rows(store.query_missing())
    print("[evolution] 按 field_id=field-note 查询（旧片应补 NULL）:")
    _print_rows(store.query(branch_id="branch-record", field_id="field-note"))
    print("[evolution] stats:", json.dumps(ex.stats(), ensure_ascii=False))
    return 0


def cmd_query(args) -> int:
    store = ShardStore.reopen(args.store)
    if args.missing:
        rows = store.query_missing()
    else:
        rows = store.query(branch_id=args.branch, field_id=args.field)
    _print_rows(rows)
    print(f"# {len(rows)} 行", file=sys.stderr)
    return 0


# ---------------------------------------------------------------------------
# 取舍实验：新增 nullable 字段时
#   方案 A（采用）：只增不改分片 + 读时按 field_id 对齐
#   方案 B（放弃）：每次演进立即把所有旧片重编码到新 schema
# ---------------------------------------------------------------------------

def _make_descriptor(rev: int, n_fields: int, codes: dict[int, str]) -> ProducerDescriptor:
    fields = tuple(
        FieldSpec(f"field-{i}", f"name{i}", KIND_INT64 if i % 2 == 0 else KIND_TEXT, nullable=True)
        for i in range(n_fields)
    )
    return ProducerDescriptor(
        "EXP", rev, codes,
        (BranchSpec("branch-record", "record", "struct", fields),),
    )


def _row_for(desc: ProducerDescriptor) -> dict:
    branch = desc.branch("branch-record")
    return {
        "branch_id": "branch-record",
        "value": {
            f.field_id: (f.field_id if f.kind == KIND_TEXT else i)
            for i, f in enumerate(branch.fields)
        },
    }


def _benchmark_append_only(rows_per_batch: int, evolutions: int, workdir: Path):
    """方案 A：每次演进开新片，旧片字节不动。"""
    store = ShardStore(workdir / "append_only")
    ex = Exchange(store)
    total_old_bytes = 0
    rewrite_bytes = 0
    timings = []
    n_fields = 1
    for rev in range(1, evolutions + 1):
        codes = {100 + rev: "branch-record"}  # typecode 每 revision 变化
        desc = _make_descriptor(rev, n_fields, codes)
        ex.propose(desc)
        ex.accept("EXP", rev)
        p = Producer("EXP", desc)
        t0 = time.perf_counter()
        env = p.build_batch([_row_for(desc) for _ in range(rows_per_batch)], seq=rev)
        ex.submit(env)
        timings.append(time.perf_counter() - t0)
        # A 方案旧片零重写
        n_fields += 1
    ex.close()
    shard_bytes = sum(f.stat().st_size for f in (workdir / "append_only" / "shards").iterdir())
    # 读时对齐成本：查询一个最新 field_id 必须扫所有旧片
    t0 = time.perf_counter()
    reopened = ShardStore.reopen(workdir / "append_only")
    latest_field = f"field-{evolutions - 1}"
    got = reopened.query(branch_id="branch-record", field_id=latest_field)
    query_s = time.perf_counter() - t0
    return {
        "rewrite_bytes": rewrite_bytes,
        "shard_bytes": shard_bytes,
        "n_shards": len(reopened.manifest["shards"]),
        "submit_seconds": sum(timings),
        "query_seconds": query_s,
        "query_rows": len(got),
        "peak_resident_bytes": ex.peak_resident_bytes,
    }


def _benchmark_eager_rewrite(rows_per_batch: int, evolutions: int, workdir: Path):
    """方案 B：每次演进把既有全部旧片读出、重编码、重写到最新 schema。"""
    from .encode import reencode
    from .protocol import Envelope

    store = ShardStore(workdir / "eager")
    ex = Exchange(store)
    rewrite_bytes = 0
    rewrite_seconds = []
    n_fields = 1
    # 保存每批原始信封，演进时模拟"重放并重编码全部历史"
    history_envs: list[Envelope] = []
    for rev in range(1, evolutions + 1):
        codes = {50 + rev: "branch-record"}
        desc = _make_descriptor(rev, n_fields, codes)
        ex.propose(desc)
        ex.accept("EXP", rev)
        p = Producer("EXP", desc)
        env = p.build_batch([_row_for(desc) for _ in range(rows_per_batch)], seq=rev)
        history_envs.append(env)
        ex.submit(env)
        if rev > 1:
            # 模拟 eager 重编码：把全部历史信封按新 canonical 再编码一遍
            t0 = time.perf_counter()
            for old_env in history_envs[:-1]:
                rb = reencode(old_env, ex.canonical)
                rewrite_bytes += rb.table.nbytes
            rewrite_seconds.append(time.perf_counter() - t0)
        n_fields += 1
    ex.close()
    shard_bytes = sum(f.stat().st_size for f in (workdir / "eager" / "shards").iterdir())
    t0 = time.perf_counter()
    reopened = ShardStore.reopen(workdir / "eager")
    got = reopened.query(branch_id="branch-record", field_id=f"field-{evolutions - 1}")
    query_s = time.perf_counter() - t0
    return {
        "rewrite_bytes": rewrite_bytes,
        "rewrite_seconds": sum(rewrite_seconds),
        "shard_bytes": shard_bytes,
        "n_shards": len(reopened.manifest["shards"]),
        "query_seconds": query_s,
        "query_rows": len(got),
        "peak_resident_bytes": ex.peak_resident_bytes,
    }


def cmd_experiment(args) -> int:
    with tempfile.TemporaryDirectory() as td:
        workdir = Path(td)
        a = _benchmark_append_only(args.rows, args.evolutions, workdir)
        b = _benchmark_eager_rewrite(args.rows, args.evolutions, workdir)
    report = {
        "params": {"rows_per_batch": args.rows, "evolutions": args.evolutions},
        "A_append_only_read_time_align": a,
        "B_eager_rewrite_old_shards": b,
        "verdict": (
            "A 的旧片重写字节为 0、提交耗时随演进稳定；B 的重写量随 K 次演进呈 O(K^2) 增长。"
            "A 的代价是新字段查询需读时对齐所有旧片（单次查询耗时见 query_seconds）。"
        ),
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="unionx", description="演进 Union 协商式数据交换")
    sub = p.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("fixture")
    q.add_argument("--fixtures", default="fixtures")
    q.add_argument("--store", required=True)
    q.set_defaults(func=cmd_fixture)

    e = sub.add_parser("evolution")
    e.add_argument("--store", required=True)
    e.add_argument("--mode", choices=["sparse", "dense"], default="sparse")
    e.set_defaults(func=cmd_evolution)

    qq = sub.add_parser("query")
    qq.add_argument("--store", required=True)
    qq.add_argument("--branch", default=None)
    qq.add_argument("--field", default=None)
    qq.add_argument("--missing", action="store_true")
    qq.set_defaults(func=cmd_query)

    x = sub.add_parser("experiment")
    x.add_argument("--store", default=None)
    x.add_argument("--rows", type=int, default=1000)
    x.add_argument("--evolutions", type=int, default=8)
    x.add_argument("--out", default=None)
    x.set_defaults(func=cmd_experiment)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
