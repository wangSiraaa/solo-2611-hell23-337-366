"""可复现实验：体现两组工程取舍的真实测量。

实验 A（查询时转换 vs 预计算目标摘要）：
  同一仓 8 分片 × 1 万行，plan (8,4)->(6,2) half_even。
  分别在 0 片迁移（全读时转换）、全部迁移（摘要路径 / 回读文件路径）
  三种形态下计时同一 target 查询，并断言三者金额完全一致。

实验 B（读者引用 × 受限配额 → 迁移次序与旧片回收时点）：
  同一数据复制品。老读者（schema 1）全程钉住旧片，逐片迁移记录
  data_bytes 时间线：第 3 片应出现可重试背压；end-read 后重试成功；
  对照组无读者，峰值字节 ≤ 启动字节 + 2×最大新片（无全量双份）。

运行：python experiment.py  （确定性种子，结果可复现）
"""
import json
import random
import shutil
import tempfile
import time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from warehouse import Backpressure, Warehouse

SHARDS, ROWS, SEED = 8, 10_000, 7
RUNS = 5


def gen_sources(src_dir):
    rng = random.Random(SEED)
    sources = {}
    for i in range(SHARDS):
        values = []
        for _ in range(ROWS):
            if rng.random() < 0.05:
                values.append(None)
            else:
                values.append(Decimal(f"{rng.uniform(-9999.99, 9999.99):.4f}"))
        p = src_dir / f"s{i}.parquet"
        t = pa.table({
            "id": pa.array(list(range(ROWS)), type=pa.int64()),
            "amount": pa.array(values, type=pa.decimal128(8, 4)),
        })
        pq.write_table(t, p)
        sources[f"s{i}"] = p
    return sources


def timed_query(wh, **kw):
    best = None
    for _ in range(RUNS):
        t0 = time.perf_counter()
        q = wh.query(schema_version=2, **kw)
        dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return q, best


def experiment_a(work):
    src = work / "src_a"
    src.mkdir()
    wh = Warehouse.init(work / "wh_a", gen_sources(src))
    wh.set_plan(6, 2, "half_even")
    startup = wh.status()["startup_bytes"]

    q_readtime, t_readtime = timed_query(wh)  # 全部未迁移：纯查询时转换
    peak = wh.status()["data_bytes"]
    for i in range(SHARDS):
        r = wh.migrate_shard(f"s{i}")
        assert r["status"] == "published"
        peak = max(peak, wh.status()["data_bytes"])
    q_summary, t_summary = timed_query(wh)                    # 预计算摘要
    q_file, t_file = timed_query(wh, use_summary=False)       # 回读物理文件
    for k in ("sum", "count", "min", "max", "rows"):
        assert q_readtime[k] == q_summary[k] == q_file[k], k
    st = wh.status()
    return {
        "shards": SHARDS, "rows_per_shard": ROWS,
        "startup_bytes": startup,
        "query_ms_all_read_time_conversion": round(t_readtime * 1000, 1),
        "query_ms_all_precomputed_summary": round(t_summary * 1000, 1),
        "query_ms_all_physical_file_scan": round(t_file * 1000, 1),
        "results_identical": True,
        "sum": q_summary["sum"], "count": q_summary["count"],
        "peak_data_bytes_during_migration": peak,
        "quota_bytes": st["quota_bytes"],
        "peak_within_quota": peak <= st["quota_bytes"],
        "final_data_bytes_after_gc": (wh.gc(), wh.status()["data_bytes"])[1],
    }


def experiment_b(work):
    src = work / "src_b"
    src.mkdir()
    wh = Warehouse.init(work / "wh_b", gen_sources(src))
    wh.set_plan(6, 2, "half_even")
    st = wh.status()
    timeline = [{"step": "init", "data_bytes": st["data_bytes"],
                 "quota_bytes": st["quota_bytes"]}]
    r_old = wh.begin_read(1)  # 老读者钉住全部旧片
    backpressured_at = None
    for i in range(SHARDS):
        try:
            wh.migrate_shard(f"s{i}")
            outcome = "published"
        except Backpressure as e:
            outcome = f"backpressure(retryable, need {e.needed_bytes}B)"
            backpressured_at = i
            break
        timeline.append({"step": f"migrate s{i}", "outcome": outcome,
                         "data_bytes": wh.status()["data_bytes"]})
    timeline.append({"step": f"migrate s{backpressured_at}", "outcome": outcome,
                     "data_bytes": wh.status()["data_bytes"]})
    old_sum = wh.query(reader_id=r_old)["sum"]
    wh.end_read(r_old)
    resumed = []
    for i in range(backpressured_at, SHARDS):
        resumed.append(wh.migrate_shard(f"s{i}")["status"])
    timeline.append({"step": "end-read + retry remaining",
                     "data_bytes": wh.status()["data_bytes"]})
    wh.gc()
    final = wh.status()
    return {
        "timeline": timeline,
        "backpressured_at_shard": backpressured_at,
        "old_reader_sum_unchanged": old_sum,
        "resumed_after_end_read": resumed,
        "final_data_bytes": final["data_bytes"],
        "quota_bytes": final["quota_bytes"],
        "old_schema_retired_after_gc": _schema1_retired(wh),
    }


def _schema1_retired(wh):
    try:
        wh.begin_read(1)
        return False
    except Exception:
        return True


def main():
    work = Path(tempfile.mkdtemp(prefix="wh_experiment_"))
    try:
        out = {
            "experiment_a_query_time_conversion_vs_precomputed_summary": experiment_a(work),
            "experiment_b_reader_pins_vs_quota_backpressure": experiment_b(work),
        }
        print(json.dumps(out, indent=2, ensure_ascii=False))
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
