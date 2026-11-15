"""External-memory set-operation engine (UNION ALL / INTERSECT ALL / EXCEPT ALL).

Semantics (identical to the in-memory reference in operators.py):

* Occurrence identity = (AST scan path, source, source row number).  A source
  scanned twice by the AST yields two independent occurrence families; two
  rows with equal values never share an identity.
* UNION ALL concatenates left occurrences then right occurrences.
* EXCEPT ALL consumes, per equality group, the earliest `m` left occurrences
  (m = right multiplicity); INTERSECT ALL keeps the earliest min(n, m).
  Survivors are emitted with their left witness, in surviving-witness order.
* The AST is evaluated exactly as parenthesised -- no commutativity or
  associativity rewriting is ever applied.

Strategy: external sort + merge-join (see REPORT.md for the tradeoff study
against partitioned hashing).  Per EXCEPT/INTERSECT node:

  1. right subtree -> external sort of row keys -> streaming aggregation into
     a sorted (key -> count) spill file.  Right witnesses are never needed.
  2. left subtree -> external sort by (row_key, path_tuple, rownum), so that
     within each equality group occurrences arrive in left-witness order.
  3. single forward merge-join consumes the quota and writes survivors.
  4. survivors are re-sorted by (path_tuple, rownum) so the node's output
     order is exactly the surviving-witness order; spill partitioning can
     never change which sources are retained or their order.

Intermediate results are streams; every stream is consumed exactly once by its
parent and its spill files are deleted immediately afterwards, so the tree is
never materialised all at once.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

from .encoding import (
    INT64_MIN, INT64_MAX, encode_record, split_record, decode_key, read_uvarint,
    uvarint_bytes,
)
from .meter import Meter, Budget
from .streams import (
    Segment, SpillWriter, SpillReader, SourceStream, SegmentStream,
    ChainedStream, sort_stream, IO_BUFFER_BYTES,
)

MAX_COLS = 4
MAX_ROW_BYTES = 1024
MAX_SOURCE_ROWS = 100_000
MAX_SOURCES = 4
MAX_LEAVES = 16
MAX_DEPTH = 6

SET_OPS = ("union_all", "intersect_all", "except_all")


class PlanError(ValueError):
    pass


class DataError(ValueError):
    pass


# ---------------------------------------------------------------------------
# Plan compilation
# ---------------------------------------------------------------------------

class Leaf:
    __slots__ = ("id", "path", "path_tuple", "source")

    def __init__(self, leaf_id, path, path_tuple, source):
        self.id = leaf_id
        self.path = path            # e.g. "root.L.R" -- same shape as reference
        self.path_tuple = path_tuple  # e.g. (0, 1); 0 = left, 1 = right
        self.source = source


class Node:
    __slots__ = ("op", "left", "right", "leaf", "path")

    def __init__(self, op, path, left=None, right=None, leaf=None):
        self.op = op
        self.path = path
        self.left = left
        self.right = right
        self.leaf = leaf


def compile_plan(plan, source_names):
    leaves = []

    def build(spec, path, pt, depth):
        if depth > MAX_DEPTH:
            raise PlanError(f"AST deeper than {MAX_DEPTH} at {path}")
        if not isinstance(spec, dict):
            raise PlanError(f"node at {path} is not an object")
        op = spec.get("op")
        if op == "scan":
            src = spec.get("source")
            if src not in source_names:
                raise PlanError(f"scan at {path} references unknown source {src!r}")
            leaf = Leaf(len(leaves), path, pt, src)
            leaves.append(leaf)
            if len(leaves) > MAX_LEAVES:
                raise PlanError(f"more than {MAX_LEAVES} scan leaves")
            return Node("scan", path, leaf=leaf)
        if op in SET_OPS:
            left = build(spec.get("left"), path + ".L", pt + (0,), depth + 1)
            right = build(spec.get("right"), path + ".R", pt + (1,), depth + 1)
            return Node(op, path, left=left, right=right)
        raise PlanError(f"unknown op {op!r} at {path}")

    root = build(plan, "root", (), 1)
    return root, leaves


def plan_depth(node):
    if node.op == "scan":
        return 1
    return 1 + max(plan_depth(node.left), plan_depth(node.right))


# ---------------------------------------------------------------------------
# Run context
# ---------------------------------------------------------------------------

class Ctx:
    MAX_ROW_BYTES = MAX_ROW_BYTES
    MAX_SOURCE_ROWS = MAX_SOURCE_ROWS

    def __init__(self, workdir, meter, budget, leaves, sources, schema,
                 audit_groups_path=None):
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.meter = meter
        self.budget = budget
        self.leaves = leaves
        self.sources = sources          # name -> file path
        self.schema = schema
        self.audit_groups_path = audit_groups_path
        self.tmp_seq = 0
        self.summaries = []             # per-operator audit summaries
        self.source_stats = {}
        self.expected_ncols = None

    def new_temp_path(self, name):
        self.tmp_seq += 1
        safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in name)
        return self.workdir / f"{self.tmp_seq:05d}-{safe}.spill"

    def validate_row(self, row):
        if not isinstance(row, list):
            raise DataError("row must be a JSON array")
        n = len(row)
        if n == 0 or n > MAX_COLS:
            raise DataError(f"row must have 1..{MAX_COLS} columns, got {n}")
        if self.expected_ncols is None:
            self.expected_ncols = n
        elif n != self.expected_ncols:
            raise DataError(
                f"inconsistent column count: expected {self.expected_ncols}, got {n}")
        cols = self.schema["columns"] if self.schema else None
        if cols is not None and len(cols) != n:
            raise DataError(
                f"schema has {len(cols)} columns but row has {n}")
        for i, v in enumerate(row):
            if v is None:
                if cols is not None and not cols[i].get("nullable", False):
                    raise DataError(f"NULL in non-nullable column {i}")
            elif isinstance(v, bool):
                raise DataError("bool is not a supported column value")
            elif isinstance(v, int):
                if not (INT64_MIN <= v <= INT64_MAX):
                    raise DataError(f"int out of int64 range: {v}")
                if cols is not None and cols[i]["type"] != "int64":
                    raise DataError(f"column {i} expects {cols[i]['type']}, got int")
            elif isinstance(v, str):
                if cols is not None and cols[i]["type"] != "text":
                    raise DataError(f"column {i} expects {cols[i]['type']}, got text")
            else:
                raise DataError(f"unsupported column value: {v!r}")

    def cleanup(self):
        for path in list(self.meter.temp_files):
            self.meter.temp_drop(path)


def validate_schema(schema):
    if not isinstance(schema, dict) or not isinstance(schema.get("columns"), list):
        raise PlanError("schema must be an object with a 'columns' list")
    cols = schema["columns"]
    if not (1 <= len(cols) <= MAX_COLS):
        raise PlanError(f"schema must have 1..{MAX_COLS} columns")
    for c in cols:
        if c.get("type") not in ("int64", "text"):
            raise PlanError(f"bad column type: {c.get('type')!r}")
        if "nullable" in c and not isinstance(c["nullable"], bool):
            raise PlanError("nullable must be a boolean")
    return schema


# ---------------------------------------------------------------------------
# Sort keys
# ---------------------------------------------------------------------------

def _left_sortkey(ctx):
    leaves = ctx.leaves

    def fn(item):
        pid, rownum, key = split_record(item)
        return (key, leaves[pid].path_tuple, rownum)

    return fn


def _witness_sortkey(ctx):
    leaves = ctx.leaves

    def fn(item):
        pid, rownum, _ = split_record(item)
        return (leaves[pid].path_tuple, rownum)

    return fn


def _record_key(rec):
    _, _, key = split_record(rec)
    return key


# ---------------------------------------------------------------------------
# Right-side aggregation: stream -> sorted (key -> count) spill file
# ---------------------------------------------------------------------------

class _KeyAggregator:
    def __init__(self, ctx, name):
        self.w = SpillWriter(ctx, name, kind="counts")
        self.cur_key = None
        self.cur_count = 0
        self.stats = {"total": 0, "distinct": 0}

    def feed(self, key):
        if key == self.cur_key:
            self.cur_count += 1
        else:
            self._flush()
            self.cur_key = key
            self.cur_count = 1
        self.stats["total"] += 1

    def _flush(self):
        if self.cur_key is None:
            return
        self.w.write(uvarint_bytes(self.cur_count) + self.cur_key)
        self.stats["distinct"] += 1

    def finish(self):
        self._flush()
        return self.w.finish()


def count_keys(stream, ctx, name):
    agg = _KeyAggregator(ctx, name)
    sort_stream(stream, lambda k: k, ctx, name + "-keys",
                item_of=_record_key, emit=agg.feed)
    return agg.finish(), agg.stats


# ---------------------------------------------------------------------------
# Merge join for EXCEPT ALL / INTERSECT ALL
# ---------------------------------------------------------------------------

def join_setop(left_seg, counts_seg, op, ctx, node, audit):
    """left_seg sorted by (key, path_tuple, rownum); counts_seg sorted by key.

    Consumes quotas in left-witness order inside each equality group and
    returns (survivor_segment_unsorted, summary).  Both inputs are dropped.
    """
    meter = ctx.meter
    left_reader = SpillReader(ctx, left_seg, ctx.budget.reader_buf)
    counts_reader = SpillReader(ctx, counts_seg, ctx.budget.counts_buf)
    writer = SpillWriter(ctx, f"join{node.path}", kind="records")
    matched = 0
    emitted = 0
    left_in = 0

    def next_count():
        item = counts_reader.read()
        if item is None:
            return None
        c, pos = read_uvarint(item, 0)
        return [item[pos:], c, c]  # key, remaining, total

    cur = next_count()
    g_key = None
    g_n = g_m = g_consumed = g_emitted = 0

    def flush_group():
        if audit is not None and g_key is not None:
            audit.write({
                "node": node.path, "op": op, "key": decode_key(g_key),
                "left": g_n, "right": g_m,
                "consumed": g_consumed, "emitted": g_emitted})

    try:
        while True:
            item = left_reader.read()
            if item is None:
                break
            left_in += 1
            _, _, key = split_record(item)
            if key != g_key:
                flush_group()
                g_key, g_n, g_m, g_consumed, g_emitted = key, 0, 0, 0, 0
                while cur is not None and cur[0] < key:
                    cur = next_count()
                if cur is not None and cur[0] == key:
                    g_m = cur[2]
            g_n += 1
            hit = cur is not None and cur[0] == key and cur[1] > 0
            if hit:
                cur[1] -= 1
                matched += 1
                g_consumed += 1
            survive = (op == "except_all") != hit
            if survive:
                writer.write(item)
                emitted += 1
                g_emitted += 1
        flush_group()
    finally:
        left_reader.close()
        counts_reader.close()
    seg = writer.finish()
    meter.temp_drop(left_seg.path)
    meter.temp_drop(counts_seg.path)

    if op == "except_all":
        ok = emitted == left_in - matched
    else:
        ok = emitted == matched
    summary = {
        "path": node.path, "op": op,
        "left_in": left_in, "matched": matched, "emitted": emitted,
        "conservation_ok": ok,
    }
    if not ok:
        raise AssertionError(f"count conservation violated at {node.path}: {summary}")
    return seg, summary


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

class _CountingStream:
    """Wraps a stream returned early (empty-right EXCEPT) so the operator
    summary still gets real left_in/emitted counts once consumed."""

    def __init__(self, inner, summary):
        self.inner = inner
        self.summary = summary
        self.rows = None

    def open(self, ctx):
        n = 0
        try:
            for item in self.inner.open(ctx):
                n += 1
                yield item
        finally:
            self.rows = n
            self.summary["left_in"] = n
            self.summary["emitted"] = n

    def dispose(self, ctx):
        self.inner.dispose(ctx)


def eval_node(node, ctx):
    if node.op == "scan":
        return SourceStream(node.leaf, ctx.sources[node.leaf.source])
    if node.op == "union_all":
        return ChainedStream(
            [eval_node(node.left, ctx), eval_node(node.right, ctx)], node.path)

    # except_all / intersect_all
    left = eval_node(node.left, ctx)
    right = eval_node(node.right, ctx)
    counts_seg, cstats = count_keys(right, ctx, f"counts{node.path}")
    if cstats["total"] == 0:
        ctx.meter.temp_drop(counts_seg.path)
        summary = {
            "path": node.path, "op": node.op,
            "left_in": None, "right_in": 0, "right_distinct": 0,
            "matched": 0, "emitted": None, "conservation_ok": True,
            "note": "empty right side",
        }
        ctx.summaries.append(summary)
        if node.op == "except_all":
            return _CountingStream(left, summary)  # already in witness order
        left.dispose(ctx)
        summary["left_in"] = 0
        summary["emitted"] = 0
        empty = SpillWriter(ctx, f"empty{node.path}").finish()
        return SegmentStream(empty)

    sorted_left = sort_stream(left, _left_sortkey(ctx), ctx, f"left{node.path}")
    surv, summary = join_setop(sorted_left, counts_seg, node.op, ctx, node,
                               ctx.audit_writer)
    summary["right_in"] = cstats["total"]
    summary["right_distinct"] = cstats["distinct"]
    ctx.summaries.append(summary)
    ordered = sort_stream(SegmentStream(surv), _witness_sortkey(ctx), ctx,
                          f"order{node.path}")
    return SegmentStream(ordered)


class _GroupAuditWriter:
    """Optional per-group audit log (key, n, m, consumed, emitted).  Written
    to a temp file and committed (renamed) at end of run, like the output."""

    def __init__(self, ctx, final_path):
        self.ctx = ctx
        self.final_path = str(final_path)
        self.tmp_path = self.final_path + ".tmp"
        ctx.meter.temp_register(self.tmp_path)
        ctx.meter.hold(ctx.budget.writer_buf, "audit-writer")
        self.f = open(self.tmp_path, "w", encoding="utf-8",
                      buffering=IO_BUFFER_BYTES)
        self.buf = []
        self.nbytes = 0
        self.rows = 0

    def write(self, rec):
        self.buf.append(json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
        if len(self.buf) >= self.ctx.budget.writer_buf:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        data = "".join(l + "\n" for l in self.buf)
        self.f.write(data)
        n = len(data.encode("utf-8"))
        self.nbytes += n
        self.rows += len(self.buf)
        self.ctx.meter.io_write += n
        self.buf.clear()
        self.ctx.meter.temp_sync(self.tmp_path, self.nbytes, self.rows)

    def commit(self):
        self.flush()
        self.f.close()
        self.ctx.meter.release(self.ctx.budget.writer_buf, "audit-writer")
        os.replace(self.tmp_path, self.final_path)
        self.ctx.meter.temp_commit(self.tmp_path)

    def abort(self):
        try:
            self.f.close()
        finally:
            self.ctx.meter.release(self.ctx.budget.writer_buf, "audit-writer")
            if str(self.tmp_path) in self.ctx.meter.temp_files:
                self.ctx.meter.temp_drop(self.tmp_path)


class _JsonlOutputWriter:
    """Final output: metered as temp while being written, committed on rename."""

    def __init__(self, ctx, final_path):
        self.ctx = ctx
        self.final_path = str(final_path)
        self.tmp_path = self.final_path + ".tmp"
        ctx.meter.temp_register(self.tmp_path)
        ctx.meter.hold(ctx.budget.writer_buf, "output-writer")
        self.f = open(self.tmp_path, "w", encoding="utf-8",
                      buffering=IO_BUFFER_BYTES)
        self.buf = []
        self.nbytes = 0
        self.rows = 0

    def write_record_item(self, item):
        pid, rownum, key = split_record(item)
        leaf = self.ctx.leaves[pid]
        line = json.dumps(
            {"witness": f"{leaf.path}:{leaf.source}:{rownum}",
             "row": decode_key(key)},
            ensure_ascii=False, separators=(",", ":"))
        self.buf.append(line)
        if len(self.buf) >= self.ctx.budget.writer_buf:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        data = "".join(l + "\n" for l in self.buf)
        self.f.write(data)
        n = len(data.encode("utf-8"))
        self.nbytes += n
        self.rows += len(self.buf)
        self.ctx.meter.io_write += n
        self.buf.clear()
        self.ctx.meter.temp_sync(self.tmp_path, self.nbytes, self.rows)

    def commit(self):
        self.flush()
        self.f.close()
        self.ctx.meter.release(self.ctx.budget.writer_buf, "output-writer")
        os.replace(self.tmp_path, self.final_path)
        self.ctx.meter.temp_commit(self.tmp_path)

    def abort(self):
        """Error path: close and drop the uncommitted output."""
        try:
            self.f.close()
        finally:
            self.ctx.meter.release(self.ctx.budget.writer_buf, "output-writer")
            if str(self.tmp_path) in self.ctx.meter.temp_files:
                self.ctx.meter.temp_drop(self.tmp_path)


# ---------------------------------------------------------------------------
# Top-level driver
# ---------------------------------------------------------------------------

def run_plan(plan, sources, out_path, workdir, schema=None,
             audit_groups_path=None, budget=None, payload_limit=None):
    """Execute `plan` over JSONL `sources` (name -> path); write JSONL output.

    Returns a report dict with meter peaks, I/O totals and per-operator
    count/cancellation summaries.
    """
    if not (1 <= len(sources) <= MAX_SOURCES):
        raise PlanError(f"need 1..{MAX_SOURCES} sources, got {len(sources)}")
    for name, p in sources.items():
        if not os.path.isfile(p):
            raise PlanError(f"source {name!r} not found: {p}")
    if schema is not None:
        schema = validate_schema(schema)
    root, leaves = compile_plan(plan, set(sources))

    budget = budget or Budget()
    if payload_limit is not None:
        budget.payload_limit = payload_limit
    meter = Meter(budget)
    ctx = Ctx(workdir, meter, budget, leaves, dict(sources), schema,
              audit_groups_path)
    ctx.audit_writer = (_GroupAuditWriter(ctx, audit_groups_path)
                        if audit_groups_path else None)

    t0 = time.monotonic()
    out = None
    try:
        root_stream = eval_node(root, ctx)
        out = _JsonlOutputWriter(ctx, out_path)
        n_out = 0
        for item in root_stream.open(ctx):
            out.write_record_item(item)
            n_out += 1
        root_stream.dispose(ctx)
        out.commit()
        if ctx.audit_writer is not None:
            ctx.audit_writer.commit()
        if meter.temp_files:
            raise AssertionError(f"leaked temp files: {list(meter.temp_files)}")
    finally:
        if out is not None and str(out.tmp_path) in meter.temp_files:
            out.abort()
        if (ctx.audit_writer is not None
                and str(ctx.audit_writer.tmp_path) in meter.temp_files):
            ctx.audit_writer.abort()
        ctx.cleanup()

    report = {
        "plan_leaves": len(leaves),
        "plan_depth": plan_depth(root),
        "sources": ctx.source_stats,
        "input_bytes_total": sum(s["bytes"] for s in ctx.source_stats.values()),
        "output_rows": n_out,
        "operators": ctx.summaries,
        "meter": meter.snapshot(),
        "config": {
            "engine": "stdlib external sort + merge-join",
            "budget": budget.describe(),
            "sqlite": "not used (every byte/slot metered directly)",
            "python_io_buffer_bytes_per_file": IO_BUFFER_BYTES,
            "library_buffer_ceiling_bytes": 4 * 1024 * 1024,
            "library_buffer_worst_case_bytes": IO_BUFFER_BYTES * (budget.merge_fanin + 4),
        },
        "wall_seconds": round(time.monotonic() - t0, 3),
    }
    return report
