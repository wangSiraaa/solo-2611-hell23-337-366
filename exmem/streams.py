"""Spill files, metered readers/writers, occurrence streams and external sort.

A *spill file* is a sequence of framed items (uvarint length + bytes).  Items
are occurrence records, bare row keys, or (count, key) aggregates depending on
the phase.  All spill files live under the run workdir and are registered with
the Meter; they are dropped as soon as their consumer has drained them.

Python file objects are opened with an explicit 8 KiB buffer.  With at most
(merge_fanin + a few) files open at once, this library-level buffering stays
under ~1.1 MiB -- disclosed in the run report next to the (unused) 4 MiB
SQLite-cache allowance.  We do not use SQLite: every byte and every payload
slot is metered directly here.
"""
from __future__ import annotations

import heapq
import json

from .encoding import uvarint_bytes, frame
from .meter import Budget

IO_BUFFER_BYTES = 8192  # per open file; disclosed as library-level buffering


class Segment:
    """A spilled file of framed items."""
    __slots__ = ("path", "items", "nbytes", "kind")

    def __init__(self, path, items, nbytes, kind):
        self.path = path
        self.items = items
        self.nbytes = nbytes
        self.kind = kind

    def __repr__(self):
        return f"Segment({self.path}, items={self.items}, bytes={self.nbytes}, {self.kind})"


class SpillWriter:
    """Buffered, metered writer for one spill file."""

    def __init__(self, ctx, name, kind="records", buf_capacity=None):
        self.ctx = ctx
        self.cap = buf_capacity or ctx.budget.writer_buf
        self.path = ctx.new_temp_path(name)
        ctx.meter.hold(self.cap, f"writer:{name}")
        self.f = open(self.path, "wb", buffering=IO_BUFFER_BYTES)
        ctx.meter.temp_register(self.path)
        self.buf = []
        self.items = 0
        self.nbytes = 0
        self.kind = kind

    def write(self, item: bytes):
        self.buf.append(item)
        if len(self.buf) >= self.cap:
            self.flush()

    def flush(self):
        if not self.buf:
            return
        data = b"".join(frame(i) for i in self.buf)
        self.f.write(data)
        self.nbytes += len(data)
        self.items += len(self.buf)
        self.ctx.meter.io_write += len(data)
        self.buf.clear()
        self.ctx.meter.temp_sync(self.path, self.nbytes, self.items)

    def finish(self) -> Segment:
        self.flush()
        self.f.close()
        self.ctx.meter.release(self.cap, "writer")
        return Segment(self.path, self.items, self.nbytes, self.kind)


class SpillReader:
    """Buffered, metered forward-only reader for one spill file."""

    def __init__(self, ctx, segment, buf_capacity=None):
        self.ctx = ctx
        self.cap = buf_capacity or ctx.budget.reader_buf
        ctx.meter.hold(self.cap, "reader")
        self.f = open(segment.path, "rb", buffering=IO_BUFFER_BYTES)
        self.buf = []
        self.eof = False

    def _read_frame(self):
        shift = 0
        length = 0
        hdr = 0
        while True:
            b = self.f.read(1)
            if not b:
                if hdr == 0:
                    return None
                raise IOError("truncated spill frame header")
            hdr += 1
            byte = b[0]
            length |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        item = self.f.read(length)
        if len(item) != length:
            raise IOError("truncated spill frame body")
        self.ctx.meter.io_read += hdr + length
        return item

    def read(self):
        if not self.buf and not self.eof:
            while len(self.buf) < self.cap:
                item = self._read_frame()
                if item is None:
                    self.eof = True
                    break
                self.buf.append(item)
        if not self.buf:
            return None
        return self.buf.pop(0)

    def close(self):
        if self.f is not None:
            self.f.close()
            self.f = None
            self.ctx.meter.release(self.cap, "reader")

    def __del__(self):  # safety net; normal path closes explicitly
        try:
            self.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Occurrence streams.  A stream yields occurrence records exactly once, in the
# canonical output order of its subtree: sort key (path_tuple, rownum).
# ---------------------------------------------------------------------------

class Stream:
    rows = None  # known after (or before) consumption depending on subclass

    def open(self, ctx):
        raise NotImplementedError

    def dispose(self, ctx):
        raise NotImplementedError


class SourceStream(Stream):
    """A scan leaf: reads the source JSONL file directly (no temp copy)."""

    def __init__(self, leaf, path):
        self.leaf = leaf
        self.path = path
        self.rows = None
        self._opened = False

    def open(self, ctx):
        if self._opened:
            raise AssertionError("stream consumed twice")
        self._opened = True
        ctx.meter.hold(4, "scan")
        stats = ctx.source_stats.setdefault(
            self.leaf.source, {"rows": 0, "bytes": 0, "scans": 0})
        stats["scans"] += 1
        n = 0
        try:
            with open(self.path, "rb", buffering=IO_BUFFER_BYTES) as f:
                for raw in f:
                    content = raw.rstrip(b"\r\n")
                    if len(content) > ctx.MAX_ROW_BYTES:
                        from .engine import DataError
                        raise DataError(
                            f"{self.path}: row {n} exceeds {ctx.MAX_ROW_BYTES} bytes")
                    row = json.loads(content)
                    ctx.validate_row(row)
                    ctx.meter.io_read += len(raw)
                    stats["rows"] += 1
                    stats["bytes"] += len(raw)
                    if n >= ctx.MAX_SOURCE_ROWS:
                        from .engine import DataError
                        raise DataError(
                            f"{self.path}: more than {ctx.MAX_SOURCE_ROWS} rows")
                    from .encoding import encode_record
                    yield encode_record(self.leaf.id, n, row)
                    n += 1
        finally:
            self.rows = n
            ctx.summaries.append({
                "path": self.leaf.path, "op": "scan",
                "source": self.leaf.source, "rows": n})
            ctx.meter.release(4, "scan")

    def dispose(self, ctx):
        pass  # nothing to recycle: sources are inputs, not temp


class SegmentStream(Stream):
    def __init__(self, segment):
        self.segment = segment
        self.rows = segment.items
        self._opened = False

    def open(self, ctx):
        if self._opened:
            raise AssertionError("stream consumed twice")
        self._opened = True
        reader = SpillReader(ctx, self.segment, ctx.budget.reader_buf)
        try:
            while True:
                item = reader.read()
                if item is None:
                    break
                yield item
        finally:
            reader.close()

    def dispose(self, ctx):
        ctx.meter.temp_drop(self.segment.path)


class ChainedStream(Stream):
    """UNION ALL: concatenation of child streams, left then right.  Zero temp."""

    def __init__(self, parts, node_path):
        self.parts = parts
        self.node_path = node_path
        self.rows = None
        self._opened = False

    def open(self, ctx):
        if self._opened:
            raise AssertionError("stream consumed twice")
        self._opened = True
        n = 0
        try:
            for p in self.parts:
                for item in p.open(ctx):
                    n += 1
                    yield item
        finally:
            self.rows = n
            ctx.summaries.append({
                "path": self.node_path, "op": "union_all",
                "left_rows": self.parts[0].rows, "right_rows": self.parts[1].rows,
                "rows": n})

    def dispose(self, ctx):
        for p in self.parts:
            p.dispose(ctx)


# ---------------------------------------------------------------------------
# External sort: bounded run buffer, multi-pass k-way merge.
# ---------------------------------------------------------------------------

def sort_stream(stream, sortkey_of, ctx, name, item_of=None, emit=None):
    """Consume `stream`, sort items by `sortkey_of`.

    `item_of` optionally transforms each record before buffering (used to
    project a record down to its row key).  If `emit` is given, the final
    sorted items are passed to emit(item) and nothing is returned; otherwise
    the sorted items are written to a new Segment which is returned.
    The input stream is always disposed after the run-generation pass.
    """
    if item_of is None:
        item_of = lambda rec: rec
    budget = ctx.budget
    runs = []
    buf = []
    # run buffer + the transient decorated keys list Python's sort builds
    ctx.meter.hold(2 * budget.run_rows, "sort-run")
    try:
        for rec in stream.open(ctx):
            buf.append(item_of(rec))
            if len(buf) >= budget.run_rows:
                runs.append(_flush_run(buf, sortkey_of, ctx, name))
                buf = []
        if buf:
            runs.append(_flush_run(buf, sortkey_of, ctx, name))
            buf = []
    finally:
        ctx.meter.release(2 * budget.run_rows, "sort-run")
        stream.dispose(ctx)
    return _merge_all(runs, sortkey_of, ctx, name, emit)


def _flush_run(buf, sortkey_of, ctx, name):
    buf.sort(key=sortkey_of)
    w = SpillWriter(ctx, name)
    for item in buf:
        w.write(item)
    return w.finish()


def _merge_all(runs, sortkey_of, ctx, name, emit):
    if not runs:
        if emit is not None:
            return None
        return SpillWriter(ctx, name).finish()  # empty segment
    fanin = ctx.budget.merge_fanin
    while len(runs) > 1:
        if len(runs) <= fanin:
            return _merge_pass(runs, sortkey_of, ctx, name, emit)
        nxt = []
        for i in range(0, len(runs), fanin):
            chunk = runs[i:i + fanin]
            if len(chunk) == 1:
                nxt.append(chunk[0])
            else:
                nxt.append(_merge_pass(chunk, sortkey_of, ctx, name, None))
        runs = nxt
    # exactly one run left
    if emit is not None:
        seg = runs[0]
        reader = SpillReader(ctx, seg, ctx.budget.reader_buf)
        try:
            while True:
                item = reader.read()
                if item is None:
                    break
                emit(item)
        finally:
            reader.close()
            ctx.meter.temp_drop(seg.path)
        return None
    return runs[0]


def _merge_pass(runs, sortkey_of, ctx, name, emit):
    readers = [SpillReader(ctx, seg, ctx.budget.merge_buf_rows) for seg in runs]
    # heap holds one item (+ its sort key) per run
    ctx.meter.hold(2 * len(runs), "merge-heap")
    heap = []
    writer = None if emit is not None else SpillWriter(ctx, name)
    try:
        for idx, r in enumerate(readers):
            item = r.read()
            if item is not None:
                heapq.heappush(heap, (sortkey_of(item), idx, item))
        while heap:
            _, idx, item = heapq.heappop(heap)
            if writer is not None:
                writer.write(item)
            else:
                emit(item)
            nxt = readers[idx].read()
            if nxt is not None:
                heapq.heappush(heap, (sortkey_of(nxt), idx, nxt))
        out_seg = writer.finish() if writer is not None else None
    finally:
        for r in readers:
            r.close()
        ctx.meter.release(2 * len(runs), "merge-heap")
        for old in runs:
            ctx.meter.temp_drop(old.path)
    return out_seg
