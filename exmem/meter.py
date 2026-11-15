"""Resource metering: resident payload slots, temp disk bytes/rows, I/O bytes.

The engine never holds more than ``payload_limit`` row payloads / witnesses in
candidate-owned structures (input buffers, sort run buffer, merge heap, hash
buckets -- we have none --, output pending).  Every buffer acquires its
*capacity* up front via :meth:`Meter.hold`, so the peak is enforced, not
estimated.  Scalar counters and file offsets are a handful of ints and are
reported separately; no sequences are hidden in them.

Temp files are registered on creation, re-synced as they grow, and dropped
(deleted) or committed (renamed to final output) explicitly.  Peak temp bytes
and peak temp rows are tracked over the whole run, for every AST -- the 6x
input-bytes budget is only *checked* for the documented hot-key benchmark.
"""
from __future__ import annotations

import os


class BudgetExceeded(RuntimeError):
    pass


class Budget:
    """Tunable capacities (in row-payload slots).  Tests shrink these to force
    multi-pass merging on tiny inputs."""

    def __init__(self, run_rows=1024, merge_fanin=128, merge_buf_rows=4,
                 reader_buf=128, writer_buf=128, counts_buf=256,
                 payload_limit=4096):
        self.run_rows = run_rows                # external-sort run buffer
        self.merge_fanin = merge_fanin          # max runs merged in one pass
        self.merge_buf_rows = merge_buf_rows    # per-run read buffer during merge
        self.reader_buf = reader_buf            # sequential spill/source reader
        self.writer_buf = writer_buf            # spill writer flush buffer
        self.counts_buf = counts_buf            # (key, count) cursor buffer
        self.payload_limit = payload_limit

    def describe(self):
        return {k: getattr(self, k) for k in (
            "run_rows", "merge_fanin", "merge_buf_rows", "reader_buf",
            "writer_buf", "counts_buf", "payload_limit")}


class Meter:
    def __init__(self, budget: Budget):
        self.budget = budget
        # resident payload slots (row payloads + occurrence witnesses)
        self.payload_cur = 0
        self.payload_peak = 0
        self._holds = []          # (n, what) currently held; for peak breakdown
        self.payload_peak_holds = []
        # temp disk
        self.temp_files: dict[str, list] = {}   # path -> [nbytes, nitems]
        self.temp_bytes_cur = 0
        self.temp_bytes_peak = 0
        self.temp_rows_cur = 0
        self.temp_rows_peak = 0
        # total logical I/O (disclosed; re-scans of sources included)
        self.io_read = 0
        self.io_write = 0

    # ---- resident payload slots ----
    def hold(self, n: int, what: str = ""):
        self.payload_cur += n
        self._holds.append((n, what))
        if self.payload_cur > self.payload_peak:
            self.payload_peak = self.payload_cur
            self.payload_peak_holds = list(self._holds)
        if self.payload_cur > self.budget.payload_limit:
            raise BudgetExceeded(
                f"resident payload budget exceeded: {self.payload_cur} > "
                f"{self.budget.payload_limit} (at {what})")

    def release(self, n: int, what: str = ""):
        self.payload_cur -= n
        # remove one matching hold (capacity-based holds release what they got)
        for i in range(len(self._holds) - 1, -1, -1):
            if self._holds[i][0] == n:
                del self._holds[i]
                break
        if self.payload_cur < 0:
            raise AssertionError(f"payload accounting went negative (at {what})")

    # ---- temp files ----
    def temp_register(self, path):
        key = str(path)
        assert key not in self.temp_files, f"temp path re-registered: {key}"
        self.temp_files[key] = [0, 0]

    def temp_sync(self, path, nbytes: int, nitems: int):
        e = self.temp_files[str(path)]
        self.temp_bytes_cur += nbytes - e[0]
        self.temp_rows_cur += nitems - e[1]
        e[0] = nbytes
        e[1] = nitems
        if self.temp_bytes_cur > self.temp_bytes_peak:
            self.temp_bytes_peak = self.temp_bytes_cur
        if self.temp_rows_cur > self.temp_rows_peak:
            self.temp_rows_peak = self.temp_rows_cur

    def temp_drop(self, path):
        e = self.temp_files.pop(str(path))
        self.temp_bytes_cur -= e[0]
        self.temp_rows_cur -= e[1]
        try:
            os.remove(path)
        except FileNotFoundError:
            pass  # already gone (e.g. renamed); accounting is what matters

    def temp_commit(self, path):
        """File becomes committed output: no longer temp, not deleted."""
        e = self.temp_files.pop(str(path))
        self.temp_bytes_cur -= e[0]
        self.temp_rows_cur -= e[1]

    # ---- reporting ----
    def snapshot(self) -> dict:
        return {
            "payload_peak": self.payload_peak,
            "payload_limit": self.budget.payload_limit,
            "payload_peak_holds": [
                {"slots": n, "where": w} for n, w in self.payload_peak_holds],
            "temp_bytes_peak": self.temp_bytes_peak,
            "temp_rows_peak": self.temp_rows_peak,
            "temp_bytes_current": self.temp_bytes_cur,
            "temp_files_open": sorted(self.temp_files),
            "io_read_bytes": self.io_read,
            "io_write_bytes": self.io_write,
        }
