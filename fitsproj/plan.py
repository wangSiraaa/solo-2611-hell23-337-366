"""Shared interval cache + request coalescing planner.

Why an interval (segment) cache rather than a fixed page grid
--------------------------------------------------------------
Descriptors describe byte ranges that can start and end at almost any offset
inside the table/heap.  A fixed page grid is simple, but rounding to a page grid
would let a descriptor read cross into the heap; for an *invalid* descriptor
that would violate "illegal ranges must not touch other blocks".  It also makes
it awkward to keep descriptor and heap bytes in one budget when the two regions
are adjacent.

The cache here stores disjoint byte segments in absolute file coordinates and is
bounded by total resident bytes (8 KiB by default).  Fetches may optionally be
rounded outward to ``page_size`` boundaries (a byte-vs-request-count knob), but
the rounding is always clamped to a caller-supplied ``[min_start, max_end)``
window, so descriptor fetches never cross into the heap and heap fetches never
touch table/header blocks.  Parsed FITS header blocks never enter this cache;
they are fetched through the raw HTTP client and bounded separately at 9 KiB
(they are not "data pages").

Two coalescing layers:

* the planner merges logically requested intervals whose gap is at most
  ``*_merge_gap`` bytes (one over-read instead of two transfers);
* the cache subtracts already-resident coverage and merges the remaining
  *missing* byte ranges before issuing HTTP requests.

Knobs (``page_size``, ``descriptor_merge_gap``, ``heap_merge_gap``) trade
response-body bytes against request count; see :mod:`fitsproj.experiment`.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import ProtocolError, VersionMismatch
from .httpclient import HttpRangeClient


@dataclass
class PlanConfig:
    """Planner knobs.  Defaults are the ones reported in the README."""

    #: Alignment of a remote read (0 = exact byte ranges).
    page_size: int = 0
    #: Total resident data bytes (descriptors + heap).  Parsed headers excluded.
    cache_capacity: int = 8192
    #: Merge requested descriptor intervals when the gap is <= this many bytes.
    descriptor_merge_gap: int = 8
    #: Merge requested heap intervals when the gap is <= this many bytes.
    heap_merge_gap: int = 0

    def __post_init__(self):
        if self.page_size < 0:
            raise ValueError("page_size must be >= 0")
        if self.cache_capacity <= 0:
            raise ValueError("cache_capacity must be positive")


def merge_intervals(intervals, gap: int = 0):
    """Merge half-open ``[start, end)`` intervals separated by <= ``gap``."""

    items = sorted((int(s), int(e)) for s, e in intervals if e > s)
    if not items:
        return []
    merged = [list(items[0])]
    for start, end in items[1:]:
        if start <= merged[-1][1] + gap:
            if end > merged[-1][1]:
                merged[-1][1] = end
        else:
            merged.append([start, end])
    return [(s, e) for s, e in merged]


@dataclass
class _Segment:
    start: int
    end: int
    data: bytes
    seq: int


class IntervalCache:
    """Bounded, version-bound interval cache over a single remote object."""

    def __init__(
        self,
        client: HttpRangeClient,
        etag: str,
        file_size: int,
        config: PlanConfig,
        *,
        attempt: int = 0,
    ):
        self.client = client
        self.etag = etag
        self.file_size = file_size
        self.config = config
        self.attempt = attempt
        self._segments: list[_Segment] = []
        self._seq = 0
        self.resident_bytes = 0
        self.peak_bytes = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.segments_fetched = 0

    # -- coverage ----------------------------------------------------------
    def _missing(self, start: int, end: int):
        """Return sub-ranges of ``[start, end)`` not resident in the cache."""

        if end <= start:
            return []
        missing = []
        cursor = start
        for seg in sorted(self._segments, key=lambda s: s.start):
            if seg.end <= cursor:
                continue
            if seg.start >= end:
                break
            if seg.start > cursor:
                missing.append((cursor, min(seg.start, end)))
            cursor = max(cursor, seg.end)
            if cursor >= end:
                break
        if cursor < end:
            missing.append((cursor, end))
        return [(a, b) for a, b in missing if b > a]

    def _pieces(self, start: int, end: int):
        pieces = []
        for seg in self._segments:
            lo = max(start, seg.start)
            hi = min(end, seg.end)
            if hi > lo:
                pieces.append((lo, hi, seg.data[lo - seg.start : hi - seg.start]))
        return pieces

    def _store(self, start: int, data: bytes) -> None:
        if not data:
            return
        cap = self.config.cache_capacity
        if len(data) > cap:
            # Keep the suffix that fits; earlier bytes were already returned to
            # the caller and need not stay resident.
            start = start + len(data) - cap
            data = data[-cap:]
        while self.resident_bytes + len(data) > cap and self._segments:
            victim = min(self._segments, key=lambda s: s.seq)
            self._segments.remove(victim)
            self.resident_bytes -= len(victim.data)
        if self.resident_bytes + len(data) > cap:
            return
        self._segments.append(_Segment(start, start + len(data), data, self._seq))
        self._seq += 1
        self.resident_bytes += len(data)
        if self.resident_bytes > self.peak_bytes:
            self.peak_bytes = self.resident_bytes

    # -- IO ----------------------------------------------------------------
    def _fetch(self, start: int, end: int, kind: str) -> bytes:
        """Fetch half-open ``[start, end)`` and verify the bound version."""

        response = self.client.fetch(
            start, end - 1, if_match=self.etag, kind=kind, attempt=self.attempt
        )
        if response.etag != self.etag:
            raise VersionMismatch(
                f"server returned ETag {response.etag!r} while bound to "
                f"{self.etag!r}"
            )
        return response.body

    def get_interval(
        self,
        start: int,
        end: int,
        *,
        kind: str = "data",
        align: int | None = None,
        min_start: int = 0,
        max_end: int | None = None,
        merge_gap: int = 0,
    ) -> bytes:
        """Return exactly ``[start, end)``, reading/coalescing as needed.

        ``align`` rounds remote reads outward to that boundary but never past
        ``[min_start, max_end)``; the returned value is always exactly the
        requested slice.
        """

        if end <= start:
            return b""
        limit = self.file_size if max_end is None else min(max_end, self.file_size)
        if start < min_start or end > limit:
            raise ProtocolError(
                f"interval [{start}, {end}) outside supported window "
                f"[{min_start}, {limit})"
            )
        align = self.config.page_size if align is None else align

        if not self._missing(start, end):
            self.cache_hits += 1
            return self._assemble(start, end, [])

        self.cache_misses += 1
        expanded = []
        for lo, hi in self._missing(start, end):
            if align:
                lo = max(min_start, (lo // align) * align)
                hi = min(limit, ((hi + align - 1) // align) * align)
            expanded.append((lo, hi))
        expanded = merge_intervals(expanded, merge_gap)

        to_fetch = []
        for lo, hi in expanded:
            to_fetch.extend(self._missing(lo, hi))
        to_fetch = merge_intervals(to_fetch, 0)

        extra = []
        for lo, hi in to_fetch:
            body = self._fetch(lo, hi, kind)
            self.segments_fetched += 1
            self._store(lo, body)
            extra.append((lo, body))
        return self._assemble(start, end, extra)

    def _assemble(self, start: int, end: int, extra) -> bytes:
        pieces = self._pieces(start, end)
        for lo, body in extra:
            hi = lo + len(body)
            a, b = max(start, lo), min(end, hi)
            if b > a:
                pieces.append((a, b, body[a - lo : b - lo]))
        pieces.sort(key=lambda item: (item[0], item[1]))
        out = bytearray()
        cursor = start
        for lo, hi, data in pieces:
            if hi <= cursor:
                continue
            if lo > cursor:
                raise ProtocolError(
                    f"internal cache gap at {cursor} while reading {start}-{end}"
                )
            take = max(lo, cursor)
            out += data[take - lo : hi - lo]
            cursor = hi
            if cursor >= end:
                break
        if cursor < end:
            raise ProtocolError(
                f"internal cache gap at {cursor} while reading {start}-{end}"
            )
        return bytes(out)

    def stats(self) -> dict:
        return {
            "cache_capacity": self.config.cache_capacity,
            "page_size": self.config.page_size,
            "peak_cache_bytes": self.peak_bytes,
            "resident_cache_bytes": self.resident_bytes,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "segments_fetched": self.segments_fetched,
            "segments_resident": len(self._segments),
        }
