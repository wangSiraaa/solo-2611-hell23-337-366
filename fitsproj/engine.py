"""Version-consistent projection engine.

Pipeline
--------
1. **Metadata / row-descriptor plan** (inspectable, version-bound):
   ranged reads of the FITS header blocks discover the table layout; ranged
   reads of only the requested table rows yield the ``(count, offset)`` P
   descriptors for each requested row.  The plan records the ETag it was built
   against and is serialisable via ``result["plan"]``.
2. **Heap reads**: for every requested row *occurrence* (order preserved,
   duplicates kept) and every requested element interval, the engine reads the
   minimum heap bytes.  Descriptors are not deduplicated: two row descriptors
   may point at the same heap object and each occurrence keeps its own source
   row identity.
3. **Version binding**: every ranged read after the first header read carries
   ``If-Match: <etag>``; a 412 (or a 206 whose ETag no longer matches) discards
   the uncommitted plan and the whole query is retried exactly once from
   scratch.  Results are assembled in memory and only returned after all reads
   succeeded, so a mid-flight version change can never combine an old
   descriptor with a new heap, and a twice-changing source fails explicitly
   with no partial result.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import (
    DescriptorBoundsError,
    PreconditionFailed,
    ProjectionError,
    RowOutOfRange,
    VersionConflictError,
    VersionMismatch,
)
from .fitsmeta import Layout, discover_layout
from .httpclient import HttpRangeClient
from .plan import IntervalCache, PlanConfig, merge_intervals

FULL = "full"


@dataclass(frozen=True)
class RowSpec:
    """One requested row occurrence.

    ``intervals`` is a tuple of half-open element ranges ``(start, stop)``.  An
    empty tuple means "the whole array".  Ranges may be out of order and may
    overlap; they are returned in the exact requested order.
    """

    row: int
    intervals: tuple[tuple[int, int], ...] = ()


def _as_intervals(value) -> tuple[tuple[int, int], ...]:
    if value is None:
        return ()
    if (
        isinstance(value, (list, tuple))
        and len(value) == 2
        and all(x is None or isinstance(x, int) for x in value)
    ):
        return (tuple(value),)
    return tuple(tuple(None if x is None else int(x) for x in iv) for iv in value)


def normalize_specs(rows, intervals=None) -> list[RowSpec]:
    """Normalise the accepted row-specification shapes into ``RowSpec`` list.

    Accepted::

        rows = 7                                   # one row, whole array
        rows = 7, intervals=(0, 8)                  # one range
        rows = [7, 83], intervals=(0, 8)            # same range for all
        rows = [(7, (0, 8)), (83, [(0, 4), (4, 8)])]
        rows = [RowSpec(7, ((0, 8),)), RowSpec(7, ((10, 20),))]
    """

    if isinstance(rows, RowSpec):
        return [rows]
    if isinstance(rows, int):
        return [RowSpec(int(rows), _as_intervals(intervals))]
    specs: list[RowSpec] = []
    default = _as_intervals(intervals)
    for item in rows:
        if isinstance(item, RowSpec):
            specs.append(item)
        elif isinstance(item, int):
            specs.append(RowSpec(int(item), default))
        elif isinstance(item, (list, tuple)):
            row = int(item[0])
            if len(item) > 1 and item[1] is not None:
                specs.append(RowSpec(row, _as_intervals(item[1])))
            else:
                specs.append(RowSpec(row, default))
        else:
            raise TypeError(f"unsupported row specification: {item!r}")
    return specs


@dataclass
class DescriptorPlan:
    """Inspectable metadata plan bound to one object version (ETag)."""

    etag: str
    layout: Layout
    specs: list[RowSpec]
    descriptors: dict  # row -> (count, offset)
    resolved: list  # parallel to specs: list[(el_start, el_stop)] | FULL
    descriptor_reads: list
    stats: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        rows = []
        for spec, intervals in zip(self.specs, self.resolved):
            count, offset = self.descriptors[spec.row]
            rows.append(
                {
                    "source_row": spec.row,
                    "descriptor": {"count": count, "heap_offset": offset},
                    "element_requests": FULL
                    if intervals == FULL
                    else [list(iv) for iv in intervals],
                }
            )
        return {
            "etag": self.etag,
            "layout": self.layout.as_dict(),
            "rows": rows,
            "descriptor_reads": [
                {"range": [lo, hi], "bytes": hi - lo}
                for lo, hi in self.descriptor_reads
            ],
            "stats": self.stats,
        }


class PreparedQuery:
    """Result of phase 1: a descriptor plan bound to one ETag.

    ``plan`` is inspectable/serialisable (:meth:`DescriptorPlan.as_dict`);
    ``jobs`` is the ordered list of heap reads (one per requested interval
    occurrence) that phase 2 will perform under the same version identity.
    """

    def __init__(self, engine, client, cache, plan, jobs):
        self.engine = engine
        self.client = client
        self.cache = cache
        self.plan = plan
        self.jobs = jobs

    @property
    def etag(self) -> str:
        return self.plan.etag

    def plan_dict(self) -> dict:
        return self.plan.as_dict()

    def execute(self):
        """Phase 2: perform the heap reads; return the ordered row entries."""

        return self.engine._read_heap(self)


class ProjectionEngine:
    """Ranged, version-consistent projection over the supported FITS subset."""

    def __init__(
        self,
        url: str,
        *,
        config: PlanConfig | None = None,
        timeout: float = 30.0,
    ):
        self.url = url
        self.config = config or PlanConfig()
        self.timeout = timeout

    # -- plan --------------------------------------------------------------
    def _build_plan(self, client, cache, layout, specs):
        var = self.config
        var_col = layout.var_column

        bad_rows = sorted({s.row for s in specs if not (0 <= s.row < layout.nrows)})
        if bad_rows:
            raise RowOutOfRange(f"row(s) {bad_rows} outside [0, {layout.nrows})")

        descriptor_ranges = {}
        for spec in specs:
            off = layout.descriptor_offset(spec.row)
            descriptor_ranges.setdefault(spec.row, (off, off + var_col.size))

        reads = merge_intervals(
            list(descriptor_ranges.values()), gap=var.descriptor_merge_gap
        )
        # Descriptor reads are clamped to the table region so a bad descriptor
        # can never cause a read into the heap / header blocks.
        reads = [
            (max(lo, layout.table_start), min(hi, layout.heap_start))
            for lo, hi in reads
        ]
        reads = [(lo, hi) for lo, hi in reads if hi > lo]

        descriptors = {}
        # Prefetch the coalesced descriptor ranges exactly once; parse every
        # row descriptor out of those buffers.  This makes
        # ``descriptor_merge_gap`` actually collapse adjacent row descriptors
        # into one request and avoids a second cache lookup per row.
        descriptor_buffers = []
        for lo, hi in reads:
            descriptor_buffers.append(
                (
                    lo,
                    cache.get_interval(
                        lo,
                        hi,
                        kind="descriptor",
                        min_start=layout.table_start,
                        max_end=layout.heap_start,
                        merge_gap=var.descriptor_merge_gap,
                    ),
                )
            )

        def descriptor_bytes(lo, hi):
            for base, data in descriptor_buffers:
                if base <= lo and hi <= base + len(data):
                    return data[lo - base : hi - base]
            raise AssertionError(f"descriptor [{lo}, {hi}) not in plan buffers")

        for row, (lo, hi) in descriptor_ranges.items():
            lo = max(lo, layout.table_start)
            hi = min(hi, layout.heap_start)
            if hi - lo < var_col.size:
                raise DescriptorBoundsError(
                    [f"row {row}: descriptor range outside the table"]
                )
            raw = descriptor_bytes(lo, hi)
            descriptors[row] = (
                int.from_bytes(raw[0:4], "big", signed=True),
                int.from_bytes(raw[4:8], "big", signed=True),
            )

        esize = var_col.element_size
        resolved = []
        problems = []
        for spec in specs:
            count, offset = descriptors[spec.row]
            if count < 0 or offset < 0:
                problems.append(
                    f"row {spec.row}: negative descriptor count/offset "
                    f"({count}, {offset})"
                )
                resolved.append(FULL)
                continue
            if offset + count * esize > layout.pcount:
                problems.append(
                    f"row {spec.row}: heap extent [{offset}, "
                    f"{offset + count * esize}) exceeds PCOUNT {layout.pcount}"
                )
                resolved.append(FULL)
                continue
            if not spec.intervals:
                resolved.append(FULL)
                continue
            normalized = []
            for start, stop in spec.intervals:
                # ``None`` bounds mean "open": None start => 0, None stop =>
                # the array end.  This keeps the legacy ``query(url, rows,
                # start=None/stop=None)`` shape working.
                lo = 0 if start is None else start
                hi = count if stop is None else stop
                if lo < 0 or hi < lo or hi > count:
                    problems.append(
                        f"row {spec.row}: element interval [{start}, {stop}) "
                        f"invalid for length {count}"
                    )
                else:
                    normalized.append((lo, hi))
            resolved.append(normalized)
        if problems:
            raise DescriptorBoundsError(problems)

        return DescriptorPlan(
            etag=layout.etag,
            layout=layout,
            specs=list(specs),
            descriptors=descriptors,
            resolved=resolved,
            descriptor_reads=reads,
            stats=cache.stats(),
        )

    # -- heap --------------------------------------------------------------
    def _heap_jobs(self, plan: DescriptorPlan):
        esize = plan.layout.var_column.element_size
        base = plan.layout.heap_start
        jobs = []
        for occurrence, (spec, intervals) in enumerate(
            zip(plan.specs, plan.resolved)
        ):
            count, offset = plan.descriptors[spec.row]
            pieces = [(0, count)] if intervals == FULL else intervals
            for el_start, el_stop in pieces:
                job = {
                    "occurrence": occurrence,
                    "source_row": spec.row,
                    "element_start": el_start,
                    "element_stop": el_stop,
                    "empty": el_stop <= el_start,
                }
                if not job["empty"]:
                    job["file_start"] = base + offset + el_start * esize
                    job["file_end"] = base + offset + el_stop * esize
                    if job["file_end"] > plan.layout.heap_end:
                        raise DescriptorBoundsError(
                            [
                                f"row {spec.row}: heap bytes "
                                f"[{job['file_start']}, {job['file_end']}) exceed "
                                f"heap end {plan.layout.heap_end}"
                            ]
                        )
                jobs.append(job)
        return jobs

    # -- core split: prepare (metadata/descriptor plan) -> read (heap) ------
    def _prepare(self, client, specs, attempt=0):
        """Phase 1: discover layout and read only descriptors.

        Returns a :class:`PreparedQuery` that carries the ETag it is bound to,
        the layout, the resolved descriptors and the ordered heap jobs.  No heap
        byte is read here, so callers can inspect/serialise the plan first.
        """

        layout = discover_layout(client, attempt=attempt)
        cache = IntervalCache(
            client, layout.etag, layout.file_size, self.config, attempt=attempt
        )
        plan = self._build_plan(client, cache, layout, specs)
        return PreparedQuery(self, client, cache, plan, self._heap_jobs(plan))

    def _read_heap(self, prepared: "PreparedQuery"):
        """Phase 2: read heap bytes for the prepared jobs under the same ETag."""

        plan = prepared.plan
        jobs = prepared.jobs
        cache = prepared.cache

        # Coalesce the logical heap requests *before* reading: duplicate rows,
        # repeated element intervals and two descriptors pointing at the same
        # heap object all collapse into one transfer.  Merged bytes are held in
        # a per-query working buffer (the eventual result), not in the bounded
        # page cache; the cache itself stays within ``cache_capacity``.
        requested = merge_intervals(
            [(j["file_start"], j["file_end"]) for j in jobs if not j["empty"]],
            gap=self.config.heap_merge_gap,
        )
        buffers = []
        for lo, hi in requested:
            data = cache.get_interval(
                lo,
                hi,
                kind="heap",
                min_start=plan.layout.heap_start,
                max_end=plan.layout.heap_end,
                merge_gap=self.config.heap_merge_gap,
            )
            buffers.append((lo, data))

        def take(start, end):
            for lo, data in buffers:
                hi = lo + len(data)
                if lo <= start and end <= hi:
                    return data[start - lo : end - lo]
            raise AssertionError(f"heap working buffer missing [{start}, {end})")

        values = []
        for job in jobs:
            if job["empty"]:
                samples = []
            else:
                raw = take(job["file_start"], job["file_end"])
                samples = [
                    int.from_bytes(raw[i : i + 4], "big", signed=True)
                    for i in range(0, len(raw), 4)
                ]
            values.append(
                {
                    "source_row": job["source_row"],
                    "occurrence": job["occurrence"],
                    "element_start": job["element_start"],
                    "element_stop": job["element_stop"],
                    "samples": samples,
                }
            )
        return values

    # -- public ------------------------------------------------------------
    def plan(self, rows, intervals=None) -> dict:
        """Build and return the metadata/row-descriptor plan only.

        The plan is bound to the ETag it was built against (``plan['etag']``).
        No heap bytes are read, so this is a cheap, inspectable preparation
        step; :meth:`query` performs the same plan internally and then reads
        the heap under that same version identity.
        """

        specs = normalize_specs(rows, intervals)
        client = HttpRangeClient(self.url, timeout=self.timeout)
        prepared = self._prepare(client, specs)
        result = prepared.plan_dict()
        result["trace"] = [r.as_dict() for r in client.requests]
        result["response_body_bytes"] = sum(r.body_bytes for r in client.requests)
        result["cache"] = prepared.cache.stats()
        return result

    def prepare(self, rows, intervals=None) -> "PreparedQuery":
        """Return the phase-1 object (layout + descriptors + heap jobs).

        Uses a fresh :class:`HttpRangeClient`; the returned object exposes the
        bound ETag and can be executed with :meth:`PreparedQuery.execute`.  The
        caller owns that client's lifecycle for the duration of the query.
        """

        specs = normalize_specs(rows, intervals)
        client = HttpRangeClient(self.url, timeout=self.timeout)
        return self._prepare(client, specs)

    def query(self, rows, intervals=None) -> dict:
        """Run a projection and return a JSON-serialisable result envelope.

        Phase 1 builds the descriptor plan, phase 2 reads the heap; both share
        one ETag.  On 412 (or an ETag mismatch on a 206) the uncommitted plan is
        discarded and the query retried once from scratch.  A second version
        change raises :class:`VersionConflictError` with no rows.
        """

        specs = normalize_specs(rows, intervals)
        attempts = []
        all_requests = []

        for attempt in range(2):
            client = HttpRangeClient(self.url, timeout=self.timeout)
            try:
                prepared = self._prepare(client, specs, attempt)
                values = self._read_heap(prepared)
            except (PreconditionFailed, VersionMismatch) as version_exc:
                all_requests.extend(r.as_dict() for r in client.requests)
                attempts.append(
                    {
                        "attempt": attempt,
                        "status": "412"
                        if isinstance(version_exc, PreconditionFailed)
                        else "version-mismatch",
                        "etag": None,
                        "body_bytes": sum(r.body_bytes for r in client.requests),
                        "requests": len(client.requests),
                    }
                )
                continue
            except ProjectionError as exc:
                # Deterministic failure (illegal descriptor/interval, bad row,
                # unparseable header): no retry, no partial rows.  Attach the
                # per-attempt accounting so callers can report the wasted bytes
                # and confirm the *result* body is empty (zero rows).
                exc.response_body_bytes = sum(
                    r.body_bytes for r in client.requests
                )
                exc.trace = [r.as_dict() for r in client.requests]
                exc.rows = []
                exc.attempts = [
                    {
                        "attempt": attempt,
                        "status": "error",
                        "etag": None,
                        "body_bytes": exc.response_body_bytes,
                        "requests": len(client.requests),
                        "detail": type(exc).__name__,
                    }
                ]
                raise

            plan = prepared.plan
            cache = prepared.cache
            all_requests.extend(r.as_dict() for r in client.requests)
            attempt_body = sum(r.body_bytes for r in client.requests)
            attempts.append(
                {
                    "attempt": attempt,
                    "status": "ok",
                    "etag": plan.etag,
                    "body_bytes": attempt_body,
                    "requests": len(client.requests),
                    "peak_cache_bytes": cache.peak_bytes,
                }
            )
            return {
                "etag": plan.etag,
                "rows": values,
                "plan": plan.as_dict(),
                "trace": all_requests,
                "response_body_bytes": sum(a["body_bytes"] for a in attempts),
                "attempts": attempts,
                "cache": cache.stats(),
                "header_bytes": plan.layout.header_bytes,
            }

        conflict = VersionConflictError(attempts, all_requests)
        conflict.response_body_bytes = sum(a["body_bytes"] for a in attempts)
        raise conflict
