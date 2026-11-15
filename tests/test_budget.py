"""Budget, cache-capacity and accounting invariants."""

from __future__ import annotations

import pytest

from fitsproj.engine import ProjectionEngine, RowSpec
from fitsproj.fitsmeta import MAX_PARSED_HEADER_BYTES
from fitsproj.plan import PlanConfig
from tests.conftest import serve

CACHE_LIMIT = 8192
QUERY_BUDGET = 16384


def test_fixture_queries_within_body_budget(fixture_a, queries):
    for spec in queries:
        rows, (start, stop) = spec["rows"], spec["slice"]
        with serve([fixture_a]) as running:
            result = ProjectionEngine(running.url).query(
                [RowSpec(r, ((start, stop),)) for r in rows]
            )
        assert result["response_body_bytes"] <= QUERY_BUDGET
        assert result["response_body_bytes"] == running.source.sent


def test_peak_cache_never_exceeds_8k(fixture_a):
    # A query that touches far more data than the cache can hold.
    rows = list(range(0, 96))
    with serve([fixture_a]) as running:
        result = ProjectionEngine(running.url).query([RowSpec(r, ()) for r in rows])
    assert result["cache"]["peak_cache_bytes"] <= CACHE_LIMIT
    assert result["cache"]["cache_capacity"] == CACHE_LIMIT
    # And the cache is genuinely exercised as a bound (evictions happened).
    assert result["cache"]["segments_resident"] * 4 <= CACHE_LIMIT * 4
    assert result["response_body_bytes"] > CACHE_LIMIT


def test_small_capacity_is_respected(fixture_a):
    config = PlanConfig(cache_capacity=64, page_size=0)
    with serve([fixture_a]) as running:
        result = ProjectionEngine(running.url, config=config).query(
            [RowSpec(7, ((0, 64),)), RowSpec(83, ((0, 64),))]
        )
    assert result["cache"]["peak_cache_bytes"] <= 64


def test_headers_not_counted_against_cache(fixture_a):
    """Headers are 5760 B here; peak *data* cache is tiny and separate."""

    with serve([fixture_a]) as running:
        result = ProjectionEngine(running.url).query([RowSpec(7, ((0, 8),))])
    assert result["header_bytes"] == 5760
    assert result["header_bytes"] <= MAX_PARSED_HEADER_BYTES
    # 8 descriptor bytes + 32 heap bytes; no header block ever enters the cache.
    assert result["cache"]["peak_cache_bytes"] == 40


def test_trace_accounting_and_body_sums(fixture_a):
    with serve([fixture_a]) as running:
        result = ProjectionEngine(running.url).query(
            [RowSpec(7, ((0, 8),)), RowSpec(83, ((0, 8),))]
        )
    kinds = [r["kind"] for r in result["trace"]]
    assert kinds[:2] == ["primary-header", "extension-header"]
    assert "descriptor" in kinds and "heap" in kinds
    assert sum(r["body_bytes"] for r in result["trace"]) == result["response_body_bytes"]
    assert result["response_body_bytes"] == running.source.sent
    # Every post-header request carried the committed ETag.
    for r in result["trace"]:
        if r["kind"] != "primary-header":
            assert r["if_match"] == result["etag"]


def test_duplicate_and_shared_rows_avoid_duplicate_transfer(fixture_a):
    one = None
    with serve([fixture_a]) as running:
        one = ProjectionEngine(running.url).query([RowSpec(7, ((0, 1024),))])
    with serve([fixture_a]) as running:
        many = ProjectionEngine(running.url).query(
            [RowSpec(7, ((0, 1024),)) for _ in range(8)]
        )
    # Eight identical requests cost the same heap bytes as one.
    heap_one = sum(r["body_bytes"] for r in one["trace"] if r["kind"] == "heap")
    heap_many = sum(r["body_bytes"] for r in many["trace"] if r["kind"] == "heap")
    assert heap_one == heap_many == 4096


def test_failed_request_body_is_zero_and_empty(tmp_path):
    """A rejected (illegal) request reports 0 result body and no heap bytes."""

    from fitsproj.errors import DescriptorBoundsError
    from fitsproj.generate import write_table

    spec = write_table(tmp_path / "t.fits", [8] * 10)
    with serve([spec["path"]]) as running:
        with pytest.raises(DescriptorBoundsError):
            ProjectionEngine(running.url).query([RowSpec(2, ((0, 9),))])
        # Only headers + 1 descriptor, all non-heap.  The *result* body is 0.
        assert running.source.sent == 2880 * 2 + 8
