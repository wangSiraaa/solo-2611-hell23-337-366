"""Correctness of row/order/duplicate/element-interval semantics."""

from __future__ import annotations

import pytest
from astropy.io import fits

from fitsproj.engine import ProjectionEngine, RowSpec
from tests.conftest import serve

BUDGET = 16384


def _oracle(path, row, start, stop):
    with fits.open(path) as handle:
        return [int(x) for x in handle[1].data[row]["samples"][start:stop]]


def test_fixture_queries_match_oracle(fixture_a, queries):
    for spec in queries:
        rows = spec["rows"]
        start, stop = spec["slice"]
        with serve([fixture_a]) as running:
            result = ProjectionEngine(running.url).query(
                [RowSpec(r, ((start, stop),)) for r in rows]
            )
        assert result["response_body_bytes"] <= spec["body_budget"]
        assert len(result["rows"]) == len(rows)
        for entry, row in zip(result["rows"], rows):
            assert entry["source_row"] == row
            assert entry["samples"] == _oracle(fixture_a, row, start, stop)
            assert entry["element_start"] == start
            assert entry["element_stop"] == stop


def test_arbitrary_order_and_duplicates(fixture_a):
    order = [83, 7, 83, 7, 83]
    with serve([fixture_a]) as running:
        result = ProjectionEngine(running.url).query(
            [RowSpec(r, ((0, 5),)) for r in order]
        )
    assert [e["source_row"] for e in result["rows"]] == order
    assert [e["occurrence"] for e in result["rows"]] == list(range(len(order)))
    for entry in result["rows"]:
        assert entry["samples"] == _oracle(fixture_a, entry["source_row"], 0, 5)


def test_per_row_and_per_column_intervals_and_empty_interval(fixture_a):
    specs = [
        RowSpec(7, ((0, 3), (10, 13), (3, 3))),   # out of order + empty
        RowSpec(83, ((1020, 1024),)),               # tail only
        RowSpec(7, ((0, 3),)),                      # duplicate row, other window
        RowSpec(4, ((0, 0),)),                       # empty interval only
    ]
    with serve([fixture_a]) as running:
        result = ProjectionEngine(running.url).query(specs)
    got = [(e["source_row"], e["element_start"], e["element_stop"], e["samples"]) for e in result["rows"]]
    expected = [
        (7, 0, 3, _oracle(fixture_a, 7, 0, 3)),
        (7, 10, 13, _oracle(fixture_a, 7, 10, 13)),
        (7, 3, 3, []),
        (83, 1020, 1024, _oracle(fixture_a, 83, 1020, 1024)),
        (7, 0, 3, _oracle(fixture_a, 7, 0, 3)),
        (4, 0, 0, []),
    ]
    assert got == expected
    assert result["response_body_bytes"] <= BUDGET


def test_shared_heap_object_keeps_distinct_source_rows(fixture_a):
    # Fixture rows 7 and 83 point at the same heap offset (one shared object).
    with serve([fixture_a]) as running:
        result = ProjectionEngine(running.url).query(
            [RowSpec(7, ((0, 1024),)), RowSpec(83, ((0, 1024),))]
        )
    assert [e["source_row"] for e in result["rows"]] == [7, 83]
    assert result["rows"][0]["samples"] == result["rows"][1]["samples"]
    heap_requests = [r for r in result["trace"] if r["kind"] == "heap"]
    # One shared object => one coalesced heap transfer, not two.
    assert len(heap_requests) == 1


def test_generated_shared_and_overlapping_heap(variant_pair):
    spec_a, _spec_b = variant_pair
    path = spec_a["path"]
    # 5/9 share one object; 11 partially overlaps 5; verify with the oracle.
    with serve([path]) as running:
        result = ProjectionEngine(running.url).query(
            [RowSpec(5, ()), RowSpec(9, ()), RowSpec(11, ())]
        )
    for entry in result["rows"]:
        assert entry["samples"] == spec_a["expected"][entry["source_row"]]
    assert result["rows"][0]["samples"] == result["rows"][1]["samples"]
    assert result["rows"][2]["samples"] == spec_a["expected"][5][7:]


def test_generator_oracle_matches_astropy(variant_pair):
    spec_a, spec_b = variant_pair
    for spec in (spec_a, spec_b):
        with fits.open(spec["path"]) as handle:
            assert int(handle[1].header["NAXIS2"]) == spec["nrows"]
            for row in range(spec["nrows"]):
                got = [int(x) for x in handle[1].data[row]["samples"]]
                assert got == spec["expected"][row], (spec["path"], row)


def test_plan_is_inspectable_and_reads_no_heap(fixture_a):
    with serve([fixture_a]) as running:
        plan = ProjectionEngine(running.url).plan([RowSpec(7, ((0, 8),))])
    assert plan["etag"]
    assert plan["layout"]["nrows"] == 96
    assert plan["rows"][0]["descriptor"]["count"] == 1024
    kinds = {r["kind"] for r in plan["trace"]}
    assert "heap" not in kinds
    # Plan body is only headers + descriptors, well under the query budget.
    assert plan["response_body_bytes"] < BUDGET


def test_legacy_query_supports_open_ended_slices(fixture_a):
    from query import query

    with serve([fixture_a]) as running:
        full = query(running.url, [7])
        from_start = query(running.url, [7], start=4)
        to_stop = query(running.url, [7], stop=4)
    assert len(full["rows"][0]["samples"]) == 1024
    assert from_start["rows"][0]["samples"] == _oracle(fixture_a, 7, 4, 1024)
    assert to_stop["rows"][0]["samples"] == _oracle(fixture_a, 7, 0, 4)


def test_out_of_range_row_rejected_before_io(fixture_a):
    from fitsproj.errors import RowOutOfRange

    with serve([fixture_a]) as running:
        with pytest.raises(RowOutOfRange):
            ProjectionEngine(running.url).query([RowSpec(96, ())])
        # Only the two header blocks were fetched; row validation happens
        # before any descriptor or heap request.
        requests = running.source.log()
        assert len(requests) == 2
        assert [r["range_start"] for r in requests] == [0, 2880]
