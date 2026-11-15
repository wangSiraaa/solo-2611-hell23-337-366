"""The metadata/descriptor plan and heap read are separate, version-shared."""

from __future__ import annotations

import pytest

from fitsproj.engine import ProjectionEngine, RowSpec
from fitsproj.errors import PreconditionFailed
from tests.conftest import serve


def test_prepare_then_execute_shares_etag(fixture_a):
    with serve([fixture_a]) as running:
        engine = ProjectionEngine(running.url)
        prepared = engine.prepare([RowSpec(7, ((0, 8),))])
        # Phase 1 touched no heap byte.
        assert "heap" not in {r.kind for r in prepared.client.requests}
        plan = prepared.plan_dict()
        assert plan["etag"] == prepared.etag
        values = prepared.execute()
    assert values[0]["samples"] == [70000 + i for i in range(8)]


def test_prepare_plan_is_json_serialisable(fixture_a):
    import json

    with serve([fixture_a]) as running:
        plan = ProjectionEngine(running.url).plan(
            [RowSpec(83, ((4, 12),)), RowSpec(7, ())]
        )
    json.dumps(plan)  # must not raise
    assert [r["source_row"] for r in plan["rows"]] == [83, 7]
    assert plan["rows"][0]["element_requests"] == [[4, 12]]
    assert plan["rows"][1]["element_requests"] == "full"
    assert plan["descriptor_reads"]


def test_stale_prepared_plan_cannot_read_new_heap(fixture_a, fixture_b):
    """A plan bound to A must not accept heap bytes from B (412)."""

    from range_server import Source

    source = Source([fixture_a, fixture_b], switch_at=[])  # switch manually
    url = source.start()
    try:
        engine = ProjectionEngine(url)
        prepared = engine.prepare([RowSpec(7, ((0, 8),))])
        # Simulate the object advancing after the plan was built.
        with source._lock:
            source._version = 1
            source._switches = 1
        with pytest.raises(PreconditionFailed):
            prepared.execute()
    finally:
        source.close()
