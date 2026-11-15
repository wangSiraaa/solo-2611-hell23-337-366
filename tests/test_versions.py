"""Version-consistency invariants under deterministic object switches."""

from __future__ import annotations

import pytest

from fitsproj.engine import ProjectionEngine, RowSpec
from fitsproj.errors import VersionConflictError
from tests.conftest import serve


def _heap_switch(index, start, end, heap_start):
    return start >= heap_start


def test_single_switch_after_descriptors_before_heap(fixture_a, fixture_b):
    """Deterministically switch once, after descriptors and before heap.

    The engine must get a 412 on the first heap read, discard the uncommitted
    plan, retry exactly once, and return rows wholly from the *new* version.
    """

    with serve(
        [fixture_a, fixture_b],
        switch_when=lambda i, s, e: _heap_switch(i, s, e, 6912),
    ) as running:
        result = ProjectionEngine(running.url).query(
            [RowSpec(7, ((0, 1024),)), RowSpec(83, ((0, 1024),))]
        )
        server = running.source

    assert server.switches == 1
    assert [a["status"] for a in result["attempts"]] == ["412", "ok"]
    assert result["attempts"][0]["body_bytes"] > 0
    committed_attempt = result["attempts"][-1]["attempt"]
    etags = {
        r["etag"]
        for r in result["trace"]
        if r["status"] == 206 and r["attempt"] == committed_attempt
    }
    # Every successful 206 in the committed attempt belongs to its version.
    assert etags == {result["etag"]}
    # Rows come from version B: delta 17.
    assert result["rows"][0]["samples"][0] == 70017
    assert result["rows"][1]["samples"][0] == 70017
    # No stale-descriptor / fresh-heap mixture: both rows share the same bytes.
    assert result["rows"][0]["samples"] == result["rows"][1]["samples"]


def test_switch_between_descriptors(fixture_a, fixture_b):
    """Switch after the first descriptor request, before the second."""

    with serve(
        [fixture_a, fixture_b],
        switch_at=[2],
    ) as running:
        result = ProjectionEngine(running.url).query(
            [RowSpec(7, ((0, 8),)), RowSpec(83, ((0, 8),))]
        )
        server = running.source

    assert server.switches == 1
    assert [a["status"] for a in result["attempts"]] == ["412", "ok"]
    assert all(e["samples"][0] == 70017 for e in result["rows"])


def test_switch_during_heap_retry_after_first_heap_read(fixture_a, fixture_b):
    """Switch on the retry's first heap read too => explicit failure, no rows."""

    with serve(
        [fixture_a, fixture_b, fixture_a],
        switch_when=lambda i, s, e: _heap_switch(i, s, e, 6912),
    ) as running:
        with pytest.raises(VersionConflictError) as info:
            ProjectionEngine(running.url).query(
                [RowSpec(7, ((0, 8),)), RowSpec(83, ((0, 8),))]
            )
        server = running.source

    err = info.value
    assert server.switches == 2
    assert [a["status"] for a in err.attempts] == ["412", "412"]
    assert err.result is None
    assert err.response_body_bytes == sum(a["body_bytes"] for a in err.attempts)
    # Each attempt reports its own body total and the failure total is their sum.
    assert err.response_body_bytes > 0


def test_switch_with_permuted_lengths_never_mixes_versions(tmp_path):
    """Version B reassigns lengths, so a mixed descriptor/heap read is wrong.

    Row 3 has length 3 in A and 2000 in B; a stale descriptor paired with B's
    heap (or vice versa) would produce a different result.  The engine must
    return the whole row from exactly one version.
    """

    from fitsproj.generate import make_pair

    lengths_a = [3, 5, 2000, 7, 0, 2048]
    lengths_b = [2000, 5, 3, 7, 0, 2048]
    spec_a, spec_b = make_pair(
        tmp_path / "perm", len(lengths_a), delta_b=1000,
        lengths=lengths_a, lengths_b=lengths_b,
    )

    with serve(
        [spec_a["path"], spec_b["path"]],
        switch_when=lambda i, s, e: _heap_switch(i, s, e, spec_a["heap_start"]),
    ) as running:
        result = ProjectionEngine(running.url).query(
            [RowSpec(0, ()), RowSpec(2, ()), RowSpec(5, ())]
        )

    # Committed version is B (the switch happened before heap).
    for entry in result["rows"]:
        row = entry["source_row"]
        assert entry["samples"] == spec_b["expected"][row], row
    assert len(result["rows"][0]["samples"]) == 2000
    assert len(result["rows"][1]["samples"]) == 3


def test_stable_source_has_single_attempt_and_one_etag(fixture_a):
    with serve([fixture_a]) as running:
        result = ProjectionEngine(running.url).query(
            [RowSpec(7, ((0, 8),))]
        )
    assert [a["status"] for a in result["attempts"]] == ["ok"]
    assert len({r["etag"] for r in result["trace"] if r["status"] == 206}) == 1


def test_attempt_bodies_reported_separately(fixture_a, fixture_b):
    with serve(
        [fixture_a, fixture_b],
        switch_when=lambda i, s, e: _heap_switch(i, s, e, 6912),
    ) as running:
        result = ProjectionEngine(running.url).query([RowSpec(7, ((0, 8),))])
    assert len(result["attempts"]) == 2
    assert result["response_body_bytes"] == sum(
        a["body_bytes"] for a in result["attempts"]
    )
