"""Randomised (seeded) differential test against the generator oracle."""

from __future__ import annotations

import random

import pytest

from fitsproj.engine import ProjectionEngine, RowSpec
from fitsproj.generate import make_pair
from tests.conftest import serve


@pytest.fixture(scope="module")
def random_family(tmp_path_factory):
    rng = random.Random(1234)
    nrows = 120
    lengths = [
        rng.choice([0, 1, 2, 7, 100, 1024, 2048, rng.randint(0, 2048)])
        for _ in range(nrows)
    ]
    a, b = make_pair(
        tmp_path_factory.mktemp("random"),
        nrows,
        77,
        lengths=lengths,
        share={9: 5},
        overlap={11: (5, 3)},
    )
    return a, b, nrows


def test_random_requests_match_oracle(random_family):
    a, _b, nrows = random_family
    rng = random.Random(99)
    with serve([a["path"]]) as running:
        engine = ProjectionEngine(running.url)
        for _ in range(25):
            specs = []
            expected = []
            for _ in range(rng.randint(1, 8)):
                row = rng.randrange(nrows)
                length = len(a["expected"][row])
                if rng.random() < 0.25:
                    specs.append(RowSpec(row, ()))
                    expected.append((row, 0, length, a["expected"][row]))
                    continue
                intervals = []
                for _ in range(rng.randint(0, 3)):
                    start = rng.randint(0, length)
                    stop = rng.randint(start, length)
                    intervals.append((start, stop))
                specs.append(RowSpec(row, tuple(intervals)))
                if not intervals:
                    # An empty interval tuple means "the whole array".
                    expected.append((row, 0, length, a["expected"][row]))
                else:
                    for start, stop in intervals:
                        expected.append(
                            (row, start, stop, a["expected"][row][start:stop])
                        )
            result = engine.query(specs)
            got = [
                (e["source_row"], e["element_start"], e["element_stop"], e["samples"])
                for e in result["rows"]
            ]
            assert got == expected
            assert result["cache"]["peak_cache_bytes"] <= 8192


def test_random_requests_with_switch_match_new_version(random_family):
    a, b, nrows = random_family
    rng = random.Random(7)
    rows = [rng.randrange(nrows) for _ in range(6)]
    specs = [RowSpec(r, ((0, min(10, len(b["expected"][r]))),)) for r in rows]
    with serve(
        [a["path"], b["path"]],
        switch_when=lambda i, s, e: s >= a["heap_start"],
    ) as running:
        result = ProjectionEngine(running.url).query(specs)
    for entry in result["rows"]:
        row = entry["source_row"]
        assert entry["samples"] == b["expected"][row][: entry["element_stop"]]
