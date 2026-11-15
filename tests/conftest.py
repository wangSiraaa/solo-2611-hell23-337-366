"""Shared helpers for the projection-engine tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from fitsproj.generate import make_pair
from range_server import Source

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture(scope="session")
def fixture_a():
    return str(FIXTURES / "a.fits")


@pytest.fixture(scope="session")
def fixture_b():
    return str(FIXTURES / "b.fits")


@pytest.fixture(scope="session")
def queries():
    with open(FIXTURES / "queries.json", "r", encoding="utf-8") as handle:
        return json.load(handle)


class RunningSource:
    def __init__(self, source):
        self.source = source
        self.url = source.start()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.source.close()


def serve(files, **kwargs):
    return RunningSource(Source(files, **kwargs))


@pytest.fixture(scope="session")
def variant_pair(tmp_path_factory):
    """Two equal-length generated versions with adversarial lengths.

    * 200 rows, zero-length arrays, 1-element arrays, arrays up to 2048.
    * Row 5 and row 9 share one heap object (distinct source rows).
    * Row 11 partially overlaps row 5 (starts 7 elements in).
    """

    directory = tmp_path_factory.mktemp("variants")
    lengths = [0, 1, 2048, 7, 0, 100, 33, 2047, 2, 12, 2048, 50]
    lengths += [(i * 97) % 2049 for i in range(200 - len(lengths))]
    share = {9: 5}
    overlap = {11: (5, 7)}
    return make_pair(
        directory,
        200,
        delta_b=17,
        lengths=lengths,
        share=share,
        overlap=overlap,
    )
