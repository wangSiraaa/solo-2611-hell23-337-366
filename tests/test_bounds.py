"""Illegal-descriptor rejection, with no reads into other blocks."""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

from fitsproj.engine import ProjectionEngine, RowSpec
from fitsproj.errors import DescriptorBoundsError
from fitsproj.generate import write_table
from tests.conftest import serve


def _patch_descriptor(path, row, count, offset, dat_loc, row_width):
    raw = bytearray(Path(path).read_bytes())
    pos = dat_loc + row * row_width + 4
    struct.pack_into(">ii", raw, pos, count, offset)
    Path(path).write_bytes(raw)


def _base_table(tmp_path, nrows=12, length=8):
    lengths = [length] * nrows
    return write_table(tmp_path / "base.fits", lengths)


@pytest.mark.parametrize(
    "count,offset",
    [
        (10**9, 0),          # count far beyond PCOUNT
        (8, 2**31 - 8),      # offset near INT_MAX
        (-1, 0),             # negative count
        (8, -4),             # negative offset
    ],
)
def test_illegal_heap_descriptor_rejected_without_heap_io(tmp_path, count, offset):
    spec = _base_table(tmp_path)
    _patch_descriptor(
        spec["path"], 3, count, offset, spec["dat_loc"], spec["row_width"]
    )

    with serve([spec["path"]]) as running:
        with pytest.raises(DescriptorBoundsError):
            ProjectionEngine(running.url).query([RowSpec(0, ()), RowSpec(3, ())])
        log = running.source.log()

    # Exactly headers + the two requested descriptors were read.  No request
    # may start inside the heap, and no body byte came from the heap.
    assert [r["range_start"] for r in log] == [
        0,
        2880,
        spec["dat_loc"] + 0 * spec["row_width"] + 4,
        spec["dat_loc"] + 3 * spec["row_width"] + 4,
    ]
    assert running.source.sent == 2880 * 2 + 8 * 2
    assert all(r["range_end"] < spec["heap_start"] for r in log)


def test_illegal_element_interval_rejected_without_heap_io(tmp_path):
    spec = _base_table(tmp_path, length=8)
    # Legal descriptor, but the requested interval runs past the array length.
    with serve([spec["path"]]) as running:
        with pytest.raises(DescriptorBoundsError):
            ProjectionEngine(running.url).query([RowSpec(2, ((0, 9),))])
        log = running.source.log()
    assert all(r["range_end"] < spec["heap_start"] for r in log)
    assert running.source.sent == 2880 * 2 + 8


def test_one_bad_row_rejects_whole_request(tmp_path):
    """A single illegal descriptor in a multi-row request rejects everything."""

    spec = _base_table(tmp_path)
    _patch_descriptor(spec["path"], 6, 10**8, 0, spec["dat_loc"], spec["row_width"])
    with serve([spec["path"]]) as running:
        with pytest.raises(DescriptorBoundsError) as info:
            ProjectionEngine(running.url).query(
                [RowSpec(0, ()), RowSpec(6, ()), RowSpec(1, ())]
            )
        log = running.source.log()
    assert info.value.problems
    assert any("row 6" in p for p in info.value.problems)
    # No partial result object leaks out and no heap bytes are read.
    assert all(r["range_end"] < spec["heap_start"] for r in log)


def test_negative_relative_offset_underflow_rejected(tmp_path):
    """Offset+count legal in count but offset so large that extent underflows."""

    spec = _base_table(tmp_path)
    # offset + count*4 overflows the PCOUNT check.
    _patch_descriptor(spec["path"], 4, 2048, spec["pcount"] - 4, spec["dat_loc"], spec["row_width"])
    with serve([spec["path"]]) as running:
        with pytest.raises(DescriptorBoundsError):
            ProjectionEngine(running.url).query([RowSpec(4, ())])
        assert all(r["range_end"] < spec["heap_start"] for r in running.source.log())
