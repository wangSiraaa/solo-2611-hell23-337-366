"""Deterministic generators for the supported FITS subset.

These helpers build *real* FITS files (via astropy) in the same legal input
family as the fixtures but with controlled lengths, values, shared heap objects
and partial overlaps.  They let the test suite validate the engine against an
independent oracle instead of only the two checked-in fixtures.

Only the supported subset is produced: one ``BINTABLE`` extension, a fixed
signed 32-bit ``J`` column and one variable-length signed 32-bit ``PJ()``
column, big-endian, uncompressed.
"""

from __future__ import annotations

import struct
from pathlib import Path

import numpy as np
from astropy.io import fits

ELEMENT = 4


def default_values(row: int, length: int, delta: int = 0) -> np.ndarray:
    """Deterministic values that are distinct per row and may be negative."""

    return (np.arange(length, dtype=np.int64) - 500 + row * 1000 + delta).astype(
        np.int32
    )


def write_table(
    path,
    lengths,
    *,
    delta: int = 0,
    values_fn=default_values,
    share=None,
    overlap=None,
):
    """Write one FITS file with the requested per-row array lengths.

    Parameters
    ----------
    lengths:
        Per-row element counts.
    delta:
        Offset added to default values (used to make a second version).
    share:
        ``{row: target_row}`` makes ``row``'s descriptor point at
        ``target_row``'s heap object (identical count/offset).  The two source
        rows stay distinct while sharing one heap object.
    overlap:
        ``{row: (target_row, skip)}`` makes ``row``'s object start ``skip``
        elements into ``target_row``'s object and run to the end of it, i.e. a
        genuine partial heap overlap.

    Returns a spec dict including ``expected[row]``: the list of int32 values
    an engine should observe for the *whole* array of each row after the
    descriptor rewrite.  This is the oracle, computed from the intended
    semantics rather than by re-reading the output file.
    """

    lengths = [int(x) for x in lengths]
    nrows = len(lengths)
    share = dict(share or {})
    overlap = dict(overlap or {})
    values = [None] * nrows
    arrays = np.empty(nrows, dtype=object)
    for row, length in enumerate(lengths):
        arr = np.asarray(values_fn(row, length, delta), dtype=np.int32)
        if arr.shape != (length,):
            raise ValueError(
                f"values_fn returned shape {arr.shape} for length {length}"
            )
        arrays[row] = arr
        values[row] = arr

    hdu = fits.BinTableHDU.from_columns(
        [
            fits.Column(name="id", format="J", array=np.arange(nrows, dtype=np.int32)),
            fits.Column(name="samples", format="PJ()", array=arrays),
        ]
    )
    fits.HDUList([fits.PrimaryHDU(), hdu]).writeto(path, overwrite=True)

    with fits.open(path) as handle:
        dat_loc = handle[1].fileinfo()["datLoc"]
        width = int(handle[1].header["NAXIS1"])
        pcount = int(handle[1].header["PCOUNT"])
    raw = bytearray(Path(path).read_bytes())

    # Original (astropy-laid-out) descriptors.
    descriptor = {}
    for row in range(nrows):
        count, offset = struct.unpack(">ii", raw[dat_loc + row * width + 4 : dat_loc + row * width + 12])
        descriptor[row] = (count, offset)

    expected = {}
    for row in range(nrows):
        target = row
        skip = 0
        if row in share:
            target = share[row]
        elif row in overlap:
            target, skip = overlap[row]
        target_values = values[target]
        count, offset = descriptor[target]
        seen = target_values[skip:]
        descriptor[row] = (len(seen), offset + skip * ELEMENT)
        expected[row] = [int(x) for x in seen]

    for row in range(nrows):
        count, offset = descriptor[row]
        struct.pack_into(">ii", raw, dat_loc + row * width + 4, count, offset)
    Path(path).write_bytes(raw)

    return {
        "path": str(path),
        "nrows": nrows,
        "lengths": lengths,
        "descriptor": descriptor,
        "expected": expected,
        "delta": delta,
        "pcount": pcount,
        "dat_loc": dat_loc,
        "row_width": width,
        "heap_start": dat_loc + width * nrows,
        "file_size": len(raw),
    }


def make_pair(
    directory,
    nrows,
    delta_b,
    *,
    seed=0,
    lengths=None,
    lengths_b=None,
    share=None,
    overlap=None,
):
    """Create two equal-length versions ``x.fits`` and ``y.fits``.

    Both versions have the same total size.  By default they also share the same
    per-row lengths (only values differ); pass ``lengths_b`` to permute lengths
    between versions while keeping the *multiset* identical, so the total file
    size is unchanged but a row's descriptor points at a different heap extent
    in version B.  That makes "old descriptor + new heap" mixing observable.

    ``share`` / ``overlap`` apply identically to both versions.
    """

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    if lengths is None:
        rng = np.random.default_rng(seed)
        lengths = rng.integers(0, 2049, size=nrows).tolist()
    lengths = [int(x) for x in lengths]
    if lengths_b is None:
        lengths_b = list(lengths)
    else:
        lengths_b = [int(x) for x in lengths_b]
        if sorted(lengths_b) != sorted(lengths):
            raise AssertionError(
                "lengths_b must be a permutation of lengths to keep file size equal"
            )
    spec_a = write_table(
        directory / "x.fits", lengths, delta=0, share=share, overlap=overlap
    )
    spec_b = write_table(
        directory / "y.fits", lengths_b, delta=delta_b, share=share, overlap=overlap
    )
    if Path(spec_a["path"]).stat().st_size != Path(spec_b["path"]).stat().st_size:
        raise AssertionError("versions must be equal length")
    return spec_a, spec_b
