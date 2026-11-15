"""Parse the supported remote FITS subset from *ranged* header bytes.

The engine never downloads the whole file.  It walks FITS header blocks over
HTTP Range requests, parses each header with ``astropy.io.fits.Header`` and
derives the byte layout of the one BINTABLE HDU we support:

* one binary table extension with fixed ``J`` (signed big-endian 32-bit) and
  variable length ``PJ()`` (signed big-endian 32-bit) columns,
* uncompressed, big-endian ``BITPIX=8`` binary table payload.

Only parsed header bytes are needed here.  Table descriptors and heap contents
are fetched later by :mod:`fitsproj.engine` and are never read from a local
copy of the source file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from astropy.io import fits

from .errors import HeaderParseError

HEADER_BLOCK = 2880
#: Parsed FITS headers up to this many bytes do not count against the 8 KiB
#: data-page cache budget (the problem statement allows 9 KiB).
MAX_PARSED_HEADER_BYTES = 9216

_TFORM_RE = re.compile(r"^(?P<code>[A-Z])(?:\s*\(\s*(?P<n>\d*)\s*\))?$")


@dataclass(frozen=True)
class Column:
    index: int  # 1-based FITS field index
    name: str
    tform: str
    offset: int  # byte offset inside the row
    size: int  # byte width inside the row (8 for a P descriptor)
    is_variable: bool
    element_size: int  # bytes per array element (4 for J)


@dataclass(frozen=True)
class Layout:
    """Byte layout of the supported FITS subset."""

    etag: str
    file_size: int
    primary_header_start: int
    primary_header_end: int
    ext_header_start: int
    ext_header_end: int
    dat_start: int
    heap_start: int
    heap_end: int
    row_width: int
    nrows: int
    pcount: int
    columns: tuple[Column, ...]
    id_column: Column
    var_column: Column
    header_bytes: int

    @property
    def table_start(self) -> int:
        return self.dat_start

    @property
    def table_end(self) -> int:
        return self.heap_start

    def row_start(self, row: int) -> int:
        return self.dat_start + row * self.row_width

    def descriptor_offset(self, row: int) -> int:
        return self.row_start(row) + self.var_column.offset

    def as_dict(self) -> dict:
        return {
            "etag": self.etag,
            "file_size": self.file_size,
            "dat_start": self.dat_start,
            "table_range": [self.table_start, self.table_end],
            "heap_range": [self.heap_start, self.heap_end],
            "row_width": self.row_width,
            "nrows": self.nrows,
            "pcount": self.pcount,
            "columns": [
                {
                    "index": c.index,
                    "name": c.name,
                    "tform": c.tform,
                    "row_offset": c.offset,
                    "row_size": c.size,
                    "variable": c.is_variable,
                    "element_size": c.element_size,
                }
                for c in self.columns
            ],
            "header_bytes": self.header_bytes,
            "primary_header": [self.primary_header_start, self.primary_header_end],
            "extension_header": [self.ext_header_start, self.ext_header_end],
        }


def _block_has_end(block: bytes) -> bool:
    if len(block) != HEADER_BLOCK:
        raise HeaderParseError(f"short FITS header block: {len(block)} bytes")
    for pos in range(0, HEADER_BLOCK, 80):
        keyword = block[pos : pos + 8].strip().upper()
        if keyword == b"END":
            return True
    return False


def read_header(client, start: int, *, if_match: str | None, kind: str, attempt: int = 0):
    """Read one complete FITS header over Range requests.

    Returns ``(header, header_end, etag, file_size)``.  The first block is
    requested without ``If-Match`` because no version identity is known yet;
    subsequent blocks use the ETag returned by the first block, so a version
    flip between header blocks surfaces as 412.
    """

    raw = b""
    offset = start
    etag = if_match
    file_size = None
    while True:
        resp = client.fetch(
            offset,
            offset + HEADER_BLOCK - 1,
            if_match=etag,
            kind=kind,
            attempt=attempt,
        )
        raw += resp.body
        if etag is None:
            etag = resp.etag
        if file_size is None:
            file_size = resp.file_size
        if len(raw) > MAX_PARSED_HEADER_BYTES:
            raise HeaderParseError(
                f"{kind} header exceeds the supported {MAX_PARSED_HEADER_BYTES} "
                f"parsed-header bytes (already read {len(raw)})"
            )
        if _block_has_end(resp.body):
            break
        offset += HEADER_BLOCK
    if etag is None:
        raise HeaderParseError("server did not supply an ETag")
    # astropy gives us robust card parsing / value decoding for the cards we
    # actually use.  Header.fromstring accepts the padded block bytes.
    header = fits.Header.fromstring(raw)
    return header, start + len(raw), etag, file_size


def _header_data_bytes(header, default_bitpix: int = 8) -> int:
    naxis = int(header.get("NAXIS", 0))
    if naxis == 0:
        return 0
    bitpix = abs(int(header.get("BITPIX", default_bitpix)))
    gcount = int(header.get("GCOUNT", 1))
    count = gcount
    for axis in range(1, naxis + 1):
        count *= int(header[f"NAXIS{axis}"])
    return (bitpix // 8) * count


def _pad2880(length: int) -> int:
    return ((length + HEADER_BLOCK - 1) // HEADER_BLOCK) * HEADER_BLOCK


def _parse_tform(tform: str) -> tuple[str, int, bool]:
    text = tform.strip().upper()
    if text.startswith("P"):
        variable = True
        rest = text[1:]
    else:
        variable = False
        rest = text
    match = _TFORM_RE.match(rest)
    if not match:
        raise HeaderParseError(f"unsupported TFORM {tform!r}")
    code = match.group("code")
    if code != "J":
        raise HeaderParseError(
            f"unsupported TFORM {tform!r}; only J and PJ() are supported"
        )
    element_size = 4
    size = 8 if variable else element_size
    return code, size, variable


def discover_layout(client, *, attempt: int = 0) -> Layout:
    primary, primary_end, etag, file_size = read_header(
        client, 0, if_match=None, kind="primary-header", attempt=attempt
    )
    if int(primary.get("NAXIS", 0)) != 0:
        raise HeaderParseError("only a data-less primary HDU is supported")
    ext_start = primary_end + _pad2880(_header_data_bytes(primary))

    ext, ext_end, etag2, file_size2 = read_header(
        client, ext_start, if_match=etag, kind="extension-header", attempt=attempt
    )
    if etag2 != etag:
        raise HeaderParseError("extension header version differs from primary")
    if str(ext.get("XTENSION", "")).strip().upper() != "BINTABLE":
        raise HeaderParseError("only a single BINTABLE extension is supported")
    if int(ext.get("GCOUNT", 1)) != 1:
        raise HeaderParseError("only GCOUNT=1 binary tables are supported")
    if int(ext.get("BITPIX", 8)) != 8:
        raise HeaderParseError("only BITPIX=8 binary tables are supported")

    nrows = int(ext["NAXIS2"])
    row_width = int(ext["NAXIS1"])
    pcount = int(ext.get("PCOUNT", 0))
    tfields = int(ext.get("TFIELDS", 0))
    if nrows < 0 or row_width < 0 or pcount < 0:
        raise HeaderParseError("negative BINTABLE dimensions")

    columns: list[Column] = []
    cursor = 0
    for index in range(1, tfields + 1):
        tform = str(ext.get(f"TFORM{index}", "")).strip()
        name = str(ext.get(f"TTYPE{index}", f"col{index}")).strip()
        code, size, variable = _parse_tform(tform)
        columns.append(
            Column(
                index=index,
                name=name,
                tform=tform,
                offset=cursor,
                size=size,
                is_variable=variable,
                element_size=4 if code == "J" else 0,
            )
        )
        cursor += size
    if not columns:
        raise HeaderParseError("BINTABLE has no columns")
    if cursor != row_width:
        raise HeaderParseError(
            f"row width mismatch: TFORM sizes {cursor} vs NAXIS1 {row_width}"
        )

    variable_columns = [c for c in columns if c.is_variable]
    if len(variable_columns) != 1:
        raise HeaderParseError("exactly one variable-length PJ() column is required")
    var_column = variable_columns[0]
    id_columns = [c for c in columns if not c.is_variable and c.name == "id"]
    if not id_columns:
        id_columns = [c for c in columns if not c.is_variable]
    if not id_columns:
        raise HeaderParseError("a fixed J 'id' column is required")
    id_column = id_columns[0]

    dat_start = ext_end
    heap_start = dat_start + int(ext.get("THEAP", row_width * nrows))
    heap_end = heap_start + pcount
    if heap_start < dat_start or heap_end < heap_start:
        raise HeaderParseError("invalid heap bounds")
    if file_size is None:
        file_size = file_size2
    if file_size is not None and heap_end > file_size:
        raise HeaderParseError(
            f"heap [{heap_start}, {heap_end}) exceeds remote file size {file_size}"
        )

    header_bytes = (primary_end - 0) + (ext_end - ext_start)
    if header_bytes > MAX_PARSED_HEADER_BYTES:
        raise HeaderParseError(
            f"parsed FITS headers {header_bytes} exceed {MAX_PARSED_HEADER_BYTES}"
        )

    return Layout(
        etag=etag,
        file_size=int(file_size),
        primary_header_start=0,
        primary_header_end=primary_end,
        ext_header_start=ext_start,
        ext_header_end=ext_end,
        dat_start=dat_start,
        heap_start=heap_start,
        heap_end=heap_end,
        row_width=row_width,
        nrows=nrows,
        pcount=pcount,
        columns=tuple(columns),
        id_column=id_column,
        var_column=var_column,
        header_bytes=header_bytes,
    )
