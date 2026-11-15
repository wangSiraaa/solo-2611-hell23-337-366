"""Canonical binary encodings for rows, occurrence records and spill frames.

Row key encoding is injective and totally ordered (any consistent total order
suffices for sort-merge; both sides of a join always use this same encoding):

    null  -> 0x00
    int   -> 0x01 + 8-byte big-endian two's complement (int64 range)
    text  -> 0x02 + uvarint(utf8 length) + utf8 bytes

Length-prefixing the text payload makes the concatenation unambiguous: two
different rows never share a key byte string.  Text equality is exact Unicode
codepoint equality because UTF-8 encoding is injective.  NULL equals NULL
because every None maps to the same single byte.

Occurrence record:  uvarint(path_id) + uvarint(rownum) + row_key
Spill frame:        uvarint(item_length) + item bytes
"""
from __future__ import annotations

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

TAG_NULL = 0
TAG_INT = 1
TAG_TEXT = 2


class EncodingError(ValueError):
    pass


def uvarint_bytes(n: int) -> bytes:
    if n < 0:
        raise EncodingError("uvarint of negative number")
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def read_uvarint(buf: bytes, pos: int) -> tuple[int, int]:
    shift = 0
    result = 0
    while True:
        if pos >= len(buf):
            raise EncodingError("truncated varint")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
        if shift > 70:
            raise EncodingError("varint too long")


def encode_key(values: list) -> bytes:
    out = bytearray()
    for v in values:
        if v is None:
            out.append(TAG_NULL)
        elif isinstance(v, bool):
            raise EncodingError("bool is not a supported column value")
        elif isinstance(v, int):
            if not (INT64_MIN <= v <= INT64_MAX):
                raise EncodingError(f"int out of int64 range: {v}")
            out.append(TAG_INT)
            out += v.to_bytes(8, "big", signed=True)
        elif isinstance(v, str):
            b = v.encode("utf-8")
            out.append(TAG_TEXT)
            out += uvarint_bytes(len(b))
            out += b
        else:
            raise EncodingError(f"unsupported column value type: {type(v).__name__}")
    return bytes(out)


def decode_key(data: bytes) -> list:
    values = []
    pos = 0
    n = len(data)
    while pos < n:
        tag = data[pos]
        pos += 1
        if tag == TAG_NULL:
            values.append(None)
        elif tag == TAG_INT:
            values.append(int.from_bytes(data[pos:pos + 8], "big", signed=True))
            pos += 8
        elif tag == TAG_TEXT:
            ln, pos = read_uvarint(data, pos)
            values.append(data[pos:pos + ln].decode("utf-8"))
            pos += ln
        else:
            raise EncodingError(f"bad column tag {tag}")
    if pos != n:
        raise EncodingError("trailing bytes in key")
    return values


def encode_record(path_id: int, rownum: int, values: list) -> bytes:
    return uvarint_bytes(path_id) + uvarint_bytes(rownum) + encode_key(values)


def split_record(rec: bytes) -> tuple[int, int, bytes]:
    """-> (path_id, rownum, key_bytes)"""
    path_id, pos = read_uvarint(rec, 0)
    rownum, pos = read_uvarint(rec, pos)
    return path_id, rownum, rec[pos:]


def frame(item: bytes) -> bytes:
    return uvarint_bytes(len(item)) + item
