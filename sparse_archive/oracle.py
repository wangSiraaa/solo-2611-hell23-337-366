"""Independent oracle: applies delta ops to a plain bytearray, one by one.

This is the reference semantics the archive is checked against.  It knows
nothing about segments, payload files, or indexes -- just ordered ops on a
bytearray:

* resize first (truncate drops the tail; extend pads zeros -- extended
  ranges never resurrect older ancestor bytes),
* then apply ops in declared order (later writes win),
* DATA writes its payload, HOLE writes zeros.
"""
from __future__ import annotations

from .model import BLOCK_SIZE, DATA, Op, sha256_hex


class Oracle:
    def __init__(self) -> None:
        self.view = bytearray()

    def ingest_base(self, data: bytes) -> None:
        self.view = bytearray(data)

    def apply_delta(self, new_size: int, ops: list[Op]) -> None:
        if new_size < len(self.view):
            del self.view[new_size:]
        elif new_size > len(self.view):
            self.view.extend(b"\x00" * (new_size - len(self.view)))
        for op in ops:
            if op.start == op.end:
                continue
            if op.kind == DATA:
                self.view[op.start:op.end] = op.data
            else:
                self.view[op.start:op.end] = b"\x00" * (op.end - op.start)

    def read(self, start: int, end: int) -> bytes:
        return bytes(self.view[start:end])

    def snapshot(self) -> bytes:
        return bytes(self.view)

    def block_digests(self) -> list[str]:
        size = len(self.view)
        return [
            sha256_hex(bytes(self.view[b * BLOCK_SIZE:
                                       min((b + 1) * BLOCK_SIZE, size)]))
            for b in range((size + BLOCK_SIZE - 1) // BLOCK_SIZE)
        ]
