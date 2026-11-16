"""Core data model: delta declarations, op normalization, checksum blocks.

Protocol constants and pure functions shared by the archive, the CLI and
the tests.  Nothing here touches the filesystem.

Checksum blocks are 4096 bytes.  The digest of a block is sha256 over the
*logical* bytes of that block, i.e. ``min(4096, size - block_start)`` bytes
for the last (possibly partial) block -- no padding.  A digest therefore
always covers the full assembled content of the block, regardless of
whether the bytes come from DATA payload, HOLE synthesis, or ancestors.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from .errors import ValidationError

BLOCK_SIZE = 4096
MAX_FILE_BYTES = 64 * 1024 * 1024   # frozen input images are at most 64 MiB
MAX_GENERATIONS = 20                # chain length limit (stored generations)
MAX_READERS = 2                     # controlled concurrency: at most 2 readers
FORMAT_VERSION = 1

DATA = "data"
HOLE = "hole"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


_zero_digests: dict[int, str] = {}


def zero_digest(nbytes: int) -> str:
    """Digest of ``nbytes`` zero bytes (the content of an all-hole block)."""
    d = _zero_digests.get(nbytes)
    if d is None:
        d = sha256_hex(b"\x00" * nbytes)
        _zero_digests[nbytes] = d
    return d


def block_count(size: int) -> int:
    """Number of checksum blocks covering a logical file of ``size`` bytes."""
    return (size + BLOCK_SIZE - 1) // BLOCK_SIZE


@dataclass
class Op:
    """One declared change: a half-open [start, end) DATA or HOLE range.

    DATA ops carry their real payload in ``data`` (len == end - start).
    An explicit all-zero DATA op is still DATA -- the mapping records it as
    data, never as a hole.
    """
    kind: str
    start: int
    end: int
    data: bytes | None = None


@dataclass
class DeltaDecl:
    """Authoritative change log for one generation transition."""
    base_generation: int
    target_generation: int
    new_size: int
    ops: list[Op]
    block_digests: list[str]   # sha256 hex, one per checksum block of the target view


@dataclass
class Segment:
    """Normalized (non-overlapping, sorted) extent used for storage."""
    start: int
    end: int
    kind: str
    data: bytes | None = None          # only while building (DATA segments)
    payload_offset: int | None = None  # only in stored form


def _check_hex_digest(d: object) -> bool:
    return isinstance(d, str) and len(d) == 64 and all(
        c in "0123456789abcdef" for c in d)


def validate_decl(decl: DeltaDecl) -> None:
    """Reject illegal declarations before any state is touched."""
    for name in ("base_generation", "target_generation", "new_size"):
        v = getattr(decl, name)
        if not isinstance(v, int) or v < 0:
            raise ValidationError(f"{name} must be a non-negative int, got {v!r}")
    if decl.target_generation != decl.base_generation + 1:
        raise ValidationError(
            f"generations form a linear chain: target ({decl.target_generation}) "
            f"must equal base + 1 ({decl.base_generation})")
    if decl.new_size > MAX_FILE_BYTES:
        raise ValidationError(
            f"new_size {decl.new_size} exceeds {MAX_FILE_BYTES} byte limit")
    want = block_count(decl.new_size)
    if len(decl.block_digests) != want:
        raise ValidationError(
            f"need {want} block digests for size {decl.new_size}, "
            f"got {len(decl.block_digests)}")
    for i, d in enumerate(decl.block_digests):
        if not _check_hex_digest(d):
            raise ValidationError(f"block_digests[{i}] is not a sha256 hex digest")
    for i, op in enumerate(decl.ops):
        if op.kind not in (DATA, HOLE):
            raise ValidationError(f"op {i}: unknown kind {op.kind!r}")
        if not (0 <= op.start <= op.end <= decl.new_size):
            raise ValidationError(
                f"op {i}: range [{op.start},{op.end}) out of bounds "
                f"for new_size {decl.new_size}")
        if op.kind == DATA:
            if op.data is None or len(op.data) != op.end - op.start:
                raise ValidationError(
                    f"op {i}: DATA payload length must equal range length")
        elif op.data is not None:
            raise ValidationError(f"op {i}: HOLE op must not carry payload")


def normalize_ops(ops: list[Op]) -> list[Segment]:
    """Apply last-write-wins to the declared op order.

    Returns sorted, disjoint segments.  Overlapping earlier ops are split
    and the covered parts discarded; their payload bytes are dropped with
    them, so the stored payload only contains bytes that actually survive
    into the target view.
    """
    segs: list[Segment] = []
    for op in ops:
        if op.start == op.end:
            continue  # empty half-open range: legal no-op
        s, e = op.start, op.end
        out: list[Segment] = []
        for seg in segs:
            if seg.end <= s or seg.start >= e:
                out.append(seg)
                continue
            if seg.start < s:  # surviving left part of an earlier op
                out.append(Segment(
                    seg.start, s, seg.kind,
                    seg.data[: s - seg.start] if seg.kind == DATA else None))
            if seg.end > e:    # surviving right part of an earlier op
                out.append(Segment(
                    e, seg.end, seg.kind,
                    seg.data[e - seg.start:] if seg.kind == DATA else None))
        out.append(Segment(s, e, op.kind, op.data if op.kind == DATA else None))
        segs = sorted(out, key=lambda x: x.start)
    return segs


def delta_content_id(decl: DeltaDecl) -> str:
    """Content address of a delta declaration (idempotency / conflict check).

    Two declarations are 'the same delta' iff base, target, new_size, the
    ordered ops (with payload hashes) and all block digests match.
    """
    canon = {
        "base_generation": decl.base_generation,
        "target_generation": decl.target_generation,
        "new_size": decl.new_size,
        "ops": [
            {"kind": op.kind, "start": op.start, "end": op.end,
             "payload_sha256": sha256_hex(op.data) if op.kind == DATA else None}
            for op in decl.ops
        ],
        "block_digests": list(decl.block_digests),
    }
    blob = json.dumps(canon, sort_keys=True, separators=(",", ":")).encode()
    return sha256_hex(blob)


def decl_from_json(obj: dict, payload_blob: bytes | None = None) -> DeltaDecl:
    """Parse a JSON delta declaration (CLI / demo format).

    DATA ops either inline ``payload_hex`` or reference
    ``payload_offset``/``payload_length`` inside the separate payload blob.
    """
    ops: list[Op] = []
    for i, o in enumerate(obj.get("ops", [])):
        kind = o.get("type", o.get("kind"))
        start, end = int(o["start"]), int(o["end"])
        if kind == DATA:
            if "payload_hex" in o:
                data = bytes.fromhex(o["payload_hex"])
            else:
                if payload_blob is None:
                    raise ValidationError(
                        f"op {i}: DATA op needs --payload blob or payload_hex")
                off = int(o.get("payload_offset", 0))
                ln = int(o.get("payload_length", end - start))
                data = payload_blob[off:off + ln]
            ops.append(Op(DATA, start, end, data))
        elif kind == HOLE:
            ops.append(Op(HOLE, start, end, None))
        else:
            raise ValidationError(f"op {i}: unknown kind {kind!r}")
    return DeltaDecl(
        base_generation=int(obj["base_generation"]),
        target_generation=int(obj["target_generation"]),
        new_size=int(obj["new_size"]),
        ops=ops,
        block_digests=list(obj["block_digests"]),
    )
