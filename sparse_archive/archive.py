"""Sparse multi-generation archive with cross-generation range reads.

Design (see DESIGN.md for the full tradeoff discussion):

* Per-generation *extent layers*: every generation stores only its own
  normalized DATA/HOLE segments; DATA bytes live in that generation's
  ``payload.bin``.  Base and compacted-snapshot generations are
  self-contained (their segments cover the whole logical size).
* Block-granular integrity: the view of every generation is certified by
  one sha256 per 4096-byte logical block.  Reads assemble each intersected
  block across DATA/HOLE/ancestor boundaries and verify the digest before
  returning a single byte.
* Atomic publication: payload+index are staged under ``tmp/`` and fsynced,
  then the manifest is switched with an atomic rename.  A crash never
  publishes half a generation; re-applying the same delta is idempotent,
  the same target with different content is rejected.
* Retention: the two newest generations are always readable; older ones
  only while a reader pin protects them.  ``compact()`` rebases the oldest
  still-needed generation into a self-contained snapshot and reclaims
  everything below it once no reader pin covers it.

On-disk layout::

    <archive>/manifest.json            committed state (atomically replaced)
    <archive>/generations/g000001/{index.json,payload.bin}
    <archive>/pins/<reader>.json       reader pins (reclaim barriers)
    <archive>/tmp/                     staging, cleaned on open
"""
from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import shutil
import threading
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path

from .errors import (
    AncestorError,
    ArchiveError,
    CorruptionError,
    DeltaConflictError,
    DigestMismatchError,
    RangeRequestError,
    ReaderLimitError,
    RetentionError,
    ValidationError,
    WriterBusyError,
)
from .model import (
    BLOCK_SIZE,
    DATA,
    FORMAT_VERSION,
    HOLE,
    MAX_FILE_BYTES,
    MAX_GENERATIONS,
    MAX_READERS,
    DeltaDecl,
    Segment,
    block_count,
    delta_content_id,
    normalize_ops,
    sha256_hex,
    validate_decl,
    zero_digest,
)

INDEX_NAME = "index.json"
PAYLOAD_NAME = "payload.bin"


# --------------------------------------------------------------------------
# small helpers

def _write_file_sync(path: Path, data: bytes) -> None:
    with open(path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _scan_data_extents(path: Path, size: int) -> list[tuple[int, int]]:
    """Filesystem data/hole map of a frozen source file.

    Uses SEEK_DATA/SEEK_HOLE.  Coarse reporting is fine: anything the
    filesystem calls data is stored as data (never loses bytes); anything
    it calls a hole becomes a hole segment (reads back as zeros, which is
    what the file would have returned).  If the capability is missing we
    fall back to treating the whole file as data.
    """
    if size == 0:
        return []
    fd = os.open(path, os.O_RDONLY)
    try:
        try:
            os.lseek(fd, 0, os.SEEK_DATA)
        except OSError as exc:
            if exc.errno == errno.ENXIO:
                return []  # size > 0 but no data extents at all
            return [(0, size)]  # SEEK_DATA unsupported: honest fallback
        extents = []
        pos = 0
        while pos < size:
            try:
                d = os.lseek(fd, pos, os.SEEK_DATA)
            except OSError as exc:
                if exc.errno == errno.ENXIO:
                    break
                raise
            if d >= size:
                break
            h = os.lseek(fd, d, os.SEEK_HOLE)
            extents.append((d, min(h, size)))
            pos = min(h, size)
        return extents
    finally:
        os.close(fd)


def _resolve_from_segments(segments: list[dict], payload: bytes,
                           s: int, e: int) -> bytes:
    """Resolve [s, e) against a full-coverage segment list + its payload."""
    out = bytearray(e - s)
    for seg in segments:
        os_, oe = max(s, seg["start"]), min(e, seg["end"])
        if os_ >= oe:
            continue
        if seg["type"] == DATA:
            off = seg["payload_offset"] + (os_ - seg["start"])
            out[os_ - s:oe - s] = payload[off:off + (oe - os_)]
    return bytes(out)


class Meter:
    """Per-operation I/O accounting.

    Categories required by the contract are reported separately: real
    ancestor payload reads, own-generation payload reads, metadata reads,
    cache hits, and synthesized hole zeros.
    """

    __slots__ = ("own_payload_bytes", "ancestor_payload_bytes",
                 "metadata_bytes", "cache_bytes", "synthesized_zero_bytes",
                 "payload_extents", "metadata_files")

    def __init__(self) -> None:
        self.own_payload_bytes = 0
        self.ancestor_payload_bytes = 0
        self.metadata_bytes = 0
        self.cache_bytes = 0
        self.synthesized_zero_bytes = 0
        self.payload_extents: list[dict] = []
        self.metadata_files: list[str] = []

    def as_dict(self) -> dict:
        return {
            "own_payload_bytes": self.own_payload_bytes,
            "ancestor_payload_bytes": self.ancestor_payload_bytes,
            "metadata_bytes": self.metadata_bytes,
            "cache_bytes": self.cache_bytes,
            "synthesized_zero_bytes": self.synthesized_zero_bytes,
            "payload_extents": list(self.payload_extents),
            "metadata_files": list(self.metadata_files),
        }

    def merge(self, other: "Meter") -> None:
        self.own_payload_bytes += other.own_payload_bytes
        self.ancestor_payload_bytes += other.ancestor_payload_bytes
        self.metadata_bytes += other.metadata_bytes
        self.cache_bytes += other.cache_bytes
        self.synthesized_zero_bytes += other.synthesized_zero_bytes
        self.payload_extents += other.payload_extents
        self.metadata_files += other.metadata_files


class _IO:
    """Read access to generation storage, optionally metered.

    ``entries`` is the manifest generation list the caller resolved against
    (a reader's snapshot, or the live manifest for writer-side paths), so a
    compaction that commits mid-read can never change what we resolve to.
    """

    def __init__(self, archive: "Archive", entries: list[dict],
                 request_generation: int | None = None,
                 meter: Meter | None = None,
                 index_cache: dict | None = None) -> None:
        self._archive = archive
        self.entries = entries
        self.request_generation = request_generation
        self.meter = meter
        self.index_cache = index_cache if index_cache is not None else {}

    def index(self, entry: dict) -> dict:
        g = entry["generation"]
        idx = self.index_cache.get(g)
        if idx is None:
            path = self._archive._gen_dir(entry) / INDEX_NAME
            raw = path.read_bytes()
            idx = json.loads(raw)
            self.index_cache[g] = idx
            if self.meter is not None:
                self.meter.metadata_bytes += len(raw)
                self.meter.metadata_files.append(str(path))
        return idx

    def payload(self, entry: dict, offset: int, length: int) -> bytes:
        if length == 0:
            return b""
        path = self._archive._gen_dir(entry) / PAYLOAD_NAME
        with open(path, "rb") as f:
            f.seek(offset)
            data = f.read(length)
        if len(data) != length:
            raise CorruptionError(
                f"short read on {path}: wanted {length} at {offset}, "
                f"got {len(data)}")
        if self.meter is not None:
            if entry["generation"] == self.request_generation:
                self.meter.own_payload_bytes += length
            else:
                self.meter.ancestor_payload_bytes += length
            self.meter.payload_extents.append({
                "generation": entry["generation"],
                "offset": offset, "length": length})
        return data

    def zeros(self, n: int) -> None:
        if self.meter is not None:
            self.meter.synthesized_zero_bytes += n


def _resolve_map(entries: list[dict], gen: int, s: int, e: int,
                 io: _IO) -> list[tuple]:
    """Final extent map of generation ``gen``'s view over [s, e).

    Returns sorted (start, end, kind, src_entry, src_off) tuples covering
    [s, e).  Walks generations newest-first; each generation's stored
    segments override everything older.  Offsets at or beyond a
    generation's logical size are holes *in that generation's view* --
    truncate-then-extend never resurrects older ancestor bytes, so the
    walk stops there instead of falling through to older generations.
    """
    result: list[tuple] = []
    uncovered = [(s, e)]
    for entry in reversed(entries):
        if entry["generation"] > gen:
            continue
        if not uncovered:
            break
        hsize = entry["logical_size"]
        still: list[tuple[int, int]] = []
        for ps, pe in uncovered:
            lo = (ps, min(pe, hsize))
            hi = (max(ps, hsize), pe)
            if hi[0] < hi[1]:
                result.append((hi[0], hi[1], HOLE, None, None))
            if lo[0] < lo[1]:
                still.append(lo)
        uncovered = still
        if not uncovered:
            break
        idx = io.index(entry)
        for seg in idx["segments"]:
            if not uncovered:
                break
            ss, se = seg["start"], seg["end"]
            nxt: list[tuple[int, int]] = []
            for ps, pe in uncovered:
                os_, oe = max(ps, ss), min(pe, se)
                if os_ >= oe:
                    nxt.append((ps, pe))
                    continue
                if seg["type"] == DATA:
                    result.append((os_, oe, DATA, entry,
                                   seg["payload_offset"] + (os_ - ss)))
                else:
                    result.append((os_, oe, HOLE, None, None))
                if ps < os_:
                    nxt.append((ps, os_))
                if oe < pe:
                    nxt.append((oe, pe))
            uncovered = nxt
    for ps, pe in uncovered:
        result.append((ps, pe, HOLE, None, None))
    result.sort(key=lambda x: x[0])
    return result


def _resolve_range(entries: list[dict], gen: int, s: int, e: int,
                   io: _IO) -> bytes:
    """Assemble the view of generation ``gen`` over [s, e)."""
    out = bytearray(e - s)
    for rs, re_, kind, src, off in _resolve_map(entries, gen, s, e, io):
        if kind == DATA:
            out[rs - s:re_ - s] = io.payload(src, off, re_ - rs)
        else:
            io.zeros(re_ - rs)
    return bytes(out)


# --------------------------------------------------------------------------
# archive

class Archive:
    """Single-writer / two-reader sparse generation archive."""

    def __init__(self, path, hooks: dict | None = None) -> None:
        self.path = Path(path)
        self.hooks = dict(hooks or {})
        self._writer_lock = threading.Lock()
        self._readers_lock = threading.Lock()
        self._lock_fh = None
        if (self.path / "manifest.json").exists():
            self._recover()

    # -- construction / recovery -----------------------------------------

    @classmethod
    def create(cls, path, hooks: dict | None = None) -> "Archive":
        path = Path(path)
        if (path / "manifest.json").exists():
            raise ValidationError(f"archive already exists at {path}")
        path.mkdir(parents=True, exist_ok=True)
        for d in ("generations", "pins", "tmp"):
            (path / d).mkdir(exist_ok=True)
        arch = cls.__new__(cls)
        arch.path = path
        arch.hooks = dict(hooks or {})
        arch._writer_lock = threading.Lock()
        arch._readers_lock = threading.Lock()
        arch._lock_fh = None
        arch._store_manifest({
            "format": FORMAT_VERSION, "tip": 0,
            "generations": [], "pending_reclaim": []})
        return arch

    @classmethod
    def open(cls, path, hooks: dict | None = None) -> "Archive":
        if not (Path(path) / "manifest.json").exists():
            raise ValidationError(f"no archive at {path}")
        return cls(path, hooks)

    def _recover(self) -> None:
        """Crash recovery on open: drop uncommitted staging and orphans."""
        tmp_manifest = self.path / "manifest.json.tmp"
        if tmp_manifest.exists():
            tmp_manifest.unlink()  # rename never happened: old manifest stands
        m = self._load_manifest()
        referenced = {e["dir"] for e in m["generations"]}
        referenced |= {e["dir"] for e in m["pending_reclaim"]}
        genroot = self.path / "generations"
        if genroot.exists():
            for d in genroot.iterdir():
                if d.is_dir() and d.name not in referenced:
                    shutil.rmtree(d)  # staged but never published
        tmproot = self.path / "tmp"
        if tmproot.exists():
            for d in tmproot.iterdir():
                if d.is_dir():
                    shutil.rmtree(d)
                else:
                    d.unlink()

    # -- manifest ----------------------------------------------------------

    def _load_manifest(self) -> dict:
        raw, m = self._load_manifest_raw()
        return m

    def _load_manifest_raw(self) -> tuple[bytes, dict]:
        raw = (self.path / "manifest.json").read_bytes()
        m = json.loads(raw)
        if m.get("format") != FORMAT_VERSION:
            raise CorruptionError(f"unsupported manifest format {m.get('format')}")
        return raw, m

    def _store_manifest(self, m: dict) -> None:
        raw = json.dumps(m, indent=1, sort_keys=True).encode()
        tmp = self.path / "manifest.json.tmp"
        _write_file_sync(tmp, raw)
        os.replace(tmp, self.path / "manifest.json")
        _fsync_dir(self.path)

    def _gen_dir(self, entry: dict) -> Path:
        return self.path / "generations" / entry["dir"]

    def _load_index(self, entry: dict) -> dict:
        return json.loads((self._gen_dir(entry) / INDEX_NAME).read_bytes())

    # -- locking -----------------------------------------------------------

    @contextmanager
    def _writer(self):
        """Single-writer section: in-process lock + flock (crash-safe)."""
        if not self._writer_lock.acquire(blocking=False):
            raise WriterBusyError("another writer/compact is in flight")
        fh = None
        try:
            fh = open(self.path / "writer.lock", "a+b")
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                fh.close()
                fh = None
                raise WriterBusyError("writer lock held by another process")
            self._lock_fh = fh
            yield
        finally:
            if fh is not None:
                try:
                    fcntl.flock(fh, fcntl.LOCK_UN)
                except OSError:
                    pass
                fh.close()
                self._lock_fh = None
            self._writer_lock.release()

    def _fire(self, hook: str, **info) -> None:
        h = self.hooks.get(hook)
        if h is not None:
            h({"event": hook, **info})

    # -- staging / publication --------------------------------------------

    def _stage_generation(self, index: dict, payload: bytes) -> Path:
        """Write payload+index into tmp/ and fsync.  Nothing is visible yet."""
        staging = self.path / "tmp" / ("stage-" + uuid.uuid4().hex)
        staging.mkdir(parents=True)
        _write_file_sync(staging / PAYLOAD_NAME, payload)
        _write_file_sync(staging / INDEX_NAME,
                         json.dumps(index, indent=1, sort_keys=True).encode())
        _fsync_dir(staging)
        return staging

    def _publish_staged(self, staging: Path, dirname: str) -> Path:
        final = self.path / "generations" / dirname
        if final.exists():
            raise CorruptionError(f"generation dir {dirname} already exists")
        os.rename(staging, final)
        _fsync_dir(self.path / "generations")
        return final

    # -- ingest-base --------------------------------------------------------

    def ingest_base(self, source) -> dict:
        """Ingest a frozen plain file as generation 1 of an empty archive."""
        source = Path(source)
        with self._writer():
            m = self._load_manifest()
            if m["generations"]:
                raise ValidationError("ingest-base requires an empty archive")
            size = source.stat().st_size
            if size > MAX_FILE_BYTES:
                raise ValidationError(
                    f"source {source} is {size} bytes, limit is {MAX_FILE_BYTES}")
            extents = _scan_data_extents(source, size)
            payload = bytearray()
            segments: list[dict] = []
            file_hash = hashlib.sha256()
            prev = 0
            with open(source, "rb") as f:
                for ds, de in extents:
                    if ds > prev:
                        segments.append({"start": prev, "end": ds, "type": HOLE})
                        file_hash.update(b"\x00" * (ds - prev))
                    f.seek(ds)
                    blob = f.read(de - ds)
                    if len(blob) != de - ds:
                        raise CorruptionError(f"short read on source {source}")
                    segments.append({
                        "start": ds, "end": de, "type": DATA,
                        "payload_offset": len(payload),
                        "payload_length": de - ds})
                    payload += blob
                    file_hash.update(blob)
                    prev = de
            if prev < size:
                segments.append({"start": prev, "end": size, "type": HOLE})
                file_hash.update(b"\x00" * (size - prev))
            digests = [
                sha256_hex(_resolve_from_segments(
                    segments, bytes(payload),
                    b * BLOCK_SIZE, min((b + 1) * BLOCK_SIZE, size)))
                for b in range(block_count(size))
            ]
            index = {
                "generation": 1, "kind": "base", "base_generation": None,
                "logical_size": size, "delta_id": None,
                "segments": segments, "block_digests": digests,
                "payload_file": PAYLOAD_NAME, "payload_bytes": len(payload),
                "source_sha256": file_hash.hexdigest(),
            }
            staging = self._stage_generation(index, bytes(payload))
            published = None
            try:
                published = self._publish_staged(staging, "g%06d" % 1)
                m["generations"].append({
                    "generation": 1, "dir": "g%06d" % 1, "kind": "base",
                    "base_generation": None, "logical_size": size,
                    "delta_id": None})
                m["tip"] = 1
                self._store_manifest(m)
            except Exception:
                if published is None:
                    shutil.rmtree(staging, ignore_errors=True)
                else:
                    shutil.rmtree(published, ignore_errors=True)
                raise
            return {"generation": 1, "logical_size": size,
                    "payload_bytes": len(payload),
                    "data_extents": len([s for s in segments
                                         if s["type"] == DATA]),
                    "source_sha256": file_hash.hexdigest()}

    # -- apply-delta ---------------------------------------------------------

    def apply_delta(self, decl: DeltaDecl) -> dict:
        """Validate, verify, stage and atomically publish one generation."""
        validate_decl(decl)
        with self._writer():
            m = self._load_manifest()
            if not m["generations"]:
                raise AncestorError("no base generation; run ingest-base first")
            by = {e["generation"]: e for e in m["generations"]}
            content_id = delta_content_id(decl)
            existing = by.get(decl.target_generation)
            if existing is not None:
                idx = self._load_index(existing)
                if idx.get("delta_id") == content_id:
                    return {"generation": decl.target_generation,
                            "idempotent": True}
                raise DeltaConflictError(
                    f"generation {decl.target_generation} already exists with "
                    f"different content")
            tip = m["tip"]
            oldest = m["generations"][0]["generation"]
            if decl.base_generation != tip:
                raise AncestorError(
                    f"delta declares base_generation {decl.base_generation} "
                    f"but current tip is {tip} (older bases may have been "
                    f"reclaimed by compaction)")
            if tip - oldest + 1 >= MAX_GENERATIONS:
                raise ValidationError(
                    f"generation limit {MAX_GENERATIONS} reached; compact first")
            self._check_delta_digests(m, decl)
            segs = normalize_ops(decl.ops)
            payload = bytearray()
            segdicts: list[dict] = []
            for s in segs:
                if s.kind == DATA:
                    segdicts.append({
                        "start": s.start, "end": s.end, "type": DATA,
                        "payload_offset": len(payload),
                        "payload_length": s.end - s.start})
                    payload += s.data
                else:
                    segdicts.append({"start": s.start, "end": s.end,
                                     "type": HOLE})
            index = {
                "generation": decl.target_generation, "kind": "delta",
                "base_generation": decl.base_generation,
                "logical_size": decl.new_size, "delta_id": content_id,
                "declared_ops": [
                    {"kind": op.kind, "start": op.start, "end": op.end,
                     "payload_sha256": (sha256_hex(op.data)
                                        if op.kind == DATA else None)}
                    for op in decl.ops],
                "segments": segdicts,
                "block_digests": list(decl.block_digests),
                "payload_file": PAYLOAD_NAME, "payload_bytes": len(payload),
            }
            dirname = "g%06d" % decl.target_generation
            staging = self._stage_generation(index, bytes(payload))
            published = None
            try:
                # Pause point: data fully written, index not yet switched.
                self._fire("after_delta_data_written",
                           generation=decl.target_generation)
                published = self._publish_staged(staging, dirname)
                m["generations"].append({
                    "generation": decl.target_generation, "dir": dirname,
                    "kind": "delta", "base_generation": decl.base_generation,
                    "logical_size": decl.new_size, "delta_id": content_id})
                m["tip"] = decl.target_generation
                self._store_manifest(m)
            except Exception:
                tmp_manifest = self.path / "manifest.json.tmp"
                if tmp_manifest.exists():
                    tmp_manifest.unlink()
                if published is None:
                    shutil.rmtree(staging, ignore_errors=True)
                else:
                    shutil.rmtree(published, ignore_errors=True)
                raise
            return {"generation": decl.target_generation, "idempotent": False,
                    "payload_bytes": len(payload), "segments": len(segdicts)}

    def _check_delta_digests(self, m: dict, decl: DeltaDecl) -> None:
        """Every declared block digest must match the assembled target view.

        Blocks the delta does not touch must carry the base generation's
        recorded digest (that *is* their assembled content); blocks beyond
        the base size must be zero (truncate-then-extend never resurrects
        old bytes).  Touched blocks are assembled across DATA/HOLE/ancestor
        boundaries and verified -- never just the fresh payload.
        """
        by = {e["generation"]: e for e in m["generations"]}
        base = by[decl.base_generation]
        io = _IO(self, m["generations"])
        base_idx = io.index(base)
        base_size = base["logical_size"]
        base_digests = base_idx["block_digests"]
        nb = block_count(decl.new_size)
        touched: set[int] = set()
        for op in decl.ops:
            if op.start == op.end:
                continue
            for b in range(op.start // BLOCK_SIZE,
                           (op.end - 1) // BLOCK_SIZE + 1):
                touched.add(b)
        for b in range(nb):
            bs = b * BLOCK_SIZE
            be = min(bs + BLOCK_SIZE, decl.new_size)
            base_be = min(bs + BLOCK_SIZE, base_size)
            if b in touched:
                assemble = True
            elif be == base_be:
                # identical logical range as the base's block: the declared
                # digest must equal the base's recorded digest
                assemble = False
                expected = base_digests[b]
            elif bs >= base_size:
                # entirely beyond the base: truncate-then-extend hole
                assemble = False
                expected = zero_digest(be - bs)
            else:
                # straddles the base size (partial block): must assemble
                assemble = True
            if not assemble:
                if decl.block_digests[b] != expected:
                    raise DigestMismatchError(
                        f"block {b}: declared digest does not match the base "
                        f"generation's recorded view (wrong ancestor content "
                        f"or corrupted declaration)")
            else:
                content = bytearray(be - bs)
                if bs < base_size:
                    hi = min(be, base_size)
                    content[0:hi - bs] = _resolve_range(
                        m["generations"], decl.base_generation, bs, hi, io)
                for op in decl.ops:
                    os_, oe = max(bs, op.start), min(be, op.end)
                    if os_ >= oe:
                        continue
                    if op.kind == DATA:
                        content[os_ - bs:oe - bs] = \
                            op.data[os_ - op.start:oe - op.start]
                    else:
                        content[os_ - bs:oe - bs] = b"\x00" * (oe - os_)
                if sha256_hex(bytes(content)) != decl.block_digests[b]:
                    raise DigestMismatchError(
                        f"block {b}: assembled view does not match the "
                        f"declared digest (payload corruption or bad delta)")

    # -- readers -------------------------------------------------------------

    def _scan_pins(self) -> list[dict]:
        pins = []
        pdir = self.path / "pins"
        if pdir.exists():
            for f in sorted(pdir.glob("*.json")):
                try:
                    pins.append(json.loads(f.read_bytes()))
                except (ValueError, OSError):
                    # Unparseable pin: fail safe, treat as pinning everything.
                    pins.append({"reader_id": f.stem, "malformed": True,
                                 "pin_generation": -1,
                                 "snapshot_oldest": 0,
                                 "snapshot_tip": 1 << 62})
        return pins

    def open_reader(self, generation: int | None = None,
                    cache_capacity: int = 256) -> "Reader":
        """Pin a generation and take a manifest snapshot for consistent reads.

        At most MAX_READERS concurrent readers.  Any *stored* generation may
        be pinned (that is how generations older than tip-1 stay readable);
        reclaimed generations are gone for good.
        """
        with self._readers_lock:
            raw, m = self._load_manifest_raw()
            if not m["generations"]:
                raise ValidationError("archive is empty")
            pins = self._scan_pins()
            if len(pins) >= MAX_READERS:
                raise ReaderLimitError(
                    f"at most {MAX_READERS} concurrent readers")
            stored = [e["generation"] for e in m["generations"]]
            gen = m["tip"] if generation is None else generation
            if gen not in stored:
                raise RetentionError(
                    f"generation {gen} is not stored (reclaimed or never "
                    f"existed); stored: {stored[0]}..{stored[-1]}")
            rid = uuid.uuid4().hex[:12]
            pin = {"reader_id": rid, "pin_generation": gen,
                   "snapshot_oldest": stored[0], "snapshot_tip": m["tip"]}
            _write_file_sync(self.path / "pins" / (rid + ".json"),
                             json.dumps(pin).encode())
            _fsync_dir(self.path / "pins")
            snapshot = json.loads(raw)  # deep copy of the committed manifest
            reader = Reader(self, rid, gen, snapshot, cache_capacity)
            reader.cumulative.metadata_bytes += len(raw)
            reader.cumulative.metadata_files.append(
                str(self.path / "manifest.json"))
            return reader

    def read_range(self, generation: int, start: int, end: int) -> dict:
        """Convenience one-shot read through an ephemeral pinned reader."""
        with self.open_reader(generation=generation) as r:
            return r.read_range(start, end)

    # -- verify ----------------------------------------------------------------

    def verify(self, generation: int | None = None) -> dict:
        """Full verification: re-read and re-check every block of each
        requested generation, plus structural chain checks.

        Only a complete verify may claim whole-generation integrity; partial
        range reads verify only the blocks they touched.
        """
        with self._writer():
            m = self._load_manifest()
            report: dict = {"ok": True, "generations": {}, "errors": []}
            if generation is not None:
                gens = [e for e in m["generations"]
                        if e["generation"] == generation]
                if not gens:
                    raise RetentionError(
                        f"generation {generation} is not stored")
            else:
                gens = list(m["generations"])
            if generation is None and gens:
                nums = [e["generation"] for e in gens]
                if nums != list(range(nums[0], nums[0] + len(nums))):
                    report["errors"].append("generation chain is not contiguous")
                first = gens[0]
                if first["kind"] not in ("base", "snapshot") or \
                        first["base_generation"] is not None:
                    report["errors"].append(
                        "oldest stored generation is not self-contained")
                for prev, cur in zip(gens, gens[1:]):
                    if cur["kind"] != "delta" or \
                            cur["base_generation"] != prev["generation"]:
                        report["errors"].append(
                            f"generation {cur['generation']} does not chain "
                            f"onto {prev['generation']}")
            io = _IO(self, m["generations"])
            for e in gens:
                g = e["generation"]
                greport = {"blocks": 0, "ok": True}
                try:
                    self._verify_generation(m, e, io, greport)
                except ArchiveError as exc:
                    greport["ok"] = False
                    report["errors"].append(f"generation {g}: {exc}")
                report["generations"][g] = greport
            report["ok"] = (not report["errors"]) and all(
                gr["ok"] for gr in report["generations"].values())
            return report

    def _verify_generation(self, m: dict, entry: dict, io: _IO,
                           greport: dict) -> None:
        g = entry["generation"]
        idx = io.index(entry)
        if idx["generation"] != g or idx["logical_size"] != entry["logical_size"] \
                or idx["kind"] != entry["kind"]:
            raise CorruptionError(
                f"generation {g}: index does not match manifest entry")
        size = entry["logical_size"]
        segs = idx["segments"]
        pos = 0
        payload_len = 0
        for seg in segs:
            if seg["start"] < pos or seg["start"] >= seg["end"] \
                    or seg["end"] > size:
                raise CorruptionError(
                    f"generation {g}: segments overlap or exceed logical size")
            pos = seg["end"]
            if seg["type"] == DATA:
                if seg["payload_offset"] != payload_len:
                    raise CorruptionError(
                        f"generation {g}: payload offsets are not contiguous")
                payload_len += seg["payload_length"]
            elif seg["type"] != HOLE:
                raise CorruptionError(
                    f"generation {g}: unknown segment type {seg['type']!r}")
        if payload_len != idx["payload_bytes"]:
            raise CorruptionError(
                f"generation {g}: payload_bytes mismatch")
        actual = (self._gen_dir(entry) / PAYLOAD_NAME).stat().st_size
        if actual != payload_len:
            raise CorruptionError(
                f"generation {g}: payload file is {actual} bytes, "
                f"index says {payload_len}")
        if entry["kind"] in ("base", "snapshot"):
            covered = sum(seg["end"] - seg["start"] for seg in segs)
            if covered != size:
                raise CorruptionError(
                    f"generation {g}: self-contained generation does not "
                    f"cover its full logical size")
        if len(idx["block_digests"]) != block_count(size):
            raise CorruptionError(
                f"generation {g}: wrong number of block digests")
        for b in range(block_count(size)):
            bs = b * BLOCK_SIZE
            be = min(bs + BLOCK_SIZE, size)
            blk = _resolve_range(m["generations"], g, bs, be, io)
            if sha256_hex(blk) != idx["block_digests"][b]:
                raise DigestMismatchError(
                    f"generation {g}: block {b} digest mismatch")
            greport["blocks"] += 1

    # -- compact ------------------------------------------------------------

    def compact(self) -> dict:
        """Rebase the oldest still-needed generation into a self-contained
        snapshot, then reclaim everything older that no reader pins.

        The compacted snapshot stores only real data extents: blocks that
        are protocol-wise all-hole contribute no payload bytes; mixed
        blocks are stored at sub-block granularity with an honest
        data/hole segment map.
        """
        with self._writer():
            m = self._load_manifest()
            if not m["generations"]:
                raise ValidationError("archive is empty")
            pins = self._scan_pins()
            stored = [e["generation"] for e in m["generations"]]
            keep = {stored[-1]}
            if len(stored) > 1:
                keep.add(stored[-2])
            keep |= {p["pin_generation"] for p in pins}
            keep &= set(stored)
            r = min(keep)
            oldest = stored[0]
            result = {"rebased_generation": None, "reclaimed": [],
                      "kept_generations": sorted(keep),
                      "pending_reclaim": list(m["pending_reclaim"])}
            if r > oldest:
                by = {e["generation"]: e for e in m["generations"]}
                entry_r = by[r]
                index, payload = self._materialize_snapshot(m, entry_r)
                seq = 1
                taken = {e["dir"] for e in m["generations"]} | \
                    {e["dir"] for e in m["pending_reclaim"]}
                while "g%06ds%d" % (r, seq) in taken:
                    seq += 1
                new_dir = "g%06ds%d" % (r, seq)
                staging = self._stage_generation(index, payload)
                published = None
                try:
                    # Pause point: compacted snapshot built, old ancestors
                    # not yet reclaimed.
                    self._fire("after_compact_built", generation=r)
                    published = self._publish_staged(staging, new_dir)
                    dropped = [e for e in m["generations"]
                               if e["generation"] < r]
                    m["generations"] = [e for e in m["generations"]
                                        if e["generation"] >= r]
                    m["generations"][0] = {
                        "generation": r, "dir": new_dir, "kind": "snapshot",
                        "base_generation": None,
                        "logical_size": entry_r["logical_size"],
                        "delta_id": None}
                    m["pending_reclaim"] += [
                        {"dir": e["dir"], "generation": e["generation"]}
                        for e in dropped]
                    m["pending_reclaim"].append(
                        {"dir": entry_r["dir"], "generation": r})
                    self._store_manifest(m)
                except Exception:
                    tmp_manifest = self.path / "manifest.json.tmp"
                    if tmp_manifest.exists():
                        tmp_manifest.unlink()
                    if published is None:
                        shutil.rmtree(staging, ignore_errors=True)
                    else:
                        shutil.rmtree(published, ignore_errors=True)
                    raise
                result["rebased_generation"] = r
            # Pause point: manifest switched, old ancestors not yet reclaimed.
            self._fire("before_reclaim",
                       pending=[e["dir"] for e in m["pending_reclaim"]])
            result["reclaimed"] = self._reclaim_pass(m)
            result["pending_reclaim"] = list(m["pending_reclaim"])
            return result

    def _materialize_snapshot(self, m: dict, entry_r: dict) -> tuple[dict, bytes]:
        """Resolve generation r's full view into self-contained segments."""
        r = entry_r["generation"]
        size = entry_r["logical_size"]
        io = _IO(self, m["generations"])
        final = [{"start": a, "end": b, "type": kind, "src": src, "src_off": off}
                 for a, b, kind, src, off in
                 _resolve_map(m["generations"], r, 0, size, io)]
        # Coalesce adjacent same-type extents (data extents may come from
        # different source generations; we re-store their bytes anyway).
        coal: list[dict] = []
        for f in final:
            if coal and coal[-1]["type"] == f["type"] \
                    and coal[-1]["end"] == f["start"]:
                coal[-1]["end"] = f["end"]
                if f["type"] == DATA:
                    coal[-1]["parts"].append(
                        (f["src"], f["src_off"], f["end"] - f["start"]))
            else:
                f["parts"] = ([(f["src"], f["src_off"], f["end"] - f["start"])]
                              if f["type"] == DATA else [])
                coal.append(f)
        payload = bytearray()
        segdicts: list[dict] = []
        for f in coal:
            if f["type"] == DATA:
                off = len(payload)
                for src, src_off, ln in f["parts"]:
                    payload += io.payload(src, src_off, ln)
                segdicts.append({
                    "start": f["start"], "end": f["end"], "type": DATA,
                    "payload_offset": off,
                    "payload_length": f["end"] - f["start"]})
            else:
                segdicts.append({"start": f["start"], "end": f["end"],
                                 "type": HOLE})
        recorded = io.index(entry_r)["block_digests"]
        for b in range(block_count(size)):
            bs = b * BLOCK_SIZE
            be = min(bs + BLOCK_SIZE, size)
            content = _resolve_from_segments(segdicts, bytes(payload), bs, be)
            if sha256_hex(content) != recorded[b]:
                raise CorruptionError(
                    f"compact: generation {r} block {b} does not match its "
                    f"recorded digest; ancestor storage is corrupt, aborting")
        index = {
            "generation": r, "kind": "snapshot", "base_generation": None,
            "logical_size": size, "delta_id": None,
            "compacted_from": {"previous_dir": entry_r["dir"],
                               "previous_kind": entry_r["kind"]},
            "segments": segdicts, "block_digests": recorded,
            "payload_file": PAYLOAD_NAME, "payload_bytes": len(payload),
        }
        return index, bytes(payload)

    def _reclaim_pass(self, m: dict) -> list[str]:
        """Delete pending dirs no reader pin covers; persist the manifest."""
        pins = self._scan_pins()
        reclaimed = []
        changed = False
        for pe in list(m["pending_reclaim"]):
            g = pe["generation"]
            covered = any(
                p["snapshot_oldest"] <= g <= p["snapshot_tip"] for p in pins)
            if covered:
                continue  # a live reader may still resolve through it
            shutil.rmtree(self.path / "generations" / pe["dir"],
                          ignore_errors=True)
            m["pending_reclaim"].remove(pe)
            reclaimed.append(pe["dir"])
            changed = True
        if changed:
            self._store_manifest(m)
        return reclaimed

    def reclaim(self) -> list[str]:
        """Retry reclaiming pending dirs (e.g. after a reader released its pin)."""
        with self._writer():
            m = self._load_manifest()
            return self._reclaim_pass(m)

    # -- restore / status / stats --------------------------------------------

    def restore(self, generation: int, out_path) -> dict:
        """Materialize a generation's full view to ``out_path`` (sparse)."""
        out_path = Path(out_path)
        with self.open_reader(generation=generation) as r:
            entry = r._entry(generation)
            size = entry["logical_size"]
            data = r.read_range(0, size)["data"]
        with open(out_path, "wb") as f:
            for b in range(block_count(size)):
                bs = b * BLOCK_SIZE
                blk = data[bs:min(bs + BLOCK_SIZE, size)]
                if blk.strip(b"\x00"):
                    f.seek(bs)
                    f.write(blk)
            f.truncate(size)
            f.flush()
            os.fsync(f.fileno())
        return {"generation": generation, "path": str(out_path),
                "logical_size": size}

    def status(self) -> dict:
        m = self._load_manifest()
        pins = self._scan_pins()
        stored = [e["generation"] for e in m["generations"]]
        readable = set()
        if stored:
            readable.add(stored[-1])
            if len(stored) > 1:
                readable.add(stored[-2])
        readable |= {p["pin_generation"] for p in pins}
        return {
            "tip": m["tip"],
            "stored_generations": stored,
            "readable_generations": sorted(readable & set(stored)),
            "pins": pins,
            "pending_reclaim": list(m["pending_reclaim"]),
        }

    def storage_stats(self) -> dict:
        """Persistent-byte accounting: base, delta, index, temp, total.

        Reports every byte the archive keeps on disk, not just net new
        payload: base/snapshot payloads, delta payloads, per-generation
        indexes, the manifest, pins, tmp staging, and not-yet-reclaimed
        directories are all accounted.
        """
        m = self._load_manifest()
        live = {e["dir"]: e for e in m["generations"]}
        pending = {e["dir"] for e in m["pending_reclaim"]}
        stats = {"base_payload_bytes": 0, "delta_payload_bytes": 0,
                 "index_bytes": 0, "manifest_bytes": 0, "pin_bytes": 0,
                 "temp_bytes": 0, "pending_reclaim_bytes": 0,
                 "orphan_bytes": 0, "generations": [], "total_bytes": 0}

        def account(category: str, size: int) -> None:
            stats[category] += size
            stats["total_bytes"] += size

        for root, _dirs, files in os.walk(self.path):
            for name in files:
                p = Path(root) / name
                n = p.stat().st_size
                rel = p.relative_to(self.path)
                parts = rel.parts
                if parts[0] == "manifest.json" or parts[0] == "writer.lock":
                    account("manifest_bytes", n)
                elif parts[0] == "pins":
                    account("pin_bytes", n)
                elif parts[0] == "tmp":
                    account("temp_bytes", n)
                elif parts[0] == "generations" and len(parts) >= 3:
                    dirname = parts[1]
                    if dirname in live:
                        entry = live[dirname]
                        if name == PAYLOAD_NAME:
                            if entry["kind"] in ("base", "snapshot"):
                                account("base_payload_bytes", n)
                            else:
                                account("delta_payload_bytes", n)
                        elif name == INDEX_NAME:
                            account("index_bytes", n)
                        else:
                            account("orphan_bytes", n)
                    elif dirname in pending:
                        account("pending_reclaim_bytes", n)
                    else:
                        account("orphan_bytes", n)
                else:
                    account("orphan_bytes", n)
        for e in m["generations"]:
            d = self.path / "generations" / e["dir"]
            payload = (d / PAYLOAD_NAME).stat().st_size \
                if (d / PAYLOAD_NAME).exists() else 0
            index = (d / INDEX_NAME).stat().st_size \
                if (d / INDEX_NAME).exists() else 0
            stats["generations"].append({
                "generation": e["generation"], "dir": e["dir"],
                "kind": e["kind"], "payload_bytes": payload,
                "index_bytes": index})
        return stats


# --------------------------------------------------------------------------
# reader

class Reader:
    """A pinned, snapshot-isolated reader.  Use as a context manager."""

    def __init__(self, archive: Archive, reader_id: str, pin_generation: int,
                 snapshot: dict, cache_capacity: int = 256) -> None:
        self._archive = archive
        self.reader_id = reader_id
        self.pin_generation = pin_generation
        self._snapshot = snapshot
        self._closed = False
        self._index_cache: dict[int, dict] = {}
        self._block_cache: OrderedDict[tuple[int, int], bytes] = OrderedDict()
        self.cache_capacity = cache_capacity
        self.cumulative = Meter()

    # -- helpers -------------------------------------------------------------

    def _entry(self, generation: int) -> dict:
        for e in self._snapshot["generations"]:
            if e["generation"] == generation:
                return e
        raise RetentionError(
            f"generation {generation} is not in this reader's snapshot")

    def _readable(self, gen: int) -> bool:
        gens = [e["generation"] for e in self._snapshot["generations"]]
        if gen == self.pin_generation:
            return True
        return gen in (gens[-1], gens[-2] if len(gens) > 1 else None)

    # -- reads -----------------------------------------------------------------

    def read_range(self, start: int, end: int,
                   generation: int | None = None) -> dict:
        """Read [start, end) of a readable generation.

        Only the checksum blocks intersecting the request are assembled
        (never the whole file); each assembled block is verified against
        its recorded digest before any byte is returned.  Empty ranges are
        legal; out-of-bounds ranges are rejected.
        """
        if self._closed:
            raise ArchiveError("reader is closed")
        gen = self.pin_generation if generation is None else generation
        entry = self._entry(gen)
        if not self._readable(gen):
            raise RetentionError(
                f"generation {gen} is not readable: only the two newest "
                f"generations and explicitly pinned ones are")
        size = entry["logical_size"]
        if start < 0 or end < start or end > size:
            raise RangeRequestError(
                f"range [{start},{end}) out of bounds for generation {gen} "
                f"with logical size {size}")
        meter = Meter()
        io = _IO(self._archive, self._snapshot["generations"],
                 request_generation=gen, meter=meter,
                 index_cache=self._index_cache)
        if start == end:
            return {"generation": gen, "start": start, "end": end,
                    "data": b"", "blocks": [], "meter": meter.as_dict(),
                    "digest_verified": True}
        idx = io.index(entry)
        digests = idx["block_digests"]
        first = start // BLOCK_SIZE
        last = (end - 1) // BLOCK_SIZE
        out = bytearray()
        blocks = []
        for b in range(first, last + 1):
            blocks.append(b)
            key = (gen, b)
            blk = self._block_cache.get(key)
            if blk is not None:
                self._block_cache.move_to_end(key)
                meter.cache_bytes += len(blk)
            else:
                blk = _resolve_range(self._snapshot["generations"], gen,
                                     b * BLOCK_SIZE,
                                     min((b + 1) * BLOCK_SIZE, size), io)
                if sha256_hex(blk) != digests[b]:
                    raise DigestMismatchError(
                        f"generation {gen} block {b}: assembled content "
                        f"does not match the recorded checksum")
                self._block_cache[key] = blk
                if len(self._block_cache) > self.cache_capacity:
                    self._block_cache.popitem(last=False)
            out += blk
        rel = start - first * BLOCK_SIZE
        data = bytes(out[rel:rel + (end - start)])
        self.cumulative.merge(meter)
        return {"generation": gen, "start": start, "end": end, "data": data,
                "blocks": blocks, "meter": meter.as_dict(),
                "digest_verified": True}

    def stats(self) -> dict:
        return {"reader_id": self.reader_id,
                "pin_generation": self.pin_generation,
                "cumulative": self.cumulative.as_dict(),
                "cached_blocks": len(self._block_cache)}

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            pin = self._archive.path / "pins" / (self.reader_id + ".json")
            pin.unlink(missing_ok=True)
            try:
                _fsync_dir(self._archive.path / "pins")
            except OSError:
                pass

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
