"""Source access, content identity, and the trusted immutable chunk manifest.

Source identity
    ``(identity_span, identity_sha256)``: a digest over a fixed small prefix.
    The span is chosen once and recorded; it is stable under append and binds
    a checkpoint to the file that produced it.

Manifest
    An append-only *receipt log*: ``{off, len, sha256}`` chunks, each at most
    ``CHUNK`` bytes, over a contiguous prefix ``trusted_len`` of bytes that
    were actually read.  Once published, an entry never changes.  The manifest
    is fsynced before any target transaction that depends on those bytes
    commits, so a durable checkpoint is always covered by a durable manifest.

    The resume *boundary* digest lives in the target checkpoint, not here,
    which keeps resume's re-read bounded by the boundary block.

Read accounting
    Every byte read from the source increments ``bytes_read`` and
    ``read_calls``; the engine separates resume reads from audit reads because
    audit is a separate, full-budget operation.
"""

import hashlib
import json
import os
import struct

from .errors import AuditFailed, InputFormatError, SourceChanged

# Manifest chunk size.  Immutable once published.
CHUNK = 65536
# Fixed-size content identity prefix (small: resume's identity read is part of
# its <=64KiB fixed overhead).
IDENTITY_SPAN = 4096
# Resume re-verifies at most this many bytes of already-committed source, so
# identity + boundary stays inside 64KiB.
BOUNDARY_MAX = CHUNK - IDENTITY_SPAN


class SourceStore(object):
    """Read-only view of one source file.  The size is pinned at open and the
    file is only ever *appended to*."""

    def __init__(self, path):
        self.path = path
        st = os.stat(path)
        self.ino = st.st_ino
        self.dev = st.st_dev
        self.size_at_open = st.st_size
        self.bytes_read = 0
        self.read_calls = 0
        self._read_end = 0

    def read(self, offset, length):
        data = self._read_raw(offset, length)
        self.bytes_read += len(data)
        self.read_calls += 1
        self._read_end = max(self._read_end, offset + len(data))
        return data

    def _read_raw(self, offset, length):
        raise NotImplementedError

    def bytes_read_high_water(self):
        return self._read_end

    def stat(self):
        st = os.stat(self.path)
        return st.st_ino, st.st_dev, st.st_size

    def close(self):
        pass


class FileStore(SourceStore):
    def __init__(self, path):
        SourceStore.__init__(self, path)
        self.fh = open(path, "rb")

    def _read_raw(self, offset, length):
        self.fh.seek(offset)
        if length == 0:
            return self.fh.read()
        return self.fh.read(length)

    def close(self):
        self.fh.close()


class MemoryStore(SourceStore):
    """Generated-scenario store (same read accounting, no file)."""

    def __init__(self, data):
        self.data = bytes(data)
        self.ino = 1
        self.dev = 1
        self.size_at_open = len(self.data)
        self.bytes_read = 0
        self.read_calls = 0
        self._read_end = 0

    def _read_raw(self, offset, length):
        if length == 0:
            return self.data[offset:]
        return self.data[offset : offset + length]

    def stat(self):
        return self.ino, self.dev, len(self.data)


class Manifest(object):
    FORMAT = 1

    def __init__(self, path=None):
        self.path = path
        self.chunks = []
        self.trusted_len = 0
        self.identity = None
        self.identity_span = 0
        self.seq = 0
        self._pending = bytearray()
        # Highest source offset any run has read (persisted).  On resume the
        # region [checkpoint_end, read_hwm) was already seen but never
        # committed; re-reading it is the "replay" this budget is about.
        self.read_hwm = 0

    # ---- load / save --------------------------------------------------
    @classmethod
    def load_or_create(cls, path):
        m = cls(path)
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                doc = json.load(fh)
            if doc.get("format") != cls.FORMAT:
                raise InputFormatError("unsupported manifest format")
            m.chunks = doc["chunks"]
            m.trusted_len = doc["trusted_len"]
            m.identity = doc["identity"]
            m.identity_span = doc["identity_span"]
            m.seq = doc.get("seq", 0)
            m.read_hwm = doc.get("read_hwm", 0)
        return m

    def save(self, seal=True):
        if seal:
            self.seal()
        if not self.path:
            return
        tmp = self.path + ".tmp"
        doc = {
            "format": self.FORMAT,
            "identity": self.identity,
            "identity_span": self.identity_span,
            "trusted_len": self.trusted_len,
            "seq": self.seq,
            "read_hwm": self.read_hwm,
            "chunks": self.chunks,
        }
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    # ---- feed / seal --------------------------------------------------
    @staticmethod
    def digest(data):
        return hashlib.sha256(data).hexdigest()

    def feed(self, offset, data):
        """Record received bytes.  Only bytes at or beyond the known prefix are
        stored; bytes already covered (the resume path re-reading a verified
        block) are ignored, so a re-read never changes an existing digest."""
        if not data:
            return
        end = offset + len(data)
        known_end = self.trusted_len + len(self._pending)
        if end <= known_end:
            return
        if offset > known_end:
            raise SourceChanged(
                "manifest feed gap: bytes at %d while known prefix ends at %d"
                % (offset, known_end)
            )
        start_in_data = max(0, known_end - offset)
        self._pending += data[start_in_data:]
        # Do NOT seal here.  Sealing is driven by ``seal_upto`` at a committed
        # checkpoint, so the published chunk prefix never extends past the
        # bytes a durable checkpoint actually relies on.

    def _seal_chunk(self, n):
        chunk = bytes(self._pending[:n])
        self.chunks.append(
            {"off": self.trusted_len, "len": n, "sha256": self.digest(chunk)}
        )
        self.trusted_len += n
        del self._pending[:n]
        self.seq += 1

    def seal(self):
        while self._pending:
            self._seal_chunk(min(CHUNK, len(self._pending)))

    def seal_upto(self, offset):
        """Publish only bytes through ``offset`` (which the lexer has actually
        consumed), leaving any read-ahead pending.  This keeps the manifest's
        "trusted" claim limited to bytes that participated in a checkpoint."""
        if offset <= self.trusted_len:
            return
        avail = self.trusted_len + len(self._pending)
        if offset > avail:
            raise SourceChanged(
                "cannot seal through %d: only %d bytes are known" % (offset, avail)
            )
        take = offset - self.trusted_len
        while take > 0:
            n = min(CHUNK, take, len(self._pending))
            self._seal_chunk(n)
            take -= n

    def fed_end(self):
        return self.trusted_len + len(self._pending)

    # ---- identity -----------------------------------------------------
    def reset_identity(self):
        """Forget a bound identity.  Only legal while nothing is committed and
        no chunk has been published (an empty first attempt)."""
        if self.chunks:
            raise SourceChanged("cannot reset identity after chunks are published")
        self.identity = None
        self.identity_span = 0

    def bind_identity(self, identity, span):
        if self.identity is None:
            self.identity = identity
            self.identity_span = span
        elif identity is not None and (self.identity != identity or self.identity_span != span):
            raise SourceChanged(
                "source identity changed: manifest %s/%d, source %s/%d"
                % (self.identity, self.identity_span, identity, span)
            )

    # ---- verification -------------------------------------------------
    def verify_authored_prefix(self, store):
        """Full re-read of every published chunk.  Used by audit only."""
        bytes_read = 0
        expect_off = 0
        for idx, ch in enumerate(self.chunks):
            if ch["off"] != expect_off:
                raise AuditFailed(
                    "manifest is not contiguous at chunk %d (off %d, expected %d)"
                    % (idx, ch["off"], expect_off)
                )
            if ch["len"] <= 0 or ch["len"] > CHUNK:
                raise AuditFailed("manifest chunk %d has bad length %d" % (idx, ch["len"]))
            data = store.read(ch["off"], ch["len"])
            bytes_read += len(data)
            if len(data) != ch["len"]:
                raise AuditFailed(
                    "chunk %d at %d is short: %d of %d bytes"
                    % (idx, ch["off"], len(data), ch["len"])
                )
            if self.digest(data) != ch["sha256"]:
                raise AuditFailed(
                    "chunk %d at %d does not match its digest" % (idx, ch["off"])
                )
            expect_off += ch["len"]
        if expect_off != self.trusted_len:
            raise AuditFailed("manifest trusted_len disagrees with its chunks")
        return bytes_read

    def verify_boundary_block(self, store, start, end, expect_sha):
        """Resume's bounded check: re-read at most the boundary block
        (<= BOUNDARY_MAX) and compare it with the digest committed in the
        target database."""
        if end < start or end - start > BOUNDARY_MAX:
            raise SourceChanged("boundary block is not bounded")
        if end > self.trusted_len:
            raise SourceChanged(
                "boundary block ends at %d beyond trusted manifest length %d"
                % (end, self.trusted_len)
            )
        data = store.read(start, end - start)
        if len(data) != end - start:
            raise SourceChanged("short read of boundary block")
        if self.digest(data) != expect_sha:
            raise SourceChanged(
                "boundary block at %d differs from the checkpoint digest" % start
            )
        return len(data)

    def covers(self, offset):
        return self.trusted_len >= offset

    def tail_chunks(self, start, end):
        """Chunks overlapping [start,end), used for an optional cross-check."""
        out = []
        for ch in self.chunks:
            cstart = ch["off"]
            cend = cstart + ch["len"]
            if cend <= start:
                continue
            if cstart >= end:
                break
            out.append(ch)
        return out


def source_identity(store):
    """(identity_sha256, span) for the currently visible source head."""
    span = min(IDENTITY_SPAN, store.size_at_open)
    head = store._read_raw(0, span)
    h = hashlib.sha256()
    h.update(struct.pack("<Q", span))
    h.update(head)
    return "sha256:" + h.hexdigest(), span


class TrackedStore(object):
    """Read facade used by the lexer.  Every read is counted on the raw store;
    bytes past the manifest's known prefix are fed to the manifest.

    ``replay_until`` marks an offset below which this run is *re-reading* bytes
    that a previous run had already received (but did not commit).  Summing such
    reads lets the engine prove the "one transaction + 64 KiB" resume budget
    instead of merely claiming it.
    """

    def __init__(self, store, manifest, replay_until=0):
        self.store = store
        self.manifest = manifest
        self.replay_until = replay_until
        self.replayed_bytes = 0

    def read(self, offset, length):
        data = self.store.read(offset, length)
        if offset < self.replay_until:
            self.replayed_bytes += min(len(data), self.replay_until - offset)
        self.manifest.feed(offset, data)
        return data

    # Non-feeding read, still counted: used only for boundary verification.
    def read_verify(self, offset, length):
        return self.store.read(offset, length)

    @property
    def bytes_read(self):
        return self.store.bytes_read

    @property
    def read_calls(self):
        return self.store.read_calls
