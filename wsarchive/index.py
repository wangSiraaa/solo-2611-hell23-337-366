"""On-disk index format and the in-memory view used by the reader.

Layout (little-endian, no alignment padding)::

    magic              8   b"WSAIDX\x00\x01"
    archive_size       u64
    flags              u32 bit0 context takeover, bit1 permessage-deflate
    window_bits        u8
    reserved           3
    archive_sha256     32
    negotiation_sha256 32   sha256 of canonical negotiation JSON
    frame_count        u32
    message_count      u32
    checkpoint_count   u32
    dict_blob_len      u32
    density            u32
    frames             frame_count * 21   (<QQIB)
    messages           message_count * 99 (<IIQQIIBH + 2*32s)
    checkpoints        checkpoint_count * 53 (<IIIQB + 32s)
    dict blob          dict_blob_len bytes

No plaintext message is stored as an index record.  The only plaintext in the
index is the bounded deflate window captured at each checkpoint (at most
``1 << window_bits`` bytes per checkpoint), which is the state required to
resume the stream.  The manifest is generated purely for humans and is not
read back as authority; it records the digest of ``index.bin``.
"""

import hashlib
import json
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from .errors import ArchiveBindingError, ArchiveError, CheckpointError
from .model import FrameRecord, MessageRecord, Negotiation

MAGIC = b"WSAIDX\x00\x01"
VERSION = 1
HEADER = struct.Struct("<8sQIB3s32s32sIIIII")  # 108 bytes
FRAME = struct.Struct("<QQIB")                  # offset, body_offset, body_len, b0
MESSAGEC = struct.Struct("<IIQQIIBH")           # + 2 digests
CHECKPOINT = struct.Struct("<IIIQB")            # + dict sha256

FRAME_SIZE = FRAME.size                      # 21
MESSAGE_SIZE = MESSAGEC.size + 64            # 99
CHECKPOINT_SIZE = CHECKPOINT.size + 32       # 53

FLAG_TAKEOVER = 1 << 0
FLAG_DEFLATE = 1 << 1
MSG_FLAG_COMPRESSED = 1 << 0
CP_FLAG_FRESH = 1 << 0


def canonical_negotiation(negotiation: Negotiation) -> bytes:
    obj = {
        "permessage_deflate": negotiation.permessage_deflate,
        "server_no_context_takeover": negotiation.server_no_context_takeover,
        "server_max_window_bits": negotiation.server_max_window_bits,
        "client_no_context_takeover": negotiation.client_no_context_takeover,
        "client_max_window_bits": negotiation.client_max_window_bits,
    }
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


def negotiation_digest(negotiation: Negotiation) -> bytes:
    return hashlib.sha256(canonical_negotiation(negotiation)).digest()


@dataclass
class Checkpoint:
    message_index: int
    dict_offset: int
    dict_len: int
    plain_offset: int       # cumulative compressed plaintext before this anchor
    fresh: bool             # resume without a preset dictionary
    dict_sha256: bytes

    @property
    def resume_mode(self) -> str:
        return "fresh" if self.fresh else "dict"


@dataclass
class ArchiveIndex:
    archive_size: int
    archive_sha256: bytes
    negotiation_sha256: bytes
    negotiation: Negotiation
    frames: List[FrameRecord]
    messages: List[MessageRecord]
    checkpoints: List[Checkpoint]
    dict_blob: bytes
    density: int
    dict_sha256: bytes = b""  # digest of the blob, for integrity diagnostics

    # -- derived -----------------------------------------------------------
    @property
    def takeover(self) -> bool:
        return self.negotiation.context_takeover and self.negotiation.permessage_deflate

    @property
    def cp_by_message(self) -> Dict[int, Checkpoint]:
        return {cp.message_index: cp for cp in self.checkpoints}

    def checkpoint_for(self, message_index: int) -> Optional[Checkpoint]:
        """Largest checkpoint at or before ``message_index``."""
        best = None
        for cp in self.checkpoints:
            if cp.message_index <= message_index:
                if best is None or cp.message_index > best.message_index:
                    best = cp
        return best

    def checkpoint_dict(self, cp: Checkpoint) -> bytes:
        if cp.dict_len == 0:
            return b""
        blob = self.dict_blob[cp.dict_offset : cp.dict_offset + cp.dict_len]
        if len(blob) != cp.dict_len:
            raise CheckpointError(
                f"checkpoint for message {cp.message_index} is truncated in blob"
            )
        got = hashlib.sha256(blob).digest()
        if got != cp.dict_sha256:
            raise CheckpointError(
                f"checkpoint for message {cp.message_index} failed its digest check"
            )
        return blob

    # -- budget accounting -------------------------------------------------
    @property
    def frame_table_bytes(self) -> int:
        return len(self.frames) * FRAME_SIZE

    @property
    def message_table_bytes(self) -> int:
        return len(self.messages) * MESSAGE_SIZE

    @property
    def checkpoint_table_bytes(self) -> int:
        return len(self.checkpoints) * CHECKPOINT_SIZE

    @property
    def header_bytes(self) -> int:
        return HEADER.size

    @property
    def dictionary_state_bytes(self) -> int:
        """Bounded deflate-window snapshots plus their table (conservative)."""
        return len(self.dict_blob) + self.checkpoint_table_bytes

    @property
    def metadata_bytes(self) -> int:
        """Header and the ordinary frame/message index tables."""
        return self.header_bytes + self.frame_table_bytes + self.message_table_bytes

    def size_report(self) -> dict:
        return {
            "header_bytes": self.header_bytes,
            "frame_table_bytes": self.frame_table_bytes,
            "message_table_bytes": self.message_table_bytes,
            "checkpoint_table_bytes": self.checkpoint_table_bytes,
            "metadata_bytes": self.metadata_bytes,
            "dict_blob_bytes": len(self.dict_blob),
            "dictionary_state_bytes": self.dictionary_state_bytes,
            "index_bin_bytes": self.serialized_size(),
            "density": self.density,
            "checkpoint_count": len(self.checkpoints),
        }

    def serialized_size(self) -> int:
        return (
            HEADER.size
            + self.frame_table_bytes
            + self.message_table_bytes
            + self.checkpoint_table_bytes
            + len(self.dict_blob)
        )


def serialize(index: ArchiveIndex) -> bytes:
    flags = 0
    if index.negotiation.context_takeover:
        flags |= FLAG_TAKEOVER
    if index.negotiation.permessage_deflate:
        flags |= FLAG_DEFLATE
    out = bytearray()
    out += HEADER.pack(
        MAGIC,
        index.archive_size,
        flags,
        index.negotiation.window_bits,
        b"\x00\x00\x00",
        index.archive_sha256,
        index.negotiation_sha256,
        len(index.frames),
        len(index.messages),
        len(index.checkpoints),
        len(index.dict_blob),
        index.density,
    )
    for f in index.frames:
        b0 = (
            (0x80 if f.fin else 0)
            | (0x40 if f.rsv1 else 0)
            | (f.opcode & 0x0F)
        )
        out += FRAME.pack(f.offset, f.body_offset, f.body_length, b0)
    for m in index.messages:
        flags_m = 0
        if m.compressed:
            flags_m |= MSG_FLAG_COMPRESSED
        out += MESSAGEC.pack(
            m.first_frame,
            m.last_frame,
            m.first_offset,
            m.end_offset,
            m.body_bytes,
            m.declared_length or 0,
            flags_m,
            m.fragment_count,
        )
        out += m.plaintext_sha256 + m.body_sha256
    for cp in index.checkpoints:
        out += CHECKPOINT.pack(
            cp.message_index,
            cp.dict_offset,
            cp.dict_len,
            cp.plain_offset,
            CP_FLAG_FRESH if cp.fresh else 0,
        )
        out += cp.dict_sha256
    out += index.dict_blob
    return bytes(out)


def deserialize(blob: bytes, negotiation: Optional[Negotiation] = None) -> ArchiveIndex:
    if len(blob) < HEADER.size:
        raise ArchiveError("index is smaller than its header")
    (
        magic,
        archive_size,
        flags,
        window_bits,
        _reserved,
        archive_sha,
        neg_sha,
        frame_count,
        message_count,
        cp_count,
        dict_len,
        density,
    ) = HEADER.unpack_from(blob, 0)
    if magic != MAGIC:
        raise ArchiveError("bad index magic or unsupported version")
    need = (
        HEADER.size
        + frame_count * FRAME_SIZE
        + message_count * MESSAGE_SIZE
        + cp_count * CHECKPOINT_SIZE
        + dict_len
    )
    if need != len(blob):
        raise ArchiveError(
            f"index size mismatch: header implies {need} bytes, file has {len(blob)}"
        )
    p = HEADER.size
    frames: List[FrameRecord] = []
    for i in range(frame_count):
        offset, body_offset, body_len, b0 = FRAME.unpack_from(blob, p)
        p += FRAME_SIZE
        frames.append(
            FrameRecord(
                idx=i,
                offset=offset,
                end=body_offset + body_len,
                fin=bool(b0 & 0x80),
                rsv1=bool(b0 & 0x40),
                opcode=b0 & 0x0F,
                masked=False,
                body_offset=body_offset,
                body_length=body_len,
            )
        )
    messages: List[MessageRecord] = []
    for i in range(message_count):
        (
            first_frame,
            last_frame,
            first_offset,
            end_offset,
            body_bytes,
            declared_len,
            msg_flags,
            frag,
        ) = MESSAGEC.unpack_from(blob, p)
        p += MESSAGEC.size
        plain_digest = blob[p : p + 32]
        body_digest = blob[p + 32 : p + 64]
        p += 64
        # Data fragments only; interleaved control frames sit inside the
        # first_frame..last_frame range but are not part of the message.
        frame_indices = [
            fi
            for fi in range(first_frame, last_frame + 1)
            if frames[fi].opcode in (0x0, 0x1, 0x2)
        ]
        body_ranges = [
            (frames[fi].body_offset, frames[fi].body_length)
            for fi in frame_indices
        ]
        messages.append(
            MessageRecord(
                index=i,
                first_frame=first_frame,
                last_frame=last_frame,
                first_offset=first_offset,
                end_offset=end_offset,
                frame_indices=frame_indices,
                body_ranges=body_ranges,
                compressed=bool(msg_flags & MSG_FLAG_COMPRESSED),
                body_bytes=body_bytes,
                declared_length=declared_len,
                plaintext_sha256=plain_digest,
                body_sha256=body_digest,
            )
        )
    checkpoints: List[Checkpoint] = []
    for _ in range(cp_count):
        midx, doff, dlen, poff, cp_flags = CHECKPOINT.unpack_from(blob, p)
        p += CHECKPOINT.size
        dsha = blob[p : p + 32]
        p += 32
        checkpoints.append(
            Checkpoint(midx, doff, dlen, poff, bool(cp_flags & CP_FLAG_FRESH), dsha)
        )
    dict_blob = blob[p : p + dict_len]
    # reconstruct a Negotiation for callers that did not supply one
    if negotiation is None:
        negotiation = Negotiation(
            permessage_deflate=bool(flags & FLAG_DEFLATE),
            server_no_context_takeover=not bool(flags & FLAG_TAKEOVER),
            server_max_window_bits=window_bits,
        )
    return ArchiveIndex(
        archive_size=archive_size,
        archive_sha256=archive_sha,
        negotiation_sha256=neg_sha,
        negotiation=negotiation,
        frames=frames,
        messages=messages,
        checkpoints=checkpoints,
        dict_blob=dict_blob,
        density=density,
        dict_sha256=hashlib.sha256(dict_blob).digest(),
    )


def write_index(index: ArchiveIndex, out_dir: str | Path, archive_path: str | Path) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    blob = serialize(index)
    index_path = out / "index.bin"
    index_path.write_bytes(blob)
    manifest = {
        "format": "wsarchive-index",
        "version": VERSION,
        "index_bin_sha256": hashlib.sha256(blob).hexdigest(),
        "archive_path": str(archive_path),
        "archive_sha256": index.archive_sha256.hex(),
        "archive_size": index.archive_size,
        "negotiation": json.loads(canonical_negotiation(index.negotiation)),
        "negotiation_sha256": index.negotiation_sha256.hex(),
        "frames": len(index.frames),
        "messages": len(index.messages),
        "density": index.density,
        "checkpoints": len(index.checkpoints),
        "sizes": index.size_report(),
        "stored_plaintext": (
            "only bounded deflate window snapshots at checkpoints; no complete "
            "message payloads are indexed"
        ),
    }
    (out / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    return index_path


def load_index(
    index_dir: str | Path,
    negotiation: Optional[Negotiation] = None,
) -> ArchiveIndex:
    d = Path(index_dir)
    index_path = d / "index.bin"
    if not index_path.exists():
        raise ArchiveError(f"no index.bin in {d}")
    blob = index_path.read_bytes()
    manifest = {}
    mpath = d / "manifest.json"
    if mpath.exists():
        manifest = json.loads(mpath.read_text(encoding="utf-8"))
        recorded = manifest.get("index_bin_sha256")
        if recorded and recorded != hashlib.sha256(blob).hexdigest():
            raise ArchiveError("manifest.json does not match index.bin")
    idx = deserialize(blob, negotiation)
    if negotiation is not None:
        got = negotiation_digest(negotiation)
        if got != idx.negotiation_sha256:
            raise ArchiveBindingError(
                "negotiation parameters do not match the index; refusing to use "
                "its checkpoints"
            )
    # cross-check manifest if present
    if manifest:
        if manifest.get("archive_sha256") and manifest["archive_sha256"] != idx.archive_sha256.hex():
            raise ArchiveError("manifest archive hash disagrees with index header")
    return idx
