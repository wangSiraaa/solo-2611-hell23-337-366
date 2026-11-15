"""Synthetic archive generator covering the legal input family.

The generator compresses with real raw deflate exactly as an emitting server
would (``Z_SYNC_FLUSH`` per message, tail marker stripped) and can produce the
variations the upgrade must handle:

* server context takeover on/off,
* compressed and uncompressed messages interleaved,
* ping/pong/control frames between fragments and between messages,
* empty messages,
* multi-fragment messages with the flush tail split arbitrarily,
* 9..15 bit window sizes,
* a truncated final frame (for negative tests; the generator just emits the
  bytes, the reader must reject them).

Uncompressed messages never touch the deflate compressor, matching the rule
that they do not enter the deflate history.
"""

import hashlib
import json
import struct
import zlib
from dataclasses import dataclass
from typing import List, Sequence, Tuple


def frame(
    payload: bytes,
    opcode: int = 2,
    fin: bool = True,
    rsv1: bool = False,
) -> bytes:
    head = bytes(
        [(0x80 if fin else 0) | (0x40 if rsv1 else 0) | opcode]
    )
    n = len(payload)
    if n < 126:
        return head + bytes([n]) + payload
    if n < 65536:
        return head + b"\x7e" + struct.pack(">H", n) + payload
    return head + b"\x7f" + struct.pack(">Q", n) + payload


@dataclass
class Spec:
    messages: Sequence[bytes]
    compressed: Sequence[bool]
    takeover: bool = True
    window_bits: int = 15
    level: int = 6
    strategy: int = zlib.Z_DEFAULT_STRATEGY
    fragments: Sequence[int] = (1,)       # fragments per message, cycled
    control: str = "between"              # "between" | "interleaved" | "mixed" | "none"
    split_tail: bool = False              # move the flush tail across fragments
    flush_tail_bytes: int = 4


def _split(length: int, parts: int) -> List[int]:
    if parts <= 1 or length == 0:
        return [length]
    base = length // parts
    out = [base] * parts
    for i in range(length - base * parts):
        out[i] += 1
    return out


def generate(spec: Spec) -> Tuple[bytes, List[dict]]:
    """Return (wire bytes, manifest). Manifest mirrors source-truth shape."""
    wire = bytearray()
    manifest: List[dict] = []
    compressor = zlib.compressobj(
        spec.level, zlib.DEFLATED, -spec.window_bits, 8, spec.strategy
    )
    ping_no = 0
    for i, (msg, compressed) in enumerate(zip(spec.messages, spec.compressed)):
        start = len(wire)
        if compressed:
            if not spec.takeover:
                compressor = zlib.compressobj(
                    spec.level, zlib.DEFLATED, -spec.window_bits, 8, spec.strategy
                )
            payload = compressor.compress(msg) + compressor.flush(zlib.Z_SYNC_FLUSH)
            assert payload.endswith(b"\x00\x00\xff\xff")
            body = payload[: -spec.flush_tail_bytes] if spec.flush_tail_bytes else payload
        else:
            body = msg
        parts = max(1, spec.fragments[i % len(spec.fragments)])
        chunks = _split(len(body), parts)
        cursor = 0
        frames_for_msg = []
        for fi, ln in enumerate(chunks):
            chunk = body[cursor : cursor + ln]
            cursor += ln
            last = fi == len(chunks) - 1
            fstart = len(wire)
            wire += frame(
                chunk,
                opcode=(2 if fi == 0 else 0),
                fin=last,
                rsv1=(compressed and fi == 0),
            )
            frames_for_msg.append({"start": fstart, "end": len(wire), "opcode": 2 if fi == 0 else 0, "fin": last, "rsv1": bool(compressed and fi == 0), "body": len(chunk)})
            # Control frame after the first fragment of a multi-fragment message.
            if (not last) and spec.control in ("interleaved", "mixed"):
                payload_c = b"pong" if ping_no % 2 else b"ping"
                op = 0xA if ping_no % 2 else 0x9
                cstart = len(wire)
                wire += frame(payload_c, opcode=op)
                frames_for_msg.append({"start": cstart, "end": len(wire), "opcode": op, "fin": True, "rsv1": False, "body": len(payload_c)})
                ping_no += 1
        if spec.control in ("between", "mixed"):
            cstart = len(wire)
            payload_c = b"pong" if ping_no % 2 else b"ping"
            op = 0xA if ping_no % 2 else 0x9
            wire += frame(payload_c, opcode=op)
            frames_for_msg.append({"start": cstart, "end": len(wire), "opcode": op, "fin": True, "rsv1": False, "body": len(payload_c)})
            ping_no += 1
        manifest.append(
            {
                "message": i,
                "start": start,
                "end": len(wire),
                "sha256": hashlib.sha256(msg).hexdigest(),
                "uncompressed_bytes": len(msg),
                "compressed": compressed,
                "fragments": len(chunks),
                "frames": frames_for_msg,
            }
        )
    return bytes(wire), manifest


def negotiation_json(spec: Spec) -> dict:
    return {
        "permessage_deflate": True,
        "server_no_context_takeover": not spec.takeover,
        "server_max_window_bits": spec.window_bits,
    }


def write_corpus(
    directory,
    spec: Spec,
    *,
    name: str = "archive",
) -> dict:
    from pathlib import Path

    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    wire, manifest = generate(spec)
    (d / f"{name}.frames").write_bytes(wire)
    (d / f"{name}.negotiation.json").write_text(
        json.dumps(negotiation_json(spec)), encoding="utf-8"
    )
    (d / f"{name}.truth.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return {
        "archive": str(d / f"{name}.frames"),
        "negotiation": str(d / f"{name}.negotiation.json"),
        "truth": str(d / f"{name}.truth.json"),
        "manifest": manifest,
        "wire_bytes": len(wire),
    }


# -- deterministic payloads for the canonical workload ---------------------

def workload_messages(n: int = 100, size: int = 4096) -> List[bytes]:
    """The specified 100 x 4096-byte corpus.

    Messages share a common block plus per-message entropy.  Sharing a block is
    what makes context takeover compress well: later messages reference the
    earlier window, so a random access genuinely needs prior state.
    """
    common = bytes(range(256)) * (size // 256)
    out = []
    for i in range(n):
        entropy = (
            hashlib.sha256(f"wsarchive-workload-{i}".encode()).digest()
            * (size // 32)
        )[: size // 8]
        body = (common[: size - len(entropy)] + entropy)[:size]
        out.append(body)
    return out
