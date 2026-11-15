"""RFC 6455 frame parsing and message assembly.

Only the server-to-client direction is supported, so masked frames are
rejected.  Control frames may be interleaved between data fragments; they are
not messages and never contribute to a message's payload or deflate history.

Message framing rules enforced here (RFC 6455 section 5.4):

* the first frame of a message has opcode 1/2 and the rest are continuation
  frames (opcode 0);
* a message's final frame has FIN=1;
* RSV1 is only honoured on the first data frame.  RSV1 on a continuation frame
  is a protocol error rather than a second "compressed" declaration.
"""

import struct
from typing import List

from .errors import FrameError
from .model import (
    CONTROL_OPCODES,
    ArchiveLayout,
    FrameRecord,
    MessageRecord,
    Negotiation,
)

# RFC 6455 section 5.5: control frames are never fragmented and carry at most
# 125 bytes.  A control frame in the middle of a fragmented message is legal.
MAX_CONTROL_BODY = 125


def _parse_frames(data: bytes) -> List[FrameRecord]:
    frames: List[FrameRecord] = []
    p = 0
    n = len(data)
    while p < n:
        start = p
        if p + 2 > n:
            raise FrameError(f"truncated frame header at offset {p}")
        b0, b1 = data[p], data[p + 1]
        p += 2
        fin = bool(b0 & 0x80)
        rsv1 = bool(b0 & 0x40)
        rsv2 = bool(b0 & 0x20)
        rsv3 = bool(b0 & 0x10)
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
        length = b1 & 0x7F
        if length == 126:
            if p + 2 > n:
                raise FrameError(f"truncated 16-bit length at offset {start}")
            length = struct.unpack(">H", data[p : p + 2])[0]
            p += 2
        elif length == 127:
            if p + 8 > n:
                raise FrameError(f"truncated 64-bit length at offset {start}")
            length = struct.unpack(">Q", data[p : p + 8])[0]
            p += 8
            if length >> 63:
                raise FrameError(f"invalid 64-bit length at offset {start}")
        if masked:
            raise FrameError(
                f"masked frame at offset {start}: archive is server direction "
                "(unmasked) only"
            )
        if rsv2 or rsv3:
            raise FrameError(f"RSV2/RSV3 set at offset {start}")
        if p + length > n:
            raise FrameError(
                f"truncated frame body at offset {start}: need {length}, "
                f"have {n - p}"
            )
        body_offset = p
        p += length
        if opcode in CONTROL_OPCODES:
            if not fin:
                raise FrameError(f"fragmented control frame at offset {start}")
            if length > MAX_CONTROL_BODY:
                raise FrameError(
                    f"control frame body {length} > {MAX_CONTROL_BODY} at offset {start}"
                )
            if rsv1:
                raise FrameError(f"RSV1 set on control frame at offset {start}")
        elif opcode in (0x1, 0x2):
            if rsv1 and opcode == 0x1:
                # permessage-deflate applies to data frames; a text frame could
                # in principle be compressed too, so we allow it only when the
                # negotiation enables deflate.  Binary is the supported payload
                # kind, and text is rejected during assembly.
                pass
        elif opcode == 0x0:
            pass
        else:
            raise FrameError(f"unknown opcode {opcode} at offset {start}")
        frames.append(
            FrameRecord(
                idx=len(frames),
                offset=start,
                end=p,
                fin=fin,
                rsv1=rsv1,
                opcode=opcode,
                masked=masked,
                body_offset=body_offset,
                body_length=length,
            )
        )
    return frames


def _assemble_messages(
    data: bytes, frames: List[FrameRecord], negotiation: Negotiation
) -> List[MessageRecord]:
    messages: List[MessageRecord] = []
    i = 0
    total = len(frames)
    while i < total:
        first = frames[i]
        if first.opcode in CONTROL_OPCODES:
            i += 1  # control frame between messages; not a message
            continue
        if first.opcode == 0x0:
            raise FrameError(
                f"continuation frame at offset {first.offset} without a start frame"
            )
        if first.opcode == 0x1:
            raise FrameError(
                f"text frame at offset {first.offset}: archive reader supports "
                "binary messages only"
            )
        compressed = first.rsv1
        if compressed and not negotiation.permessage_deflate:
            raise FrameError(
                f"frame at offset {first.offset} sets RSV1 but permessage-deflate "
                "was not negotiated"
            )
        frame_indices = [first.idx]
        body_ranges = [(first.body_offset, first.body_length)]
        body_bytes = first.body_length
        last = first
        j = i + 1
        while not last.fin:
            if j >= total:
                raise FrameError(
                    f"message starting at offset {first.offset} is truncated: "
                    "no terminating FIN frame"
                )
            f = frames[j]
            if f.opcode in CONTROL_OPCODES:
                # Interleaved control frame: skipped, does not end the message.
                j += 1
                continue
            if f.opcode != 0x0:
                raise FrameError(
                    f"new data frame opcode {f.opcode} at offset {f.offset} while "
                    f"message at offset {first.offset} is still open"
                )
            if f.rsv1:
                raise FrameError(
                    f"RSV1 set on continuation frame at offset {f.offset}: only the "
                    "first data frame may declare compression"
                )
            frame_indices.append(f.idx)
            body_ranges.append((f.body_offset, f.body_length))
            body_bytes += f.body_length
            last = f
            j += 1
        messages.append(
            MessageRecord(
                index=len(messages),
                first_frame=first.idx,
                last_frame=last.idx,
                first_offset=first.offset,
                end_offset=last.end,
                frame_indices=frame_indices,
                body_ranges=body_ranges,
                compressed=compressed,
                body_bytes=body_bytes,
            )
        )
        i = j
    return messages


def parse_archive(
    data: bytes, negotiation: Negotiation, sha256: str
) -> ArchiveLayout:
    from hashlib import sha256 as _sha

    if sha256 is None:
        sha256 = _sha(data).hexdigest()
    frames = _parse_frames(data)
    messages = _assemble_messages(data, frames, negotiation)
    return ArchiveLayout(
        data=data,
        sha256=sha256,
        negotiation=negotiation,
        frames=frames,
        messages=messages,
    )
