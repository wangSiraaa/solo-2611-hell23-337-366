"""Raw-deflate decoding with explicit byte accounting.

Two facts drive this module:

* permessage-deflate with context takeover is one continuous deflate stream.
  Each message ends with ``Z_SYNC_FLUSH`` whose four-byte tail
  ``00 00 FF FF`` is stripped on the wire, so a decoder appends
  ``b"\\x00\\x00\\xff\\xff"`` and keeps using the *same* decompressor.
  Treating each message as a fresh stream is wrong: distances may point into
  earlier messages.
* a ``zlib.decompressobj`` can be seeded with the trailing window of the
  previous plaintext (``zdict``).  Because ``Z_SYNC_FLUSH`` ends a block and
  byte-aligns the stream, a message boundary is a clean place to swap the
  decompressor for one seeded with the last ``1 << window_bits`` bytes of
  compressed-message plaintext.  That is the persistent checkpoint.

Every byte returned by ``decompress`` is added to the counter, including
messages that are decoded only to rebuild history for the requested message
and are then thrown away.  Nothing here caches plaintext; a repeated query
inflates again and is counted again.
"""

import hashlib
import zlib
from dataclasses import dataclass, field
from typing import List, Optional

from .errors import CorruptStreamError, PartialMessageError
from .model import MessageRecord

MARKER = b"\x00\x00\xff\xff"


@dataclass
class DecodeCounter:
    """Mutable byte accounting for one query or one batch."""

    compressed_body_bytes: int = 0   # raw archive body bytes fed to zlib
    fed_bytes: int = 0               # body bytes + synthetic flush markers
    inflated_bytes: int = 0          # every plaintext byte inflate produced
    raw_message_bytes: int = 0       # uncompressed messages materialised
    output_bytes: int = 0            # bytes actually returned to the caller
    unique_output_bytes: int = 0
    messages_decoded: int = 0        # all messages materialised (incl. prefix)
    compressed_messages_decoded: int = 0
    uncompressed_messages_decoded: int = 0
    inflate_calls: int = 0
    checkpoints_used: List[int] = field(default_factory=list)
    checkpoint_dict_bytes: int = 0   # dictionary bytes loaded for this query
    prefix_inflated_bytes: int = 0   # inflated bytes that were discarded
    prefix_raw_bytes: int = 0
    duplicate_output_bytes: int = 0
    requested: int = 0
    unique_requested: int = 0

    @property
    def materialized_bytes(self) -> int:
        """Compressed-message plaintext produced by inflate plus raw bytes."""
        return self.inflated_bytes + self.raw_message_bytes

    def as_dict(self) -> dict:
        return {
            "requested": self.requested,
            "unique_requested": self.unique_requested,
            "compressed_body_bytes": self.compressed_body_bytes,
            "fed_bytes": self.fed_bytes,
            "inflated_bytes": self.inflated_bytes,
            "raw_message_bytes": self.raw_message_bytes,
            "materialized_bytes": self.materialized_bytes,
            "output_bytes": self.output_bytes,
            "unique_output_bytes": self.unique_output_bytes,
            "duplicate_output_bytes": self.duplicate_output_bytes,
            "prefix_inflated_bytes": self.prefix_inflated_bytes,
            "prefix_raw_bytes": self.prefix_raw_bytes,
            "messages_decoded": self.messages_decoded,
            "compressed_messages_decoded": self.compressed_messages_decoded,
            "uncompressed_messages_decoded": self.uncompressed_messages_decoded,
            "inflate_calls": self.inflate_calls,
            "checkpoint_dict_bytes": self.checkpoint_dict_bytes,
            "checkpoints_used": sorted(set(self.checkpoints_used)),
        }


def make_inflater(window_bits: int, zdict: bytes = b""):
    if zdict:
        return zlib.decompressobj(-window_bits, zdict=zdict)
    return zlib.decompressobj(-window_bits)


def message_body(data: bytes, msg: MessageRecord) -> bytes:
    """Concatenate a message's data-fragment bodies, skipping control frames."""
    if len(msg.body_ranges) == 1:
        off, ln = msg.body_ranges[0]
        return data[off : off + ln]
    return b"".join(data[o : o + n] for o, n in msg.body_ranges)


def _inflate(inflater, fed: bytes, counter: DecodeCounter, msg_index: int) -> bytes:
    try:
        out = inflater.decompress(fed)
    except zlib.error as exc:
        raise CorruptStreamError(
            f"inflate failed for message {msg_index}: {exc}"
        ) from exc
    counter.inflate_calls += 1
    counter.inflated_bytes += len(out)
    if inflater.unconsumed_tail:
        raise CorruptStreamError(
            f"message {msg_index}: zlib left {len(inflater.unconsumed_tail)} "
            "unconsumed input bytes"
        )
    return out


def verify_message(
    msg: MessageRecord, plaintext: bytes, expected_len: Optional[int],
    expected_digest: Optional[bytes],
) -> None:
    if expected_len is not None and len(plaintext) != expected_len:
        raise PartialMessageError(
            f"message {msg.index}: decoded {len(plaintext)} bytes, "
            f"index records {expected_len}; refusing to return a partial message"
        )
    if expected_digest is not None:
        got = hashlib.sha256(plaintext).digest()
        if got != expected_digest:
            raise CorruptStreamError(
                f"message {msg.index}: plaintext digest mismatch "
                f"({got.hex()} != {expected_digest.hex()}); compressed bytes are "
                "damaged or the wrong checkpoint/history was used"
            )


def decode_compressed(
    data: bytes,
    msg: MessageRecord,
    inflater,
    counter: DecodeCounter,
    body: Optional[bytes] = None,
) -> bytes:
    if body is None:
        body = message_body(data, msg)
    counter.compressed_body_bytes += len(body)
    fed = body + MARKER
    counter.fed_bytes += len(fed)
    counter.compressed_messages_decoded += 1
    return _inflate(inflater, fed, counter, msg.index)


def decode_uncompressed(
    data: bytes,
    msg: MessageRecord,
    counter: DecodeCounter,
    body: Optional[bytes] = None,
) -> bytes:
    if body is None:
        body = message_body(data, msg)
    counter.raw_message_bytes += len(body)
    counter.uncompressed_messages_decoded += 1
    return body
