"""Plain data records shared by the parser, index and reader."""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

CONTROL_OPCODES = frozenset((0x8, 0x9, 0xA))
DATA_OPCODES = frozenset((0x1, 0x2, 0x0))  # text, binary, continuation


@dataclass(frozen=True)
class Negotiation:
    """The permessage-deflate parameters that shape how the archive decodes.

    ``server_no_context_takeover`` is the direction that matters here: the
    frames are all server-to-client, so only the server flag changes whether
    message boundaries share a deflate history.
    """

    permessage_deflate: bool
    server_no_context_takeover: bool
    server_max_window_bits: int
    client_no_context_takeover: bool = False
    client_max_window_bits: Optional[int] = None

    @property
    def window_bits(self) -> int:
        return self.server_max_window_bits

    @property
    def context_takeover(self) -> bool:
        return not self.server_no_context_takeover

    @classmethod
    def from_json(cls, obj: dict) -> "Negotiation":
        from .errors import NegotiationError

        if not isinstance(obj, dict):
            raise NegotiationError("negotiation must be a JSON object")
        # Tolerate the parameters being nested under a permessage-deflate key,
        # which is how RFC 7692 extensions are commonly marshalled.
        if "permessage_deflate" not in obj and isinstance(obj.get("parameters"), dict):
            obj = obj["parameters"]
        enabled = bool(obj.get("permessage_deflate", True))
        nb = obj.get("server_max_window_bits", 15)
        if isinstance(nb, str):  # client may send "client_max_window_bits" as a string
            nb = int(nb)
        nb = int(nb)
        if not 9 <= nb <= 15:
            raise NegotiationError(f"server_max_window_bits {nb} outside 9..15")
        cmb = obj.get("client_max_window_bits")
        if isinstance(cmb, str):
            try:
                cmb = int(cmb)
            except ValueError:
                cmb = None
        return cls(
            permessage_deflate=enabled,
            server_no_context_takeover=bool(obj.get("server_no_context_takeover", False)),
            server_max_window_bits=nb,
            client_no_context_takeover=bool(obj.get("client_no_context_takeover", False)),
            client_max_window_bits=cmb,
        )


@dataclass
class FrameRecord:
    """One WebSocket frame as it appears on the wire (server direction)."""

    idx: int
    offset: int
    end: int
    fin: bool
    rsv1: bool          # only meaningful on the *first* data frame of a message
    opcode: int
    masked: bool
    body_offset: int
    body_length: int
    live_rsv1: bool = False  # rsv1 restricted to the first data frame


@dataclass
class MessageRecord:
    """A reconstructed binary message and the frames that carried it."""

    index: int
    first_frame: int
    last_frame: int
    first_offset: int
    end_offset: int
    frame_indices: List[int]
    body_ranges: List[Tuple[int, int]]  # (offset, length) of compressed bytes
    compressed: bool
    body_bytes: int                     # raw compressed payload bytes read
    declared_length: Optional[int] = None  # filled after decode
    plaintext_sha256: bytes = b""       # verification digest, not stored data
    body_sha256: bytes = b""            # digest of the wire compressed body

    @property
    def fragment_count(self) -> int:
        return len(self.body_ranges)


@dataclass
class ArchiveLayout:
    """Parsed archive structure; no plaintext here."""

    data: bytes
    sha256: str
    negotiation: Negotiation
    frames: List[FrameRecord] = field(default_factory=list)
    messages: List[MessageRecord] = field(default_factory=list)
