"""Package entry points for the compressed-websocket archive reader."""

from .build import STATE_BUDGET, build_from_files, build_index, choose_density
from .codec import DecodeCounter
from .errors import (
    ArchiveBindingError,
    ArchiveError,
    CheckpointError,
    CorruptStreamError,
    FrameError,
    NegotiationError,
    PartialMessageError,
)
from .index import ArchiveIndex, Checkpoint, load_index
from .model import Negotiation
from .reader import QueryResult, Reader

__all__ = [
    "STATE_BUDGET",
    "build_from_files",
    "build_index",
    "choose_density",
    "DecodeCounter",
    "ArchiveBindingError",
    "ArchiveError",
    "CheckpointError",
    "CorruptStreamError",
    "FrameError",
    "NegotiationError",
    "PartialMessageError",
    "ArchiveIndex",
    "Checkpoint",
    "load_index",
    "Negotiation",
    "QueryResult",
    "Reader",
]
