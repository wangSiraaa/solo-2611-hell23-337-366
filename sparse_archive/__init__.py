"""Sparse multi-generation image archive.

Implements the contract in contract.md: ingest-base, apply-delta,
read-range, compact, verify over a generation chain of sparse images with
per-block integrity, reader pins, and crash-safe publication.
"""
from .archive import Archive, Meter, Reader
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
    HOLE,
    MAX_FILE_BYTES,
    MAX_GENERATIONS,
    MAX_READERS,
    DeltaDecl,
    Op,
    block_count,
    decl_from_json,
    delta_content_id,
    normalize_ops,
    sha256_hex,
    validate_decl,
    zero_digest,
)
from .oracle import Oracle

__all__ = [
    "Archive", "Meter", "Reader", "Oracle",
    "DeltaDecl", "Op", "decl_from_json", "delta_content_id", "normalize_ops",
    "validate_decl", "block_count", "sha256_hex", "zero_digest",
    "BLOCK_SIZE", "DATA", "HOLE", "MAX_FILE_BYTES", "MAX_GENERATIONS",
    "MAX_READERS",
    "ArchiveError", "ValidationError", "AncestorError", "DeltaConflictError",
    "DigestMismatchError", "RangeRequestError", "ReaderLimitError",
    "RetentionError", "WriterBusyError", "CorruptionError",
]
