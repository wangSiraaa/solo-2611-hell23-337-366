"""Exception hierarchy for the archive reader.

Every failure carries enough context for a caller to know *which* archive,
checkpoint or message produced it, and none of them ever return a partially
decoded message as if it were a success.
"""


class ArchiveError(Exception):
    """Base class for all archive failures."""


class FrameError(ArchiveError):
    """Malformed or truncated WebSocket frame data."""


class NegotiationError(ArchiveError):
    """Missing, inconsistent or unsupported permessage-deflate parameters."""


class ArchiveBindingError(ArchiveError):
    """Archive bytes do not match the hash the index was built for, or the
    negotiated parameters differ from the ones the index was built for."""


class CheckpointError(ArchiveError):
    """A checkpoint is missing, malformed, or cannot reconstruct its history."""


class CorruptStreamError(ArchiveError):
    """Inflate reported corrupt data or produced an impossible result."""


class PartialMessageError(ArchiveError):
    """A message could not be reconstructed completely; no prefix is returned.

    Raised for truncated message frame sequences and for damage that only
    becomes visible at a message boundary (for example a truncated final
    block).
    """
