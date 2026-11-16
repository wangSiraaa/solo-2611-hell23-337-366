"""Error taxonomy for the sparse archive.

Every rejection path raises one of these so callers (and tests) can tell
*why* an operation was refused: illegal declaration, wrong ancestor,
digest mismatch, retention policy, reader limit, or on-disk corruption.
"""


class ArchiveError(Exception):
    """Base class for all archive errors."""


class ValidationError(ArchiveError):
    """A delta declaration, range, or argument is malformed or out of bounds."""


class AncestorError(ArchiveError):
    """The declared base_generation is not the current tip / was reclaimed."""


class DeltaConflictError(ArchiveError):
    """target_generation already exists with different content (not idempotent)."""


class DigestMismatchError(ArchiveError):
    """An assembled logical block does not match its recorded checksum."""


class RangeRequestError(ArchiveError):
    """Read range is out of bounds for the requested generation."""


class RetentionError(ArchiveError):
    """Generation is not readable (reclaimed, or not tip/tip-1/pinned)."""


class ReaderLimitError(ArchiveError):
    """More than the allowed number of concurrent readers."""


class WriterBusyError(ArchiveError):
    """A writer/compact operation is already in flight."""


class CorruptionError(ArchiveError):
    """On-disk state is inconsistent with the manifest/index protocol."""
