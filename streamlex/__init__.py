"""Transaction-boundary-safe recovery for a restricted SQL export stream.

Public entry points: :class:`streamlex.engine.Engine`, the store types in
:mod:`streamlex.store`, and the CLI in :mod:`streamlex.cli`.
"""

from .errors import (  # noqa: F401
    AuditFailed,
    InputFormatError,
    SourceChanged,
    StreamLexError,
    TxnRejected,
)
from .engine import Engine  # noqa: F401
from .store import FileStore, Manifest, MemoryStore  # noqa: F401

__all__ = [
    "Engine",
    "FileStore",
    "MemoryStore",
    "Manifest",
    "StreamLexError",
    "InputFormatError",
    "SourceChanged",
    "AuditFailed",
    "TxnRejected",
]

__version__ = "2.0.0"
