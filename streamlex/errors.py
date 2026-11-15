"""Typed failures.

The CLI maps these to exit codes so a caller can tell an *input* problem (the
source stream is malformed / tampered) apart from an *operator* problem (the
source grew under us, or the on-disk metadata disagrees with the input).
"""


class StreamLexError(Exception):
    """Base class for every rejection this package makes."""


class SourceChanged(StreamLexError):
    """The on-disk source no longer matches its trusted manifest.

    Its prefix is *not* re-verified during resume (that costs a full read); we
    only require it to still be byte-identical to what was already hashed and
    all newly appended bytes to start exactly at the old EOF.
    """


class InputFormatError(StreamLexError):
    """Unknown statement, malformed COPY row, unterminated construct, or a
    resumable cut that lands in a place we refuse to resume from."""


class AuditFailed(StreamLexError):
    """An explicit `audit-source` full read found a tampered historical prefix."""


class TxnRejected(StreamLexError):
    """The target refused the source transaction (e.g. broken foreign key).

    The guard already rolled the partial target work back, so the previous
    committed source transaction is the last thing visible.
    """


class GuardViolation(StreamLexError):
    """A CowGuard query would add a statement to an open target transaction
    beyond the resources the plan reserved for it (a bug in the planner, not a
    source problem)."""
