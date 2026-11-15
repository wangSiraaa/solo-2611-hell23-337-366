"""Exception hierarchy for the version-consistent projection engine."""


class ProjectionError(Exception):
    """Base class for all engine errors."""


class ProtocolError(ProjectionError):
    """The remote did not answer with a usable ranged FITS response."""


class HeaderParseError(ProjectionError):
    """The remote bytes could not be parsed as the supported FITS subset."""


class RowOutOfRange(ProjectionError):
    """A requested row index is outside the table."""


class DescriptorBoundsError(ProjectionError):
    """One or more variable-length descriptors/interval requests are illegal.

    Raised *before* any heap byte is fetched.  When raised for a multi-row
    request the whole request is rejected; no partial result is produced.
    """

    def __init__(self, problems):
        self.problems = list(problems)
        detail = "; ".join(str(p) for p in self.problems)
        super().__init__(f"illegal variable-length descriptor/interval: {detail}")


class VersionMismatch(ProjectionError):
    """Internal: the object version observed differs from the bound plan."""


class PreconditionFailed(VersionMismatch):
    """The server answered 412 because the object version changed."""

    def __init__(self, record):
        self.record = record
        super().__init__("HTTP 412 Precondition Failed (object version changed)")


class VersionConflictError(ProjectionError):
    """The version moved twice; the retry budget is exhausted.

    Carries the per-attempt statistics but deliberately no rows: no partial
    result is ever exposed for a failed version-consistency run.
    """

    def __init__(self, attempts, requests):
        self.attempts = list(attempts)
        self.requests = list(requests)
        self.result = None
        body = sum(a["body_bytes"] for a in self.attempts)
        super().__init__(
            "object version changed again during the single full retry; "
            f"abandoned after {len(self.attempts)} attempts, "
            f"{body} response body bytes"
        )
