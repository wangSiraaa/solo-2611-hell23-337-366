"""Error taxonomy for the constrained MERGE executor.

PlanInvalid  - deterministic content problem (bad AST, bad source row,
               overflow, plan_id mismatch). Retrying the identical plan
               can never succeed.
PlanConflict - deterministic intra-plan conflict (two sources hit the same
               target, or two actions produce the same new unique key).
"""


class MergeError(Exception):
    """Base class for all merge-plan errors."""


class PlanInvalid(MergeError):
    """The plan content itself is illegal; rejection is deterministic."""

    def __init__(self, reason, detail=None):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


class PlanConflict(MergeError):
    """Deterministic conflict detected while freezing the plan."""

    def __init__(self, kind, identity):
        super().__init__(kind)
        self.kind = kind
        self.identity = identity
