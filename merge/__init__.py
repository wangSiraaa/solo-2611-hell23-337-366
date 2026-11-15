"""Constrained MERGE executor with frozen-match planning and read-set
validation at commit time. See README.md for the design."""

from .errors import MergeError, PlanConflict, PlanInvalid
from .executor import MergeExecutor, connect
from .planner import compute_plan_id, plan
from .schema import init_schema

__all__ = [
    "MergeError",
    "PlanConflict",
    "PlanInvalid",
    "MergeExecutor",
    "connect",
    "compute_plan_id",
    "plan",
    "init_schema",
]
