"""Restricted MERGE AST v1: structural validation and evaluation.

Grammar (see fixtures/ast-spec.md):

  ValueExpr = {"literal": int64|text|null}
            | {"source": SourceField}
            | {"target": TargetColumn}
            | {"op": "+"|"-", "args": [ValueExpr, ValueExpr]}
  BoolExpr  = {"literal": true|false|null}
            | {"op": "="|"<>"|"<"|">"|"<="|">=", "args": [ValueExpr, ValueExpr]}
            | {"op": "and"|"or", "args": [BoolExpr, BoolExpr]}
            | {"op": "not", "args": [BoolExpr]}
            | {"op": "is_null"|"is_not_null", "args": [ValueExpr]}

Semantics:
  * SQL three-valued logic for comparisons and AND/OR/NOT.
  * Text comparison follows SQLite BINARY collation (code-point order,
    which matches UTF-8 byte order); mixed int/text comparison follows
    SQLite type ordering (any integer sorts below any text).
  * Integer arithmetic: NULL propagates; non-integer operands or int64
    overflow reject the whole plan (PlanInvalid).
  * Expressions are evaluated against the frozen pre-statement snapshot;
    every target column actually read is reported through the read tracker
    so the commit phase can re-verify exactly those values.
"""

from .errors import PlanInvalid

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

MAX_AST_DEPTH = 8

SOURCE_FIELDS = ("source_id", "key", "delta", "new_key", "parent_id")
# Optional source fields: omitted is NOT the same as explicit null.
OPTIONAL_SOURCE_FIELDS = ("new_key", "parent_id")
TARGET_COLUMNS = ("id", "business_key", "amount", "parent_id", "rev")
SETTABLE_COLUMNS = ("business_key", "amount", "parent_id")

_CMP_OPS = ("=", "<>", "<", ">", "<=", ">=")
_ARITH_OPS = ("+", "-")


def is_int64(v):
    return isinstance(v, int) and not isinstance(v, bool) and INT64_MIN <= v <= INT64_MAX


def _depth(expr):
    if not isinstance(expr, dict):
        raise PlanInvalid("ast node must be an object", expr)
    if "op" in expr:
        args = expr.get("args")
        if not isinstance(args, list) or not args:
            raise PlanInvalid("op node needs a non-empty args list", expr)
        return 1 + max(_depth(a) for a in args)
    return 1


def check_depth(expr):
    d = _depth(expr)
    if d > MAX_AST_DEPTH:
        raise PlanInvalid("ast depth %d exceeds limit %d" % (d, MAX_AST_DEPTH))
    return d


def _validate_value(expr, allow_target):
    if not isinstance(expr, dict):
        raise PlanInvalid("invalid value expression", expr)
    if "literal" in expr:
        if set(expr) != {"literal"}:
            raise PlanInvalid("literal node must have exactly one key", expr)
        v = expr["literal"]
        if v is not None and not is_int64(v) and not isinstance(v, str):
            raise PlanInvalid("literal must be int64, text or null", expr)
        return
    if "source" in expr:
        if set(expr) != {"source"} or expr["source"] not in SOURCE_FIELDS:
            raise PlanInvalid("unknown source field", expr)
        return
    if "target" in expr:
        if set(expr) != {"target"} or expr["target"] not in TARGET_COLUMNS:
            raise PlanInvalid("unknown target column", expr)
        if not allow_target:
            raise PlanInvalid("not_matched branch may not reference target", expr)
        return
    if "op" in expr:
        if set(expr) != {"op", "args"} or expr["op"] not in _ARITH_OPS:
            raise PlanInvalid("unknown value operator", expr)
        args = expr["args"]
        if not isinstance(args, list) or len(args) != 2:
            raise PlanInvalid("arithmetic needs exactly 2 args", expr)
        _validate_value(args[0], allow_target)
        _validate_value(args[1], allow_target)
        return
    raise PlanInvalid("invalid value expression", expr)


def _validate_bool(expr, allow_target):
    if not isinstance(expr, dict):
        raise PlanInvalid("invalid boolean expression", expr)
    if "literal" in expr:
        if set(expr) != {"literal"} or not (
            expr["literal"] is None or isinstance(expr["literal"], bool)
        ):
            raise PlanInvalid("boolean literal must be true, false or null", expr)
        return
    if "op" not in expr or "args" not in expr:
        raise PlanInvalid("invalid boolean expression", expr)
    if set(expr) != {"op", "args"}:
        raise PlanInvalid("op node must have exactly op and args", expr)
    op, args = expr["op"], expr["args"]
    if op in _CMP_OPS:
        if not isinstance(args, list) or len(args) != 2:
            raise PlanInvalid("comparison needs exactly 2 args", expr)
        _validate_value(args[0], allow_target)
        _validate_value(args[1], allow_target)
    elif op in ("and", "or"):
        if not isinstance(args, list) or len(args) != 2:
            raise PlanInvalid("and/or needs exactly 2 args", expr)
        _validate_bool(args[0], allow_target)
        _validate_bool(args[1], allow_target)
    elif op == "not":
        if not isinstance(args, list) or len(args) != 1:
            raise PlanInvalid("not needs exactly 1 arg", expr)
        _validate_bool(args[0], allow_target)
    elif op in ("is_null", "is_not_null"):
        if not isinstance(args, list) or len(args) != 1:
            raise PlanInvalid("is_null needs exactly 1 arg", expr)
        _validate_value(args[0], allow_target)
    else:
        raise PlanInvalid("unknown boolean operator", expr)


def validate_bool_expr(expr, allow_target):
    check_depth(expr)
    _validate_bool(expr, allow_target)


def validate_value_expr(expr, allow_target):
    check_depth(expr)
    _validate_value(expr, allow_target)


class _ReadTracker:
    """Collects the target columns actually read during evaluation."""

    def __init__(self):
        self.columns = set()

    def __call__(self, column):
        self.columns.add(column)


def _sqlite_cmp(a, b):
    """SQLite BINARY comparison of two non-NULL values."""
    a_num = isinstance(a, int) and not isinstance(a, bool)
    b_num = isinstance(b, int) and not isinstance(b, bool)
    if a_num and b_num:
        return (a > b) - (a < b)
    if isinstance(a, str) and isinstance(b, str):
        return (a > b) - (a < b)
    # SQLite type ordering: INTEGER/REAL < TEXT. Mixed comparison.
    return -1 if a_num else 1


def _eval_value(expr, source_row, target_row, track):
    if "literal" in expr:
        return expr["literal"]
    if "source" in expr:
        field = expr["source"]
        if field not in source_row:
            raise PlanInvalid(
                "source field %r referenced but omitted in source row %r"
                % (field, source_row.get("source_id"))
            )
        return source_row[field]
    if "target" in expr:
        col = expr["target"]
        track(col)
        return target_row[col]
    op = expr["op"]
    a = _eval_value(expr["args"][0], source_row, target_row, track)
    b = _eval_value(expr["args"][1], source_row, target_row, track)
    if a is None or b is None:
        return None
    if not is_int64(a) or not is_int64(b):
        raise PlanInvalid("arithmetic on non-integer operand", expr)
    r = a + b if op == "+" else a - b
    if not (INT64_MIN <= r <= INT64_MAX):
        raise PlanInvalid("int64 overflow in arithmetic", expr)
    return r


def _eval_bool(expr, source_row, target_row, track):
    if "literal" in expr:
        return expr["literal"]
    op, args = expr["op"], expr["args"]
    if op in _CMP_OPS:
        a = _eval_value(args[0], source_row, target_row, track)
        b = _eval_value(args[1], source_row, target_row, track)
        if a is None or b is None:
            return None
        c = _sqlite_cmp(a, b)
        return {
            "=": c == 0,
            "<>": c != 0,
            "<": c < 0,
            ">": c > 0,
            "<=": c <= 0,
            ">=": c >= 0,
        }[op]
    if op == "and":
        a = _eval_bool(args[0], source_row, target_row, track)
        b = _eval_bool(args[1], source_row, target_row, track)
        if a is False or b is False:
            return False
        if a is None or b is None:
            return None
        return True
    if op == "or":
        a = _eval_bool(args[0], source_row, target_row, track)
        b = _eval_bool(args[1], source_row, target_row, track)
        if a is True or b is True:
            return True
        if a is None or b is None:
            return None
        return False
    if op == "not":
        v = _eval_bool(args[0], source_row, target_row, track)
        return None if v is None else (not v)
    if op == "is_null":
        return _eval_value(args[0], source_row, target_row, track) is None
    if op == "is_not_null":
        return _eval_value(args[0], source_row, target_row, track) is not None
    raise PlanInvalid("unknown boolean operator", expr)  # unreachable after validation


def eval_value(expr, source_row, target_row, track):
    """Evaluate a validated ValueExpr; `track` records target column reads."""
    return _eval_value(expr, source_row, target_row, track)


def eval_condition(expr, source_row, target_row):
    """Evaluate a validated BoolExpr; returns (tri_value, read_columns)."""
    tracker = _ReadTracker()
    value = _eval_bool(expr, source_row, target_row, tracker)
    return value, tracker.columns


def eval_assignment(expr, source_row, target_row):
    """Evaluate a validated ValueExpr; returns (value, read_columns)."""
    tracker = _ReadTracker()
    value = _eval_value(expr, source_row, target_row, tracker)
    return value, tracker.columns
