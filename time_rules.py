"""Time rules: legacy API (kept for baseline_demo.py) plus the new
leap-aware, integer-nanosecond rule engine used by the event catalog.

Model
-----
Canonical absolute time is TAI nanoseconds on a single integer axis:
``tai_ns = (posix_label + tai_minus_utc) * 1e9 + nanos`` for UTC instants.
TAI and GPS events carry an absolute identity that never depends on the
installed UTC rule table:

* TAI text ``YYYY-MM-DDTHH:MM:SS[.fffffffff]`` (no ``Z``) is read as a
  naive calendar reading on the TAI axis.
* GPS seconds ``S[.fffffffff]`` map to ``tai = GPS_EPOCH_POSIX + 19 + S``.

UTC text may use ``:60`` for a positive leap second; it is valid only when
the installed table declares a ``+1`` offset change effective at the
following midnight.  All arithmetic is integer nanoseconds; no floats.
"""
from datetime import datetime, timezone, date
import re

# ---------------------------------------------------------------------------
# Legacy API (question baseline; do not change semantics — baseline_demo.py
# reproduces the known cross-module staleness gap with these).
# ---------------------------------------------------------------------------

def seconds(s):
    return int(datetime.fromisoformat(s.replace('Z', '+00:00')).replace(tzinfo=timezone.utc).timestamp())


def normalize(event, table):
    if event['scale'] == 'TAI':
        return seconds(event['value'])
    t = seconds(event['value'])
    offset = table['initial_offset']
    for e in table['changes']:
        if t >= seconds(e['effective']):
            offset = e['offset']
    return t + offset


# ---------------------------------------------------------------------------
# New rule engine
# ---------------------------------------------------------------------------

NS_PER_SEC = 1_000_000_000
GPS_EPOCH_POSIX = 315_964_800          # 1980-01-06T00:00:00Z
TAI_MINUS_GPS = 19                     # TAI - GPS, constant

_EPOCH = date(1970, 1, 1)
_UTC_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$")
_TAI_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$")
_GPS_RE = re.compile(r"^(\d+)(?:\.(\d{1,9}))?$")
_GPS_FRAC_RE = re.compile(r"^\d+\.\d+$")

OUTSIDE_COVERAGE = "outside_coverage"
LEAP_NOT_IN_TABLE = "leap_second_not_in_table"


class RuleError(ValueError):
    """Raised for malformed timestamps or illegal rule tables."""


def _days(y, m, d):
    try:
        return (date(y, m, d) - _EPOCH).days
    except ValueError as exc:
        raise RuleError(f"invalid calendar date {y:04d}-{m:02d}-{d:02d}: {exc}")


def _frac_ns(frac):
    return int(frac.ljust(9, '0')) if frac else 0


def parse_utc_text(text):
    """Parse UTC text -> (label_sec, nanos, is_leap60).

    ``label_sec`` is the naive POSIX label; for ``:60`` it equals the POSIX
    label of the following midnight (the leap second's slot on the UTC axis).
    """
    m = _UTC_RE.match(text) if isinstance(text, str) else None
    if not m:
        raise RuleError(f"invalid UTC timestamp: {text!r}")
    y, mo, d, h, mi, s = (int(m.group(i)) for i in range(1, 7))
    if h > 23 or mi > 59 or s > 60:
        raise RuleError(f"UTC time out of range: {text!r}")
    label = _days(y, mo, d) * 86400 + h * 3600 + mi * 60 + s
    return label, _frac_ns(m.group(7)), s == 60


def parse_tai_text(text):
    """Parse TAI text (no Z, no leap seconds) -> absolute tai_ns."""
    m = _TAI_RE.match(text) if isinstance(text, str) else None
    if not m:
        raise RuleError(f"invalid TAI timestamp: {text!r}")
    y, mo, d, h, mi, s = (int(m.group(i)) for i in range(1, 7))
    if h > 23 or mi > 59 or s > 59:
        raise RuleError(f"TAI time out of range (no leap seconds in TAI): {text!r}")
    sec = _days(y, mo, d) * 86400 + h * 3600 + mi * 60 + s
    return sec * NS_PER_SEC + _frac_ns(m.group(7))


def parse_gps_text(text):
    """Parse GPS seconds (<= 9 fractional digits) -> absolute tai_ns."""
    if not isinstance(text, str):
        raise RuleError(f"invalid GPS value: {text!r}")
    m = _GPS_RE.match(text)
    if not m:
        if _GPS_FRAC_RE.match(text):
            raise RuleError(f"GPS fraction too precise (max 9 digits): {text!r}")
        raise RuleError(f"invalid GPS value: {text!r}")
    sec = int(m.group(1))
    return (GPS_EPOCH_POSIX + TAI_MINUS_GPS + sec) * NS_PER_SEC + _frac_ns(m.group(2))


def _parse_plain_utc(text, what):
    label, nanos, leap = parse_utc_text(text)
    if leap or nanos:
        raise RuleError(f"{what} must be a whole non-leap UTC second: {text!r}")
    return label


class RuleTable:
    """A versioned leap-second rule table.

    Declares ``id``, ``coverage`` (half-open UTC interval), ``sealed_before``
    and offset entries (``initial_offset`` plus ``+1`` leap changes).
    """

    __slots__ = ("table_id", "initial_offset", "changes", "sealed_before",
                 "coverage", "source")

    def __init__(self, table_id, initial_offset, changes, sealed_before, coverage, source):
        self.table_id = table_id
        self.initial_offset = initial_offset
        self.changes = changes            # list of (effective_sec, offset), sorted
        self.sealed_before = sealed_before
        self.coverage = coverage          # (c0_sec, c1_sec), half-open
        self.source = source              # original JSON dict (kept for audit)

    # -- construction / validation ----------------------------------------

    @classmethod
    def from_json(cls, obj):
        if not isinstance(obj, dict):
            raise RuleError("rule table must be a JSON object")
        allowed = {"id", "initial_offset", "changes", "sealed_before", "coverage"}
        unknown = set(obj) - allowed
        if unknown:
            raise RuleError(f"unknown rule table fields: {sorted(unknown)}")
        missing = allowed - set(obj)
        if missing:
            raise RuleError(f"missing rule table fields: {sorted(missing)}")

        table_id = obj["id"]
        if not isinstance(table_id, str) or not table_id:
            raise RuleError("rule table id must be a non-empty string")
        initial = obj["initial_offset"]
        if not isinstance(initial, int) or isinstance(initial, bool) or initial < 0:
            raise RuleError("initial_offset must be a non-negative integer")

        changes = []
        prev_eff, prev_off = None, initial
        for entry in obj["changes"]:
            if not isinstance(entry, dict) or set(entry) != {"effective", "offset"}:
                raise RuleError(f"change entries must be {{effective, offset}}: {entry!r}")
            eff = _parse_plain_utc(entry["effective"], "change effective")
            if eff % 86400 != 0:
                raise RuleError(f"leap changes must be effective at UTC midnight: {entry['effective']!r}")
            off = entry["offset"]
            if not isinstance(off, int) or isinstance(off, bool):
                raise RuleError(f"change offset must be an integer: {entry!r}")
            if prev_eff is not None and eff <= prev_eff:
                raise RuleError("changes must be strictly increasing by effective time")
            if off != prev_off + 1:
                raise RuleError(
                    f"only positive leap seconds supported: offset {prev_off} -> {off} "
                    f"at {entry['effective']!r} is not a +1 step")
            changes.append((eff, off))
            prev_eff, prev_off = eff, off

        sealed = _parse_plain_utc(obj["sealed_before"], "sealed_before")
        cov = obj["coverage"]
        if not (isinstance(cov, list) and len(cov) == 2):
            raise RuleError("coverage must be [start, end]")
        c0 = _parse_plain_utc(cov[0], "coverage start")
        c1 = _parse_plain_utc(cov[1], "coverage end")
        if not c0 < c1:
            raise RuleError("coverage start must be before coverage end")
        if not (c0 <= sealed <= c1):
            raise RuleError("sealed_before must lie within coverage")
        return cls(table_id, initial, changes, sealed, (c0, c1), obj)

    # -- queries -----------------------------------------------------------

    def offset_at(self, sec):
        off = self.initial_offset
        for eff, o in self.changes:
            if sec >= eff:
                off = o
            else:
                break
        return off

    def covers(self, sec):
        return self.coverage[0] <= sec < self.coverage[1]

    def has_leap_at(self, midnight_sec):
        return any(eff == midnight_sec for eff, _ in self.changes)

    def normalize_utc(self, label_sec, nanos, is_leap60):
        """-> (tai_ns, None) or (None, reason)."""
        if is_leap60:
            if not self.has_leap_at(label_sec):
                return None, LEAP_NOT_IN_TABLE
            # The leap second sits in the last slot of the previous UTC day;
            # use the previous second's label for the coverage test.
            if not self.covers(label_sec - 1):
                return None, OUTSIDE_COVERAGE
            offset = self.offset_at(label_sec) - 1  # offset in force during the leap
        else:
            if not self.covers(label_sec):
                return None, OUTSIDE_COVERAGE
            offset = self.offset_at(label_sec)
        return (label_sec + offset) * NS_PER_SEC + nanos, None


def check_sealed_consistency(old, new):
    """New table must reproduce the installed table exactly before
    ``new.sealed_before``; tentative coverage at/after it may be corrected
    or extended.  Raises RuleError otherwise."""
    if new.sealed_before < old.sealed_before:
        raise RuleError(
            f"sealed_before may not regress: {old.sealed_before} -> {new.sealed_before}")
    if new.initial_offset != old.initial_offset:
        raise RuleError("initial_offset is sealed history and may not change")
    s = new.sealed_before
    old_hist = [(e, o) for e, o in old.changes if e < s]
    new_hist = [(e, o) for e, o in new.changes if e < s]
    if old_hist != new_hist:
        raise RuleError(f"offset entries before sealed_before ({s}) may not change")

    def cov_below(tab):
        lo, hi = tab.coverage[0], min(tab.coverage[1], s)
        return (lo, hi) if lo < hi else None

    if cov_below(old) != cov_below(new):
        raise RuleError("coverage before sealed_before may not change")


def table_from_legacy(leaps_json, table_id="legacy-leaps"):
    """Convert a legacy/leaps.json style table into the new rule-table JSON."""
    entries = leaps_json["entries"]
    if not entries:
        raise RuleError("legacy table has no entries")
    return {
        "id": table_id,
        "initial_offset": entries[0]["tai_minus_utc"],
        "changes": [{"effective": e["effective_utc"], "offset": e["tai_minus_utc"]}
                    for e in entries[1:]],
        "sealed_before": leaps_json["valid_from"],
        "coverage": [leaps_json["valid_from"], leaps_json["valid_until"]],
    }
