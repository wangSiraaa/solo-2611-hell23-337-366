"""Parse a user-facing query specification for the CLI / fixtures.

The specification language is intentionally small and JSON-friendly::

    {"rows": [7, 83], "slice": [0, 8]}
    {"rows": [4, 5, 4], "slice": [1000, 1024]}
    {"requests": [
        {"row": 7, "intervals": [[0, 8], [4, 12]]},
        {"row": 83, "intervals": []},           # [] means whole array
        {"row": 7, "intervals": [[10, 10]]}      # empty interval is legal
    ]}

Both shapes normalise to ``fitsproj.engine.RowSpec`` objects, preserving request
order and duplicates.
"""

from __future__ import annotations

from .engine import RowSpec


def parse_request(spec) -> list[RowSpec]:
    if not isinstance(spec, dict):
        raise TypeError("query specification must be a JSON object")

    if "requests" in spec:
        out = []
        for item in spec["requests"]:
            row = int(item["row"])
            intervals = item.get("intervals", [])
            out.append(
                RowSpec(
                    row,
                    tuple((int(a), int(b)) for a, b in intervals),
                )
            )
        return out

    if "rows" in spec:
        rows = spec["rows"]
        slice_ = spec.get("slice")
        interval = tuple(int(x) for x in slice_) if slice_ is not None else ()
        intervals = (interval,) if interval else ()
        return [RowSpec(int(r), intervals) for r in rows]

    raise ValueError("query specification needs 'requests' or 'rows'")


def rows_to_spec(specs) -> dict:
    """Serialise ``RowSpec`` objects back to the explicit request form."""

    return {
        "requests": [
            {
                "row": s.row,
                "intervals": [list(iv) for iv in s.intervals],
            }
            for s in specs
        ]
    }
