"""Backwards-compatible query entry point.

The original baseline downloaded the whole object and handed it to astropy.
This version routes the same call shape through the ranged, version-consistent
:class:`fitsproj.engine.ProjectionEngine`, so only the requested descriptor and
heap bytes are transferred.

    query(url, [7, 83])            # whole arrays for those rows
    query(url, [7, 83], 0, 8)      # elements [0, 8) of those rows

The returned ``rows`` list keeps the legacy shape ``{'row': i, 'samples': [...]}``
but each entry also carries ``source_row`` / ``element_start`` / ``element_stop``
so duplicate and reordered requests remain distinguishable.
"""

from __future__ import annotations

from fitsproj.engine import ProjectionEngine


def query(url, rows, start=None, stop=None):
    engine = ProjectionEngine(url)
    if start is None and stop is None:
        intervals = None
    else:
        intervals = (0 if start is None else start, None if stop is None else stop)
    result = engine.query(rows, intervals)
    legacy = [
        {
            "row": entry["source_row"],
            "source_row": entry["source_row"],
            "occurrence": entry["occurrence"],
            "element_start": entry["element_start"],
            "element_stop": entry["element_stop"],
            "samples": entry["samples"],
        }
        for entry in result["rows"]
    ]
    return {
        "etag": result["etag"],
        "rows": legacy,
        "plan": result["plan"],
        "trace": result["trace"],
        "response_body_bytes": result["response_body_bytes"],
        "attempts": result["attempts"],
        "cache": result["cache"],
    }
