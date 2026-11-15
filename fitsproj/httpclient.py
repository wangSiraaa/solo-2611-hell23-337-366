"""Minimal ranged HTTP client with request tracing and byte accounting.

Only one transfer channel is used: ranged ``GET`` requests to the remote URL.
Every response body byte is counted, including redundant re-fetches.  Header
blocks requested while discovering the layout are counted by the engine only
in the *data* budget, not against the data-page cache (they are not paginated
[0, 9 KiB) of parsed headers is intentionally excluded from the 8 KiB cache).
"""

from __future__ import annotations

import urllib.error
import urllib.request
from dataclasses import dataclass

from .errors import PreconditionFailed, ProtocolError


@dataclass
class Response:
    body: bytes
    etag: str | None
    file_size: int | None
    status: int
    content_range: str | None
    start: int
    end: int


@dataclass
class RequestRecord:
    attempt: int
    kind: str
    start: int
    end: int
    if_match: str | None
    status: int
    body_bytes: int
    etag: str | None
    content_range: str | None

    def as_dict(self) -> dict:
        return {
            "attempt": self.attempt,
            "kind": self.kind,
            "range": [self.start, self.end],
            "if_match": self.if_match,
            "status": self.status,
            "body_bytes": self.body_bytes,
            "etag": self.etag,
            "content_range": self.content_range,
        }


def _parse_content_range(value: str | None) -> tuple[int, int, int | None]:
    if not value:
        return -1, -1, None
    # e.g. "bytes 100-199/400320"
    try:
        unit, rest = value.split(" ", 1)
        span, total = rest.split("/", 1)
        lo, hi = span.split("-", 1)
        return int(lo), int(hi), (None if total == "*" else int(total))
    except (ValueError, AttributeError):
        return -1, -1, None


class HttpRangeClient:
    """Single-channel ranged reader.  Thread-safe enough for one logical query."""

    def __init__(self, url: str, *, timeout: float = 30.0):
        self.url = url
        self.timeout = timeout
        self.requests: list[RequestRecord] = []
        self.body_bytes = 0

    def fetch(
        self,
        start: int,
        end: int,
        *,
        if_match: str | None,
        kind: str = "data",
        attempt: int = 0,
    ) -> Response:
        if start < 0 or end < start:
            raise ProtocolError(f"invalid range {start}-{end}")
        headers = {"Range": f"bytes={start}-{end}"}
        if if_match is not None:
            headers["If-Match"] = if_match
        request = urllib.request.Request(self.url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as raw:
                status = raw.status
                body = raw.read()
                etag = raw.headers.get("ETag")
                content_range = raw.headers.get("Content-Range")
                total = _parse_content_range(content_range)[2]
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = b""
            etag = exc.headers.get("ETag") if exc.headers else None
            content_range = exc.headers.get("Content-Range") if exc.headers else None
            total = None
            record = RequestRecord(
                attempt, kind, start, end, if_match, status, 0, etag, content_range
            )
            self.requests.append(record)
            if status == 412:
                raise PreconditionFailed(record) from None
            if status == 416:
                raise ProtocolError(f"server rejected range {start}-{end} (416)")
            raise ProtocolError(f"HTTP {status} for range {start}-{end}") from None

        self.body_bytes += len(body)
        self.requests.append(
            RequestRecord(
                attempt, kind, start, end, if_match, status, len(body), etag, content_range
            )
        )
        lo, hi, _ = _parse_content_range(content_range)
        expected = end - start + 1
        if status == 206 and (lo, hi) != (start, end):
            raise ProtocolError(
                f"server returned range {lo}-{hi}, expected {start}-{end}"
            )
        if status == 200 and end > 0:
            # A full-body reply is outside the ranged protocol contract and
            # would also blow the body budget; reject it instead of accepting a
            # silent full download.
            if len(body) != expected:
                raise ProtocolError(
                    "server ignored Range (returned full body); refusing to "
                    "use a second transfer channel / full download"
                )
        if len(body) != expected:
            raise ProtocolError(
                f"short body for {start}-{end}: {len(body)} != {expected}"
            )
        return Response(
            body=body,
            etag=etag,
            file_size=total,
            status=status,
            content_range=content_range,
            start=start,
            end=end,
        )
