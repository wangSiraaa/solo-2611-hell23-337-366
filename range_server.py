"""Local ranged HTTP source with version (ETag) semantics.

The server exposes exactly one object URL.  A ``Source`` holds one or more
*equal-length* versions of the same logical file.  It supports:

* ``Range`` requests, ``ETag`` per version and ``If-Match`` preconditions
  (``412`` when the bound version is stale, ``416`` for illegal ranges);
* deterministic version switching between *any* two requests, with **at most
  one switch per request boundary**:
    - ``switch_after=N`` (legacy): every request with global index ``>= N``
      is served from the next version (a single switch);
    - ``switch_when=callable(index, start, end)``: each time the predicate is
      true the object advances one version, up to ``len(files)-1`` switches.
      This lets tests switch exactly "after the descriptors were read, before
      the heap is read" (e.g. trigger on the first request whose range starts
      in the heap);
    - ``switch_at=[i0, i1, ...]``: switch before the request with those global
      indices.

Every request is logged with its range, ``If-Match``, served ETag, status and
response-body length.  ``sent`` is the sum of response-body bytes (HTTP
protocol headers excluded), matching the problem's accounting.
"""

from __future__ import annotations

import hashlib
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def _etag(data: bytes) -> str:
    return '"' + hashlib.sha256(data).hexdigest() + '"'


class Source:
    def __init__(
        self,
        files,
        switch_after=None,
        *,
        switch_when=None,
        switch_at=None,
        host="127.0.0.1",
        port=0,
    ):
        self.files = [Path(p).read_bytes() for p in files]
        if not self.files:
            raise ValueError("at least one file is required")
        lengths = {len(data) for data in self.files}
        if len(lengths) != 1:
            raise ValueError(
                f"all versions must be equal length, got {sorted(lengths)}"
            )
        self.etags = [_etag(data) for data in self.files]
        self.switch_after = switch_after
        self.switch_when = switch_when
        self.switch_at = set(switch_at) if switch_at else set()
        self._host = host
        self._port = port
        self._lock = threading.Lock()
        self._version = 0
        self._switches = 0
        self.requests = []
        self.sent = 0

    # -- state -------------------------------------------------------------
    def _maybe_switch(self, index: int, start: int, end: int) -> None:
        """Advance at most one version before serving request ``index``."""

        if self._version >= len(self.files) - 1:
            return
        advance = False
        if index in self.switch_at:
            advance = True
        elif self.switch_when is not None and self.switch_when(index, start, end):
            advance = True
        elif self.switch_after is not None and index >= self.switch_after:
            # Legacy semantics: stays on the last version afterwards.
            self._version = min(1, len(self.files) - 1)
            self._switches += 1
            return
        if advance:
            self._version += 1
            self._switches += 1

    @property
    def version(self) -> int:
        return self._version

    @property
    def switches(self) -> int:
        return self._switches

    # -- server ------------------------------------------------------------
    def start(self):
        source = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):  # noqa: D401 - silence
                pass

            def do_GET(self):
                with source._lock:
                    index = len(source.requests)
                    range_header = self.headers.get("Range")
                    start, end = 0, len(source.files[0]) - 1
                    if range_header:
                        try:
                            spec = range_header.strip()
                            if not spec.startswith("bytes="):
                                raise ValueError
                            lo_s, hi_s = spec[len("bytes=") :].split("-", 1)
                            start = int(lo_s)
                            end = int(hi_s) if hi_s else len(source.files[0]) - 1
                        except (ValueError, AttributeError):
                            self.send_error(400, "bad Range")
                            return
                    source._maybe_switch(index, start, end)
                    data = source.files[source._version]
                    etag = source.etags[source._version]
                    if_match = self.headers.get("If-Match")
                    record = {
                        "index": index,
                        "range": range_header,
                        "range_start": start,
                        "range_end": end,
                        "if_match": if_match,
                        "etag": etag,
                        "version": source._version,
                        "status": None,
                        "body_bytes": 0,
                    }
                    source.requests.append(record)

                    if if_match not in (None, etag):
                        record["status"] = 412
                        self.send_response(412)
                        self.send_header("ETag", etag)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return

                    hi = min(end, len(data) - 1)
                    if start < 0 or start > hi:
                        record["status"] = 416
                        self.send_response(416)
                        self.send_header("Content-Range", f"bytes */{len(data)}")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return

                    body = data[start : hi + 1]
                    status = 206 if range_header else 200
                    record["status"] = status
                    record["body_bytes"] = len(body)
                    source.sent += len(body)
                    self.send_response(status)
                    self.send_header("ETag", etag)
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header(
                        "Content-Range", f"bytes {start}-{hi}/{len(data)}"
                    )
                    self.end_headers()
                    self.wfile.write(body)

        self.server = ThreadingHTTPServer((self._host, self._port), Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()
        return f"http://{self._host}:{self.server.server_port}/table"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def log(self):
        return list(self.requests)
