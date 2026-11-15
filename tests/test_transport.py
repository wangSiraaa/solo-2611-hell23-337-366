"""Transport-level guarantees: single ranged channel, no full downloads."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from fitsproj.engine import ProjectionEngine, RowSpec
from fitsproj.errors import DescriptorBoundsError, ProtocolError
from fitsproj.generate import write_table


class _FullBodyServer:
    """Deliberately ignores Range and returns the whole file (200)."""

    def __init__(self, path):
        data = Path(path).read_bytes()
        self.requests = 0

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def do_GET(self):
                self.server._owner.requests += 1
                self.send_response(200)
                self.send_header("ETag", '"full"')
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server._owner = self
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self):
        self.thread.start()
        return f"http://127.0.0.1:{self.server.server_port}/t"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def test_full_body_response_is_rejected(tmp_path):
    spec = write_table(tmp_path / "t.fits", [8] * 10)
    server = _FullBodyServer(spec["path"])
    url = server.start()
    try:
        with pytest.raises(ProtocolError):
            ProjectionEngine(url).query([RowSpec(0, ((0, 8),))])
        # Stopped at the very first request instead of accepting a full download.
        assert server.requests == 1
    finally:
        server.close()


def test_deterministic_failure_reports_empty_result_rows(tmp_path):
    spec = write_table(tmp_path / "t.fits", [8] * 10)
    from range_server import Source

    source = Source([spec["path"]])
    url = source.start()
    try:
        with pytest.raises(DescriptorBoundsError) as info:
            ProjectionEngine(url).query([RowSpec(2, ((0, 9),))])
        exc = info.value
        assert exc.rows == []
        assert exc.response_body_bytes == source.sent
        assert all(q["kind"] != "heap" for q in exc.trace)
    finally:
        source.close()
