"""Byte-accurate incremental lexer for the restricted SQL export stream.

Contract
--------
* Input arrives as arbitrary byte chunks.  A chunk may end in the middle of a
  UTF-8 sequence, a ``\\t``/``\\N`` COPY escape, a quoted string, a comment,
  or even between the two bytes of ``--``/``/*``.  No decision is published
  until the enclosing lexical unit is complete.
* Every emitted event ends at a byte offset that is a legal resume point:
  the lexer mode is ``SQL`` and no partial unit is buffered.  A COPY *data
  section* is one event (``copy_end``); a cut inside it is never published as
  a boundary, so the whole section is re-read on resume.
* COPY data is never interpreted as SQL.  ``COMMIT;``, ``--``, ``/*`` or a
  quote inside a copied field are bytes in a field, nothing else.
* Memory is bounded: one statement (<= the per-transaction input cap), one
  COPY line, and one read chunk.  The dump is never materialised.
"""

import re

from .errors import InputFormatError

WS = b" \t\r\n\f\v"
_WS_SET = frozenset(WS)

MODE_SQL = "SQL"
MODE_SQ = "SQ"
MODE_SQ_END = "SQ_END"
MODE_DQ = "DQ"
MODE_DQ_END = "DQ_END"
MODE_DASH = "DASH"
MODE_SLASH = "SLASH"
MODE_LC = "LC"
MODE_BC = "BC"
MODE_BC_STAR = "BC_STAR"
MODE_COPY_DATA = "COPY_DATA"

_PLAIN_SQL = re.compile(rb"[^;'\"\-/]+")
_COPY_HEADER = re.compile(
    rb"(?is)\s*COPY\s+([A-Za-z_][A-Za-z_0-9]*)\s+FROM\s+STDIN\s*$"
)

COPY_ESCAPES = {0x74: "\t", 0x6E: "\n", 0x72: "\r", 0x5C: "\\"}

# Hard caps so a malformed stream (a statement or COPY line that never ends)
# is rejected instead of growing memory without bound.  The task caps a source
# transaction at 2 MiB; a single unit of text larger than this cannot belong to
# the accepted subset.
MAX_STATEMENT_BYTES = 4 * 1024 * 1024
MAX_COPY_LINE_BYTES = 4 * 1024 * 1024


# --------------------------------------------------------------------------
# Statement helpers
# --------------------------------------------------------------------------
def strip_leading_comments(text):
    i = 0
    n = len(text)
    while True:
        while i < n and text[i] in _WS_SET:
            i += 1
        if text.startswith(b"--", i):
            k = text.find(b"\n", i)
            i = n if k < 0 else k + 1
            continue
        if text.startswith(b"/*", i):
            k = text.find(b"*/", i + 2)
            if k < 0:
                raise InputFormatError("unterminated block comment")
            i = k + 2
            continue
        return text[i:], i


def _strip_comments_upper(text):
    """Comment-free, whitespace-collapsed, uppercase token stream (bytes).

    Quote-aware: comment markers inside a quoted string are literal text, so
    ``INSERT ... VALUES ('a--b')`` classifies as an INSERT, not as a comment.
    """
    out = bytearray()
    i = 0
    n = len(text)
    last_space = True
    while i < n:
        if text.startswith(b"--", i):
            k = text.find(b"\n", i)
            i = n if k < 0 else k + 1
            if not last_space:
                out.append(0x20)
                last_space = True
            continue
        if text.startswith(b"/*", i):
            k = text.find(b"*/", i + 2)
            if k < 0:
                raise InputFormatError("unterminated block comment")
            i = k + 2
            if not last_space:
                out.append(0x20)
                last_space = True
            continue
        c = text[i]
        if c in (0x27, 0x22):  # ' or "
            q = c
            out.append(c)
            i += 1
            while i < n:
                out.append(text[i])
                if text[i] == q:
                    if i + 1 < n and text[i + 1] == q:
                        out.append(text[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            last_space = False
            continue
        if c in _WS_SET:
            if not last_space:
                out.append(0x20)
                last_space = True
        else:
            out.append(c)
            last_space = False
        i += 1
    return bytes(out).strip().upper()


def classify_sql(text):
    """Classify a complete statement body (may have leading comments)."""
    body, _off = strip_leading_comments(text)
    if not body.strip():
        raise InputFormatError("empty statement")
    toks = _strip_comments_upper(body)
    if toks in (
        b"BEGIN",
        b"BEGIN TRANSACTION",
        b"BEGIN DEFERRED TRANSACTION",
        b"BEGIN IMMEDIATE TRANSACTION",
        b"BEGIN EXCLUSIVE TRANSACTION",
    ):
        return {"kind": "begin"}
    if toks in (b"COMMIT", b"COMMIT TRANSACTION", b"END", b"END TRANSACTION"):
        return {"kind": "commit"}
    head = toks.split(b" ")
    if head[:2] == [b"CREATE", b"TABLE"]:
        return {"kind": "create_table", "sql": body}
    if head[:2] == [b"CREATE", b"INDEX"] or head[:3] == [b"CREATE", b"UNIQUE", b"INDEX"]:
        return {"kind": "create_index", "sql": body}
    if head and head[0] == b"COPY":
        m = _COPY_HEADER.match(body)
        if not m:
            raise InputFormatError("COPY header outside declared subset: %r" % body[:80])
        return {"kind": "copy", "table": m.group(1).decode("ascii")}
    m = re.match(rb"(?is)^\s*INSERT\s+INTO\s+([A-Za-z_][A-Za-z_0-9]*)\s+VALUES\s*\(", body)
    if m:
        values, end = _parse_insert_values(body, m.end() - 1)
        if body[end + 1 :].strip():
            raise InputFormatError("trailing tokens after INSERT ... VALUES")
        return {
            "kind": "insert",
            "table": m.group(1).decode("ascii"),
            "values": values,
        }
    first = toks.split(b" ")[0].decode("ascii", "replace") if toks else "?"
    raise InputFormatError(
        "unsupported statement outside declared subset (%s): %r"
        % (first, body[:80])
    )


def _parse_insert_values(body, pos):
    n = len(body)
    depth = 0
    i = pos
    end = None
    while i < n:
        c = body[i : i + 1]
        if c in (b"'", b'"'):
            q = c
            i += 1
            while i < n:
                if body[i : i + 1] == q:
                    if body[i + 1 : i + 2] == q:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if c == b"(":
            depth += 1
        elif c == b")":
            depth -= 1
            if depth == 0:
                end = i
                break
        i += 1
    if end is None:
        raise InputFormatError("unbalanced INSERT ... VALUES")
    return _parse_literals(body[pos + 1 : end]), end


def _parse_literals(chunk):
    text = chunk.decode("utf-8", "surrogatepass")
    n = len(text)
    i = 0
    out = []

    def skip():
        nonlocal i
        while i < n and text[i] in " \t\r\n\f\v":
            i += 1

    skip()
    if i >= n:
        raise InputFormatError("INSERT ... VALUES () has no values")
    while True:
        ch = text[i]
        if ch in "'\"":
            q = ch
            i += 1
            buf = []
            while True:
                if i >= n:
                    raise InputFormatError("unterminated string in INSERT")
                if text[i] == q:
                    if i + 1 < n and text[i + 1] == q:
                        buf.append(q)
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    buf.append(text[i])
                    i += 1
            out.append("".join(buf))
        elif ch == "$":
            j = i + 1
            while j < n and text[j].isdigit():
                j += 1
            if j == i + 1:
                raise InputFormatError("bad $N literal in INSERT")
            out.append(int(text[i + 1 : j]))
            i = j
        elif ch in "-+.0123456789":
            m = re.match(r"[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?", text[i:])
            if not m:
                raise InputFormatError("bad numeric literal in INSERT")
            lit = m.group(0)
            out.append(float(lit) if any(c in lit for c in ".eE") else int(lit))
            i += len(lit)
        elif text[i : i + 4].upper() == "NULL" and (
            i + 4 >= n or not (text[i + 4].isalnum() or text[i + 4] == "_")
        ):
            out.append(None)
            i += 4
        else:
            raise InputFormatError("unsupported value token in INSERT: %r" % text[i : i + 12])
        skip()
        if i >= n:
            break
        if text[i] == ",":
            i += 1
            skip()
            if i >= n:
                raise InputFormatError("dangling comma in INSERT")
            continue
        raise InputFormatError("expected ',' between INSERT values")
    return out


def parse_copy_row(rawline, row_no):
    """Parse one raw COPY line (without its newline).  ``\\N`` is the only
    NULL spelling; inside a non-NULL field only \\t \\n \\r \\\\ are valid."""
    raw_fields = bytes(rawline).split(b"\t")
    out = []
    for raw in raw_fields:
        if raw == b"\\N":
            out.append(None)
            continue
        buf = bytearray()
        i = 0
        m = len(raw)
        while i < m:
            b = raw[i]
            if b == 0x5C:
                if i + 1 >= m:
                    raise InputFormatError("COPY row %d: trailing backslash" % row_no)
                e = raw[i + 1]
                if e not in COPY_ESCAPES:
                    raise InputFormatError(
                        "COPY row %d: unsupported escape \\%s"
                        % (row_no, bytes((e,)).decode("latin-1"))
                    )
                buf += COPY_ESCAPES[e].encode("utf-8")
                i += 2
                continue
            buf.append(b)
            i += 1
        try:
            out.append(bytes(buf).decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise InputFormatError("COPY row %d: field is not valid UTF-8: %s" % (row_no, exc))
    return out


# --------------------------------------------------------------------------
# Events
# --------------------------------------------------------------------------
class Event(object):
    __slots__ = (
        "kind", "start", "end", "order", "table", "values", "verbatim",
        "stmt", "txn", "row_no", "verbatim_has_newline",
    )

    def __init__(self, kind, start, end):
        self.kind = kind          # sql | copy_begin | copy_row | copy_end
        self.start = start
        self.end = end
        self.order = None
        self.table = None
        self.values = None
        self.verbatim = b""       # exact source bytes identifying this event
        self.stmt = None
        self.txn = 0
        self.row_no = 0
        self.verbatim_has_newline = False


# --------------------------------------------------------------------------
# Lexer
# --------------------------------------------------------------------------
class StreamLexer(object):
    def __init__(self, store, start_offset=0, chunk_size=65536, on_control=None):
        self.store = store
        self.chunk = chunk_size
        self.on_control = on_control
        self.buf = bytearray()
        self.ts = start_offset      # absolute offset of buf[0]
        self.pos = 0
        self.eof = False

        self.mode = MODE_SQL
        self.stmt = bytearray()
        self.stmt_start = None
        self.in_stmt = False

        self.txn = 0
        self.order = 0
        self.txn_start = 0

        self.copy_start = None
        self.copytab = None
        self.copyrows = 0
        self.linebuf = bytearray()
        self.line_abs = start_offset
        self._copy_skip_cmd_line = False

        self.last_end = start_offset

    # ---- input --------------------------------------------------------
    def _refill(self):
        if self.pos < len(self.buf):
            return True
        if self.eof:
            return False
        self.ts += len(self.buf)
        data = self.store.read(self.ts, self.chunk)
        self.buf = bytearray(data)
        self.pos = 0
        if not data:
            self.eof = True
            return False
        return True

    # ---- control points (for observation / crash injection) -----------
    def _control(self, name):
        if self.on_control is not None:
            self.on_control(name)

    # ---- main entry ---------------------------------------------------
    def next_event(self):
        while True:
            if self.mode == MODE_COPY_DATA:
                ev = self._copy_step()
                if ev is not None:
                    return ev
                continue
            if not self._refill():
                # A line comment is a complete unit at EOF; trailing trivia
                # with no statement is not an error.  Anything else is a cut.
                if self.mode == MODE_LC and not self._is_only_trivia():
                    raise InputFormatError(
                        "truncated statement at EOF (started at byte %s)" % self.stmt_start
                    )
                if self.in_stmt and not self._is_only_trivia():
                    raise InputFormatError(
                        "truncated statement at EOF (started at byte %s)" % self.stmt_start
                    )
                if self.mode in (MODE_SQ, MODE_SQ_END, MODE_DQ, MODE_DQ_END,
                                 MODE_BC, MODE_BC_STAR, MODE_DASH, MODE_SLASH):
                    raise InputFormatError(
                        "truncated construct at EOF (statement started at byte %s)"
                        % self.stmt_start
                    )
                return None
            ev = self._sql_step()
            if ev is not None:
                return ev

    def _is_only_trivia(self):
        """True when the buffered statement is only whitespace/comments."""
        if not self.in_stmt:
            return True
        try:
            body, _ = strip_leading_comments(bytes(self.stmt))
        except InputFormatError:
            return False
        return not body.strip()

    # ---- SQL statement state machine ----------------------------------
    def _sql_step(self):
        b = self.buf
        n = len(b)
        i = self.pos
        while i < n:
            mode = self.mode

            if mode == MODE_SQL:
                if not self.in_stmt:
                    while i < n and b[i] in _WS_SET:
                        i += 1
                    if i >= n:
                        self.pos = i
                        return None
                    self.in_stmt = True
                    self.stmt_start = self.ts + i
                    self.stmt = bytearray()
                m = _PLAIN_SQL.match(b, i)
                if m:
                    self.stmt += m.group(0)
                    i = m.end()
                    self._check_stmt_bound()
                    if i >= n:
                        self.pos = i
                        return None
                ch = b[i]
                if ch == 0x3B:  # ';'
                    i += 1
                    self.pos = i
                    return self._finish_sql()
                self.stmt.append(ch)
                i += 1
                self._check_stmt_bound()
                if ch == 0x27:
                    self.mode = MODE_SQ
                elif ch == 0x22:
                    self.mode = MODE_DQ
                elif ch == 0x2D:
                    self.mode = MODE_DASH
                elif ch == 0x2F:
                    self.mode = MODE_SLASH
                self.pos = i
                continue

            # --- continuation modes consume exactly one byte each ---
            if i >= n:
                self.pos = i
                return None
            ch = b[i]

            if mode in (MODE_SQ, MODE_DQ):
                self.stmt.append(ch)
                i += 1
                self.pos = i
                q = 0x27 if mode == MODE_SQ else 0x22
                if ch == q:
                    self.mode = MODE_SQ_END if mode == MODE_SQ else MODE_DQ_END
                continue

            if mode in (MODE_SQ_END, MODE_DQ_END):
                q = 0x27 if mode == MODE_SQ_END else 0x22
                if ch == q:  # a doubled quote: consume it, stay in string
                    self.stmt.append(ch)
                    i += 1
                    self.pos = i
                    self.mode = MODE_SQ if mode == MODE_SQ_END else MODE_DQ
                else:
                    # the previous quote was the terminator; reprocess this byte
                    self.mode = MODE_SQL
                continue

            if mode == MODE_DASH:
                if ch == 0x2D:
                    self.stmt.append(ch)
                    i += 1
                    self.pos = i
                    self.mode = MODE_LC
                    continue
                # a lone '-': reprocess this byte in SQL
                self.mode = MODE_SQL
                continue

            if mode == MODE_SLASH:
                if ch == 0x2A:
                    self.stmt.append(ch)
                    i += 1
                    self.pos = i
                    self.mode = MODE_BC
                    continue
                self.mode = MODE_SQL
                continue

            if mode == MODE_LC:
                self.stmt.append(ch)
                i += 1
                self.pos = i
                if ch == 0x0A:
                    self.mode = MODE_SQL
                continue

            if mode == MODE_BC:
                self.stmt.append(ch)
                i += 1
                self.pos = i
                if ch == 0x2A:
                    self.mode = MODE_BC_STAR
                continue

            if mode == MODE_BC_STAR:
                self.stmt.append(ch)
                i += 1
                self.pos = i
                if ch == 0x2F:
                    self.mode = MODE_SQL
                elif ch != 0x2A:
                    self.mode = MODE_BC
                continue

            raise InputFormatError("internal lexer mode %r" % mode)

        self.pos = i
        return None

    def _finish_sql(self):
        end = self.ts + self.pos
        raw = bytes(self.stmt)
        start = self.stmt_start
        self.in_stmt = False
        self.mode = MODE_SQL
        self.stmt = bytearray()

        info = classify_sql(raw)
        kind = info["kind"]

        if kind == "copy":
            ev = Event("copy_begin", start, end)
            ev.order = self._next_order()
            ev.table = info["table"]
            ev.verbatim = raw
            ev.txn = self.txn
            self.copy_start = start
            self.copytab = info["table"]
            self.copyrows = 0
            self.linebuf = bytearray()
            self.line_abs = end
            self.mode = MODE_COPY_DATA
            # The COPY command's own line terminator is not data: skip through
            # the next newline before the first data row.
            self._copy_skip_cmd_line = True
            self.last_end = end
            self._control("copy_begin")
            return ev

        if kind in ("begin", "commit"):
            if kind == "begin":
                if self.txn:
                    raise InputFormatError("nested BEGIN at byte %d" % start)
                self.txn = 1
                self.txn_start = start
            else:
                if not self.txn:
                    raise InputFormatError("COMMIT outside a source transaction at byte %d" % start)
                self.txn = 0
            ev = Event("sql", start, end)
            ev.order = self._next_order()
            ev.stmt = {"kind": kind}
            ev.verbatim = raw
            ev.txn = self.txn
            self.last_end = end
            return ev

        if kind == "insert" and not self.txn:
            raise InputFormatError("DML outside a source transaction at byte %d" % start)

        ev = Event("sql", start, end)
        ev.order = self._next_order()
        ev.stmt = info
        ev.verbatim = raw
        ev.txn = self.txn
        self.last_end = end
        return ev

    # ---- COPY data ----------------------------------------------------
    def _copy_step(self):
        if self._copy_skip_cmd_line:
            # Consume the rest of the COPY command's line (the terminator after
            # ';'), which is not COPY data.
            while True:
                if not self._refill():
                    raise InputFormatError(
                        "truncated COPY %s header line" % self.copytab
                    )
                b = self.buf
                k = b.find(b"\n", self.pos)
                if k < 0:
                    self.pos = len(b)
                    continue
                self.pos = k + 1
                self.line_abs = self.ts + self.pos
                self._copy_skip_cmd_line = False
                break
        while True:
            if not self._refill():
                raise InputFormatError(
                    "truncated COPY %s: missing \\\\. terminator before EOF" % self.copytab
                )
            b = self.buf
            n = len(b)
            i = self.pos
            k = b.find(b"\n", i)
            if k < 0:
                self.linebuf += b[i:]
                if len(self.linebuf) > MAX_COPY_LINE_BYTES:
                    raise InputFormatError(
                        "COPY line exceeds the %d-byte bound" % MAX_COPY_LINE_BYTES
                    )
                self.pos = n
                continue
            rawline = bytes(self.linebuf) + bytes(b[i:k])
            self.linebuf = bytearray()
            i = k + 1
            self.pos = i
            end = self.ts + i
            line = rawline[:-1] if rawline.endswith(b"\r") else rawline
            if line == b"\\.":
                ev = Event("copy_end", self.copy_start, end)
                ev.order = self._next_order()
                ev.table = self.copytab
                ev.verbatim = rawline + b"\n" if not rawline.endswith(b"\n") else rawline
                ev.txn = self.txn
                ev.row_no = self.copyrows
                self.mode = MODE_SQL
                self.copy_start = None
                self.copytab = None
                self.linebuf = bytearray()
                self.last_end = end
                self._control("copy_end")
                return ev
            self.copyrows += 1
            try:
                values = parse_copy_row(line, self.copyrows)
            except InputFormatError:
                raise
            ev = Event("copy_row", self.line_abs, end)
            ev.table = self.copytab
            ev.values = values
            ev.verbatim = rawline + b"\n"
            ev.txn = self.txn
            ev.row_no = self.copyrows
            self.line_abs = end
            self.last_end = end
            self._control("copy_row")
            return ev

    def _next_order(self):
        self.order += 1
        return self.order

    def _check_stmt_bound(self):
        if len(self.stmt) > MAX_STATEMENT_BYTES:
            raise InputFormatError(
                "statement starting at byte %s exceeds the %d-byte bound"
                % (self.stmt_start, MAX_STATEMENT_BYTES)
            )

    # ---- resume checks ------------------------------------------------
    def is_safe_boundary(self):
        return self.mode == MODE_SQL and not self.in_stmt and not self.linebuf

