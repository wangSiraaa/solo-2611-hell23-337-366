"""Recovery engine.

Unit model
----------
A *unit* is the atomic thing the source asks us to apply:

* an explicit ``BEGIN ... COMMIT`` block (one source transaction), or
* a standalone ``CREATE TABLE`` / ``CREATE INDEX`` (pg_dump emits these
  outside any transaction), applied as its own unit.

Each unit is applied to the target in one SQLite transaction, and the durable
checkpoint row is written **inside that same transaction**.  Therefore:

* target data and "source consumed through byte N" commit atomically;
* if the target transaction does not commit, the checkpoint does not move and
  resume re-reads that input (it may never skip uncommitted input);
* if the target commits but the caller never sees a confirmation, the
  checkpoint row is itself the proof of the commit: on restart we read it,
  re-verify a bounded boundary block of the source against its digest, and
  resume *after* the unit.  It is never executed twice.

Resume budget
-------------
Resume reads at most the 4 KiB identity span plus the boundary block
(``<= 56 KiB``) of already-committed source, and then only uncommitted input
from ``byte_offset`` onward.  Un-re-read historical prefix bytes are *not*
reported as re-verified; only ``audit-source`` makes that claim.
"""

import hashlib
import sqlite3

from . import store as store_mod
from .errors import (
    AuditFailed,
    InputFormatError,
    SourceChanged,
    TxnRejected,
)
from .lexer import StreamLexer
from .target import Target

MAX_UNIT_BYTES = 4 * 1024 * 1024


class Unit(object):
    __slots__ = (
        "seq", "txn_id", "start", "end", "ops", "bytes_buffered",
        "digest_parts", "row_count", "explicit", "boundary_start",
        "boundary_sha", "boundary_len", "global_order",
    )

    def __init__(self, txn_id, start, explicit):
        self.seq = 0
        self.txn_id = txn_id
        self.start = start
        self.end = None
        self.ops = []
        self.bytes_buffered = 0
        self.digest_parts = []
        self.row_count = 0
        self.explicit = explicit
        self.boundary_start = start
        self.boundary_sha = None
        self.boundary_len = 0
        self.global_order = 0

    def add_bytes(self, payload):
        self.bytes_buffered += len(payload)
        if self.bytes_buffered > MAX_UNIT_BYTES:
            raise InputFormatError(
                "source unit exceeds the %d-byte buffering bound" % MAX_UNIT_BYTES
            )

    def digest(self):
        h = hashlib.sha256()
        for part in self.digest_parts:
            h.update(part)
        return h.hexdigest()


class Engine(object):
    def __init__(self, source_path, target_path, manifest_path=None,
                 on_control=None, store=None, chunk_size=65536):
        self.source_path = source_path
        self.target_path = target_path
        self.manifest_path = manifest_path
        self.on_control = on_control
        self.chunk_size = chunk_size

        self.store = store if store is not None else store_mod.FileStore(source_path)
        self.manifest = store_mod.Manifest.load_or_create(manifest_path)
        self.target = Target(target_path)
        self.lexer = None

        self.identity = None
        self.identity_span = 0
        self.resume_from = None
        self.resume_reads = 0
        self.resume_read_calls = 0
        self.audit_reads = 0
        self.audit_read_calls = 0

    # ---- lifecycle ----------------------------------------------------
    def close(self):
        try:
            self.target.close()
        finally:
            self.store.close()

    def _control(self, name, **kw):
        if self.on_control is not None:
            return self.on_control(name, self, **kw)
        return None

    # ---- invariants ---------------------------------------------------
    def _pin_size(self):
        ino, dev, size = self.store.stat()
        if ino != self.store.ino or dev != self.store.dev:
            raise SourceChanged("source was replaced, not appended to")
        if size < self.store.size_at_open:
            raise SourceChanged("source shrank under the reader")
        return size

    def _bind_identity(self, count_as_resume, span=None):
        # ``span`` is fixed the first time the manifest is bound and recorded
        # in the manifest and checkpoint.  Reuse it afterwards: for a source
        # shorter than IDENTITY_SPAN the span is the size at that moment, and
        # it must not change merely because the source was later appended to.
        if span is None:
            if self.manifest.identity_span:
                span = self.manifest.identity_span
            else:
                span = min(store_mod.IDENTITY_SPAN, self.store.size_at_open)
        if span > self.store.size_at_open:
            raise SourceChanged(
                "source is shorter than its identity span (%d > %d)"
                % (span, self.store.size_at_open)
            )
        data = self.store.read(0, span)
        if count_as_resume:
            self.resume_reads += len(data)
            self.resume_read_calls += 1
        h = hashlib.sha256()
        h.update(store_mod.struct.pack("<Q", span))
        h.update(data)
        self.identity = "sha256:" + h.hexdigest()
        self.identity_span = span
        self.manifest.bind_identity(self.identity, span)
        return self.identity, span

    # ---- entry --------------------------------------------------------
    def run(self):
        self.target.ensure_schema()

        reason = self.target.untrusted_reason()
        if reason is not None:
            raise AuditFailed(
                "target marked untrusted by audit; refusing to resume (%s)" % reason
            )

        cp = self.target.checkpoint()
        self.store.size_at_open = self.store.stat()[2]
        # Captured before this run advances it: bytes below this offset were
        # already seen by an earlier run, so re-reading them on resume is the
        # "uncommitted input replay" whose size the budget bounds.
        prev_read_hwm = self.manifest.read_hwm

        if cp is None:
            # Nothing has been committed, so no durable fact depends on a
            # previously bound identity.  If the first attempt saw an empty
            # file, re-fix the identity now that bytes exist.
            if not self.manifest.chunks:
                self.manifest.reset_identity()
            self._bind_identity(count_as_resume=False)
            self._pin_size()
            start_offset = 0
            ctx = dict(
                prev_digest="",
                last_order=0,
                unit_seq=0,
                txn_count=0,
                rows_applied=0,
            )
            self.resume_from = None
            # Persist identity/format, but do not claim any bytes are trusted
            # before a checkpoint exists (seal=False; the pending read-ahead
            # stays pending).
            self.manifest.save(seal=False)
        else:
            self._pin_size()
            # The size pinned when the checkpoint was written is the lower
            # bound: the source may only have grown since.
            if self.store.size_at_open < cp["source_size_at_open"]:
                raise SourceChanged(
                    "source shrank below the checkpoint's pinned size (%d < %d)"
                    % (self.store.size_at_open, cp["source_size_at_open"])
                )
            self._bind_identity(count_as_resume=True, span=cp["identity_span"])
            if cp["source_identity"] != self.identity:
                raise SourceChanged(
                    "checkpoint identity %s != source identity %s"
                    % (cp["source_identity"], self.identity)
                )
            start_offset = self._verify_resume(cp)
            ctx = dict(
                prev_digest=cp["txn_digest"],
                last_order=cp["last_order"],
                unit_seq=cp["unit_seq"],
                txn_count=cp["txn_count"],
                rows_applied=cp["rows_applied"],
            )
            self.resume_from = start_offset
            self.manifest.save(seal=False)

        lexer = StreamLexer(
            store_mod.TrackedStore(
                self.store,
                self.manifest,
                # Bytes below the previous run's read high-water were already
                # read but not committed at that checkpoint: re-reading them is
                # the replay the resume budget explicitly allows.
                replay_until=prev_read_hwm,
            ),
            start_offset=start_offset,
            chunk_size=self.chunk_size,
            on_control=lambda name: self._control(name),
        )
        self.lexer = lexer
        result = self._drive(lexer, start_offset, ctx)
        # ``replay_until`` / ``prev_read_hwm`` describe bytes that an earlier
        # run had already read but had not committed at its last checkpoint.
        result["replay_bytes_read"] = lexer.store.replayed_bytes
        return result

    def _verify_resume(self, cp):
        start = cp["boundary_start"]
        end = cp["byte_offset"]
        if end - start > store_mod.BOUNDARY_MAX:
            raise SourceChanged("checkpoint boundary block exceeds the resume bound")
        if end > self.manifest.trusted_len:
            raise SourceChanged(
                "checkpoint offset %d beyond trusted manifest length %d"
                % (end, self.manifest.trusted_len)
            )
        if cp["manifest_seq"] > self.manifest.seq:
            raise SourceChanged(
                "checkpoint cites manifest seq %d but only %d entries exist"
                % (cp["manifest_seq"], self.manifest.seq)
            )
        n = self.manifest.verify_boundary_block(
            self.store, start, end, cp["boundary_sha256"]
        )
        self.resume_reads += n
        self.resume_read_calls += 1
        # Bytes after ``end`` may be covered by the manifest if a previous run
        # made the manifest durable and then crashed before the target commit
        # (the manifest is intentionally ahead of the checkpoint).  They were
        # never committed, so they are NOT trusted: resume re-reads and
        # re-executes them from ``end``.  That uncommitted tail is at most one
        # source transaction plus one read chunk, which is exactly the allowed
        # resume budget; no historical committed prefix is re-read.
        return end

    # ---- drive --------------------------------------------------------
    def _drive(self, lexer, start_offset, ctx):
        cur = None
        order_base = ctx["last_order"]
        prev_digest = ctx["prev_digest"]
        unit_seq = ctx["unit_seq"]
        txn_count = ctx["txn_count"]
        rows_applied = ctx["rows_applied"]
        stats = {
            "units_committed": 0,
            "txns_committed": 0,
            "txns_seen": 0,
            "ddl_applied": 0,
            "rows_applied": 0,
            "statements": 0,
        }

        def close_unit(unit, last_stmt_bytes, global_order):
            nonlocal prev_digest, unit_seq, txn_count, rows_applied
            unit_seq += 1
            unit.seq = unit_seq
            if unit.explicit:
                txn_count += 1
            n = min(len(last_stmt_bytes), store_mod.BOUNDARY_MAX)
            unit.boundary_start = unit.end - n
            unit.boundary_len = n
            unit.boundary_sha = hashlib.sha256(
                last_stmt_bytes[len(last_stmt_bytes) - n :]
            ).hexdigest()
            unit.global_order = global_order
            self._apply_unit(
                unit,
                prev_digest=prev_digest,
                txns_applied=txn_count,
                rows_applied=rows_applied + unit.row_count,
            )
            rows_applied += unit.row_count
            prev_digest = unit.digest()
            stats["units_committed"] += 1
            stats["ddl_applied"] += sum(1 for op in unit.ops if op[0] == "ddl")
            stats["rows_applied"] += unit.row_count
            if unit.explicit:
                stats["txns_committed"] += 1

        try:
            while True:
                ev = lexer.next_event()
                if ev is None:
                    break
                stats["statements"] += 1
                kind = ev.kind

                if kind == "copy_row":
                    if cur is None:
                        raise InputFormatError(
                            "COPY row outside a BEGIN/COMMIT unit at byte %d" % ev.start
                        )
                    cur.ops.append(("row", ev.table, ev.values))
                    cur.row_count += 1
                    cur.add_bytes(ev.verbatim)
                    cur.digest_parts.append(b"R" + ev.verbatim)
                    continue

                if kind == "copy_begin":
                    if cur is None:
                        raise InputFormatError(
                            "COPY outside a BEGIN/COMMIT unit at byte %d" % ev.start
                        )
                    cur.add_bytes(ev.verbatim)
                    cur.digest_parts.append(b"B" + ev.verbatim)
                    continue

                if kind == "copy_end":
                    cur.digest_parts.append(b"E")
                    self._control("after_copy_end", unit=cur, table=ev.table)
                    continue

                skind = ev.stmt["kind"]
                if skind == "begin":
                    if cur is not None:
                        raise InputFormatError("nested BEGIN at byte %d" % ev.start)
                    stats["txns_seen"] += 1
                    cur = Unit(stats["txns_seen"], ev.start, True)
                    cur.digest_parts.append(b"BEGIN")
                    continue

                if skind == "commit":
                    if cur is None:
                        raise InputFormatError(
                            "COMMIT without an open source transaction at byte %d"
                            % ev.start
                        )
                    cur.digest_parts.append(b"COMMIT")
                    full = ev.verbatim + b";"
                    cur.end = ev.end
                    self._control(
                        "after_source_commit_before_target_commit", unit=cur
                    )
                    close_unit(cur, full, ev.order + order_base)
                    self._control("after_target_commit", unit=cur)
                    cur = None
                    continue

                if skind in ("create_table", "create_index"):
                    sql = ev.stmt["sql"]
                    if cur is not None:
                        cur.ops.append(("ddl", sql))
                        cur.add_bytes(sql)
                        cur.digest_parts.append(b"D" + sql)
                    else:
                        unit = Unit(0, ev.start, False)
                        unit.ops.append(("ddl", sql))
                        unit.add_bytes(sql)
                        unit.digest_parts.append(b"D" + sql)
                        unit.end = ev.end
                        close_unit(unit, ev.verbatim + b";", ev.order + order_base)
                        self._control("after_target_commit", unit=unit)
                    continue

                if skind == "insert":
                    if cur is None:
                        raise InputFormatError(
                            "DML outside a BEGIN/COMMIT unit at byte %d" % ev.start
                        )
                    cur.ops.append(("insert", ev.stmt["table"], ev.stmt["values"]))
                    cur.row_count += 1
                    cur.add_bytes(ev.verbatim)
                    cur.digest_parts.append(b"I" + ev.verbatim)
                    continue

                raise InputFormatError("unhandled statement kind %r" % skind)

        except BaseException as exc:
            if cur is not None:
                self._control("txn_discarded", unit=cur, error=exc)
            raise

        if cur is not None:
            raise InputFormatError(
                "truncated source transaction: BEGIN at byte %d has no COMMIT"
                % cur.start
            )

        # Do not seal the read-ahead tail: only bytes covered by a committed
        # checkpoint are trusted.  Bytes after the last checkpoint are re-read
        # (and re-verified) on any resume.
        self.manifest.save(seal=False)

        return {
            "checkpoint": self.target.checkpoint(),
            "stats": stats,
            "resume_from": self.resume_from,
            "resume_bytes_read": self.resume_reads,
            "resume_read_calls": self.resume_read_calls,
            "replay_bytes_read": lexer.store.replayed_bytes,
            "source_bytes_read": self.store.bytes_read,
            "source_read_calls": self.store.read_calls,
        }

    # ---- apply one unit ----------------------------------------------
    def _apply_unit(self, unit, prev_digest, txns_applied, rows_applied):
        # Publish exactly the bytes the lexer has consumed through this unit's
        # end.  Not the read-ahead: the manifest's "trusted" claim must be
        # bounded to bytes that participated in a checkpoint.
        self.manifest.seal_upto(unit.end)
        # Record the read high-water mark so a later resume can report how much
        # already-seen-but-uncommitted input it had to replay.
        self.manifest.read_hwm = max(
            self.manifest.read_hwm, self.manifest.fed_end()
        )
        self.manifest.save(seal=False)

        self.target.begin_unit()
        try:
            for op in unit.ops:
                if op[0] == "ddl":
                    self.target.exec_ddl(op[1])
                elif op[0] in ("row", "insert"):
                    self.target.insert_row(op[1], op[2])
                else:
                    raise InputFormatError("bad unit op %r" % (op[0],))
            cp = {
                "source_identity": self.identity,
                "identity_span": self.identity_span,
                "byte_offset": unit.end,
                "unit_seq": unit.seq,
                "txn_count": txns_applied,
                "last_order": unit.global_order,
                "boundary_start": unit.boundary_start,
                "boundary_sha256": unit.boundary_sha,
                "boundary_len": unit.boundary_len,
                "manifest_seq": self.manifest.seq,
                "source_size_at_open": self.store.size_at_open,
                # The only lexer state needed to resume is "not inside a
                # statement, not inside a source transaction".  Every unit
                # boundary satisfies it (an explicit unit has just seen COMMIT;
                # standalone DDL is outside any transaction).  It is recorded
                # so an incoherent checkpoint can be detected, not guessed.
                "parse_state": "SQL;txn=0;stmt=none",
                "txn_digest": unit.digest(),
                "prev_txn_digest": prev_digest,
                "rows_applied": rows_applied,
            }
            self._control("before_target_commit", unit=unit)
            self.target.commit_unit(cp)
        except Exception as exc:
            self.target.rollback_unit()
            if isinstance(exc, sqlite3.Error):
                raise TxnRejected(
                    "target rejected source unit %d (%s); last committed source "
                    "unit is preserved" % (unit.seq, exc)
                ) from exc
            raise
        self._control("after_checkpoint", unit=unit)

    # ---- audit --------------------------------------------------------
    def audit(self):
        """Full, separately-accounted re-verification of the source prefix.

        On any mismatch — including an identity change within the audited
        prefix — the target is marked untrusted and further resumes are refused.
        """
        self.target.ensure_schema()
        cp = self.target.checkpoint()
        before = self.store.bytes_read
        before_calls = self.store.read_calls
        try:
            span = self.manifest.identity_span
            data = self.store.read(0, span) if span else b""
            h = hashlib.sha256()
            h.update(store_mod.struct.pack("<Q", span))
            h.update(data)
            ident = "sha256:" + h.hexdigest()
            if self.manifest.identity is not None and ident != self.manifest.identity:
                raise AuditFailed(
                    "source identity span (first %d bytes) does not match the "
                    "manifest" % span
                )
            self.manifest.verify_authored_prefix(self.store)
        except (AuditFailed, SourceChanged) as exc:
            self.audit_reads = self.store.bytes_read - before
            self.audit_read_calls = self.store.read_calls - before_calls
            self._mark_untrusted(str(exc))
            raise AuditFailed(str(exc)) from exc
        self.audit_reads = self.store.bytes_read - before
        self.audit_read_calls = self.store.read_calls - before_calls
        if cp is not None:
            self.manifest.verify_boundary_block(
                self.store, cp["boundary_start"], cp["byte_offset"],
                cp["boundary_sha256"],
            )
        return {
            "bytes_read": self.audit_reads,
            "read_calls": self.audit_read_calls,
            "chunks": len(self.manifest.chunks),
            "trusted_len": self.manifest.trusted_len,
            "checkpoint": cp,
        }

    def _mark_untrusted(self, reason):
        conn = self.target.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT OR REPLACE INTO __streamlex_meta(key,value) VALUES(?,?)",
                ("audit_untrusted", reason),
            )
            conn.execute("DELETE FROM __streamlex_checkpoint WHERE id=1")
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
