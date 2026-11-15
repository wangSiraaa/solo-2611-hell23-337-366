"""Target database: one source unit == one SQLite transaction, and the durable
checkpoint row is written in the **same** transaction.

Consequences
------------
* ``target data for unit U`` and ``consumed through source byte N`` are one
  atomic fact.  A crash before commit leaves neither; a crash after commit
  leaves both.  Resume can therefore neither skip uncommitted input nor apply
  committed input twice.
* ``foreign_keys`` is enabled at connection creation and never toggled.  We do
  not "turn FKs off to make the copy work": a broken dependency rolls the
  whole unit back and the previous committed unit stays visible.
* DDL is executed in the same transaction as the COPY rows / INSERTs that
  depend on it, so a source transaction that creates a parent table, copies
  parent rows, creates a child table and copies child rows is atomic.
"""

import sqlite3

CP_COLUMNS = [
    "source_identity", "identity_span", "byte_offset", "unit_seq",
    "txn_count", "last_order", "boundary_start", "boundary_sha256",
    "boundary_len", "manifest_seq", "source_size_at_open", "parse_state",
    "txn_digest", "prev_txn_digest", "rows_applied",
]

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS __streamlex_meta(
           key TEXT PRIMARY KEY,
           value TEXT NOT NULL
       )""",
    """CREATE TABLE IF NOT EXISTS __streamlex_checkpoint(
           id INTEGER PRIMARY KEY CHECK(id = 1),
           source_identity TEXT NOT NULL,
           identity_span INTEGER NOT NULL,
           byte_offset INTEGER NOT NULL,
           unit_seq INTEGER NOT NULL,
           txn_count INTEGER NOT NULL,
           last_order INTEGER NOT NULL,
           boundary_start INTEGER NOT NULL,
           boundary_sha256 TEXT NOT NULL,
           boundary_len INTEGER NOT NULL,
           manifest_seq INTEGER NOT NULL,
           source_size_at_open INTEGER NOT NULL,
           parse_state TEXT NOT NULL,
           txn_digest TEXT NOT NULL,
           prev_txn_digest TEXT NOT NULL,
           rows_applied INTEGER NOT NULL
       )""",
]


class Target(object):
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.in_unit = False
        # Observability: how many source operations this target connection
        # actually executed.  Used to prove a resume does not re-execute
        # already-committed units.
        self.ops_executed = 0

    def close(self):
        self.conn.close()

    # ---- setup --------------------------------------------------------
    def ensure_schema(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for stmt in SCHEMA:
                self.conn.execute(stmt)
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    def fk_enabled(self):
        return self.conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1

    def foreign_key_violations(self):
        return self.conn.execute("PRAGMA foreign_key_check").fetchall()

    def checkpoint(self):
        row = self.conn.execute(
            "SELECT %s FROM __streamlex_checkpoint WHERE id=1" % ",".join(CP_COLUMNS)
        ).fetchone()
        if row is None:
            return None
        return dict(zip(CP_COLUMNS, row))

    def untrusted_reason(self):
        row = self.conn.execute(
            "SELECT value FROM __streamlex_meta WHERE key='audit_untrusted'"
        ).fetchone()
        return row[0] if row else None

    # ---- one source unit ---------------------------------------------
    def begin_unit(self):
        if self.in_unit:
            raise RuntimeError("target unit already open")
        self.conn.execute("BEGIN IMMEDIATE")
        self.in_unit = True

    def commit_unit(self, cp):
        try:
            # Foreign keys were ON for every statement of this unit, so this
            # check is a redundant confirmation that the *whole* target is
            # consistent at the commit point (not a substitute for per-statement
            # enforcement).  A violation rejects the unit.
            violations = self.conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise sqlite3.IntegrityError(
                    "foreign key check failed: %r" % (violations[:3],)
                )
            self._write_checkpoint(cp)
            self.conn.execute("COMMIT")
            self.in_unit = False
        except BaseException:
            self._rollback()
            raise

    def rollback_unit(self):
        self._rollback()

    def _rollback(self):
        if self.in_unit:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            self.in_unit = False

    def exec_ddl(self, verbatim_sql):
        self.conn.execute(verbatim_sql.decode("utf-8", "surrogatepass"))
        self.ops_executed += 1

    def insert_row(self, table, values):
        placeholders = ",".join("?" for _ in values)
        self.conn.execute(
            'INSERT INTO "%s" VALUES(%s)' % (table.replace('"', '""'), placeholders),
            values,
        )
        self.ops_executed += 1

    def _write_checkpoint(self, c):
        marks = ",".join("?" for _ in CP_COLUMNS)
        self.conn.execute(
            "INSERT OR REPLACE INTO __streamlex_checkpoint(id,%s) VALUES(1,%s)"
            % (",".join(CP_COLUMNS), marks),
            tuple(c[k] for k in CP_COLUMNS),
        )

    # ---- observation --------------------------------------------------
    def user_tables(self):
        rows = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name NOT LIKE '__streamlex_%' ORDER BY name"
        ).fetchall()
        return [r[0] for r in rows]

    def counts(self):
        out = {}
        for t in self.user_tables():
            out[t] = self.conn.execute(
                'SELECT COUNT(*) FROM "%s"' % t.replace('"', '""')
            ).fetchone()[0]
        return out

    def snapshot_rows(self):
        out = {}
        for t in self.user_tables():
            out[t] = self.conn.execute(
                'SELECT * FROM "%s"' % t.replace('"', '""')
            ).fetchall()
        return out

    def table_columns(self, table):
        try:
            return [
                r[1]
                for r in self.conn.execute(
                    'PRAGMA table_info("%s")' % table.replace('"', '""')
                )
            ]
        except sqlite3.OperationalError:
            return None
