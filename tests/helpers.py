"""Shared test helpers.

The project deliberately has no third-party test dependency, so tests use
stdlib ``unittest`` (``python -m unittest discover``).
"""

import os
import shutil
import sqlite3
import tempfile

from streamlex.engine import Engine


class Crash(Exception):
    """Raised by a control hook to simulate an abrupt process death."""


class Workspace(object):
    """A temp dir holding one source/target/manifest triple."""

    def __init__(self, data=None):
        self.dir = tempfile.mkdtemp(prefix="streamlex-test-")
        self.source = os.path.join(self.dir, "export.sql")
        self.target = os.path.join(self.dir, "target.db")
        self.manifest = os.path.join(self.dir, "manifest.json")
        if data is not None:
            with open(self.source, "wb") as fh:
                fh.write(data)

    def append(self, data):
        with open(self.source, "ab") as fh:
            fh.write(data)

    def write_source(self, data):
        with open(self.source, "wb") as fh:
            fh.write(data)

    def read_source(self):
        with open(self.source, "rb") as fh:
            return fh.read()

    def engine(self, **kw):
        return Engine(self.source, self.target, self.manifest, **kw)

    def rows(self, table):
        c = sqlite3.connect(self.target)
        try:
            return c.execute('SELECT * FROM "%s" ORDER BY 1' % table).fetchall()
        finally:
            c.close()

    def counts(self):
        c = sqlite3.connect(self.target)
        try:
            out = {}
            for (name,) in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
                " AND name NOT LIKE '__streamlex_%'"
            ):
                out[name] = c.execute('SELECT COUNT(*) FROM "%s"' % name).fetchone()[0]
            return out
        finally:
            c.close()

    def fk_violations(self):
        c = sqlite3.connect(self.target)
        try:
            return c.execute("PRAGMA foreign_key_check").fetchall()
        finally:
            c.close()

    def checkpoint(self):
        c = sqlite3.connect(self.target)
        try:
            row = c.execute(
                "SELECT byte_offset, unit_seq, txn_count, rows_applied,"
                " boundary_start, boundary_len FROM __streamlex_checkpoint"
            ).fetchone()
            return row
        finally:
            c.close()

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def run_to_completion(ws, **kw):
    eng = ws.engine(**kw)
    try:
        return eng.run()
    finally:
        eng.close()


def run_with_crash(ws, point, occurrence=1, **kw):
    """Run until the named control point fires for the ``occurrence``-th time,
    then raise Crash.  Returns the exception for the caller to assert on."""
    seen = {"n": 0}

    def hook(name, engine, **kwargs):
        if name == point:
            seen["n"] += 1
            if seen["n"] >= occurrence:
                raise Crash("%s#%d" % (point, occurrence))

    eng = ws.engine(on_control=hook, **kw)
    try:
        eng.run()
        raise AssertionError("expected a simulated crash at %s" % point)
    except Crash:
        return seen["n"]
    finally:
        eng.close()
