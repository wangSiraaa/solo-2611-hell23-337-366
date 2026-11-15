"""End-to-end demonstration of the upgraded recovery system.

Run:  python demo.py            (all scenarios)
      python demo.py --quick    (skip the exhaustive byte-cut sweep)

Each scenario prints measured, reproducible output; the assertions are part of
the demo so a regression fails loudly instead of printing a nice story.

Scenarios
---------
1. normalize          normal path: the fixture loads fully and consistently.
2. pseudo-boundaries  COPY content containing COMMIT;/comments/quotes, quoted
                      SQL strings, and a transaction that creates its parent
                      table, copies parent rows, creates the child table and
                      copies child rows.
3. partial-utf8       feed the source one byte at a time (splits every UTF-8
                      sequence and every escape), whole-stream result equals
                      the normal path.
4. atom1 / atom2      a reader connection observes the target at control points
                      and never sees a half source transaction.
5. crash/resume       crash after target commit (confirmation lost), restart:
                      resume re-reads only a bounded boundary block and the
                      committed transaction is not executed twice.
6. multi-resume       append three more transactions of the same family and
                      resume after each; resume reads stay bounded.
7. audit              an old, already-committed prefix byte is corrupted;
                      audit-source detects it, marks the target untrusted and
                      resume is refused.
8. exhaustive-cuts    cut the fixture at every byte offset, resume, compare.
"""

import argparse
import json
import os
import shutil
import sqlite3
import sys
import tempfile

from streamlex.engine import Engine
from streamlex.errors import AuditFailed, InputFormatError, TxnRejected
from streamlex.target import Target

ROOT = os.path.dirname(os.path.abspath(__file__))
EXPORT = os.path.join(ROOT, "fixtures", "export.sql")


class Crash(Exception):
    pass


class Demo(object):
    def __init__(self):
        self.root = tempfile.mkdtemp(prefix="streamlex-demo-")
        self.passed = 0
        self.failed = 0

    # ---- plumbing -----------------------------------------------------
    def case(self, name):
        def deco(fn):
            try:
                detail = fn()
                self.passed += 1
                print("[PASS] %-18s %s" % (name, detail or ""))
            except Exception as exc:  # noqa: BLE001 - demo reports, then fails
                self.failed += 1
                print("[FAIL] %-18s %s: %s" % (name, type(exc).__name__, exc))
            return fn

        return deco

    def workdir(self, tag):
        d = os.path.join(self.root, tag)
        if os.path.isdir(d):
            shutil.rmtree(d)
        os.makedirs(d)
        return d

    def paths(self, d, name="s.sql"):
        return os.path.join(d, name), os.path.join(d, "t.db"), os.path.join(d, "m.json")

    def engine(self, src, tgt, man, **kw):
        return Engine(src, tgt, man, **kw)

    def run(self, src, tgt, man, **kw):
        e = self.engine(src, tgt, man, **kw)
        try:
            return e.run()
        finally:
            e.close()

    def rows(self, db, sql):
        c = sqlite3.connect(db)
        try:
            return c.execute(sql).fetchall()
        finally:
            c.close()

    def copy(self, tag, data=None):
        d = self.workdir(tag)
        src, tgt, man = self.paths(d)
        with open(src, "wb") as fh:
            fh.write(data if data is not None else open(EXPORT, "rb").read())
        return d, src, tgt, man


RICH = (
    b"-- header ; COMMIT; (comment)\n"
    b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"
    b"CREATE TABLE child(id INTEGER PRIMARY KEY,"
    b" parent_id INTEGER REFERENCES parent(id), note TEXT);\n"
    b"BEGIN;\n"
    b"/* block ; COMMIT; */\n"
    b"COPY parent FROM STDIN;\n"
    b"1\talpha; COMMIT;\n"
    b"2\tcaf\xc3\xa9 \xe4\xb8\xad\xe6\x96\x87\n"
    b"3\ta\\tb\\nc\\\\d\n"
    b"4\t-- not a comment\n"
    b"5\t\\N\n"
    b"6\tit's \"quoted\"\n"
    b"\\.\n"
    b"INSERT INTO parent VALUES (7, 'it''s; ok');\n"
    b"COPY child FROM STDIN;\n"
    b"10\t1\tCOMMIT; data\n"
    b"11\t2\tline\\\\two\n"
    b"12\t6\t\xf0\x9f\x98\x80 emoji\n"
    b"\\.\n"
    b"COMMIT;\n"
)

TXN_ADDED_DDL = (
    b"BEGIN;\n"
    b"CREATE TABLE p2(id INTEGER PRIMARY KEY, label TEXT);\n"
    b"COPY p2 FROM STDIN;\n1\tx\n\\.\n"
    b"CREATE TABLE c2(id INTEGER PRIMARY KEY,"
    b" parent_id INTEGER REFERENCES p2(id));\n"
    b"COPY c2 FROM STDIN;\n10\t1\n\\.\n"
    b"COMMIT;\n"
)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args(argv)
    demo = Demo()
    print("work dir:", demo.root)

    @demo.case("normalize")
    def _():
        d, src, tgt, man = demo.copy("normal")
        r = demo.run(src, tgt, man)
        p = demo.rows(tgt, "SELECT * FROM parent ORDER BY id")
        c = demo.rows(tgt, "SELECT * FROM child ORDER BY id")
        assert p == [(1, "alpha"), (2, "beta")], p
        assert c == [(10, 1, "COMMIT; is data"), (11, 2, "second")], c
        assert demo.rows(tgt, "PRAGMA foreign_key_check") == []
        return "2 txns, 4 rows; units=%d" % r["stats"]["units_committed"]

    @demo.case("pseudo-boundaries")
    def _():
        d, src, tgt, man = demo.copy("rich", RICH)
        demo.run(src, tgt, man)
        p = demo.rows(tgt, "SELECT * FROM parent ORDER BY id")
        c = demo.rows(tgt, "SELECT * FROM child ORDER BY id")
        assert ("1", 1) and p[0] == (1, "alpha; COMMIT;"), p
        assert p[3] == (4, "-- not a comment"), p
        assert p[4] == (5, None), p
        assert c[0] == (10, 1, "COMMIT; data"), c
        assert demo.rows(tgt, "PRAGMA foreign_key_check") == []
        return "COPY payload kept as data; FK clean"

    @demo.case("txn-added-ddl")
    def _():
        d, src, tgt, man = demo.copy("addddl", TXN_ADDED_DDL)
        demo.run(src, tgt, man)
        assert demo.rows(tgt, "SELECT * FROM p2") == [(1, "x")]
        assert demo.rows(tgt, "SELECT * FROM c2") == [(10, 1)]
        assert demo.rows(tgt, "PRAGMA foreign_key_check") == []
        return "parent/child tables created and filled in one txn"

    @demo.case("partial-utf8")
    def _():
        d, src, tgt, man = demo.copy("chunk", RICH)
        # chunk_size=1 forces every multibyte sequence and escape to be split.
        demo.run(src, tgt, man, chunk_size=1)
        p = demo.rows(tgt, "SELECT * FROM parent ORDER BY id")
        c = demo.rows(tgt, "SELECT * FROM child ORDER BY id")
        assert p[1] == (2, "café 中文"), p
        assert p[2] == (3, "a\tb\nc\\d"), p
        assert c[2] == (12, 6, "😀 emoji"), c
        return "1-byte chunks, same rows"

    @demo.case("no-half-txn")
    def _():
        d, src, tgt, man = demo.copy("half")
        seen = []

        def hook(name, eng, **kw):
            if name in ("after_copy_end", "after_source_commit_before_target_commit",
                        "after_target_commit"):
                c = sqlite3.connect("file:%s?mode=ro" % tgt, uri=True)
                try:
                    tables = [
                        r[0]
                        for r in c.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                            " AND name NOT LIKE '__streamlex_%'"
                        )
                    ]
                    view = {
                        t: c.execute('SELECT COUNT(*) FROM "%s"' % t).fetchone()[0]
                        for t in tables
                    }
                finally:
                    c.close()
                seen.append((name, view))
            if name == "after_copy_end":
                raise Crash()

        e = demo.engine(src, tgt, man, on_control=hook)
        try:
            e.run()
        except Crash:
            pass
        finally:
            e.close()
        # Every observation must be transactionally whole: parent and child
        # counts are either both 0 or both 1 at these points.
        for name, view in seen:
            counts = sorted(view.values())
            assert len(set(counts)) <= 1, (name, view)
        return "%d observations, no half txn" % len(seen)

    @demo.case("crash-resume")
    def _():
        d, src, tgt, man = demo.copy("resume")
        seen = {"n": 0}

        def hook(name, eng, **kw):
            if name == "after_target_commit":
                seen["n"] += 1
                # Units 1 and 2 are the two CREATE TABLE statements; unit 3 is
                # the first source transaction.  Crash right after it commits.
                if seen["n"] == 3:
                    raise Crash()

        e = demo.engine(src, tgt, man, on_control=hook)
        try:
            e.run()
        except Crash:
            pass
        finally:
            e.close()
        cp = demo.rows(tgt, "SELECT byte_offset, unit_seq, txn_count"
                       " FROM __streamlex_checkpoint")[0]
        before_p = demo.rows(tgt, "SELECT * FROM parent ORDER BY id")
        before_c = demo.rows(tgt, "SELECT * FROM child ORDER BY id")
        r = demo.run(src, tgt, man)
        assert demo.rows(tgt, "SELECT * FROM parent ORDER BY id") == [
            (1, "alpha"), (2, "beta")], "resume produced duplicates"
        assert demo.rows(tgt, "SELECT * FROM child ORDER BY id") == [
            (10, 1, "COMMIT; is data"), (11, 2, "second")]
        assert demo.rows(tgt, "PRAGMA foreign_key_check") == []
        assert r["resume_bytes_read"] <= 65536
        assert before_p == [(1, "alpha")] and before_c == [(10, 1, "COMMIT; is data")]
        return (
            "durable checkpoint %r; seen before resume p=%r c=%r; "
            "resume_from=%r reads=%d"
            % (cp, before_p, before_c, r["resume_from"], r["resume_bytes_read"])
        )

    @demo.case("multi-resume")
    def _():
        d, src, tgt, man = demo.copy("multi")
        demo.run(src, tgt, man)
        reads = []
        for k in range(1, 4):
            with open(src, "ab") as fh:
                fh.write(
                    b"BEGIN;\nCOPY parent FROM STDIN;\n%d\tp%d\n\\.\n"
                    b"COPY child FROM STDIN;\n%d\t%d\tc%d\n\\.\nCOMMIT;\n"
                    % (100 + k, k, 200 + k, 100 + k, k)
                )
            r = demo.run(src, tgt, man)
            reads.append(r["resume_bytes_read"])
            assert r["stats"]["txns_committed"] == 1, r["stats"]
        assert len(demo.rows(tgt, "SELECT * FROM parent")) == 5
        assert len(demo.rows(tgt, "SELECT * FROM child")) == 5
        assert max(reads) <= 65536, reads
        return "resume reads per append: %s" % reads

    @demo.case("audit-tamper")
    def _():
        d, src, tgt, man = demo.copy("audit")
        demo.run(src, tgt, man)
        data = bytearray(open(src, "rb").read())
        data[5] = ord("X")  # corrupt an old, committed byte
        with open(src, "wb") as fh:
            fh.write(bytes(data))
        eng = demo.engine(src, tgt, man)
        try:
            try:
                eng.audit()
                raise AssertionError("audit should have failed")
            except AuditFailed:
                pass
        finally:
            eng.close()
        eng = demo.engine(src, tgt, man)
        try:
            try:
                eng.run()
                raise AssertionError("resume should have been refused")
            except AuditFailed:
                pass
        finally:
            eng.close()
        # committed data is preserved but no longer resumable
        assert len(demo.rows(tgt, "SELECT * FROM parent")) == 2
        return "detected, target untrusted, resume refused"

    @demo.case("exhaustive-cuts")
    def _():
        data = open(EXPORT, "rb").read()
        d, src, tgt, man = demo.copy("ref")
        demo.run(src, tgt, man)
        want_p = demo.rows(tgt, "SELECT * FROM parent ORDER BY id")
        want_c = demo.rows(tgt, "SELECT * FROM child ORDER BY id")
        if args.quick:
            cuts = list(range(0, len(data) + 1, 16))
        else:
            cuts = range(len(data) + 1)
        tested = 0
        for cut in cuts:
            wd = demo.workdir("cut")
            cs, ct, cm = demo.paths(wd)
            with open(cs, "wb") as fh:
                fh.write(data[:cut])
            try:
                demo.run(cs, ct, cm)
            except (InputFormatError, TxnRejected):
                pass
            with open(cs, "wb") as fh:
                fh.write(data)
            demo.run(cs, ct, cm)
            assert demo.rows(ct, "SELECT * FROM parent ORDER BY id") == want_p, cut
            assert demo.rows(ct, "SELECT * FROM child ORDER BY id") == want_c, cut
            assert demo.rows(ct, "PRAGMA foreign_key_check") == [], cut
            tested += 1
        return "%d cut positions, all converge" % tested

    print("\n%d passed, %d failed" % (demo.passed, demo.failed))
    print("work dir kept for inspection:", demo.root)
    return 0 if demo.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
