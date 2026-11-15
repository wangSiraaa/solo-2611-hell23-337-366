"""Lexer invariants: boundaries, chunk splitting, COPY payload safety."""

import hashlib
import unittest

from streamlex.lexer import (
    MODE_COPY_DATA,
    MODE_SQL,
    StreamLexer,
    classify_sql,
    parse_copy_row,
)
from streamlex.store import Manifest, MemoryStore, TrackedStore

from . import fixtures


def lex(data, chunk=7, start=0):
    st = MemoryStore(data)
    man = Manifest()
    lx = StreamLexer(TrackedStore(st, man), start_offset=start, chunk_size=chunk)
    out = []
    while True:
        ev = lx.next_event()
        if ev is None:
            break
        out.append(ev)
    return out


def simplified(events):
    out = []
    for ev in events:
        if ev.kind == "copy_row":
            out.append(("row", ev.table, tuple(ev.values)))
        elif ev.kind == "sql":
            out.append(("sql", ev.stmt["kind"], ev.verbatim))
        else:
            out.append((ev.kind, ev.table))
    return out


class LexerBoundaryTests(unittest.TestCase):
    def test_every_byte_split_matches_whole_stream(self):
        data = fixtures.EXPORT
        baseline = simplified(lex(data, chunk=len(data) + 1))
        for cut in range(1, len(data) + 1):
            with self.subTest(chunk=cut):
                self.assertEqual(simplified(lex(data, chunk=cut)), baseline)

    def test_events_end_at_safe_boundaries(self):
        events = lex(fixtures.EXPORT, chunk=3)
        for ev in events:
            self.assertIn(ev.kind, ("sql", "copy_begin", "copy_row", "copy_end"))
        # copy_row boundaries are line ends; copy_end and sql ends are safe.
        ends = [ev.end for ev in events if ev.kind in ("sql", "copy_end")]
        self.assertEqual(ends, sorted(ends))
        self.assertEqual(ends[-1], len(fixtures.EXPORT) - 1)  # final ';'

    def test_state_after_last_event_is_safe(self):
        st = MemoryStore(fixtures.EXPORT)
        lx = StreamLexer(TrackedStore(st, Manifest()), chunk_size=5)
        while lx.next_event() is not None:
            pass
        self.assertTrue(lx.is_safe_boundary())
        self.assertEqual(lx.mode, MODE_SQL)

    def test_copy_commit_text_is_data_not_boundary(self):
        evs = lex(fixtures.EXPORT)
        rows = [e for e in evs if e.kind == "copy_row"]
        self.assertIn(
            ("row", "child", ("10", "1", "COMMIT; is data")),
            simplified(evs),
        )
        commits = [e for e in evs if e.kind == "sql" and e.stmt["kind"] == "commit"]
        self.assertEqual(len(commits), 2)
        self.assertEqual(len(rows), 4)

    def test_semicolon_and_comment_markers_inside_copy(self):
        data = (
            b"BEGIN;\nCOPY t FROM STDIN;\n"
            b"1\ta; BEGIN; COMMIT;\n"
            b"2\t-- not a comment\n"
            b"3\t/* not a block */\n"
            b"4\t\\N\n"
            b"\\.\nCOMMIT;\n"
        )
        evs = lex(data, chunk=1)
        rows = [e.values for e in evs if e.kind == "copy_row"]
        self.assertEqual(
            rows,
            [
                ["1", "a; BEGIN; COMMIT;"],
                ["2", "-- not a comment"],
                ["3", "/* not a block */"],
                ["4", None],
            ],
        )

    def test_quotes_comments_and_multiline_sql(self):
        data = (
            b"/* leading ; COMMIT; */\n"
            b"CREATE TABLE t(a TEXT, b TEXT);\n"
            b"BEGIN;\n"
            b"-- line ; comment\n"
            b"INSERT INTO t VALUES ('a;b', 'it''s');\n"
            b"INSERT INTO t VALUES (\"x;y\", 'z');\n"
            b"COMMIT;\n"
        )
        evs = lex(data, chunk=2)
        kinds = [(e.stmt["kind"] if e.kind == "sql" else e.kind) for e in evs]
        self.assertEqual(
            kinds,
            ["create_table", "begin", "insert", "insert", "commit"],
        )
        inserts = [e for e in evs if e.kind == "sql" and e.stmt["kind"] == "insert"]
        self.assertEqual(inserts[0].stmt["values"], ["a;b", "it's"])
        self.assertEqual(inserts[1].stmt["values"], ["x;y", "z"])

    def test_utf8_split_inside_codepoint(self):
        # '中' is e4 b8 ad: feed one byte at a time through a COPY row
        data = (
            b"BEGIN;\nCOPY t FROM STDIN;\n"
            b"1\t\xe4\xb8\xad\xe6\x96\x87\n"
            b"\\.\nCOMMIT;\n"
        )
        evs = lex(data, chunk=1)
        rows = [e.values for e in evs if e.kind == "copy_row"]
        self.assertEqual(rows, [["1", "中文"]])

    def test_copy_escapes(self):
        data = (
            b"BEGIN;\nCOPY t FROM STDIN;\n"
            b"1\ta\\tb\\nc\\rd\\\\e\n"
            b"2\t\\N\n"
            b"\\.\nCOMMIT;\n"
        )
        evs = lex(data, chunk=1)
        rows = [e.values for e in evs if e.kind == "copy_row"]
        self.assertEqual(rows, [["1", "a\tb\nc\rd\\e"], ["2", None]])

    def test_dash_and_slash_split_across_chunks(self):
        data = b"-- comment\n/* block */\nCREATE TABLE t(a TEXT);\n"
        for chunk in (1, 2):
            evs = lex(data, chunk=chunk)
            self.assertEqual(len(evs), 1)
            self.assertEqual(evs[0].stmt["kind"], "create_table")

    def test_verbatim_bytes_are_exact(self):
        evs = lex(fixtures.EXPORT, chunk=1)
        for ev in evs:
            if ev.kind in ("sql", "copy_begin"):
                # ``verbatim`` is the statement body without the terminating
                # ';'; ``end`` points just past it.
                self.assertEqual(
                    fixtures.EXPORT[ev.start : ev.end - 1], ev.verbatim
                )


class RejectionTests(unittest.TestCase):
    def test_unknown_statement_rejected(self):
        with self.assertRaises(Exception):
            lex(b"UPDATE t SET a=1;\n")

    def test_copy_with_columns_rejected(self):
        with self.assertRaises(Exception):
            lex(b"COPY t (a,b) FROM STDIN;\n\\.\n")

    def test_copy_from_other_source_rejected(self):
        with self.assertRaises(Exception):
            lex(b"COPY t FROM '/etc/passwd';\n")

    def test_bad_escape_rejected(self):
        with self.assertRaises(Exception):
            lex(b"BEGIN;\nCOPY t FROM STDIN;\n1\t\\q\n\\.\nCOMMIT;\n")

    def test_trailing_backslash_rejected(self):
        with self.assertRaises(Exception):
            lex(b"BEGIN;\nCOPY t FROM STDIN;\n1\tab\\\n\\.\nCOMMIT;\n")

    def test_invalid_utf8_field_rejected(self):
        with self.assertRaises(Exception):
            lex(b"BEGIN;\nCOPY t FROM STDIN;\n1\t\xff\n\\.\nCOMMIT;\n")

    def test_truncated_copy_rejected(self):
        with self.assertRaises(Exception):
            lex(b"BEGIN;\nCOPY t FROM STDIN;\n1\ta\n")

    def test_truncated_statement_rejected(self):
        with self.assertRaises(Exception):
            lex(b"CREATE TABLE t(a TEXT)")

    def test_commit_outside_txn_rejected(self):
        with self.assertRaises(Exception):
            lex(b"COMMIT;\n")

    def test_nested_begin_rejected(self):
        with self.assertRaises(Exception):
            lex(b"BEGIN;\nBEGIN;\nCOMMIT;\n")

    def test_copy_outside_txn_rejected_at_engine(self):
        # The lexer yields the event; the engine is the one that requires a
        # transaction.  Here we only assert the lexer does not invent a COMMIT.
        evs = lex(b"COPY t FROM STDIN;\n1\n\\.\n")
        self.assertEqual([e.kind for e in evs], ["copy_begin", "copy_row", "copy_end"])


class CopyRowTests(unittest.TestCase):
    def test_null_spelling(self):
        self.assertEqual(parse_copy_row(b"\\N", 1), [None])
        self.assertEqual(parse_copy_row(b"", 1), [""])
        # ``\N`` is only NULL when it is the whole field; inside a field it is
        # not one of the declared escapes and must be rejected.
        with self.assertRaises(Exception):
            parse_copy_row(b"a\\Nb", 1)

    def test_all_escapes(self):
        self.assertEqual(
            parse_copy_row(b"a\\tb\\nc\\rd\\\\", 1),
            ["a\tb\nc\rd\\"],
        )


class ClassifyTests(unittest.TestCase):
    def test_case_insensitive_keywords(self):
        self.assertEqual(classify_sql(b"begin")["kind"], "begin")
        self.assertEqual(classify_sql(b"COMMIT")["kind"], "commit")
        self.assertEqual(classify_sql(b"End transaction")["kind"], "commit")

    def test_insert_literals(self):
        info = classify_sql(b"INSERT INTO t VALUES (1, 'a', NULL, $5)")
        self.assertEqual(info["values"], [1, "a", None, 5])


if __name__ == "__main__":
    unittest.main()
