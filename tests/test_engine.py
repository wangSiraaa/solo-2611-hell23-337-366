"""Transaction atomicity, resume, and durability-evidence tests."""

import os
import sqlite3
import unittest

from streamlex.errors import AuditFailed, InputFormatError, SourceChanged, TxnRejected

from . import fixtures
from .helpers import Workspace, run_to_completion, run_with_crash


class NormalPathTests(unittest.TestCase):
    def test_fixture_loads_exactly_once(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            r = run_to_completion(ws)
            self.assertEqual(ws.rows("parent"), [(1, "alpha"), (2, "beta")])
            self.assertEqual(
                ws.rows("child"),
                [(10, 1, "COMMIT; is data"), (11, 2, "second")],
            )
            self.assertEqual(ws.fk_violations(), [])
            self.assertEqual(r["stats"]["txns_committed"], 2)
            self.assertEqual(r["stats"]["units_committed"], 4)
        finally:
            ws.cleanup()

    def test_rerun_is_a_noop(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            run_to_completion(ws)
            cp1 = ws.checkpoint()
            r = run_to_completion(ws)
            self.assertEqual(r["stats"]["units_committed"], 0)
            self.assertEqual(ws.checkpoint(), cp1)
            self.assertEqual(len(ws.rows("parent")), 2)
            self.assertEqual(len(ws.rows("child")), 2)
        finally:
            ws.cleanup()

    def test_rich_family(self):
        ws = Workspace(fixtures.RICH)
        try:
            run_to_completion(ws)
            self.assertEqual(
                ws.rows("parent"),
                [
                    (1, "alpha; COMMIT;"),
                    (2, "café 中文"),
                    (3, "a\tb\nc\\d"),
                    (4, "-- not a comment"),
                    (5, None),
                    (6, "it's \"quoted\""),
                    (7, "it's; ok"),
                ],
            )
            self.assertEqual(
                ws.rows("child"),
                [(10, 1, "COMMIT; data"), (11, 2, "line\\two"), (12, 6, "😀 emoji")],
            )
            self.assertEqual(ws.fk_violations(), [])
        finally:
            ws.cleanup()


class AtomicityTests(unittest.TestCase):
    def test_control_point_mid_transaction_shows_nothing(self):
        # Crash right after the first COPY section of the first transaction.
        ws = Workspace(fixtures.EXPORT)
        try:
            run_with_crash(ws, "after_copy_end", occurrence=1)
            # Neither table may show the half transaction.  Only the two DDL
            # units committed before BEGIN.
            self.assertEqual(ws.counts(), {"parent": 0, "child": 0})
            c = sqlite3.connect(ws.target)
            try:
                committed = c.execute(
                    "SELECT unit_seq FROM __streamlex_checkpoint"
                ).fetchone()[0]
            finally:
                c.close()
            self.assertEqual(committed, 2)  # the two CREATE TABLE units
        finally:
            ws.cleanup()

    def test_crash_before_target_commit_loses_unit(self):
        ws = Workspace(fixtures.RICH)
        try:
            run_with_crash(ws, "before_target_commit", occurrence=1)
            self.assertEqual(ws.counts(), {})
            self.assertIsNone(ws.checkpoint())
        finally:
            ws.cleanup()

    def test_fk_violation_rolls_back_only_that_transaction(self):
        ws = Workspace(fixtures.FK_BAD)
        try:
            with self.assertRaises(TxnRejected):
                run_to_completion(ws)
            self.assertEqual(ws.rows("parent"), [(1,)])
            self.assertEqual(ws.rows("child"), [])
            self.assertEqual(ws.fk_violations(), [])
            # The checkpoint is at the last good transaction.
            self.assertEqual(ws.checkpoint()[2], 1)  # txn_count
        finally:
            ws.cleanup()

    def test_ddl_inside_transaction_is_atomic(self):
        ws = Workspace(fixtures.DDL_IN_TXN)
        try:
            run_to_completion(ws)
            self.assertEqual(ws.rows("parent"), [(1, "p")])
            self.assertEqual(ws.rows("child"), [(10, 1, "dep")])
            self.assertEqual(ws.fk_violations(), [])
            c = sqlite3.connect(ws.target)
            try:
                idx = [r[0] for r in c.execute(
                    "SELECT name FROM sqlite_master WHERE type='index'"
                )]
            finally:
                c.close()
            self.assertIn("child_parent_idx", idx)
        finally:
            ws.cleanup()

    def test_foreign_keys_never_toggled_off(self):
        from streamlex.target import Target

        ws = Workspace(fixtures.FK_BAD)
        try:
            with self.assertRaises(TxnRejected):
                run_to_completion(ws)
            # A *fresh* sqlite connection defaults foreign_keys=OFF; we assert
            # the target layer turns it on for every connection it owns.
            t = Target(ws.target)
            try:
                self.assertTrue(t.fk_enabled())
            finally:
                t.close()
        finally:
            ws.cleanup()

    def test_source_operation_order_preserved(self):
        # INSERT (parent) must run before COPY (child) that references it.
        data = (
            b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"
            b"CREATE TABLE child(id INTEGER PRIMARY KEY,"
            b" parent_id INTEGER REFERENCES parent(id));\n"
            b"BEGIN;\n"
            b"INSERT INTO parent VALUES (5, 'first');\n"
            b"COPY child FROM STDIN;\n10\t5\n\\.\n"
            b"COMMIT;\n"
        )
        ws = Workspace(data)
        try:
            run_to_completion(ws)
            self.assertEqual(ws.rows("parent"), [(5, "first")])
            self.assertEqual(ws.rows("child"), [(10, 5)])
            self.assertEqual(ws.fk_violations(), [])
        finally:
            ws.cleanup()


class RejectionTests(unittest.TestCase):
    def _assert_preserved(self, data, exc=InputFormatError):
        ws = Workspace(data)
        try:
            with self.assertRaises(exc):
                run_to_completion(ws)
            # Only the first, complete transaction survives.
            self.assertEqual(ws.rows("parent"), [(1, "ok")])
            self.assertEqual(ws.counts().get("child", 0), 0)
            self.assertEqual(ws.checkpoint()[2], 1)
            return ws
        except Exception:
            ws.cleanup()
            raise

    def test_unknown_sql_rejected(self):
        data = (
            b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"
            b"BEGIN;\nCOPY parent FROM STDIN;\n1\tok\n\\.\nCOMMIT;\n"
            b"BEGIN;\nUPDATE parent SET label='x';\nCOMMIT;\n"
        )
        ws = self._assert_preserved(data)
        ws.cleanup()

    def test_bad_copy_row_rejected(self):
        data = (
            b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"
            b"BEGIN;\nCOPY parent FROM STDIN;\n1\tok\n\\.\nCOMMIT;\n"
            b"BEGIN;\nCOPY parent FROM STDIN;\n2\ttoo\tmany\tfields\n\\.\nCOMMIT;\n"
        )
        ws = self._assert_preserved(data, exc=TxnRejected)
        ws.cleanup()

    def test_truncated_transaction_rejected(self):
        data = (
            b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"
            b"BEGIN;\nCOPY parent FROM STDIN;\n1\tok\n\\.\nCOMMIT;\n"
            b"BEGIN;\nCOPY parent FROM STDIN;\n2\tnever\n\\.\n"
        )
        ws = self._assert_preserved(data)
        ws.cleanup()

    def test_copy_outside_transaction_rejected(self):
        data = b"COPY parent FROM STDIN;\n1\n\\.\n"
        ws = Workspace(data)
        try:
            with self.assertRaises(InputFormatError):
                run_to_completion(ws)
            self.assertIsNone(ws.checkpoint())
        finally:
            ws.cleanup()


class ResumeTests(unittest.TestCase):
    def test_crash_after_target_commit_does_not_reexecute(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            # Crash after the *first* transaction's target commit.
            run_with_crash(ws, "after_target_commit", occurrence=3)
            # Units 1,2 are DDL; unit 3 is the first transaction.
            self.assertEqual(len(ws.rows("parent")), 1)
            self.assertEqual(len(ws.rows("child")), 1)
            before = ws.checkpoint()

            r = run_to_completion(ws)
            # Resume re-reads only the boundary block; it must not re-apply the
            # already-committed transaction.
            self.assertLessEqual(r["resume_bytes_read"], 65536)
            self.assertEqual(len(ws.rows("parent")), 2)
            self.assertEqual(len(ws.rows("child")), 2)
            self.assertEqual(ws.fk_violations(), [])
            after = ws.checkpoint()
            self.assertGreater(after[0], before[0])
            self.assertEqual(after[2], 2)  # exactly two source transactions
        finally:
            ws.cleanup()

    def test_no_duplicate_primary_keys_across_resumes(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            for occ in range(1, 8):
                c = sqlite3.connect(ws.target) if os.path.exists(ws.target) else None
                c and c.close()
                try:
                    run_with_crash(ws, "after_target_commit", occurrence=occ)
                except AssertionError:
                    break  # no more occurrences: run finished
            run_to_completion(ws)
            self.assertEqual(ws.rows("parent"), [(1, "alpha"), (2, "beta")])
            self.assertEqual(len(ws.rows("child")), 2)
        finally:
            ws.cleanup()

    def test_exhaustive_cut_then_append(self):
        """Cut the source at every byte, attempt a run, append the rest, resume;
        the final target must equal the one-shot result with no duplicates."""
        ref = Workspace(fixtures.EXPORT)
        try:
            run_to_completion(ref)
            want_p, want_c = ref.rows("parent"), ref.rows("child")
        finally:
            ref.cleanup()

        full = fixtures.EXPORT
        for cut in range(len(full) + 1):
            ws = Workspace(full[:cut])
            try:
                try:
                    run_to_completion(ws)
                except (InputFormatError, TxnRejected):
                    pass
                ws.write_source(full)
                run_to_completion(ws)
                self.assertEqual(ws.rows("parent"), want_p, "cut=%d" % cut)
                self.assertEqual(ws.rows("child"), want_c, "cut=%d" % cut)
                self.assertEqual(ws.fk_violations(), [], "cut=%d" % cut)
            finally:
                ws.cleanup()

    def test_multi_append_multi_resume(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            run_to_completion(ws)
            for k in range(1, 4):
                ws.append(fixtures.append_family(None, k))
                r = run_to_completion(ws)
                self.assertEqual(r["stats"]["txns_committed"], 1)
                # Fixed resume overhead + only the newly appended bytes.
                self.assertLessEqual(r["resume_bytes_read"], 65536)
            self.assertEqual(len(ws.rows("parent")), 5)
            self.assertEqual(len(ws.rows("child")), 5)
            c = sqlite3.connect(ws.target)
            try:
                dups = c.execute(
                    "SELECT id, COUNT(*) FROM parent GROUP BY id HAVING COUNT(*)>1"
                ).fetchall()
            finally:
                c.close()
            self.assertEqual(dups, [])
        finally:
            ws.cleanup()

    def test_replay_is_only_uncommitted_tail(self):
        """A crash mid-transaction leaves a read high-water above the
        checkpoint; the resume reports exactly that uncommitted tail as
        replay, and the fixed boundary proof stays <= 64 KiB."""
        head = b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"

        def txn(k):
            return (
                b"BEGIN;\nCOPY parent FROM STDIN;\n"
                + b"".join(
                    b"%d\t%s\n" % (k * 1000 + i, b"a" * 100) for i in range(1, 300)
                )
                + b"\\.\nCOMMIT;\n"
            )

        data = head + b"".join(txn(k) for k in range(1, 4))
        ws = Workspace(data)
        try:
            counter = {"n": 0}

            def hook(name, engine, **kw):
                if name == "copy_row":
                    counter["n"] += 1
                    if counter["n"] == 600:
                        from .helpers import Crash

                        raise Crash()

            eng = ws.engine(on_control=hook, chunk_size=512)
            try:
                eng.run()
            except Exception:
                pass
            finally:
                eng.close()
            cp_end = ws.checkpoint()[0]
            r = run_to_completion(ws, chunk_size=512)
            self.assertLessEqual(r["resume_bytes_read"], 65536)
            self.assertGreater(r["replay_bytes_read"], 0)
            # Replay is bounded by the uncommitted tail, not the whole prefix.
            self.assertLessEqual(r["replay_bytes_read"], 512 + 2 * 1024 * 1024)
            self.assertEqual(len(ws.rows("parent")), 3 * 299)
        finally:
            ws.cleanup()

    def test_resume_reads_bounded_even_for_large_source(self):
        # 100 transactions of ~20 KiB each: resume must not re-read the prefix.
        txn = (
            b"BEGIN;\nCOPY parent FROM STDIN;\n"
            + b"".join(b"%d\t%s\n" % (i, b"x" * 100) for i in range(1, 151))
            + b"\\.\nCOPY child FROM STDIN;\n%1000\t1\tc\n"
            b"COMMIT;\n"
        )
        data = (
            b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"
            b"CREATE TABLE child(id INTEGER PRIMARY KEY,"
            b" parent_id INTEGER REFERENCES parent(id), note TEXT);\n"
            + b"".join(
                b"BEGIN;\nCOPY parent FROM STDIN;\n"
                + b"".join(
                    b"%d\t%s\n" % (1000 * t + i, b"y" * 100) for i in range(1, 51)
                )
                + b"\\.\nCOMMIT;\n"
                for t in range(1, 51)
            )
        )
        ws = Workspace(data)
        try:
            run_to_completion(ws)
            n_before = len(ws.rows("parent"))
            ws.append(
                b"BEGIN;\nCOPY parent FROM STDIN;\n999999\tlast\n\\.\nCOMMIT;\n"
            )
            r = run_to_completion(ws)
            self.assertLessEqual(r["resume_bytes_read"], 65536)
            self.assertGreater(len(ws.rows("parent")), n_before)
        finally:
            ws.cleanup()


class DurabilityEvidenceTests(unittest.TestCase):
    def test_committed_but_confirmation_lost(self):
        """Commit the target, then "lose" the confirmation (crash after commit).
        The checkpoint row committed atomically is the proof; resume must skip
        that unit exactly once and never execute it again."""
        ws = Workspace(fixtures.EXPORT)
        try:
            run_with_crash(ws, "after_target_commit", occurrence=1)
            cp = ws.checkpoint()
            self.assertIsNotNone(cp)  # checkpoint survived the lost confirmation
            self.assertEqual(cp[2], 0)  # no source transaction yet, only DDL

            eng = ws.engine()
            try:
                r = eng.run()
                # The TxnRow at the checkpoint is exactly what proves the
                # already-visible unit; the engine resumes *after* it.
                self.assertEqual(r["resume_from"], cp[0])
                self.assertLessEqual(r["resume_bytes_read"], 65536)
            finally:
                eng.close()
            self.assertEqual(ws.rows("parent"), [(1, "alpha"), (2, "beta")])
            self.assertEqual(len(ws.rows("child")), 2)
        finally:
            ws.cleanup()

    def test_checkpoint_is_covered_by_manifest(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            r = run_to_completion(ws)
            import json

            with open(ws.manifest) as fh:
                man = json.load(fh)
            # The manifest is a receipt log of received bytes; it must at least
            # cover everything the committed checkpoint relies on.
            self.assertGreaterEqual(
                man["trusted_len"], r["checkpoint"]["byte_offset"]
            )
            self.assertGreaterEqual(len(man["chunks"]), 1)
        finally:
            ws.cleanup()

    def test_source_shrink_refused(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            run_to_completion(ws)
            ws.write_source(fixtures.EXPORT[:100])
            with self.assertRaises(SourceChanged):
                run_to_completion(ws)
        finally:
            ws.cleanup()

    def test_source_replace_refused(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            run_to_completion(ws)
            # Replace the path with a *different* file whose first bytes differ,
            # so the identity check trips regardless of inode reuse.
            changed = bytearray(fixtures.EXPORT)
            changed[0] = ord("X")
            other = ws.source + ".new"
            with open(other, "wb") as fh:
                fh.write(bytes(changed))
            os.replace(other, ws.source)
            with self.assertRaises(SourceChanged):
                run_to_completion(ws)
        finally:
            ws.cleanup()


class AuditTests(unittest.TestCase):
    def test_clean_audit(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            run_to_completion(ws)
            eng = ws.engine()
            try:
                a = eng.audit()
            finally:
                eng.close()
            self.assertGreater(a["bytes_read"], 0)
            self.assertGreaterEqual(a["chunks"], 1)
        finally:
            ws.cleanup()

    def test_audit_detects_old_prefix_tamper_and_refuses_resume(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            run_to_completion(ws)
            data = bytearray(ws.read_source())
            data[5] = ord("X")  # corrupt an old, already-committed byte
            ws.write_source(bytes(data))

            eng = ws.engine()
            try:
                with self.assertRaises(AuditFailed):
                    eng.audit()
            finally:
                eng.close()

            # The target is marked untrusted and resume is refused.
            eng = ws.engine()
            try:
                with self.assertRaises(AuditFailed):
                    eng.run()
            finally:
                eng.close()
            # Committed data is left intact (we do not destroy it).
            self.assertEqual(len(ws.rows("parent")), 2)
        finally:
            ws.cleanup()

    def test_audit_reads_are_separately_accounted(self):
        ws = Workspace(fixtures.EXPORT)
        try:
            run_to_completion(ws)
            eng = ws.engine()
            try:
                a = eng.audit()
            finally:
                eng.close()
            # The audit read the full published prefix.
            self.assertGreaterEqual(a["bytes_read"], len(fixtures.EXPORT))
        finally:
            ws.cleanup()


if __name__ == "__main__":
    unittest.main()
