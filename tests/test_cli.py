"""CLI behaviour: exit codes, JSON output, crash injection, audit refusal."""

import json
import os
import subprocess
import sys
import unittest

from . import fixtures
from .helpers import Workspace

CLI = [sys.executable, "-m", "streamlex"]


def run_cli(*args):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.getcwd()
    return subprocess.run(
        CLI + list(args), capture_output=True, text=True, env=env
    )


class CliTests(unittest.TestCase):
    def setUp(self):
        self.ws = Workspace(fixtures.EXPORT)

    def tearDown(self):
        self.ws.cleanup()

    def base(self):
        return [self.ws.source, "--target", self.ws.target, "--manifest", self.ws.manifest]

    def test_restore_and_status(self):
        r = run_cli("restore", *self.base())
        self.assertEqual(r.returncode, 0, r.stderr)
        out = json.loads(r.stdout)
        self.assertEqual(out["stats"]["txns_committed"], 2)
        s = run_cli("status", "--target", self.ws.target)
        self.assertEqual(s.returncode, 0)
        s = json.loads(s.stdout)
        self.assertEqual(s["tables"], {"child": 2, "parent": 2})
        self.assertEqual(s["foreign_key_violations"], [])

    def test_crash_exit_code_then_resume(self):
        r = run_cli(
            "restore", *self.base(),
            "--crash-at", "after_source_commit_before_target_commit",
        )
        self.assertEqual(r.returncode, 130, r.stdout + r.stderr)
        self.assertIn("[crash] at control point", r.stdout)
        # Half transaction not visible.
        self.assertEqual(self.ws.counts(), {"parent": 0, "child": 0})
        r2 = run_cli("restore", *self.base())
        self.assertEqual(r2.returncode, 0, r2.stderr)
        self.assertEqual(self.ws.counts(), {"parent": 2, "child": 2})

    def test_observe_target_reader(self):
        r = run_cli(
            "restore", *self.base(),
            "--crash-at", "after_copy_end",
            "--observe-target", self.ws.target,
        )
        self.assertEqual(r.returncode, 130)
        self.assertIn("reader@after_copy_end", r.stdout)
        # The reader never saw a partial transaction.
        for line in r.stdout.splitlines():
            if "reader@" in line:
                view = json.loads(line.split(": ", 1)[1])
                if view:
                    self.assertEqual(len(set(view.values())), 1, line)

    def test_unknown_sql_exit_code(self):
        self.ws.write_source(
            b"CREATE TABLE a(id INTEGER PRIMARY KEY);\n"
            b"BEGIN;\nCOPY a FROM STDIN;\n1\n\\.\nCOMMIT;\n"
            b"BEGIN;\nDROP TABLE a;\nCOMMIT;\n"
        )
        r = run_cli("restore", *self.base())
        self.assertEqual(r.returncode, 2, r.stdout + r.stderr)
        self.assertEqual(self.ws.rows("a"), [(1,)])

    def test_audit_tamper_exit_code_and_refusal(self):
        self.assertEqual(run_cli("restore", *self.base()).returncode, 0)
        data = bytearray(self.ws.read_source())
        data[4] = ord("Z")
        self.ws.write_source(bytes(data))
        a = run_cli("audit-source", *self.base())
        self.assertEqual(a.returncode, 3, a.stdout + a.stderr)
        self.assertIn("AUDIT FAILED", a.stderr)
        # Resume now refuses with the same code.
        r = run_cli("restore", *self.base())
        self.assertEqual(r.returncode, 3, r.stdout + r.stderr)

    def test_audit_clean_reports_reads(self):
        self.assertEqual(run_cli("restore", *self.base()).returncode, 0)
        a = run_cli("audit-source", *self.base())
        self.assertEqual(a.returncode, 0, a.stderr)
        out = json.loads(a.stdout)
        self.assertTrue(out["ok"])
        self.assertGreaterEqual(
            out["accounting"]["audit_bytes_read"], len(fixtures.EXPORT)
        )

    def test_source_shrink_exit_code(self):
        self.assertEqual(run_cli("restore", *self.base()).returncode, 0)
        self.ws.write_source(fixtures.EXPORT[:50])
        r = run_cli("restore", *self.base())
        self.assertEqual(r.returncode, 4, r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
