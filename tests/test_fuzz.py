"""Differential fuzzing over the legal input family.

Random sources in the declared subset (pseudo-boundaries in COPY data, quoted
SQL strings, UTF-8, escapes, per-transaction DDL-free shape) are cut at random
byte offsets, replayed by a partial run, appended to, resumed, and compared
with the one-shot result.  This is the same check as ``test_exhaustive_cut``
but over generated members of the family rather than one fixture.
"""

import random
import unittest

from .helpers import Workspace, run_to_completion
from streamlex.errors import InputFormatError, TxnRejected


_ESC = {"\t": b"\\t", "\n": b"\\n", "\r": b"\\r", "\\": b"\\\\"}


def _copy_field(s):
    out = bytearray()
    for ch in s.encode("utf-8"):
        b = bytes((ch,))
        if b == b"\t":
            out += b"\\t"
        elif b == b"\n":
            out += b"\\n"
        elif b == b"\r":
            out += b"\\r"
        elif b == b"\\":
            out += b"\\\\"
        else:
            out += b
    return bytes(out)


LABELS = ["a", "x; COMMIT;", "-- z", "/* q */", "café 中文", "a\\tb(esc)", "", "it's"]
NOTES = ["n", "COMMIT;", "-- c", "é😀", "a\tb"]


def generate(seed):
    r = random.Random(seed)
    parts = [
        b"-- head ; COMMIT;\n",
        b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n",
        b"CREATE TABLE child(id INTEGER PRIMARY KEY,"
        b" parent_id INTEGER REFERENCES parent(id), note TEXT);\n",
    ]
    pid = cid = 0
    existing = []
    for _ in range(r.randint(1, 4)):
        parts.append(b"BEGIN;\n")
        if r.random() < 0.4:
            parts.append(b"/* txn comment ; */\n")
        parts.append(b"COPY parent FROM STDIN;\n")
        txn_parents = []
        for _ in range(r.randint(1, 5)):
            pid += 1
            label = r.choice(LABELS)
            val = b"\\N" if label == "" else _copy_field(label)
            parts.append(b"%d\t%s\n" % (pid, val))
            txn_parents.append(pid)
        parts.append(b"\\.\n")
        if r.random() < 0.3:
            new_id = pid + 100
            parts.append(
                b"INSERT INTO parent VALUES (%d, 'it''s; ok');\n" % new_id
            )
            pid = new_id
            txn_parents.append(new_id)
        existing.extend(txn_parents)
        parts.append(b"COPY child FROM STDIN;\n")
        for _ in range(r.randint(1, 5)):
            cid += 1
            note = _copy_field(r.choice(NOTES))
            parent_ref = r.choice(existing + txn_parents)
            parts.append(b"%d\t%d\t%s\n" % (cid, parent_ref, note))
        parts.append(b"\\.\n")
        parts.append(b"COMMIT;\n")
    return b"".join(parts)


class FuzzTests(unittest.TestCase):
    def test_random_cut_append_resume(self):
        for seed in range(25):
            data = generate(seed)
            ref = Workspace(data)
            try:
                run_to_completion(ref)
                want = (ref.rows("parent"), ref.rows("child"))
            finally:
                ref.cleanup()

            rng = random.Random(seed * 7919)
            for n in range(3):
                cut = rng.randint(0, len(data))
                ws = Workspace(data[:cut])
                try:
                    try:
                        run_to_completion(ws)
                    except (InputFormatError, TxnRejected):
                        pass
                    ws.write_source(data)
                    run_to_completion(ws)
                    got = (ws.rows("parent"), ws.rows("child"))
                    self.assertEqual(got, want, "seed=%d cut=%d" % (seed, cut))
                    self.assertEqual(ws.fk_violations(), [], "seed=%d cut=%d" % (seed, cut))
                finally:
                    ws.cleanup()


if __name__ == "__main__":
    unittest.main()
