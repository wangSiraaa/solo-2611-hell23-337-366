"""Persistence: index/checkpoints must work in a fresh process independently."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from helpers import ROOT, build, digests, neg

from wsarchive.index import deserialize, load_index, serialize, write_index
from wsarchive import Reader, build_index


class SerializationTests(unittest.TestCase):
    def test_roundtrip_preserves_structure_and_checkpoints(self):
        msgs = [bytes(range(256)) * 16 + bytes([i]) * 100 for i in range(30)]
        data, manifest, index, spec = build(
            msgs, [True] * 30, takeover=True, fragments=(1, 3), control="mixed"
        )
        blob = serialize(index)
        back = deserialize(blob, index.negotiation)
        self.assertEqual(len(back.frames), len(index.frames))
        self.assertEqual(len(back.messages), len(index.messages))
        self.assertEqual(len(back.checkpoints), len(index.checkpoints))
        self.assertEqual(back.archive_sha256, index.archive_sha256)
        self.assertEqual(back.negotiation_sha256, index.negotiation_sha256)
        self.assertEqual(back.dict_blob, index.dict_blob)
        for a, b in zip(index.messages, back.messages):
            self.assertEqual(a.plaintext_sha256, b.plaintext_sha256)
            self.assertEqual(a.declared_length, b.declared_length)
            self.assertEqual(a.body_ranges, b.body_ranges)
        # a reader over the deserialized index still decodes correctly
        r = Reader(data, back, verify_archive_hash=True)
        self.assertEqual(digests(r.decode_sequential()), digests(msgs))

    def test_write_and_load_index(self):
        msgs = [b"m" * 500 + bytes([i]) for i in range(12)]
        data, manifest, index, spec = build(msgs, [True] * 12, takeover=True)
        with tempfile.TemporaryDirectory() as d:
            apath = Path(d) / "archive.frames"
            apath.write_bytes(data)
            write_index(index, d, apath)
            self.assertTrue((Path(d) / "index.bin").exists())
            self.assertTrue((Path(d) / "manifest.json").exists())
            loaded = load_index(d)
            r = Reader.open(apath, d)
            self.assertEqual(
                digests(r.query([11, 0, 5, 5], mode="batch").messages),
                digests([msgs[11], msgs[0], msgs[5], msgs[5]]),
            )

    def test_roundtrip_with_interleaved_control_frames(self):
        # Interleaved control frames sit inside first..last frame ranges; they
        # must not be mistaken for message fragments after deserialization.
        msgs = [bytes(range(256)) * 16 + bytes([i]) * 100 for i in range(12)]
        data, manifest, index, spec = build(
            msgs, [True] * 12, takeover=True, fragments=(3,), control="interleaved"
        )
        self.assertTrue(
            any(
                f.opcode in (9, 10) for f in index.frames
            )
        )
        back = deserialize(serialize(index), index.negotiation)
        for a, b in zip(index.messages, back.messages):
            self.assertEqual(a.body_ranges, b.body_ranges)
            self.assertEqual(a.frame_indices, b.frame_indices)
            self.assertEqual(len(b.body_ranges), b.fragment_count)
        r = Reader(data, back)
        self.assertEqual(digests(r.decode_sequential()), digests(msgs))

    def test_manifest_hash_binding_after_index_tamper(self):
        msgs = [b"n" * 300 for _ in range(5)]
        data, manifest, index, spec = build(msgs, [True] * 5, takeover=True)
        with tempfile.TemporaryDirectory() as d:
            apath = Path(d) / "a.frames"
            apath.write_bytes(data)
            write_index(index, d, apath)
            idxp = Path(d) / "index.bin"
            b = bytearray(idxp.read_bytes())
            b[-1] ^= 0xFF
            idxp.write_bytes(bytes(b))
            with self.assertRaises(Exception):
                load_index(d)


INLINE = textwrap.dedent(
    """
    import json, sys, hashlib
    from pathlib import Path
    from wsarchive import Reader, load_index, Negotiation
    archive_path, index_dir, ids, negotiation_json = sys.argv[1:5]
    ids = [int(x) for x in ids.split(',')]
    neg = Negotiation.from_json(json.loads(negotiation_json))
    idx = load_index(index_dir, neg)
    reader = Reader.open(archive_path, index_dir, neg)
    out = reader.query(ids, mode='cold')
    print(json.dumps({
        'digests': [hashlib.sha256(m).hexdigest() for m in out.messages],
        'stats': out.stats.as_dict(),
    }))
    """
)


class CrossProcessTests(unittest.TestCase):
    def test_checkpoint_resume_in_fresh_process(self):
        msgs = [bytes(range(256)) * 16 + bytes([i]) * 300 for i in range(24)]
        data, manifest, index, spec = build(
            msgs, [True] * 24, takeover=True, fragments=(1, 2), control="mixed"
        )
        neg_json = json.dumps(
            {
                "permessage_deflate": True,
                "server_no_context_takeover": False,
                "server_max_window_bits": 15,
            }
        )
        with tempfile.TemporaryDirectory() as d:
            apath = Path(d) / "archive.frames"
            apath.write_bytes(data)
            write_index(index, d, apath)
            ids = [23, 0, 11, 17, 5]
            proc = subprocess.run(
                [sys.executable, "-c", INLINE, str(apath), d, ",".join(map(str, ids)), neg_json],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONPATH": str(ROOT)},
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            payload = json.loads(proc.stdout)
            expected = [hashlib.sha256(msgs[i]).hexdigest() for i in ids]
            self.assertEqual(payload["digests"], expected)
            self.assertEqual(payload["stats"]["requested"], len(ids))

    def test_build_in_one_process_query_in_another(self):
        msgs = [bytes([i % 256]) * 4096 for i in range(10)]
        data, manifest, index, spec = build(msgs, [True] * 10, takeover=True)
        neg_json = json.dumps(
            {
                "permessage_deflate": True,
                "server_no_context_takeover": False,
                "server_max_window_bits": 15,
            }
        )
        with tempfile.TemporaryDirectory() as d:
            apath = Path(d) / "archive.frames"
            apath.write_bytes(data)
            npath = Path(d) / "neg.json"
            npath.write_text(neg_json)
            build_proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "wsarchive",
                    "build",
                    "--archive",
                    str(apath),
                    "--negotiation",
                    str(npath),
                    "--index-dir",
                    str(Path(d) / "idx"),
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONPATH": str(ROOT)},
            )
            self.assertEqual(build_proc.returncode, 0, build_proc.stderr)
            query_proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "wsarchive",
                    "query",
                    "--archive",
                    str(apath),
                    "--negotiation",
                    str(npath),
                    "--index-dir",
                    str(Path(d) / "idx"),
                    "--ids",
                    "9,3,3,0",
                    "--mode",
                    "batch",
                    "--json",
                ],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONPATH": str(ROOT)},
            )
            self.assertEqual(query_proc.returncode, 0, query_proc.stderr)
            payload = json.loads(query_proc.stdout)
            self.assertEqual(
                [m["sha256"] for m in payload["messages"]],
                [hashlib.sha256(msgs[i]).hexdigest() for i in [9, 3, 3, 0]],
            )


if __name__ == "__main__":
    unittest.main()
