"""Framing, message assembly and index-binding invariants."""

import json
import unittest

from helpers import build, digests, fixture_paths, load_fixture_neg, neg, truth_by_id

from wsarchive import Reader, build_index
from wsarchive.errors import (
    ArchiveBindingError,
    FrameError,
    NegotiationError,
    PartialMessageError,
)
from wsarchive.model import Negotiation
from wsarchive.wire import parse_archive


class FrameParsingTests(unittest.TestCase):
    def test_control_frames_are_not_messages(self):
        msgs = [b"alpha" * 100, b"beta" * 100]
        data, manifest, index, _ = build(
            msgs, [True, True], fragments=(2,), control="interleaved"
        )
        self.assertEqual(len(manifest), 2)
        self.assertEqual(len(index.messages), 2)
        # each message has 2 data fragments plus an interleaved control frame
        self.assertEqual(len(index.frames), 2 * (2 + 1))
        self.assertEqual(
            index.messages[0].frame_indices, [0, 2]
        )  # frame 1 is the control frame
        self.assertEqual(index.frames[1].opcode, 9)

    def test_rsv1_only_on_first_data_frame(self):
        msgs = [b"x" * 300]
        data, manifest, index, _ = build(
            msgs, [True], fragments=(3,), control="none"
        )
        live = [f for f in index.frames if f.opcode in (2, 0)]
        self.assertTrue(live[0].rsv1)
        self.assertFalse(any(f.rsv1 for f in live[1:]))

    def test_rsv1_on_continuation_is_rejected(self):
        # Hand-craft: first frame FIN=0 no RSV1, continuation with RSV1.
        from wsarchive.corpus import frame

        wire = frame(b"abc", opcode=2, fin=False, rsv1=False) + frame(
            b"def", opcode=0, fin=True, rsv1=True
        )
        with self.assertRaises(FrameError):
            parse_archive(wire, neg(True), None)

    def test_new_data_frame_while_open_is_rejected(self):
        from wsarchive.corpus import frame

        wire = frame(b"abc", opcode=2, fin=False) + frame(b"def", opcode=2, fin=True)
        with self.assertRaises(FrameError):
            parse_archive(wire, neg(True), None)

    def test_masked_frame_rejected(self):
        # Server direction is unmasked by contract.
        b0 = 0x82
        body = b"hello"
        masked = bytes([b0, 0x80 | len(body)]) + b"\x00\x00\x00\x00" + body
        with self.assertRaises(FrameError):
            parse_archive(masked, neg(True), None)

    def test_truncated_frame_header_and_body(self):
        from wsarchive.corpus import frame

        good = frame(b"abc", opcode=2)
        with self.assertRaises(FrameError):
            parse_archive(good[:-1], neg(True), None)  # truncated body
        with self.assertRaises(FrameError):
            parse_archive(b"\x82", neg(True), None)  # truncated header

    def test_truncated_message_without_fin_is_rejected(self):
        from wsarchive.corpus import frame

        wire = frame(b"abc", opcode=2, fin=False)
        with self.assertRaises(FrameError):
            parse_archive(wire, neg(True), None)

    def test_text_message_rejected_as_unsupported(self):
        from wsarchive.corpus import frame

        wire = frame(b"not binary", opcode=1)
        with self.assertRaises(FrameError):
            parse_archive(wire, neg(True), None)


class BindingTests(unittest.TestCase):
    def test_archive_hash_binding(self):
        msgs = [b"payload-%d" % i * 50 for i in range(8)]
        data, manifest, index, spec = build(msgs, [True] * 8)
        reader = Reader(data, index)
        reader.query([0])  # works
        tampered = bytearray(data)
        # flip a byte inside a compressed body, not the frame headers
        tampered[30] ^= 0xFF
        with self.assertRaises(ArchiveBindingError):
            Reader(bytes(tampered), index)

    def test_negotiation_binding(self):
        msgs = [b"z" * 200 for _ in range(5)]
        data, manifest, index, spec = build(msgs, [True] * 5, takeover=True)
        wrong = neg(takeover=False)
        from wsarchive.index import negotiation_digest

        self.assertNotEqual(negotiation_digest(wrong), index.negotiation_sha256)
        # Loading with mismatched parameters must be refused.
        from wsarchive import load_index

        import tempfile
        from wsarchive.index import write_index
        from pathlib import Path

        with tempfile.TemporaryDirectory() as d:
            write_index(index, d, "x.frames")
            with self.assertRaises(ArchiveBindingError):
                load_index(d, wrong)

    def test_fixture_archive_binding(self):
        (apath, _, tpath) = fixture_paths()
        data = apath.read_bytes()
        index = build_index(data, load_fixture_neg())
        reader = Reader(data, index)
        self.assertEqual(len(reader.query([99]).messages[0]), 4096)


class MessageAssemblyTests(unittest.TestCase):
    def test_cross_fragment_reconstruction(self):
        msgs = [bytes(range(256)) * 20, b"second-" * 500]
        data, manifest, index, _ = build(
            msgs, [True, True], fragments=(7,), control="mixed"
        )
        reader = Reader(data, index)
        out = reader.query([0, 1], mode="cold")
        self.assertEqual(digests(out.messages), digests(msgs))
        self.assertGreater(index.messages[0].fragment_count, 1)

    def test_uncompressed_messages_do_not_enter_history(self):
        # If an uncompressed message were fed to the inflater, later compressed
        # messages would either fail or decode differently.
        msgs = [b"A" * 4096, b"B" * 4096, b"C" * 4096]
        data, manifest, index, _ = build(
            msgs, [True, False, True], takeover=True, fragments=(2,), control="mixed"
        )
        reader = Reader(data, index)
        self.assertEqual(digests(reader.query([2], mode="cold").messages), digests([msgs[2]]))
        self.assertEqual(
            digests(reader.decode_sequential()), digests(msgs)
        )

    def test_empty_messages(self):
        msgs = [b"", b"data" * 10, b"", b"more" * 10]
        data, manifest, index, _ = build(
            msgs, [True, True, False, True], takeover=True, fragments=(1,)
        )
        reader = Reader(data, index)
        for i in range(4):
            self.assertEqual(
                digests(reader.query([i], mode="cold").messages), digests([msgs[i]])
            )
        self.assertEqual(digests(reader.decode_sequential()), digests(msgs))

    def test_flush_boundaries_are_not_independent_streams(self):
        # Long shared history forces back-references across message boundaries.
        base = bytes(range(256)) * 16  # 4096
        msgs = [base + bytes([i]) * 64 for i in range(10)]
        data, manifest, index, _ = build(
            msgs, [True] * 10, takeover=True, fragments=(1,), control="none"
        )
        import zlib

        from wsarchive.codec import MARKER, message_body

        isolated_correct = 0
        for i in range(1, 10):  # message 0 legitimately starts a fresh stream
            body = message_body(data, index.messages[i])
            try:
                got = zlib.decompressobj(-15).decompress(body + MARKER)
            except zlib.error:
                continue
            if got == msgs[i]:
                isolated_correct += 1
        # All later messages depend on prior window history; decoding each as
        # its own stream either raises or yields the wrong bytes.
        self.assertEqual(isolated_correct, 0)
        reader = Reader(data, index)
        self.assertEqual(digests(reader.decode_sequential()), digests(msgs))


if __name__ == "__main__":
    unittest.main()
