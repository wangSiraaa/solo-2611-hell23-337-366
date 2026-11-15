"""Damage detection: corrupted bytes, truncated tails, partial messages."""

import hashlib
import unittest

from helpers import build, digests, neg

from wsarchive import Reader, build_index
from wsarchive.codec import message_body
from wsarchive.errors import (
    ArchiveBindingError,
    CorruptStreamError,
    FrameError,
    PartialMessageError,
)


class CorruptionAfterCheckpointTests(unittest.TestCase):
    def _build_with_density(self, msgs, density):
        from wsarchive.corpus import Spec, generate

        spec = Spec(msgs, [True] * len(msgs), takeover=True, fragments=(2,), control="none")
        data, manifest = generate(spec)
        index = build_index(data, neg(True), density=density)
        return data, index

    def test_corrupt_body_after_checkpoint_fails_cleanly(self):
        msgs = [bytes(range(256)) * 16 + bytes([i]) * 200 for i in range(40)]
        data, index = self._build_with_density(msgs, density=5)
        target = 23  # checkpoint at 20, so target is after a checkpoint
        cp = index.checkpoint_for(target)
        self.assertEqual(cp.message_index, 20)
        msg = index.messages[target]
        off, ln = msg.body_ranges[0]
        tampered = bytearray(data)
        tampered[off + max(1, ln // 2)] ^= 0xFF
        # Whole-archive hash is disabled so the finer per-message guard runs.
        reader = Reader(bytes(tampered), index, verify_archive_hash=False)
        with self.assertRaises((CorruptStreamError, PartialMessageError)):
            reader.query([target], mode="cold")
        # A different, undamaged message still decodes from its own checkpoint.
        other = 21
        out = reader.query([other], mode="cold").messages
        self.assertEqual(
            hashlib.sha256(out[0]).hexdigest(),
            hashlib.sha256(msgs[other]).hexdigest(),
        )

    def test_corrupt_bytes_in_prefix_are_detected(self):
        msgs = [bytes(range(256)) * 16 + bytes([i]) * 200 for i in range(40)]
        data, index = self._build_with_density(msgs, density=5)
        target = 24  # checkpoint at 20; 22 is a prefix message
        off, ln = index.messages[22].body_ranges[0]
        tampered = bytearray(data)
        tampered[off + 2] ^= 0xFF
        reader = Reader(bytes(tampered), index, verify_archive_hash=False)
        with self.assertRaises((CorruptStreamError, PartialMessageError)):
            reader.query([target], mode="cold")

    def test_whole_archive_hash_guard(self):
        msgs = [b"q" * 500 for _ in range(10)]
        data, index = self._build_with_density(msgs, density=2)
        tampered = bytearray(data)
        tampered[len(tampered) // 2] ^= 0xFF
        with self.assertRaises(ArchiveBindingError):
            Reader(bytes(tampered), index)

    def test_no_partial_output_on_failure(self):
        msgs = [bytes(range(256)) * 16 + bytes([i]) * 200 for i in range(20)]
        data, index = self._build_with_density(msgs, density=3)
        # Truncate a message body semantically by damaging its final block.
        off, ln = index.messages[10].body_ranges[0]
        tampered = bytearray(data)
        tampered[off + ln - 2] ^= 0xFF
        reader = Reader(bytes(tampered), index, verify_archive_hash=False)
        try:
            result = reader.query([10], mode="cold")
        except (CorruptStreamError, PartialMessageError):
            pass
        else:  # pragma: no cover - a successful decode must be fully correct
            self.assertEqual(
                hashlib.sha256(result.messages[0]).hexdigest(),
                hashlib.sha256(msgs[10]).hexdigest(),
            )


class TruncatedTailTests(unittest.TestCase):
    def test_truncated_final_frame_rejected_at_build(self):
        from wsarchive.corpus import Spec, generate

        msgs = [b"a" * 300, b"b" * 300]
        data, _ = generate(Spec(msgs, [True, True], fragments=(1,)))
        truncated = data[:-10]
        with self.assertRaises(FrameError):
            build_index(truncated, neg(True))

    def test_truncated_archive_after_index_rejected(self):
        msgs = [b"a" * 300, b"b" * 300]
        data, manifest, index, spec = build(msgs, [True, True], fragments=(1,))
        with self.assertRaises(ArchiveBindingError):
            Reader(data[:-10], index)

    def test_unterminated_message_rejected(self):
        from wsarchive.corpus import frame

        wire = frame(b"abc", opcode=2, fin=False) + frame(b"de", opcode=0, fin=False)
        with self.assertRaises(FrameError):
            build_index(wire, neg(True))


if __name__ == "__main__":
    unittest.main()
