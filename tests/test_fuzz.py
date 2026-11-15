"""Randomized differential testing over the legal input family.

For many random configurations the reader's random access must agree with a
straight sequential decode of the same wire bytes, and with the generator's
ground truth.  This is property-based rather than example-based.
"""

import hashlib
import random
import unittest
import zlib

from helpers import build, digests, neg

from wsarchive import Reader
from wsarchive.corpus import Spec, generate


def _random_payload(rng, size):
    kind = rng.choice(["uniform", "repeat", "runs", "text", "mixed"])
    if kind == "uniform":
        return bytes(rng.getrandbits(8) for _ in range(size))
    if kind == "repeat":
        return (bytes(range(256)) * (size // 256 + 1))[:size]
    if kind == "runs":
        out = bytearray()
        while len(out) < size:
            out += bytes([rng.getrandbits(8)]) * rng.randint(1, 64)
        return bytes(out[:size])
    if kind == "text":
        words = [b"alpha ", b"beta ", b"gamma ", b"delta ", b"epsilon "]
        return (b"".join(rng.choice(words) for _ in range(size // 3 + 1)))[:size]
    return bytes(rng.getrandbits(8) for _ in range(size // 2)) + bytes(size - size // 2)


class FuzzTests(unittest.TestCase):
    def test_random_configurations(self):
        rng = random.Random(20240101)
        for case in range(25):
            n = rng.randint(2, 20)
            take = rng.choice([True, False])
            wbits = rng.choice([9, 10, 12, 15])
            level = rng.choice([0, 1, 6, 9])
            strategy = rng.choice(
                [zlib.Z_DEFAULT_STRATEGY, zlib.Z_FIXED, zlib.Z_RLE, zlib.Z_HUFFMAN_ONLY]
            )
            control = rng.choice(["none", "between", "interleaved", "mixed"])
            fragments = tuple(rng.randint(1, 6) for _ in range(rng.randint(1, 3)))
            msgs = []
            for i in range(n):
                size = rng.choice([0, 1, rng.randint(2, 200), rng.randint(200, 9000)])
                msgs.append(_random_payload(rng, size))
            compressed = [rng.random() < 0.75 for _ in range(n)]
            if not any(compressed):
                compressed[0] = True
            data, manifest, index, spec = build(
                msgs,
                compressed,
                takeover=take,
                window_bits=wbits,
                level=level,
                strategy=strategy,
                fragments=fragments,
                control=control,
            )
            reader = Reader(data, index)
            self.assertEqual(
                digests(reader.decode_sequential()),
                digests(msgs),
                msg=f"case {case} sequential",
            )
            ids = [rng.randrange(n) for _ in range(rng.randint(1, 3 * n))]
            cfg = (
                f"take={take} wbits={wbits} level={level} "
                f"strategy={strategy} control={control} fragments={fragments}"
            )
            for mode in ("cold", "batch"):
                out = reader.query(ids, mode=mode)
                self.assertEqual(
                    [hashlib.sha256(m).hexdigest() for m in out.messages],
                    [hashlib.sha256(msgs[i]).hexdigest() for i in ids],
                    msg=f"case {case} {cfg} mode={mode}",
                )

    def test_control_frames_do_not_shift_message_ids(self):
        rng = random.Random(99)
        msgs = [b"M%03d" % i + bytes([i]) * rng.randint(0, 500) for i in range(15)]
        for control in ("none", "between", "interleaved", "mixed"):
            data, manifest, index, spec = build(
                msgs, [True] * 15, takeover=True, fragments=(3,), control=control
            )
            reader = Reader(data, index)
            for i in range(15):
                self.assertEqual(
                    reader.query([i], mode="cold").messages[0], msgs[i],
                    msg=f"control={control} id={i}",
                )


if __name__ == "__main__":
    unittest.main()
