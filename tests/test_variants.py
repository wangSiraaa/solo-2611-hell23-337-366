"""Same legal input family, varied shapes: the index must not be fixture-specific."""

import hashlib
import random
import unittest
import zlib

from helpers import build, digests, neg

from wsarchive import Reader
from wsarchive.corpus import Spec, generate, workload_messages


def _check(test, msgs, compressed, **kwargs):
    data, manifest, index, spec = build(msgs, compressed, **kwargs)
    reader = Reader(data, index)
    test.assertEqual(digests(reader.decode_sequential()), digests(msgs))
    ids = list(range(len(msgs)))
    random.Random(7).shuffle(ids)
    for mode in ("cold", "batch"):
        got = reader.query(ids, mode=mode)
        test.assertEqual(
            [hashlib.sha256(m).hexdigest() for m in got.messages],
            [hashlib.sha256(msgs[i]).hexdigest() for i in ids],
            f"mode={mode}",
        )
    return data, index, reader


class VariantTests(unittest.TestCase):
    def test_window_bits_9_through_15(self):
        msgs = [bytes(range(256)) * 8 + bytes([i]) * 50 for i in range(20)]
        for wbits in (9, 10, 12, 14, 15):
            data, index, reader = _check(
                self, msgs, [True] * 20, takeover=True, window_bits=wbits
            )
            self.assertEqual(index.negotiation.window_bits, wbits)
            # Dictionary capture never exceeds the negotiated window.
            for cp in index.checkpoints:
                self.assertLessEqual(cp.dict_len, 1 << wbits)

    def test_levels_and_strategies(self):
        base = bytes(range(256)) * 8
        for level, strategy in [
            (1, zlib.Z_DEFAULT_STRATEGY),
            (9, zlib.Z_DEFAULT_STRATEGY),
            (6, zlib.Z_FIXED),
            (6, zlib.Z_HUFFMAN_ONLY),
            (0, zlib.Z_DEFAULT_STRATEGY),
        ]:
            msgs = [base + bytes([i]) * 100 for i in range(12)]
            _check(
                self,
                msgs,
                [True] * 12,
                takeover=True,
                level=level,
                strategy=strategy,
                fragments=(2,),
            )

    def test_many_fragments_and_interleaved_control(self):
        msgs = [workload_messages(1, 4096)[0] + bytes([i]) for i in range(30)]
        _check(
            self,
            msgs,
            [True] * 30,
            takeover=True,
            fragments=(10,),
            control="mixed",
        )

    def test_uncompressed_interleaving(self):
        msgs = [b"msg-%03d-" % i * 300 for i in range(30)]
        compressed = [i % 4 != 1 for i in range(30)]
        data, index, reader = _check(
            self, msgs, compressed, takeover=True, fragments=(1, 3), control="mixed"
        )
        self.assertFalse(index.messages[1].compressed)
        # Querying an uncompressed message does not inflate anything.
        st = reader.query([1], mode="cold").stats
        self.assertEqual(st.inflated_bytes, 0)
        self.assertEqual(st.raw_message_bytes, len(msgs[1]))

    def test_no_takeover_window_variants(self):
        msgs = [(b"z" * 1000) + bytes([i]) for i in range(15)]
        for wbits in (9, 12, 15):
            data, index, reader = _check(
                self,
                msgs,
                [True] * 15,
                takeover=False,
                window_bits=wbits,
                fragments=(1, 2),
            )
            self.assertFalse(index.negotiation.context_takeover)

    def test_empty_and_tiny_mixed(self):
        msgs = [b"", b"x", b"", b"hello" * 100, b"", b"y" * 5000]
        compressed = [True, True, True, True, False, True]
        _check(
            self,
            msgs,
            compressed,
            takeover=True,
            fragments=(1, 2, 5),
            control="mixed",
        )

    def test_single_fragment_messages_only(self):
        msgs = [bytes([i]) * 2000 for i in range(10)]
        _check(self, msgs, [True] * 10, takeover=True, fragments=(1,), control="none")

    def test_large_messages_near_limit(self):
        msgs = [bytes(range(256)) * 255 + bytes([i]) for i in range(8)]  # ~64 KiB
        for i, m in enumerate(msgs):
            self.assertLessEqual(len(m), 64 * 1024)
        _check(
            self,
            msgs,
            [True] * 8,
            takeover=True,
            fragments=(10,),
            control="mixed",
        )


class DensityTradeoffTests(unittest.TestCase):
    def test_denser_checkpoints_reduce_cold_inflation_but_cost_state(self):
        msgs = workload_messages(100, 4096)
        from wsarchive.corpus import Spec, generate

        spec = Spec(msgs, [True] * 100, fragments=(1,))
        data, _ = generate(spec)
        from wsarchive import build_index

        results = []
        for density in (1, 5, 13, 50, 100):
            index = build_index(data, neg(True), density=density)
            reader = Reader(data, index)
            worst = max(
                reader.query([i], mode="cold").stats.inflated_bytes
                for i in range(100)
            )
            results.append((density, index.dictionary_state_bytes, worst))
        # A larger interval (coarser grid) never inflates less per cold query,
        # and never uses more permanent state.
        for (d1, s1, w1), (d2, s2, w2) in zip(results, results[1:]):
            self.assertLessEqual(w1, w2)
            self.assertGreaterEqual(s1, s2)
        # The extremes demonstrate the trade-off concretely.
        self.assertLess(results[0][2], results[-1][2])
        self.assertGreater(results[0][1], results[-1][1])


if __name__ == "__main__":
    unittest.main()
