"""Random-access correctness, ordering, duplicate and accounting invariants."""

import hashlib
import random
import unittest
from pathlib import Path

from helpers import build, digests, fixture_paths, load_fixture_neg, neg

from wsarchive import Reader, build_index
from wsarchive.corpus import Spec, generate, workload_messages


class RandomAccessTests(unittest.TestCase):
    def setUp(self):
        random.seed(1234)
        self.msgs = [
            (bytes(range(256)) * 32)[: 4096 - (i % 7) * 100] + bytes([i]) * 100
            for i in range(60)
        ]
        self.data, self.manifest, self.index, self.spec = build(
            self.msgs, [True] * 60, takeover=True, fragments=(1, 2, 3), control="mixed"
        )
        self.reader = Reader(self.data, self.index)
        self.want = {i: hashlib.sha256(m).hexdigest() for i, m in enumerate(self.msgs)}

    def test_every_single_id_cold_matches_truth(self):
        for i in range(60):
            out = self.reader.query([i], mode="cold").messages
            self.assertEqual(hashlib.sha256(out[0]).hexdigest(), self.want[i])

    def test_arbitrary_order_and_duplicates(self):
        ids = [59, 0, 42, 42, 7, 0, 13, 58, 59, 1]
        result = self.reader.query(ids, mode="batch")
        self.assertEqual(len(result.messages), len(ids))
        for mid, m in zip(ids, result.messages):
            self.assertEqual(hashlib.sha256(m).hexdigest(), self.want[mid])
        self.assertEqual(result.stats.requested, len(ids))
        self.assertEqual(result.stats.unique_requested, len(set(ids)))
        # duplicate output accounting counts every *repeated* occurrence
        expected_dupes = sum(
            self.index.messages[i].declared_length
            for i in ids
            if ids.count(i) > 1
            and ids.index(i) != len(ids) - 1 - ids[::-1].index(i)
        )
        expected_dupes = sum(
            self.index.messages[i].declared_length
            for pos, i in enumerate(ids)
            if i in ids[:pos]
        )
        self.assertEqual(result.stats.duplicate_output_bytes, expected_dupes)

    def test_output_order_follows_request_not_sorted(self):
        ids = [30, 3, 17, 2, 44]
        out = self.reader.query(ids, mode="batch").messages
        self.assertEqual(
            [hashlib.sha256(m).hexdigest() for m in out], [self.want[i] for i in ids]
        )

    def test_cold_and_batch_equivalent(self):
        ids = list(range(60))
        random.shuffle(ids)
        cold = self.reader.query(ids, mode="cold")
        batch = self.reader.query(ids, mode="batch")
        for a, b in zip(cold.messages, batch.messages):
            self.assertEqual(a, b)
        self.assertLessEqual(batch.stats.inflated_bytes, cold.stats.inflated_bytes)

    def test_cold_duplicates_are_decoded_each_time(self):
        ids = [37, 37, 37]
        cold = self.reader.query(ids, mode="cold")
        single = self.reader.query([37], mode="cold")
        self.assertEqual(
            cold.stats.inflated_bytes, 3 * single.stats.inflated_bytes,
            "cold mode must not silently share work between duplicate ids",
        )
        batch = self.reader.query(ids, mode="batch")
        self.assertEqual(batch.stats.inflated_bytes, single.stats.inflated_bytes)
        # Every occurrence still appears in the output.
        self.assertEqual(len(batch.messages), 3)
        self.assertEqual(batch.stats.output_bytes, 3 * len(msgs := batch.messages[0]))

    def test_repeated_query_counts_again(self):
        r = self.reader
        first = r.query([37], mode="cold").stats
        second = r.query([37], mode="cold").stats
        self.assertGreater(first.inflated_bytes, 0)
        self.assertEqual(first.inflated_bytes, second.inflated_bytes)

    def test_only_output_and_prefix_are_counted(self):
        q = self.reader.query([37], mode="cold")
        st = q.stats
        # inflated == output + discarded prefix (both compressed messages)
        self.assertEqual(
            st.inflated_bytes, st.output_bytes + st.prefix_inflated_bytes
        )
        self.assertGreater(st.prefix_inflated_bytes, 0)
        self.assertEqual(st.output_bytes, len(q.messages[0]))

    def test_fixture_all_ids(self):
        apath, _, tpath = fixture_paths()
        data = apath.read_bytes()
        index = build_index(data, load_fixture_neg())
        reader = Reader(data, index)
        truth = {
            m["message"]: m["sha256"] for m in __import__("json").loads(tpath.read_text())
        }
        for i in range(100):
            out = reader.query([i], mode="cold")
            self.assertEqual(
                hashlib.sha256(out.messages[0]).hexdigest(), truth[i]
            )

    def test_out_of_range_id(self):
        from wsarchive.errors import ArchiveError

        with self.assertRaises(ArchiveError):
            self.reader.query([60])
        with self.assertRaises(ArchiveError):
            self.reader.query([-1])


class NoTakeoverTests(unittest.TestCase):
    def test_no_context_takeover_random_access(self):
        msgs = [(b"block-%03d-" % i) * 200 for i in range(40)]
        data, manifest, index, spec = build(
            msgs, [True] * 40, takeover=False, fragments=(1, 4), control="mixed"
        )
        self.assertFalse(index.negotiation.context_takeover)
        self.assertEqual(len(index.checkpoints), 1)
        reader = Reader(data, index)
        for i in (0, 39, 7, 20):
            q = reader.query([i], mode="cold")
            self.assertEqual(
                hashlib.sha256(q.messages[0]).hexdigest(),
                hashlib.sha256(msgs[i]).hexdigest(),
            )
            # no prefix replay: only the requested message's plaintext
            self.assertEqual(q.stats.prefix_inflated_bytes, 0)
            self.assertEqual(q.stats.inflated_bytes, len(msgs[i]))

    def test_no_takeover_and_takeover_differ_in_history(self):
        # Under takeover, message 5 needs prior context; under no-takeover it
        # must be self-contained. The corpus generator emits the matching wire.
        msgs = [bytes(range(256)) * 16 for _ in range(10)]
        d1, _, i1, _ = build(msgs, [True] * 10, takeover=True)
        d2, _, i2, _ = build(msgs, [True] * 10, takeover=False)
        self.assertNotEqual(d1, d2)
        r1, r2 = Reader(d1, i1), Reader(d2, i2)
        self.assertEqual(digests(r1.decode_sequential()), digests(msgs))
        self.assertEqual(digests(r2.decode_sequential()), digests(msgs))


class BudgetTests(unittest.TestCase):
    def test_workload_budgets(self):
        msgs = workload_messages(100, 4096)
        data, manifest, index, spec = build(msgs, [True] * 100, takeover=True)
        reader = Reader(data, index)
        self.assertLessEqual(index.dictionary_state_bytes, 256 * 1024)
        self.assertLessEqual(index.metadata_bytes, 64 * 1024)
        total = 0
        worst = 0
        body_total = 0
        for i in range(100):
            st = reader.query([i], mode="cold").stats
            total += st.inflated_bytes
            worst = max(worst, st.inflated_bytes)
            body_total += st.compressed_body_bytes
        # The 64 KiB limit bounds each isolated single-message query: prefix
        # replay plus the returned plaintext, all actually inflated.
        self.assertLessEqual(worst, 64 * 1024)
        # Expected total: each cold query inflates its checkpoint interval,
        # i.e. every message from the checkpoint through the target.
        expected = 0
        for i in range(100):
            cp = index.checkpoint_for(i)
            expected += sum(
                index.messages[j].declared_length
                for j in range(cp.message_index, i + 1)
                if index.messages[j].compressed
            )
        self.assertEqual(total, expected)
        self.assertGreater(body_total, 0)
        # repeated decompression is counted, not cached
        again = reader.query([99], mode="cold").stats.inflated_bytes
        self.assertEqual(again, reader.query([99], mode="cold").stats.inflated_bytes)

    def test_batch_savings_reported_and_positive(self):
        msgs = workload_messages(100, 4096)
        data, manifest, index, spec = build(msgs, [True] * 100, takeover=True)
        reader = Reader(data, index)
        ids = list(range(100))
        cold = reader.query(ids, mode="cold")
        batch = reader.query(ids, mode="batch")
        self.assertGreater(batch.savings_bytes, 0)
        self.assertEqual(
            batch.savings_bytes,
            cold.stats.inflated_bytes - batch.stats.inflated_bytes,
        )
        self.assertEqual(len(batch.messages), 100)

    def test_no_plaintext_bypass_across_queries(self):
        # A second identical cold query must inflate the same bytes again: the
        # implementation must not have secretly cached message plaintext.
        msgs = workload_messages(100, 4096)
        data, manifest, index, spec = build(msgs, [True] * 100, takeover=True)
        reader = Reader(data, index)
        a = reader.query([73], mode="cold").stats
        b = reader.query([73], mode="cold").stats
        self.assertEqual(a.inflated_bytes, b.inflated_bytes)
        self.assertEqual(a.compressed_body_bytes, b.compressed_body_bytes)
        self.assertGreater(a.inflated_bytes, len(msgs[73]))
        # The index contains only bounded deflate-window checkpoints, never
        # per-message plaintext: its total size is far below the plaintext
        # total, and a message larger than the negotiated window cannot occur
        # in it.
        from wsarchive.index import serialize

        blob = serialize(index)
        self.assertLess(len(blob), 100 * 4096)
        big_msgs = [bytes(range(256)) * 200 + bytes([i]) for i in range(10)]  # ~51 KiB
        big_data, _, big_index, _ = build(big_msgs, [True] * 10, takeover=True)
        big_blob = serialize(big_index)
        for m in big_msgs:
            self.assertNotIn(m, big_blob)
        self.assertLess(len(big_blob), sum(map(len, big_msgs)))

    def test_batch_reuses_context_not_plaintext_cache(self):
        # The batch saving is exactly the prefix that a per-checkpoint shared
        # replay avoids; it is still inflate work that other queries repeat.
        msgs = workload_messages(40, 4096)
        data, manifest, index, spec = build(msgs, [True] * 40, takeover=True)
        reader = Reader(data, index)
        ids = list(range(40))
        batch = reader.query(ids, mode="batch")
        cold = reader.query(ids, mode="cold")
        # Batch inflated each message exactly once across shared contexts.
        self.assertEqual(batch.stats.inflated_bytes, 40 * 4096)
        # cold repeats checkpoint prefixes.
        self.assertGreater(cold.stats.inflated_bytes, batch.stats.inflated_bytes)


if __name__ == "__main__":
    unittest.main()
