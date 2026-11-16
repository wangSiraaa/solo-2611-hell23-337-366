"""Full scenario suite for the sparse archive.

An independent bytearray oracle applies the same ops one by one; the
archive is checked against it on legal ranges, on full restores, on actual
read amplification, and on total persistent storage.
"""
from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sparse_archive import (
    Archive, Oracle, DeltaDecl, Op, DATA, HOLE, BLOCK_SIZE,
    MAX_FILE_BYTES, MAX_GENERATIONS,
    AncestorError, DeltaConflictError, DigestMismatchError,
    RangeRequestError, ReaderLimitError, RetentionError, ValidationError,
    WriterBusyError,
)
from sparse_archive.model import sha256_hex, zero_digest


def make_source(path: Path, size: int, writes: list[tuple[int, bytes]],
                sparse: bool = True) -> bytes:
    """Create a frozen source image; return its logical content."""
    view = bytearray(size)
    for off, data in writes:
        view[off:off + len(data)] = data
    with open(path, "wb") as f:
        if sparse:
            for off, data in writes:
                if data.strip(b"\x00"):
                    f.seek(off)
                    f.write(data)
            f.truncate(size)
        else:
            f.write(bytes(view))
    return bytes(view)


def disk_bytes(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            total += (Path(root) / name).stat().st_size
    return total


class ArchiveTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="sparse-archive-")
        self.t = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def ingest(self, arch: Archive, content: bytes,
               sparse_writes: list[tuple[int, bytes]] | None = None) -> None:
        src = self.t / "source.img"
        if sparse_writes is None:
            src.write_bytes(content)
        else:
            make_source(src, len(content), sparse_writes)
        arch.ingest_base(src)


# ---------------------------------------------------------------------------
class TestIngestAndRead(ArchiveTestBase):

    def test_ingest_sparse_base_and_ranges(self):
        arch = Archive.create(self.t / "a")
        writes = [(0, b"HEAD" + b"A" * 4092),
                  (3 * BLOCK_SIZE, b"MID" + bytes(4093)),
                  (6 * BLOCK_SIZE + 17, b"TAIL")]
        size = 8 * BLOCK_SIZE + 100
        content = make_source(self.t / "src.img", size, writes)
        arch.ingest_base(self.t / "src.img")
        # the source was written sparsely: zero runs must be hole segments
        idx = json.loads((arch.path / "generations/g000001/index.json")
                         .read_text())
        kinds = [s["type"] for s in idx["segments"]]
        self.assertIn(HOLE, kinds)
        self.assertIn(DATA, kinds)
        self.assertLess(idx["payload_bytes"], size)  # holes not stored
        with arch.open_reader() as r:
            for s, e in [(0, 4), (0, size), (4096, 4100), (3 * BLOCK_SIZE, 3 * BLOCK_SIZE + 3),
                         (6 * BLOCK_SIZE + 17, 6 * BLOCK_SIZE + 21),
                         (size - 1, size), (100, 100)]:
                got = r.read_range(s, e)["data"]
                self.assertEqual(got, content[s:e], f"range [{s},{e})")

    def test_oversize_source_rejected(self):
        arch = Archive.create(self.t / "a")
        src = self.t / "big.img"
        with open(src, "wb") as f:
            f.truncate(MAX_FILE_BYTES + 1)  # sparse: instant
        with self.assertRaises(ValidationError):
            arch.ingest_base(src)

    def test_empty_and_out_of_bounds_ranges(self):
        arch = Archive.create(self.t / "a")
        self.ingest(arch, b"X" * 5000)
        with arch.open_reader() as r:
            self.assertEqual(r.read_range(100, 100)["data"], b"")
            self.assertEqual(r.read_range(0, 0)["data"], b"")
            self.assertEqual(r.read_range(5000, 5000)["data"], b"")
            for s, e in [(-1, 5), (5, 4), (0, 5001), (4999, 5001)]:
                with self.assertRaises(RangeRequestError):
                    r.read_range(s, e)

    def test_empty_file_generation(self):
        arch = Archive.create(self.t / "a")
        self.ingest(arch, b"")
        oracle = Oracle()
        oracle.ingest_base(b"")
        oracle.apply_delta(3000, [])  # pure extend: all holes
        arch.apply_delta(DeltaDecl(1, 2, 3000, [], oracle.block_digests()))
        self.assertEqual(arch.read_range(2, 0, 3000)["data"], bytes(3000))
        self.assertTrue(arch.verify()["ok"])


# ---------------------------------------------------------------------------
class TestDeltaSemantics(ArchiveTestBase):

    def _base_with(self, arch, oracle, size, writes):
        content = make_source(self.t / "src.img", size, writes)
        arch.ingest_base(self.t / "src.img")
        oracle.ingest_base(content)
        return content

    def _apply(self, arch, oracle, gen, new_size, ops):
        oracle.apply_delta(new_size, ops)
        decl = DeltaDecl(gen, gen + 1, new_size, ops, oracle.block_digests())
        return arch.apply_delta(decl)

    def test_real_data_to_hole(self):
        """The baseline defect: a HOLE op must erase old data, semantically."""
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        self._base_with(arch, oracle, 4 * BLOCK_SIZE,
                        [(0, b"AAAA" + bytes(4092)),
                         (2 * BLOCK_SIZE, b"BBBB" + bytes(4092))])
        self._apply(arch, oracle, 1, 4 * BLOCK_SIZE, [Op(HOLE, 0, BLOCK_SIZE)])
        got = arch.read_range(2, 0, 4)["data"]
        self.assertEqual(got, bytes(4))          # not the old b"AAAA"
        self.assertEqual(arch.read_range(1, 0, 4)["data"], b"AAAA")  # old gen intact
        self.assertTrue(arch.verify()["ok"])
        # new generation stores no payload for the hole
        idx = json.loads((arch.path / "generations/g000002/index.json").read_text())
        self.assertEqual(idx["payload_bytes"], 0)
        self.assertEqual([s["type"] for s in idx["segments"]], [HOLE])

    def test_hole_to_all_zero_data_stays_data(self):
        """Explicit all-zero DATA is data: recorded as data, kept by compact."""
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        self._base_with(arch, oracle, 4 * BLOCK_SIZE, [])  # all holes
        self._apply(arch, oracle, 1, 4 * BLOCK_SIZE,
                    [Op(DATA, BLOCK_SIZE, 2 * BLOCK_SIZE, bytes(BLOCK_SIZE))])
        idx = json.loads((arch.path / "generations/g000002/index.json").read_text())
        self.assertEqual(idx["segments"][0]["type"], DATA)  # honest mapping
        self.assertEqual(idx["payload_bytes"], BLOCK_SIZE)
        self.assertEqual(arch.read_range(2, BLOCK_SIZE, BLOCK_SIZE + 8)["data"],
                         bytes(8))
        # force compaction onto generation 2 (pin it so it is the oldest kept)
        with arch.open_reader(generation=2):
            self._apply(arch, oracle, 2, 4 * BLOCK_SIZE,
                        [Op(DATA, 0, 8, b"Z" * 8)])
            res = arch.compact()
            self.assertEqual(res["rebased_generation"], 2)
        snap = json.loads((arch.path / "generations/g000002s1/index.json")
                          .read_text())
        seg = [s for s in snap["segments"]
               if s["start"] <= BLOCK_SIZE and s["end"] >= 2 * BLOCK_SIZE]
        self.assertEqual(len(seg), 1)
        self.assertEqual(seg[0]["type"], DATA)  # still data after compact
        payload = (arch.path / "generations/g000002s1/payload.bin").read_bytes()
        self.assertEqual(
            payload[seg[0]["payload_offset"]:seg[0]["payload_offset"] + BLOCK_SIZE],
            bytes(BLOCK_SIZE))
        self.assertTrue(arch.verify()["ok"])

    def test_cross_block_last_write_wins(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        self._base_with(arch, oracle, 4 * BLOCK_SIZE,
                        [(0, b"A" * 8192)])  # spans blocks 0-1
        ops = [
            Op(DATA, 4000, 5000, b"1" * 1000),       # crosses block boundary
            Op(HOLE, 4500, 5500),                    # later hole wins part of it
            Op(DATA, 5000, 5200, b"2" * 200),        # later data wins again
            Op(HOLE, 0, 100),                        # and earlier range too
        ]
        self._apply(arch, oracle, 1, 4 * BLOCK_SIZE, ops)
        with arch.open_reader() as r:
            for s in [0, 3999, 4000, 4499, 4500, 4999, 5000, 5199, 5200, 8191]:
                e = min(s + 37, 4 * BLOCK_SIZE)
                self.assertEqual(r.read_range(s, e)["data"], oracle.read(s, e),
                                 f"range [{s},{e})")
        self.assertTrue(arch.verify()["ok"])

    def test_shrink_then_extend_does_not_resurrect(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        content = self._base_with(arch, oracle, 4 * BLOCK_SIZE,
                                  [(0, b"A" * 4096),
                                   (BLOCK_SIZE, b"B" * 4096),
                                   (2 * BLOCK_SIZE, b"C" * 4096)])
        self._apply(arch, oracle, 1, BLOCK_SIZE, [])   # truncate to 1 block
        self.assertEqual(arch.read_range(2, 0, BLOCK_SIZE)["data"],
                         content[:BLOCK_SIZE])
        self._apply(arch, oracle, 2, 3 * BLOCK_SIZE, [])  # extend again
        got = arch.read_range(3, BLOCK_SIZE, 3 * BLOCK_SIZE)["data"]
        self.assertEqual(got, bytes(2 * BLOCK_SIZE))  # B/C blocks stay dead
        self.assertTrue(arch.verify()["ok"])

    def test_shrink_extend_unaligned_sizes(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        self._base_with(arch, oracle, 5000, [(0, b"Q" * 5000)])
        self._apply(arch, oracle, 1, 1234, [])
        self._apply(arch, oracle, 2, 9999, [Op(DATA, 9000, 9100, b"R" * 100)])
        with arch.open_reader() as r:
            for s, e in [(0, 100), (1200, 1300), (4000, 4100), (5000, 5001),
                         (8999, 9101), (9998, 9999)]:
                self.assertEqual(r.read_range(s, e)["data"], oracle.read(s, e))
        self.assertTrue(arch.verify()["ok"])

    def test_pure_resize_and_empty_ops(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        self._base_with(arch, oracle, 8192, [(0, b"A" * 100)])
        ops = [Op(DATA, 100, 100, b"")]  # empty half-open op: legal no-op
        self._apply(arch, oracle, 1, 8192, ops)
        self.assertEqual(arch.read_range(2, 0, 200)["data"], oracle.read(0, 200))

    def test_delta_out_of_bounds_rejected(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        self._base_with(arch, oracle, 8192, [])
        oracle.apply_delta(8192, [])
        digests = oracle.block_digests()
        for ops in [[Op(DATA, 8000, 8193, b"x" * 193)],   # beyond new_size
                    [Op(HOLE, 8192, 9000)],
                    [Op(DATA, 10, 20, b"short")]]:          # payload length
            with self.assertRaises(ValidationError):
                arch.apply_delta(DeltaDecl(1, 2, 8192, ops, digests))
        with self.assertRaises(ValidationError):  # too many bytes overall
            arch.apply_delta(DeltaDecl(1, 2, MAX_FILE_BYTES + 1, [],
                                       [zero_digest(BLOCK_SIZE)] * 0))
        self.assertEqual(arch.status()["tip"], 1)  # nothing published


# ---------------------------------------------------------------------------
class TestAncestorsAndIdempotency(ArchiveTestBase):

    def _three_generations(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        make_source(self.t / "src.img", 4 * BLOCK_SIZE, [(0, b"A" * 4096)])
        arch.ingest_base(self.t / "src.img")
        oracle.ingest_base(b"A" * 4096 + bytes(3 * BLOCK_SIZE))
        decls = []
        for gen, ops in [(1, [Op(DATA, BLOCK_SIZE, BLOCK_SIZE + 4, b"BBBB")]),
                         (2, [Op(HOLE, 0, BLOCK_SIZE)])]:
            oracle.apply_delta(4 * BLOCK_SIZE, ops)
            d = DeltaDecl(gen, gen + 1, 4 * BLOCK_SIZE, ops,
                          oracle.block_digests())
            arch.apply_delta(d)
            decls.append(d)
        return arch, oracle, decls

    def test_wrong_ancestor_rejected(self):
        arch, oracle, decls = self._three_generations()
        # base generation does not exist at all
        with self.assertRaises(AncestorError):
            arch.apply_delta(DeltaDecl(4, 5, 4 * BLOCK_SIZE, [],
                                       oracle.block_digests()))
        # non-linear numbering
        with self.assertRaises(ValidationError):
            arch.apply_delta(DeltaDecl(3, 5, 4 * BLOCK_SIZE, [],
                                       oracle.block_digests()))
        # digests computed against a different ancestor's content
        evil = Oracle()
        evil.ingest_base(b"X" * 4096 + bytes(3 * BLOCK_SIZE))
        evil.apply_delta(4 * BLOCK_SIZE, [])
        with self.assertRaises(DigestMismatchError):
            arch.apply_delta(DeltaDecl(3, 4, 4 * BLOCK_SIZE, [],
                                       evil.block_digests()))
        self.assertEqual(arch.status()["tip"], 3)

    def test_reclaimed_ancestor_rejected(self):
        arch, oracle, decls = self._three_generations()
        arch.compact()  # rebases gen 2, reclaims gen 1
        self.assertEqual(arch.status()["stored_generations"], [2, 3])
        # base generation was reclaimed: not a legal ancestor any more
        with self.assertRaises(AncestorError):
            arch.apply_delta(DeltaDecl(0, 1, 4 * BLOCK_SIZE, [],
                                       oracle.block_digests()))
        # redeclaring an existing target with different content also fails
        with self.assertRaises(DeltaConflictError):
            arch.apply_delta(DeltaDecl(1, 2, 4 * BLOCK_SIZE, [],
                                       oracle.block_digests()))

    def test_idempotent_same_target(self):
        arch, oracle, decls = self._three_generations()
        before = disk_bytes(arch.path)
        again = arch.apply_delta(decls[-1])  # same target, same content
        self.assertTrue(again["idempotent"])
        self.assertEqual(arch.status()["tip"], 3)
        self.assertEqual(disk_bytes(arch.path), before)  # no rewrite

    def test_same_target_different_content_rejected(self):
        arch, oracle, decls = self._three_generations()
        other = Oracle()
        other.ingest_base(b"A" * 4096 + bytes(3 * BLOCK_SIZE))
        other.apply_delta(4 * BLOCK_SIZE, [Op(DATA, BLOCK_SIZE, BLOCK_SIZE + 4, b"BBBB")])
        other.apply_delta(4 * BLOCK_SIZE, [Op(DATA, 0, 4, b"DIFF")])
        with self.assertRaises(DeltaConflictError):
            arch.apply_delta(DeltaDecl(2, 3, 4 * BLOCK_SIZE,
                                       [Op(DATA, 0, 4, b"DIFF")],
                                       other.block_digests()))
        self.assertEqual(arch.status()["tip"], 3)


# ---------------------------------------------------------------------------
class TestCorruption(ArchiveTestBase):

    def _archive(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        make_source(self.t / "src.img", 4 * BLOCK_SIZE,
                    [(0, b"A" * 4096), (2 * BLOCK_SIZE, b"C" * 4096)])
        arch.ingest_base(self.t / "src.img")
        oracle.ingest_base(b"A" * 4096 + bytes(BLOCK_SIZE) + b"C" * 4096
                           + bytes(BLOCK_SIZE))
        return arch, oracle

    def test_staged_payload_corruption_rejected(self):
        arch, oracle = self._archive()
        ops = [Op(DATA, BLOCK_SIZE, 2 * BLOCK_SIZE, b"B" * BLOCK_SIZE)]
        oracle.apply_delta(4 * BLOCK_SIZE, ops)
        bad = list(oracle.block_digests())
        bad[1] = zero_digest(BLOCK_SIZE)  # lie about block 1
        with self.assertRaises(DigestMismatchError):
            arch.apply_delta(DeltaDecl(1, 2, 4 * BLOCK_SIZE, ops, bad))
        self.assertEqual(arch.status()["tip"], 1)
        self.assertEqual(list((arch.path / "generations").iterdir()).__len__(), 1)
        self.assertEqual(list((arch.path / "tmp").iterdir()), [])
        self.assertTrue(arch.verify()["ok"])

    def test_committed_payload_corruption_detected(self):
        arch, oracle = self._archive()
        payload = arch.path / "generations/g000001/payload.bin"
        original = payload.read_bytes()
        flipped = bytearray(original)
        flipped[0] ^= 0xFF
        payload.write_bytes(bytes(flipped))
        # a partial read of an untouched block still works...
        self.assertEqual(arch.read_range(1, 2 * BLOCK_SIZE, 2 * BLOCK_SIZE + 1)["data"],
                         b"C")
        # ...but the corrupted block is caught, and only full verify proves all
        with self.assertRaises(DigestMismatchError):
            arch.read_range(1, 0, 4)
        report = arch.verify()
        self.assertFalse(report["ok"])
        payload.write_bytes(original)  # repair
        self.assertTrue(arch.verify()["ok"])

    def test_truncated_payload_detected(self):
        arch, oracle = self._archive()
        payload = arch.path / "generations/g000001/payload.bin"
        with open(payload, "r+b") as f:
            f.truncate(100)
        report = arch.verify()
        self.assertFalse(report["ok"])


# ---------------------------------------------------------------------------
class TestCrossGenerationReads(ArchiveTestBase):

    def test_retention_and_pins(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        history = {}
        make_source(self.t / "src.img", 2 * BLOCK_SIZE, [(0, b"A" * 100)])
        arch.ingest_base(self.t / "src.img")
        oracle.ingest_base(b"A" * 100 + bytes(2 * BLOCK_SIZE - 100))
        history[1] = oracle.snapshot()
        for gen in range(1, 5):  # generations 2..5
            ops = [Op(DATA, gen * 100, gen * 100 + 4,
                      ("%d" % gen).encode() * 4)]
            oracle.apply_delta(2 * BLOCK_SIZE, ops)
            arch.apply_delta(DeltaDecl(gen, gen + 1, 2 * BLOCK_SIZE, ops,
                                       oracle.block_digests()))
            history[gen + 1] = oracle.snapshot()
        with arch.open_reader() as r:  # pins tip (5)
            # tip and tip-1 readable
            self.assertEqual(r.read_range(0, 200, generation=5)["data"],
                             history[5][:200])
            self.assertEqual(r.read_range(0, 200, generation=4)["data"],
                             history[4][:200])
            # older generations are not readable without a pin
            with self.assertRaises(RetentionError):
                r.read_range(0, 4, generation=3)
        with arch.open_reader(generation=2) as pinned:
            self.assertEqual(pinned.read_range(0, 500)["data"],
                             history[2][:500])
            # a second reader can coexist and read the tip
            with arch.open_reader() as r2:
                self.assertEqual(r2.read_range(400, 404)["data"],
                                 history[5][400:404])
                # a third reader exceeds the controlled limit
                with self.assertRaises(ReaderLimitError):
                    arch.open_reader()
        # after the pin is released, gen 2 is again not readable
        with arch.open_reader() as r:
            with self.assertRaises(RetentionError):
                r.read_range(0, 4, generation=2)


# ---------------------------------------------------------------------------
class TestCompaction(ArchiveTestBase):

    def _build(self, n=5):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        history = {}
        make_source(self.t / "src.img", 6 * BLOCK_SIZE,
                    [(0, b"A" * 4096), (5 * BLOCK_SIZE, b"F" * 4096)])
        arch.ingest_base(self.t / "src.img")
        oracle.ingest_base(b"A" * 4096 + bytes(4 * BLOCK_SIZE) + b"F" * 4096)
        history[1] = oracle.snapshot()
        for gen in range(1, n):
            ops = [Op(DATA, gen * BLOCK_SIZE, gen * BLOCK_SIZE + 8,
                      str(gen).encode() * 8),
                   Op(HOLE, (gen % 3) * BLOCK_SIZE, (gen % 3) * BLOCK_SIZE + 16)]
            oracle.apply_delta(6 * BLOCK_SIZE, ops)
            arch.apply_delta(DeltaDecl(gen, gen + 1, 6 * BLOCK_SIZE, ops,
                                       oracle.block_digests()))
            history[gen + 1] = oracle.snapshot()
        return arch, oracle, history

    def test_compact_detaches_ancestors(self):
        arch, oracle, history = self._build(5)
        before = arch.storage_stats()
        res = arch.compact()
        self.assertEqual(res["rebased_generation"], 4)  # oldest of {4,5}
        self.assertEqual(arch.status()["stored_generations"], [4, 5])
        # reclaimed generation directories are really gone
        dirs = sorted(p.name for p in (arch.path / "generations").iterdir())
        self.assertEqual(len(dirs), 2)
        self.assertTrue(any(d.startswith("g000004s") for d in dirs))
        # views of the survivors are byte-identical
        self.assertEqual(arch.read_range(4, 0, 6 * BLOCK_SIZE)["data"], history[4])
        self.assertEqual(arch.read_range(5, 0, 6 * BLOCK_SIZE)["data"], history[5])
        self.assertTrue(arch.verify()["ok"])
        after = arch.storage_stats()
        self.assertLess(after["total_bytes"], before["total_bytes"])
        self.assertEqual(after["total_bytes"], disk_bytes(arch.path))

    def test_compacted_payload_has_no_all_hole_blocks(self):
        arch, oracle, history = self._build(5)
        arch.compact()
        snap_dir = [p for p in (arch.path / "generations").iterdir()
                    if "s" in p.name][0]
        idx = json.loads((snap_dir / "index.json").read_text())
        size = idx["logical_size"]
        for b in range((size + BLOCK_SIZE - 1) // BLOCK_SIZE):
            bs, be = b * BLOCK_SIZE, min((b + 1) * BLOCK_SIZE, size)
            data_segs = [s for s in idx["segments"]
                         if s["type"] == DATA and s["start"] < be and s["end"] > bs]
            block_content = history[4][bs:be]
            if not data_segs:
                # protocol-wise all-hole block: no payload, content is zeros
                self.assertEqual(block_content, bytes(be - bs))
            else:
                # mixed blocks keep an honest sub-block data/hole map
                hole_segs = [s for s in idx["segments"]
                             if s["type"] == HOLE and s["start"] < be and s["end"] > bs]
                if hole_segs:
                    for hs in hole_segs:
                        s, e = max(hs["start"], bs), min(hs["end"], be)
                        self.assertEqual(history[4][s:e], bytes(e - s))

    def test_compact_interruption_before_switch(self):
        arch, oracle, history = self._build(5)
        calls = {"n": 0}

        def boom(info):
            calls["n"] += 1
            raise RuntimeError("simulated crash before manifest switch")

        arch.hooks["after_compact_built"] = boom
        with self.assertRaises(RuntimeError):
            arch.compact()
        # nothing changed: all generations intact, no leftover staging
        self.assertEqual(arch.status()["stored_generations"], [1, 2, 3, 4, 5])
        self.assertEqual(list((arch.path / "tmp").iterdir()), [])
        reopened = Archive.open(self.t / "a")
        self.assertEqual(reopened.status()["stored_generations"], [1, 2, 3, 4, 5])
        self.assertTrue(reopened.verify()["ok"])
        self.assertEqual(reopened.read_range(3, 0, 100)["data"], history[3][:100])

    def test_compact_interruption_before_reclaim(self):
        arch, oracle, history = self._build(5)

        def boom(info):
            if info["event"] == "before_reclaim":
                raise RuntimeError("simulated crash after switch")

        arch.hooks["before_reclaim"] = boom
        with self.assertRaises(RuntimeError):
            arch.compact()
        # manifest switched, reclaim pending: consistent either way
        reopened = Archive.open(self.t / "a")
        self.assertEqual(reopened.status()["stored_generations"], [4, 5])
        self.assertTrue(reopened.status()["pending_reclaim"])
        self.assertTrue(reopened.verify()["ok"])
        self.assertEqual(reopened.read_range(4, 0, 6 * BLOCK_SIZE)["data"],
                         history[4])
        # a later compact finishes the interrupted reclaim
        res = reopened.compact()
        self.assertTrue(res["reclaimed"])
        self.assertEqual(reopened.status()["pending_reclaim"], [])
        dirs = sorted(p.name for p in (reopened.path / "generations").iterdir())
        self.assertEqual(len(dirs), 2)

    def test_reader_pin_blocks_reclaim_until_released(self):
        arch, oracle, history = self._build(5)
        with arch.open_reader(generation=2) as pinned:
            res = arch.compact()
            # oldest needed is the pinned gen 2: rebase 2, reclaim gen 1 only
            self.assertEqual(res["rebased_generation"], 2)
            self.assertEqual(arch.status()["stored_generations"], [2, 3, 4, 5])
            self.assertEqual(pinned.read_range(0, 6 * BLOCK_SIZE)["data"],
                             history[2])
            # even a second compact cannot reclaim what the pin covers
            arch.compact()
            self.assertEqual(pinned.read_range(0, 6 * BLOCK_SIZE)["data"],
                             history[2])
        # pin released: now reclaim can proceed past generation 2
        arch.compact()
        stored = arch.status()["stored_generations"]
        self.assertEqual(stored, [4, 5])
        self.assertTrue(arch.verify()["ok"])

    def test_compact_failure_publishes_no_half_generation(self):
        arch, oracle, history = self._build(3)

        def boom(info):
            raise RuntimeError("crash after data written, before index switch")

        arch.hooks["after_delta_data_written"] = boom
        ops = [Op(DATA, 0, 4, b"qqqq")]
        oracle.apply_delta(6 * BLOCK_SIZE, ops)
        with self.assertRaises(RuntimeError):
            arch.apply_delta(DeltaDecl(3, 4, 6 * BLOCK_SIZE, ops,
                                       oracle.block_digests()))
        self.assertEqual(arch.status()["tip"], 3)
        self.assertEqual(list((arch.path / "tmp").iterdir()), [])
        reopened = Archive.open(self.t / "a")
        self.assertEqual(reopened.status()["tip"], 3)
        self.assertTrue(reopened.verify()["ok"])
        # the delta can be retried cleanly after the failure
        reopened.hooks.clear()
        reopened.apply_delta(DeltaDecl(3, 4, 6 * BLOCK_SIZE, ops,
                                       oracle.block_digests()))
        self.assertEqual(reopened.status()["tip"], 4)


# ---------------------------------------------------------------------------
class TestConcurrency(ArchiveTestBase):

    def _two_gens(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        make_source(self.t / "src.img", 2 * BLOCK_SIZE, [(0, b"A" * 4096)])
        arch.ingest_base(self.t / "src.img")
        oracle.ingest_base(b"A" * 4096 + bytes(BLOCK_SIZE))
        return arch, oracle

    def test_writer_pause_readers_see_only_committed(self):
        arch, oracle = self._two_gens()
        written = threading.Event()
        release = threading.Event()
        errors = []

        def pause(info):
            written.set()
            if not release.wait(timeout=30):
                errors.append("writer timed out waiting for release")

        arch.hooks["after_delta_data_written"] = pause
        ops = [Op(HOLE, 0, BLOCK_SIZE)]
        oracle.apply_delta(2 * BLOCK_SIZE, ops)
        decl = DeltaDecl(1, 2, 2 * BLOCK_SIZE, ops, oracle.block_digests())
        result = {}

        def writer():
            try:
                result["res"] = arch.apply_delta(decl)
            except Exception as exc:  # pragma: no cover
                errors.append(repr(exc))

        th = threading.Thread(target=writer)
        th.start()
        self.assertTrue(written.wait(timeout=30))
        # data written, index not switched: readers see only generation 1
        with arch.open_reader() as r1, arch.open_reader() as r2:
            self.assertEqual(r1.pin_generation, 1)
            self.assertEqual(r1.read_range(0, 4)["data"], b"AAAA")
            self.assertEqual(r2.read_range(0, 4)["data"], b"AAAA")
            with self.assertRaises(RetentionError):
                r1.read_range(0, 4, generation=2)  # half generation invisible
            with self.assertRaises(ReaderLimitError):
                arch.open_reader()
        release.set()
        th.join(timeout=30)
        self.assertFalse(errors)
        self.assertEqual(result["res"]["generation"], 2)
        # new readers now see the complete target generation
        with arch.open_reader() as r3:
            self.assertEqual(r3.pin_generation, 2)
            self.assertEqual(r3.read_range(0, 4)["data"], bytes(4))

    def test_single_writer_enforced(self):
        arch, oracle = self._two_gens()
        written = threading.Event()
        release = threading.Event()

        def pause(info):
            written.set()
            release.wait(timeout=30)

        arch.hooks["after_delta_data_written"] = pause
        ops = [Op(HOLE, 0, BLOCK_SIZE)]
        oracle.apply_delta(2 * BLOCK_SIZE, ops)
        decl = DeltaDecl(1, 2, 2 * BLOCK_SIZE, ops, oracle.block_digests())
        th = threading.Thread(target=lambda: arch.apply_delta(decl))
        th.start()
        self.assertTrue(written.wait(timeout=30))
        # a second writer operation is refused while the first is paused
        with self.assertRaises(WriterBusyError):
            arch.compact()
        with self.assertRaises(WriterBusyError):
            arch.verify()
        release.set()
        th.join(timeout=30)
        self.assertEqual(arch.status()["tip"], 2)

    def test_compact_pause_old_reader_continues(self):
        arch, oracle = self._two_gens()
        for gen, byte in [(1, b"B"), (2, b"C")]:
            ops = [Op(DATA, BLOCK_SIZE, BLOCK_SIZE + 4, byte * 4)]
            oracle.apply_delta(2 * BLOCK_SIZE, ops)
            arch.apply_delta(DeltaDecl(gen, gen + 1, 2 * BLOCK_SIZE, ops,
                                       oracle.block_digests()))
        paused = threading.Event()
        release = threading.Event()

        def pause(info):
            if info["event"] == "before_reclaim":
                paused.set()
                release.wait(timeout=30)

        arch.hooks["before_reclaim"] = pause
        result = {}

        def compactor():
            result["res"] = arch.compact()

        # reader pins generation 2; its resolution reaches into generation 1
        with arch.open_reader(generation=2) as old_reader:
            th = threading.Thread(target=compactor)
            th.start()
            self.assertTrue(paused.wait(timeout=30))
            # compact switched the manifest, reclaim is paused: the old
            # reader keeps reading its generation through the old ancestors
            self.assertEqual(old_reader.read_range(0, 4)["data"], b"AAAA")
            self.assertEqual(old_reader.read_range(BLOCK_SIZE, BLOCK_SIZE + 4)["data"],
                             b"BBBB")
            release.set()
            th.join(timeout=30)
            # the pin still protects generation 1's directory from reclaim
            self.assertTrue(
                (arch.path / "generations/g000001").exists())
        # pin released: reclaim may now collect the old ancestors
        arch.reclaim()
        self.assertFalse((arch.path / "generations/g000001").exists())
        self.assertEqual(arch.status()["stored_generations"], [2, 3])
        self.assertTrue(arch.verify()["ok"])


# ---------------------------------------------------------------------------
class TestMetering(ArchiveTestBase):

    def test_read_amplification_and_categories(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        size = 64 * BLOCK_SIZE
        make_source(self.t / "src.img", size,
                    [(10 * BLOCK_SIZE, b"D" * BLOCK_SIZE)])
        arch.ingest_base(self.t / "src.img")
        oracle.ingest_base(bytes(10 * BLOCK_SIZE) + b"D" * BLOCK_SIZE
                           + bytes(size - 11 * BLOCK_SIZE))
        ops = [Op(DATA, 20 * BLOCK_SIZE, 21 * BLOCK_SIZE, b"E" * BLOCK_SIZE)]
        oracle.apply_delta(size, ops)
        arch.apply_delta(DeltaDecl(1, 2, size, ops, oracle.block_digests()))
        with arch.open_reader() as r:
            # 4-byte read inside a data block of generation 2
            res = r.read_range(20 * BLOCK_SIZE + 3, 20 * BLOCK_SIZE + 7)
            m = res["meter"]
            self.assertEqual(res["data"], b"EEEE")
            self.assertEqual(res["blocks"], [20])
            self.assertEqual(m["own_payload_bytes"], BLOCK_SIZE)  # one block only
            self.assertEqual(m["ancestor_payload_bytes"], 0)
            self.assertGreater(m["metadata_bytes"], 0)
            # read amplification: a 4-byte request touched exactly one block
            self.assertEqual(m["own_payload_bytes"] + m["ancestor_payload_bytes"]
                             + m["synthesized_zero_bytes"], BLOCK_SIZE)
            for ext in m["payload_extents"]:
                self.assertGreaterEqual(ext["offset"] + ext["length"], 0)
            # hole block: zeros are synthesized, nothing is read from disk
            res2 = r.read_range(40 * BLOCK_SIZE, 40 * BLOCK_SIZE + 4)
            m2 = res2["meter"]
            self.assertEqual(m2["synthesized_zero_bytes"], BLOCK_SIZE)
            self.assertEqual(m2["own_payload_bytes"], 0)
            self.assertEqual(m2["ancestor_payload_bytes"], 0)
            # ancestor payload: block 10 lives in generation 1
            res3 = r.read_range(10 * BLOCK_SIZE, 10 * BLOCK_SIZE + 4)
            m3 = res3["meter"]
            self.assertEqual(m3["ancestor_payload_bytes"], BLOCK_SIZE)
            self.assertEqual(m3["own_payload_bytes"], 0)
            # cache: repeating the same block is served from cache
            res4 = r.read_range(10 * BLOCK_SIZE + 1, 10 * BLOCK_SIZE + 5)
            m4 = res4["meter"]
            self.assertEqual(m4["cache_bytes"], BLOCK_SIZE)
            self.assertEqual(m4["ancestor_payload_bytes"], 0)
            # extents actually read stay inside the intersected blocks
            res5 = r.read_range(11 * BLOCK_SIZE - 1, 11 * BLOCK_SIZE + 1)
            self.assertEqual(res5["blocks"], [10, 11])
            for ext in res5["meter"]["payload_extents"]:
                self.assertLess(ext["offset"] + ext["length"],
                                12 * BLOCK_SIZE)

    def test_storage_stats_categories(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        make_source(self.t / "src.img", 4 * BLOCK_SIZE, [(0, b"A" * 4096)])
        arch.ingest_base(self.t / "src.img")
        oracle.ingest_base(b"A" * 4096 + bytes(3 * BLOCK_SIZE))
        for gen, byte in [(1, b"B"), (2, b"C")]:
            ops = [Op(DATA, gen * BLOCK_SIZE, (gen + 1) * BLOCK_SIZE,
                      byte * BLOCK_SIZE)]
            oracle.apply_delta(4 * BLOCK_SIZE, ops)
            arch.apply_delta(DeltaDecl(gen, gen + 1, 4 * BLOCK_SIZE, ops,
                                       oracle.block_digests()))
        stats = arch.storage_stats()
        self.assertGreater(stats["base_payload_bytes"], 0)
        self.assertGreater(stats["delta_payload_bytes"], 0)
        self.assertGreater(stats["index_bytes"], 0)
        self.assertGreater(stats["manifest_bytes"], 0)
        self.assertEqual(stats["temp_bytes"], 0)
        self.assertEqual(stats["total_bytes"], disk_bytes(arch.path))
        # during a paused compact, the staged snapshot counts as temp bytes
        seen = {}

        def peek(info):
            if info["event"] == "after_compact_built":
                seen["temp"] = arch.storage_stats()["temp_bytes"]

        arch.hooks["after_compact_built"] = peek
        res = arch.compact()
        self.assertEqual(res["rebased_generation"], 2)  # compact had work
        self.assertGreater(seen["temp"], 0)
        self.assertEqual(arch.storage_stats()["temp_bytes"], 0)
        self.assertEqual(arch.storage_stats()["total_bytes"],
                         disk_bytes(arch.path))


# ---------------------------------------------------------------------------
class TestGenerationLimit(ArchiveTestBase):

    def test_twenty_generation_limit_and_compact_relief(self):
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        make_source(self.t / "src.img", BLOCK_SIZE, [(0, b"A" * 10)])
        arch.ingest_base(self.t / "src.img")
        oracle.ingest_base(b"A" * 10 + bytes(BLOCK_SIZE - 10))
        gen = 1
        while gen < MAX_GENERATIONS:
            ops = [Op(DATA, gen % 50, gen % 50 + 1, bytes([gen % 256]))]
            oracle.apply_delta(BLOCK_SIZE, ops)
            arch.apply_delta(DeltaDecl(gen, gen + 1, BLOCK_SIZE, ops,
                                       oracle.block_digests()))
            gen += 1
        ops = [Op(DATA, 0, 1, b"z")]
        oracle.apply_delta(BLOCK_SIZE, ops)
        with self.assertRaises(ValidationError):
            arch.apply_delta(DeltaDecl(gen, gen + 1, BLOCK_SIZE, ops,
                                       oracle.block_digests()))
        arch.compact()  # frees chain room
        arch.apply_delta(DeltaDecl(gen, gen + 1, BLOCK_SIZE, ops,
                                   oracle.block_digests()))
        self.assertEqual(arch.status()["tip"], gen + 1)


# ---------------------------------------------------------------------------
class TestOracleRandomSequence(ArchiveTestBase):

    def test_random_sequence_against_oracle(self):
        rng = random.Random(20261115)
        arch = Archive.create(self.t / "a")
        oracle = Oracle()
        history: dict[int, bytes] = {}

        size = rng.randint(0, 10 * BLOCK_SIZE)
        content = bytearray(size)
        for _ in range(rng.randint(0, 6)):
            if size:
                off = rng.randrange(size)
                ln = min(rng.randint(1, 2 * BLOCK_SIZE), size - off)
                content[off:off + ln] = rng.randbytes(ln)
        src = self.t / "src.img"
        src.write_bytes(bytes(content))
        arch.ingest_base(src)
        oracle.ingest_base(bytes(content))
        history[1] = oracle.snapshot()

        def battery(gen: int) -> None:
            view = history[gen]
            n = len(view)
            points = {0, n}
            for b in range(n // BLOCK_SIZE + 2):
                for d in (-1, 0, 1):
                    p = b * BLOCK_SIZE + d
                    if 0 <= p <= n:
                        points.add(p)
            for _ in range(12):
                points.add(rng.randrange(n + 1) if n else 0)
            pts = sorted(points)
            pairs = [(a, b) for a in pts for b in pts if a <= b]
            rng.shuffle(pairs)
            with arch.open_reader(generation=gen) as r:
                for a, b in pairs[:220]:
                    got = r.read_range(a, b)["data"]
                    self.assertEqual(got, view[a:b],
                                     f"gen {gen} range [{a},{b})")
                    m = r.read_range(a, b)["meter"]
                    assembled = (m["own_payload_bytes"] + m["ancestor_payload_bytes"]
                                 + m["cache_bytes"] + m["synthesized_zero_bytes"])
                    if a == b:
                        self.assertEqual(assembled, 0)
                    else:
                        nb = (b - 1) // BLOCK_SIZE - a // BLOCK_SIZE + 1
                        self.assertLessEqual(assembled, nb * BLOCK_SIZE)

        gen = 1
        for step in range(26):
            # build a random delta
            new_size = rng.choice([
                rng.randrange(0, 12 * BLOCK_SIZE),
                rng.randrange(0, 12 * BLOCK_SIZE),
                len(oracle.view),
                max(0, len(oracle.view) - rng.randrange(0, 4 * BLOCK_SIZE)),
                len(oracle.view) + rng.randrange(0, 4 * BLOCK_SIZE),
            ])
            ops = []
            for _ in range(rng.randint(0, 6)):
                if new_size == 0:
                    break
                a = rng.randrange(new_size)
                b = min(new_size, a + rng.randint(1, 3 * BLOCK_SIZE))
                if rng.random() < 0.35:
                    ops.append(Op(HOLE, a, b))
                else:
                    if rng.random() < 0.25:
                        payload = bytes(b - a)  # explicit all-zero DATA
                    else:
                        payload = rng.randbytes(b - a)
                    ops.append(Op(DATA, a, b, payload))
            oracle.apply_delta(new_size, ops)
            decl = DeltaDecl(gen, gen + 1, new_size, ops, oracle.block_digests())
            arch.apply_delta(decl)
            gen += 1
            history[gen] = oracle.snapshot()

            # idempotent re-apply of the same delta
            self.assertTrue(arch.apply_delta(decl)["idempotent"])

            battery(gen)
            # full restore byte-for-byte
            out = self.t / f"restore-{gen}.img"
            arch.restore(gen, out)
            self.assertEqual(out.read_bytes(), history[gen])
            out.unlink()

            self.assertTrue(arch.verify()["ok"])
            self.assertEqual(arch.storage_stats()["total_bytes"],
                             disk_bytes(arch.path))

            # occasionally compact, sometimes with a pin held
            if gen >= 4 and rng.random() < 0.4:
                stored = arch.status()["stored_generations"]
                pin_gen = rng.choice(stored) if rng.random() < 0.5 else None
                if pin_gen is None:
                    arch.compact()
                else:
                    with arch.open_reader(generation=pin_gen) as pinned:
                        arch.compact()
                        self.assertEqual(
                            pinned.read_range(0, len(history[pin_gen]))["data"],
                            history[pin_gen])
                self.assertTrue(arch.verify()["ok"])
                for g in arch.status()["readable_generations"]:
                    self.assertEqual(
                        arch.read_range(g, 0, len(history[g]))["data"],
                        history[g])
                self.assertEqual(arch.storage_stats()["total_bytes"],
                                 disk_bytes(arch.path))
            if gen >= MAX_GENERATIONS:
                arch.compact()

        # final: every readable generation matches the oracle everywhere
        for g, view in history.items():
            if g in arch.status()["readable_generations"]:
                self.assertEqual(arch.read_range(g, 0, len(view))["data"], view)


# ---------------------------------------------------------------------------
class TestCli(ArchiveTestBase):

    def test_cli_end_to_end(self):
        arch_dir = self.t / "cli-arch"
        src = self.t / "src.img"
        make_source(src, 4 * BLOCK_SIZE, [(0, b"A" * 4096)])

        def run(*argv, expect=0):
            proc = subprocess.run(
                [sys.executable, "-m", "sparse_archive.cli", *argv],
                cwd=Path(__file__).resolve().parent.parent,
                capture_output=True, text=True)
            if expect == 0 and proc.returncode != 0:
                self.fail(f"cli {argv} failed: {proc.stderr}")
            self.assertEqual(proc.returncode, expect, proc.stderr)
            return proc.stdout

        run("ingest-base", str(arch_dir), str(src))
        oracle = Oracle()
        oracle.ingest_base(b"A" * 4096 + bytes(3 * BLOCK_SIZE))
        ops = [{"type": "hole", "start": 0, "end": BLOCK_SIZE},
               {"type": "data", "start": 2 * BLOCK_SIZE,
                "end": 3 * BLOCK_SIZE, "payload_offset": 0}]
        payload = b"C" * BLOCK_SIZE
        (self.t / "payload.bin").write_bytes(payload)
        oracle.apply_delta(4 * BLOCK_SIZE,
                           [Op(HOLE, 0, BLOCK_SIZE),
                            Op(DATA, 2 * BLOCK_SIZE, 3 * BLOCK_SIZE, payload)])
        decl = {"base_generation": 1, "target_generation": 2,
                "new_size": 4 * BLOCK_SIZE, "ops": ops,
                "block_digests": oracle.block_digests()}
        (self.t / "decl.json").write_text(json.dumps(decl))
        run("apply-delta", str(arch_dir), str(self.t / "decl.json"),
            "--payload", str(self.t / "payload.bin"))
        out = json.loads(run("read-range", str(arch_dir), "2", "0", "4"))
        self.assertEqual(out["data_hex"], "00000000")
        self.assertTrue(json.loads(run("verify", str(arch_dir)))["ok"])
        run("compact", str(arch_dir))
        stats = json.loads(run("stats", str(arch_dir)))
        self.assertEqual(stats["total_bytes"], disk_bytes(arch_dir))
        run("restore", str(arch_dir), "2", str(self.t / "restored.img"))
        self.assertEqual((self.t / "restored.img").read_bytes(),
                         oracle.snapshot())
        # CLI surfaces rejections as non-zero exits
        run("read-range", str(arch_dir), "2", "0", "99999999", expect=2)


if __name__ == "__main__":
    unittest.main()
