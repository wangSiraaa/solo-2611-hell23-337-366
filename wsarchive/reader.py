"""Random message access over an indexed archive.

Cold query algorithm (context takeover)
---------------------------------------
1. Find the greatest checkpoint whose message index is <= the target id.
2. Create one raw inflater seeded with that checkpoint's saved window
   (``zdict``).  An empty saved window means resume as a fresh stream.
3. Decode messages from the checkpoint forward, always with the *same*
   inflater, appending the synthetic ``Z_SYNC_FLUSH`` tail to each message's
   compressed body.  Earlier messages are materialised only to rebuild history
   and are then discarded, but every inflated byte is counted.
4. Verify each decoded message against its recorded length and sha256 *before*
   returning anything.  A failure raises; a partial message is never returned.

Batch queries group requested ids by checkpoint and replay each group once,
sharing the rebuilt prefix inside the group.  Output is reassembled in the
caller's original order.  If an id is repeated, its plaintext is materialised
once but the output bytes are counted for every occurrence.
"""

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .codec import (
    DecodeCounter,
    decode_compressed,
    decode_uncompressed,
    make_inflater,
    message_body,
    verify_message,
)
from .errors import (
    ArchiveBindingError,
    ArchiveError,
    CheckpointError,
    CorruptStreamError,
)
from .index import ArchiveIndex, Checkpoint, load_index, negotiation_digest
from .model import MessageRecord, Negotiation


@dataclass
class QueryResult:
    messages: List[bytes]
    stats: DecodeCounter
    mode: str
    cold_unique_inflated: int = 0
    savings_bytes: int = 0

    def report(self) -> dict:
        d = self.stats.as_dict()
        d.update(
            {
                "mode": self.mode,
                "cold_unique_inflated": self.cold_unique_inflated,
                "savings_bytes": self.savings_bytes,
                "savings_fraction": (
                    round(self.savings_bytes / self.cold_unique_inflated, 4)
                    if self.cold_unique_inflated
                    else 0.0
                ),
                "message_digests": [
                    hashlib.sha256(m).hexdigest() for m in self.messages
                ],
                "message_sizes": [len(m) for m in self.messages],
            }
        )
        return d


class Reader:
    """Holds immutable archive bytes plus a loaded index."""

    def __init__(
        self,
        archive: bytes,
        index: ArchiveIndex,
        *,
        verify_archive_hash: bool = True,
        verify_binding: bool = True,
    ):
        self.archive = archive
        self.index = index
        self.verify_archive_hash = verify_archive_hash
        self.verify_binding = verify_binding
        self._archive_digest = hashlib.sha256(archive).digest()
        if verify_binding:
            if index.archive_size != len(archive):
                raise ArchiveBindingError(
                    f"index was built for {index.archive_size} archive bytes, "
                    f"loaded {len(archive)}"
                )
            if verify_archive_hash and self._archive_digest != index.archive_sha256:
                raise ArchiveBindingError(
                    "archive sha256 does not match the index; refusing to reuse "
                    "checkpoints built for different bytes"
                )

    # -- construction ------------------------------------------------------
    @classmethod
    def open(
        cls,
        archive_path: str | Path,
        index_dir: str | Path,
        negotiation: Optional[Negotiation] = None,
        *,
        verify_archive_hash: bool = True,
    ) -> "Reader":
        archive = Path(archive_path).read_bytes()
        index = load_index(index_dir, negotiation)
        if negotiation is not None and negotiation_digest(negotiation) != index.negotiation_sha256:
            raise ArchiveBindingError("negotiation does not match index")
        return cls(
            archive,
            index,
            verify_archive_hash=verify_archive_hash,
            verify_binding=True,
        )

    # -- internal ----------------------------------------------------------
    def _check_archive_binding(self) -> None:
        if not self.verify_archive_hash:
            return
        if hashlib.sha256(self.archive).digest() != self.index.archive_sha256:
            raise ArchiveBindingError(
                "archive bytes changed since index build; old checkpoints must "
                "not be used on modified compressed bytes"
            )

    def _resume_inflater(self, cp: Checkpoint, counter: DecodeCounter):
        try:
            zdict = self.index.checkpoint_dict(cp)
        except CheckpointError:
            raise
        counter.checkpoint_dict_bytes += len(zdict)
        counter.checkpoints_used.append(cp.message_index)
        if cp.fresh or not zdict:
            return make_inflater(self.index.negotiation.window_bits)
        return make_inflater(self.index.negotiation.window_bits, zdict)

    def _decode_message(
        self,
        msg: MessageRecord,
        inflater,
        counter: DecodeCounter,
        *,
        takeover: bool,
    ) -> Tuple[bytes, object]:
        """Decode one message and verify it. Returns (plaintext, next_inflater)."""
        counter.messages_decoded += 1
        body = message_body(self.archive, msg)
        if len(msg.body_sha256) == 32:
            got = hashlib.sha256(body).digest()
            if got != msg.body_sha256:
                raise CorruptStreamError(
                    f"message {msg.index}: compressed body digest mismatch "
                    f"({got.hex()} != {msg.body_sha256.hex()}); archive bytes "
                    "changed after the index was built"
                )
        if msg.compressed:
            if not self.index.negotiation.permessage_deflate:
                raise ArchiveBindingError(
                    f"message {msg.index} is compressed but deflate was not negotiated"
                )
            if not takeover:
                inflater = make_inflater(self.index.negotiation.window_bits)
            out = decode_compressed(self.archive, msg, inflater, counter, body=body)
        else:
            out = decode_uncompressed(self.archive, msg, counter, body=body)
        verify_message(
            msg,
            out,
            msg.declared_length if msg.declared_length is not None else None,
            msg.plaintext_sha256 if len(msg.plaintext_sha256) == 32 else None,
        )
        return out, inflater

    def _decode_group(
        self,
        cp: Checkpoint,
        targets: Sequence[int],
        counter: DecodeCounter,
        *,
        takeover: bool,
    ) -> Dict[int, bytes]:
        """Decode from ``cp`` through the largest compressed target.

        Only compressed messages feed the inflater.  Uncompressed messages
        never enter the deflate history, so they are materialised only when
        they are themselves requested and are otherwise skipped.  Budgets count
        inflated bytes, and unreferenced uncompressed bytes cost no inflation.
        """
        wanted = set(targets)
        out: Dict[int, bytes] = {}

        if not takeover:
            # Every compressed message is an independent deflate stream, so no
            # prefix needs to be replayed and no history is shared.
            for mid in sorted(wanted):
                plaintext, _ = self._decode_message(
                    self.index.messages[mid], None, counter, takeover=False
                )
                out[mid] = plaintext
            return out

        last = max(targets)
        compressed_targets = [
            m for m in targets if self.index.messages[m].compressed
        ]
        if not compressed_targets:
            # No target needs deflate state: read the requested raw messages.
            for mid in sorted(wanted):
                msg = self.index.messages[mid]
                plaintext, _ = self._decode_message(
                    msg, None, counter, takeover=takeover
                )
                out[mid] = plaintext
            return out

        stop = max(compressed_targets)
        inflater = self._resume_inflater(cp, counter)
        for mid in range(cp.message_index, stop + 1):
            msg = self.index.messages[mid]
            if not msg.compressed:
                if mid in wanted:
                    plaintext, _ = self._decode_message(
                        msg, None, counter, takeover=takeover
                    )
                    out[mid] = plaintext
                continue
            plaintext, inflater = self._decode_message(
                msg, inflater, counter, takeover=takeover
            )
            if mid in wanted:
                out[mid] = plaintext
            else:
                counter.prefix_inflated_bytes += len(plaintext)
        # Targets after the last compressed target are uncompressed.
        for mid in sorted(wanted):
            if mid > stop:
                plaintext, _ = self._decode_message(
                    self.index.messages[mid], None, counter, takeover=takeover
                )
                out[mid] = plaintext
        return out

    def _cold_cost_estimate(self, mid: int, takeover: bool) -> int:
        """Inflated bytes a lone cold query for ``mid`` would produce.

        Uses the index's checkpoint grid and recorded message lengths, so it
        is a prediction used only for reporting savings; the real counter is
        authoritative.
        """
        cp = self.index.checkpoint_for(mid)
        if cp is None:
            return 0
        if not takeover:
            m = self.index.messages[mid]
            return (m.declared_length or 0) if m.compressed else 0
        total = 0
        for i in range(cp.message_index, mid + 1):
            m = self.index.messages[i]
            if m.compressed:
                total += m.declared_length or 0
        return total

    # -- public API --------------------------------------------------------
    def query(self, ids: Sequence[int], *, mode: str = "auto") -> QueryResult:
        """Return messages for ``ids`` in the requested order.

        ``mode``:
          * ``"cold"`` -- each requested occurrence is decoded independently
            from its checkpoint.  A repeated id is decompressed (and counted)
            again, exactly as two separate CLI calls would be.
          * ``"batch"`` -- deduplicate ids, group by checkpoint and share the
            rebuilt prefix; repeated ids reuse one materialisation but their
            output bytes are counted per occurrence.
          * ``"auto"`` -- batch.
        """
        self._check_archive_binding()
        ids = list(ids)
        for mid in ids:
            if not isinstance(mid, int) or mid < 0 or mid >= len(self.index.messages):
                raise ArchiveError(
                    f"message id {mid!r} out of range 0..{len(self.index.messages)-1}"
                )
        if mode not in ("cold", "batch", "auto"):
            raise ArchiveError(f"unknown query mode {mode!r}")
        if mode == "auto":
            mode = "batch"
        takeover = self.index.takeover
        counter = DecodeCounter(requested=len(ids))
        unique_order = list(dict.fromkeys(ids))
        counter.unique_requested = len(unique_order)
        cold_est = sum(self._cold_cost_estimate(m, takeover) for m in unique_order)

        decoded: Dict[int, bytes] = {}
        if mode == "cold":
            # No sharing at all: every occurrence gets its own replay.
            for mid in ids:
                cp = self.index.checkpoint_for(mid)
                if cp is None:
                    raise CheckpointError(f"no checkpoint at or before message {mid}")
                decoded[mid] = self._decode_group(
                    cp, [mid], counter, takeover=takeover
                )[mid]
        else:
            groups: Dict[int, List[int]] = {}
            for mid in unique_order:
                cp = self.index.checkpoint_for(mid)
                if cp is None:
                    raise CheckpointError(f"no checkpoint at or before message {mid}")
                groups.setdefault(cp.message_index, []).append(mid)
            for cp_mid in sorted(groups):
                cp = self.index.checkpoint_for(cp_mid)
                assert cp is not None and cp.message_index == cp_mid
                decoded.update(
                    self._decode_group(
                        cp, sorted(groups[cp_mid]), counter, takeover=takeover
                    )
                )

        # Output accounting: every occurrence counts; duplicates are reported.
        messages: List[bytes] = []
        seen = set()
        for mid in ids:
            m = decoded[mid]
            messages.append(m)
            counter.output_bytes += len(m)
            if mid in seen:
                counter.duplicate_output_bytes += len(m)
            else:
                counter.unique_output_bytes += len(m)
                seen.add(mid)
        return QueryResult(
            messages=messages,
            stats=counter,
            mode=mode,
            cold_unique_inflated=cold_est,
            savings_bytes=max(0, cold_est - counter.inflated_bytes),
        )

    # -- oracle / maintenance ---------------------------------------------
    def decode_sequential(self) -> List[bytes]:
        """Decode every message in order from a fresh inflater.

        Used by ``verify`` and tests as the ground-truth path.  All bytes are
        counted in a throwaway counter.
        """
        self._check_archive_binding()
        counter = DecodeCounter()
        out: List[bytes] = []
        inflater = make_inflater(self.index.negotiation.window_bits)
        for msg in self.index.messages:
            plaintext, inflater = self._decode_message(
                msg, inflater, counter, takeover=self.index.takeover
            )
            out.append(plaintext)
        return out
