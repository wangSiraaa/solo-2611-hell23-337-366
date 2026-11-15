"""Build an on-disk index and its persistent decode checkpoints.

Cost model
----------
A random access in context-takeover mode normally has to replay the deflate
stream from the last known state.  This builder records, at message
boundaries, the trailing deflate window (at most ``1 << window_bits`` bytes of
compressed-message plaintext).  A query then seeds a fresh inflater with that
window and replays at most one checkpoint interval of messages.  The window is
decoder *state*; complete message plaintext is never indexed.

Checkpoint density is chosen as the finest interval whose raw window snapshots
plus checkpoint table still fit ``state_budget`` (256 KiB by default).  When
the state budget prevents a dense enough grid, cold-query amplification grows:
that is the explicit trade-off reported by ``experiments.py``.
"""

import hashlib
import json
import math
from pathlib import Path
from typing import List, Optional, Tuple

from .codec import MARKER, make_inflater, message_body
from .errors import CorruptStreamError, PartialMessageError
from .index import (
    CHECKPOINT_SIZE,
    ArchiveIndex,
    Checkpoint,
    negotiation_digest,
    write_index,
)
from .model import ArchiveLayout, Negotiation
from .wire import parse_archive

STATE_BUDGET = 256 * 1024
METADATA_BUDGET = 64 * 1024
QUERY_BUDGET = 64 * 1024


def _sha(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def choose_density(
    message_count: int,
    window_bits: int,
    state_budget: int = STATE_BUDGET,
    takeover: bool = True,
    *,
    max_message_bytes: Optional[int] = None,
    query_budget: int = QUERY_BUDGET,
) -> int:
    """Finest message interval that keeps permanent state under budget.

    With checkpoints at ``0, d, 2d, ...`` the first has an empty dictionary and
    every later one may hold a full window.  We therefore need
    ``(ncp - 1) * W + ncp * CHECKPOINT_SIZE <= state_budget``.

    The state budget is a *hard* constraint, so this is the smallest interval
    that fits.  ``max_message_bytes`` / ``query_budget`` are advisory: they are
    returned to the caller through ``query_feasible`` accounting in the build
    stats, but never coarsen or refine the grid.  Refining below this interval
    would exceed the state budget; coarsening would waste budget without being
    required by any general archive.
    """
    if message_count <= 0:
        return 1
    if not takeover:
        # Each compressed message is an independent stream; the reader needs no
        # history.  A single fresh anchor is enough.
        return message_count
    window = 1 << window_bits
    max_cp = (state_budget + window) // (window + CHECKPOINT_SIZE)
    max_cp = max(1, min(message_count, max_cp))
    density = max(1, math.ceil(message_count / max_cp))
    return density


def worst_case_cold_bytes(
    message_count: int, density: int, max_message_bytes: int
) -> int:
    """Upper bound on a single cold query's inflated bytes for this grid."""
    if message_count <= 0:
        return 0
    return max(1, density) * max_message_bytes


def _delta_encode_checkpoints(
    message_count: int, density: int
) -> List[int]:
    return list(range(0, message_count, density))


def _decode_all(
    layout: ArchiveLayout,
    takeover: bool,
    window_bits: int,
) -> Tuple[List[int], List[bytes], int]:
    """Sequentially decode to obtain each message length and digest.

    Returns (lengths, digests, inflated_bytes).  Plaintext is dropped
    immediately; only the digest survives, which is metadata, not payload.
    """
    data = layout.data
    lengths: List[int] = []
    digests: List[bytes] = []
    inflated = 0
    inflater = make_inflater(window_bits)
    for msg in layout.messages:
        body = message_body(data, msg)
        if msg.compressed:
            if not takeover:
                inflater = make_inflater(window_bits)
            try:
                out = inflater.decompress(body + MARKER)
            except Exception as exc:  # zlib.error
                raise CorruptStreamError(
                    f"build could not inflate message {msg.index}: {exc}"
                ) from exc
            if inflater.unconsumed_tail:
                raise CorruptStreamError(
                    f"build: message {msg.index} left unconsumed input"
                )
            inflated += len(out)
        else:
            out = body
        lengths.append(len(out))
        digests.append(_sha(out))
    return lengths, digests, inflated


def build_index(
    data: bytes,
    negotiation: Negotiation,
    *,
    density: Optional[int] = None,
    state_budget: int = STATE_BUDGET,
    metadata_budget: int = METADATA_BUDGET,
    query_budget: int = QUERY_BUDGET,
    archive_sha256: Optional[str] = None,
) -> ArchiveIndex:
    sha = archive_sha256 or hashlib.sha256(data).hexdigest()
    layout = parse_archive(data, negotiation, sha)
    takeover = negotiation.permessage_deflate and negotiation.context_takeover
    window_bits = negotiation.window_bits

    lengths, digests, first_pass_inflated = _decode_all(
        layout, takeover, window_bits
    )
    for msg, ln, dg in zip(layout.messages, lengths, digests):
        msg.declared_length = ln
        msg.plaintext_sha256 = dg
        msg.body_sha256 = _sha(message_body(data, msg))

    max_len = max((m.declared_length or 0 for m in layout.messages), default=0)
    if density is None:
        density = choose_density(
            len(layout.messages),
            window_bits,
            state_budget,
            takeover,
        )
    density = max(1, int(density))
    cp_indices = _delta_encode_checkpoints(len(layout.messages), density)
    cp_set = set(cp_indices)

    # Second pass: capture the trailing window *before* each checkpoint
    # message.  Only the last W bytes of compressed-message plaintext are kept.
    W = 1 << window_bits
    history = bytearray()
    cumulative = 0
    dict_blob = bytearray()
    checkpoints: List[Checkpoint] = []
    inflater = make_inflater(window_bits)
    second_pass_inflated = 0
    for msg in layout.messages:
        if msg.index in cp_set:
            if takeover:
                snap = bytes(history[-W:]) if W else b""
            else:
                snap = b""
            off = len(dict_blob)
            dict_blob += snap
            checkpoints.append(
                Checkpoint(
                    message_index=msg.index,
                    dict_offset=off,
                    dict_len=len(snap),
                    plain_offset=cumulative,
                    fresh=(len(snap) == 0),
                    dict_sha256=_sha(snap),
                )
            )
        body = message_body(data, msg)
        if msg.compressed:
            if not takeover:
                inflater = make_inflater(window_bits)
            try:
                out = inflater.decompress(body + MARKER)
            except Exception as exc:
                raise CorruptStreamError(
                    f"build could not inflate message {msg.index}: {exc}"
                ) from exc
            if inflater.unconsumed_tail:
                raise CorruptStreamError(
                    f"build: message {msg.index} left unconsumed input"
                )
            second_pass_inflated += len(out)
            if len(out) != msg.declared_length:
                raise PartialMessageError(
                    f"build: message {msg.index} decoded to {len(out)} bytes, "
                    f"expected {msg.declared_length}"
                )
            if out:
                history += out
                if len(history) > W:
                    del history[: len(history) - W]
            cumulative += len(out)
        else:
            if len(body) != msg.declared_length:
                raise PartialMessageError(
                    f"build: uncompressed message {msg.index} length mismatch"
                )

    index = ArchiveIndex(
        archive_size=len(data),
        archive_sha256=bytes.fromhex(sha),
        negotiation_sha256=negotiation_digest(negotiation),
        negotiation=negotiation,
        frames=layout.frames,
        messages=layout.messages,
        checkpoints=checkpoints,
        dict_blob=bytes(dict_blob),
        density=density,
        dict_sha256=_sha(bytes(dict_blob)),
    )
    index.build_stats = {  # type: ignore[attr-defined]
        "inflated_bytes_two_passes": first_pass_inflated + second_pass_inflated,
        "inflated_bytes_first_pass": first_pass_inflated,
        "inflated_bytes_dictionary_pass": second_pass_inflated,
        "checkpoint_density": density,
        "checkpoint_count": len(checkpoints),
        "window_bits": window_bits,
        "takeover": takeover,
        "metadata_budget": metadata_budget,
        "metadata_ok": index.metadata_bytes <= metadata_budget,
        "query_budget": query_budget,
        "query_worst_case_upper_bound": worst_case_cold_bytes(
            len(layout.messages), density, max_len
        ),
        "query_budget_ok": worst_case_cold_bytes(
            len(layout.messages), density, max_len
        )
        <= query_budget,
        "state_budget": state_budget,
        "state_ok": index.dictionary_state_bytes <= state_budget,
    }
    return index


def build_from_files(
    archive_path: str | Path,
    negotiation_path: str | Path,
    out_dir: str | Path,
    *,
    density: Optional[int] = None,
    state_budget: int = STATE_BUDGET,
    metadata_budget: int = METADATA_BUDGET,
    query_budget: int = QUERY_BUDGET,
) -> Tuple[ArchiveIndex, Path]:
    archive = Path(archive_path)
    data = archive.read_bytes()
    neg = Negotiation.from_json(
        json.loads(Path(negotiation_path).read_text(encoding="utf-8"))
    )
    index = build_index(
        data,
        neg,
        density=density,
        state_budget=state_budget,
        metadata_budget=metadata_budget,
        query_budget=query_budget,
    )
    path = write_index(index, out_dir, archive)
    return index, path
