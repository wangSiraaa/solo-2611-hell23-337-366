"""Reproducible experiments for the checkpoint-density trade-off.

Two experiments are reported:

``run_density_experiment``
    Builds indexes over the same corpus at several checkpoint densities and
    reports permanent state bytes versus the worst-case single-message cold
    query inflation.  This is the trade-off the task asks to demonstrate, not
    an optimality proof.

``run_workload_experiment``
    The specified 100 x 4096-byte workload: every message queried cold as an
    isolated single-message query.  Reports the sum of *actual* inflated bytes
    (including discarded prefix and the final output), the largest single
    query, permanent state, and ordinary index metadata.  It also contrasts a
    batch that reuses one context per checkpoint with the cold path and reports
    how many bytes the batch actually avoided inflating.
"""

import json
from pathlib import Path
from typing import List, Optional

from .build import (
    METADATA_BUDGET,
    QUERY_BUDGET,
    STATE_BUDGET,
    build_index,
)
from .errors import ArchiveError
from .model import Negotiation
from .reader import Reader
from .wire import parse_archive


def _load(archive, negotiation):
    from .model import Negotiation as N

    data = Path(archive).read_bytes()
    neg = N.from_json(json.loads(Path(negotiation).read_text(encoding="utf-8")))
    return data, neg


def run_density_experiment(
    archive: str,
    negotiation: str,
    densities: Optional[List[int]] = None,
) -> dict:
    data, neg = _load(archive, negotiation)
    message_count = len(parse_archive(data, neg, None).messages)
    rows = []
    if densities is None:
        # Start from the budget-driven choice and explore coarser grids.
        probe = build_index(data, neg)
        base = probe.density
        densities = sorted(
            {max(1, base // 2), base, base * 2, base * 4, base * 8, 1000}
        )
    for density in densities:
        idx = build_index(data, neg, density=density)
        reader = Reader(data, idx)
        worst = 0
        total = 0
        for mid in range(len(idx.messages)):
            q = reader.query([mid], mode="cold")
            total += q.stats.inflated_bytes
            worst = max(worst, q.stats.inflated_bytes)
        rows.append(
            {
                "density": density,
                "checkpoints": len(idx.checkpoints),
                "dictionary_state_bytes": idx.dictionary_state_bytes,
                "metadata_bytes": idx.metadata_bytes,
                "worst_single_cold_inflated": worst,
                "total_all_cold_inflated": total,
                "dictionary_state_under_budget": idx.dictionary_state_bytes
                <= STATE_BUDGET,
                "worst_under_query_budget": worst <= QUERY_BUDGET,
            }
        )
    return {
        "corpus": str(archive),
        "messages": message_count,
        "budgets": {
            "dictionary_state": STATE_BUDGET,
            "metadata": METADATA_BUDGET,
            "single_cold_query": QUERY_BUDGET,
        },
        "rows": rows,
    }


def run_workload_experiment(archive: str, negotiation: str) -> dict:
    data, neg = _load(archive, negotiation)
    idx = build_index(data, neg)
    reader = Reader(data, idx)
    n = len(idx.messages)
    if n != 100:
        raise ArchiveError(
            f"workload expects exactly 100 messages, archive has {n}"
        )
    sizes = {m.declared_length for m in idx.messages}
    if sizes != {4096}:
        raise ArchiveError(f"workload expects 4096-byte messages, saw {sorted(sizes)}")

    cold_total = 0
    cold_worst = 0
    cold_total_body = 0
    per_message = []
    for mid in range(n):
        q = reader.query([mid], mode="cold")
        st = q.stats
        cold_total += st.inflated_bytes
        cold_total_body += st.compressed_body_bytes
        cold_worst = max(cold_worst, st.inflated_bytes)
        per_message.append(
            {
                "message": mid,
                "inflated": st.inflated_bytes,
                "prefix_inflated": st.prefix_inflated_bytes,
                "compressed_body": st.compressed_body_bytes,
                "checkpoint": st.checkpoints_used[0] if st.checkpoints_used else None,
            }
        )

    # Repeated cold queries must count again: decode message n-1 twice, twice.
    repeat = 0
    for _ in range(2):
        repeat += reader.query([n - 1], mode="cold").stats.inflated_bytes

    # Batch: one context per checkpoint, output in request order.
    order = list(range(n - 1, -1, -1))
    batch = reader.query(order, mode="batch")

    from hashlib import sha256

    batch_digest_ok = all(
        sha256(batch.messages[i]).digest() == idx.messages[mid].plaintext_sha256
        for i, mid in enumerate(order)
    )

    return {
        "corpus": str(archive),
        "messages": n,
        "message_bytes": 4096,
        "checkpoint_density": idx.density,
        "permanent_state": {
            "dictionary_state_bytes": idx.dictionary_state_bytes,
            "dictionary_state_limit": STATE_BUDGET,
            "dictionary_state_ok": idx.dictionary_state_bytes <= STATE_BUDGET,
            "metadata_bytes": idx.metadata_bytes,
            "metadata_limit": METADATA_BUDGET,
            "metadata_ok": idx.metadata_bytes <= METADATA_BUDGET,
            "sizes": idx.size_report(),
        },
        "cold_single_message_queries": {
            "count": n,
            "per_query_inflated_limit": QUERY_BUDGET,
            # The 64 KiB bound applies to each isolated single-message query:
            # prefix replay plus the returned plaintext.  A sum over all 100
            # queries necessarily exceeds it (their outputs alone are 400 KiB),
            # so the bound is checked per query, and the across-query sum is
            # reported for transparency only.
            "worst_single_query_inflated": cold_worst,
            "all_individual_queries_under_budget": cold_worst <= QUERY_BUDGET,
            "worst_under_budget": cold_worst <= QUERY_BUDGET,
            "total_inflated_across_all_queries": cold_total,
            "compressed_body_bytes_total": cold_total_body,
        },
        "repeated_cold_queries": {
            "queries": 2,
            "inflated_bytes_total": repeat,
            "counted_again": repeat >= 2 * 4096,
        },
        "batch": {
            "requested": batch.stats.requested,
            "unique_requested": batch.stats.unique_requested,
            "inflated_bytes": batch.stats.inflated_bytes,
            "output_bytes": batch.stats.output_bytes,
            "checkpoints_used": sorted(set(batch.stats.checkpoints_used)),
            "order_preserved": batch_digest_ok,
            "savings_bytes_vs_sum_of_cold": max(0, cold_total - batch.stats.inflated_bytes),
            "inflate_calls": batch.stats.inflate_calls,
        },
        "cold_detail": per_message,
    }
