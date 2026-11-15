"""Shared helpers for the test suite."""

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from wsarchive import Negotiation, build_index  # noqa: E402
from wsarchive.corpus import Spec, generate, workload_messages  # noqa: E402


def neg(takeover=True, wbits=15, enabled=True):
    return Negotiation(
        permessage_deflate=enabled,
        server_no_context_takeover=not takeover,
        server_max_window_bits=wbits,
    )


def build(msgs, compressed, **spec_kwargs):
    spec = Spec(msgs, compressed, **spec_kwargs)
    data, manifest = generate(spec)
    index = build_index(data, neg(spec.takeover, spec.window_bits))
    return data, manifest, index, spec


def digests(messages):
    return [hashlib.sha256(m).hexdigest() for m in messages]


def truth_by_id(manifest):
    return {m["message"]: m for m in manifest}


def fixture_paths():
    return (
        ROOT / "fixtures" / "archive.frames",
        ROOT / "fixtures" / "negotiation.json",
        ROOT / "fixtures" / "source-truth.json",
    )


def load_fixture_neg():
    _, npath, _ = fixture_paths()
    return Negotiation.from_json(json.loads(npath.read_text(encoding="utf-8")))
