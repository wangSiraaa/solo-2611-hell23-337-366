"""Asset manifest sidecars.

STAC Item assets in the legacy subset only carry an ``href``; identity and
replica metadata needed for verifiable fetching live in a manifest sidecar
(JSON) next to the catalog root.  An asset's identity is

    (collection_id, item_id, asset_key, content_sha256)

Every replica additionally carries size, sha256, logical_version and a
range-capability flag, plus optional per-4096-byte block digests for range
verification.  The same ``content_sha256`` may be referenced by several
replicas; different catalog snapshots keep their own manifest origin.
"""

import hashlib
import json
import os
from dataclasses import dataclass, field

from catalog import CatalogError, MAX_REPLICAS_PER_ASSET
from replica import BLOCK_SIZE, block_digests


@dataclass
class ReplicaSpec:
    url: str
    sha256: str
    size: int
    logical_version: str
    range_capable: bool


@dataclass
class AssetSpec:
    collection_id: str
    item_id: str
    asset_key: str
    content_sha256: str
    size: int
    replicas: list[ReplicaSpec]
    blocks: list[tuple] = field(default_factory=list)  # (start, end, sha256)
    source: str = ''          # manifest file this record came from (provenance)

    @property
    def identity(self):
        return (self.collection_id, self.item_id,
                self.asset_key, self.content_sha256)


def _is_hex_sha256(value):
    return (isinstance(value, str) and len(value) == 64
            and all(c in '0123456789abcdef' for c in value.lower()))


def load_manifest(path):
    """Load and validate one manifest JSON file into AssetSpec objects."""
    with open(path, 'r', encoding='utf-8') as f:
        doc = json.load(f)
    if doc.get('format') != 'stac-replica-manifest/1':
        raise CatalogError(
            f'{path}: manifest must declare format stac-replica-manifest/1')
    source = os.path.relpath(path)
    specs, identities = [], set()
    for rec in doc.get('assets', []):
        try:
            cid = rec['collection']
            iid = rec['item']
            key = rec['asset_key']
            content = rec['content_sha256'].lower()
            size = int(rec['size'])
        except (KeyError, TypeError, ValueError) as exc:
            raise CatalogError(f'{path}: malformed asset record: {exc}')
        if not _is_hex_sha256(content):
            raise CatalogError(f'{path}: content_sha256 must be 64 hex chars')
        replicas = rec.get('replicas', [])
        if not 1 <= len(replicas) <= MAX_REPLICAS_PER_ASSET:
            raise CatalogError(
                f'{path}: asset {cid}/{iid}/{key} needs 1..'
                f'{MAX_REPLICAS_PER_ASSET} replicas')
        rspecs = []
        replica_urls = set()
        for r in replicas:
            url = r['url']
            if url in replica_urls:
                raise CatalogError(f'{path}: duplicate replica url {url}')
            replica_urls.add(url)
            rsha = r['sha256'].lower()
            if not _is_hex_sha256(rsha):
                raise CatalogError(f'{path}: replica sha256 invalid for {url}')
            rspecs.append(ReplicaSpec(
                url=url,
                sha256=rsha,
                size=int(r['size']),
                logical_version=str(r['logical_version']),
                range_capable=bool(r['range_capable']),
            ))
        blocks = []
        for b in rec.get('blocks', []):
            if not isinstance(b, dict) or not _is_hex_sha256(b.get('sha256')):
                raise CatalogError(f'{path}: bad block digest record {b}')
            blocks.append((int(b['start']), int(b['end']),
                           b['sha256'].lower()))
        spec = AssetSpec(cid, iid, key, content, size, rspecs,
                         blocks, source)
        if spec.identity in identities:
            raise CatalogError(
                f'{path}: duplicate asset identity {spec.identity}')
        identities.add(spec.identity)
        specs.append(spec)
    return specs


def load_manifests(paths):
    out = []
    for p in paths:
        out.extend(load_manifest(p))
    return out


def derive_block_digests(replica_root, spec):
    """Hash a real local replica to derive 4096-byte block digests.

    Only used at build time when the manifest omits blocks; the file must
    already match the declared content hash or the derivation is refused.
    """
    path = os.path.join(replica_root, *[p for p in spec.replicas[0].url.split('/') if p])
    with open(path, 'rb') as f:
        data = f.read()
    if hashlib.sha256(data).hexdigest() != spec.content_sha256:
        raise CatalogError(
            f'cannot derive blocks for {spec.identity}: local replica content '
            'does not match declared content_sha256')
    return block_digests(data, BLOCK_SIZE)
