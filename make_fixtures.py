"""Generate deterministic local fixtures for the verifiable pipeline.

Run once:  python make_fixtures.py
Creates fixtures/ (STAC catalog + replica bytes) and manifest JSONs under
fixtures/manifests/.  All bytes are written deterministically so reruns are
idempotent.  No network access is involved.
"""

import hashlib
import json
import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FX = ROOT / 'fixtures'
CAT = FX / 'catalog' / 'corp'
ITEMS = CAT / 'items'
REP = FX / 'replicas'
MAN = FX / 'manifests'

BLOCK = 4096


def w(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        f.write(data)


def sha(b):
    return hashlib.sha256(b).hexdigest()


def blocks_of(data):
    return [{'start': i, 'end': min(i + BLOCK, len(data)),
             'sha256': sha(data[i:i + BLOCK])}
            for i in range(0, len(data), BLOCK)]


def write_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, indent=2)


def item(item_id, coords, bbox, dt=None, start=None, end=None,
         asset_key='data', href=None):
    geom_type = 'Polygon' if len(coords) == 1 else 'MultiPolygon'
    props = {}
    if dt is not None:
        props['datetime'] = dt
    else:
        props['datetime'] = None
        props['start_datetime'] = start
        props['end_datetime'] = end
    return {
        'stac_version': '1.0.0', 'type': 'Feature', 'id': item_id,
        'collection': 'corp', 'geometry': {'type': geom_type,
                                           'coordinates': coords},
        'bbox': bbox, 'properties': props,
        'links': [{'rel': 'collection', 'href': '../collection.json'}],
        'assets': {asset_key: {
            'href': href or f'../../../replicas/{item_id}.bin',
            'type': 'application/octet-stream', 'roles': ['data']}},
    }


def rect_ring(w, s, e, n):
    return [[[w, s], [e, s], [e, n], [w, n], [w, s]]]


def main():
    if FX.exists():
        shutil.rmtree(FX)
    ITEMS.mkdir(parents=True)
    REP.mkdir(parents=True)
    MAN.mkdir(parents=True)

    # --- STAC documents ---------------------------------------------------
    write_json(FX / 'catalog' / 'catalog.json', {
        'stac_version': '1.0.0', 'type': 'Catalog', 'id': 'root',
        'description': 'synthetic',
        'links': [{'rel': 'child', 'href': 'corp/collection.json'}]})
    write_json(CAT / 'collection.json', {
        'stac_version': '1.0.0', 'type': 'Collection', 'id': 'corp',
        'description': 'synthetic', 'license': 'proprietary',
        'extent': {
            'spatial': {'bbox': [[-180, -90, 180, 90]]},
            'temporal': {'interval': [
                ['2026-01-01T00:00:00Z', '2026-02-01T00:00:00Z']]}},
        'links': [
            {'rel': 'item', 'href': 'items/dateline.json'},
            {'rel': 'item', 'href': 'items/center.json'},
            {'rel': 'item', 'href': 'items/edge.json'},
            {'rel': 'item', 'href': 'items/later.json'},
            {'rel': 'item', 'href': 'items/twin-a.json'},
            {'rel': 'item', 'href': 'items/twin-b.json'},
            {'rel': 'item', 'href': 'items/bigrange.json'},
            # duplicate path link (normalizes to an already linked item)
            {'rel': 'item', 'href': 'items/../items/dateline.json'},
            # dangling link -> must become a build warning, not an item
            {'rel': 'item', 'href': 'items/missing.json'},
        ]})

    write_json(ITEMS / 'dateline.json', item(
        'dateline',
        [rect_ring(179, -1, 180, 1), rect_ring(-180, -1, -179, 1)],
        [-180, -1, 180, 1], dt='2026-01-01T00:00:00Z',
        href='../../../replicas/dateline-current.bin'))
    write_json(ITEMS / 'center.json', item(
        'center', rect_ring(-1, -1, 1, 1), [-1, -1, 1, 1],
        start='2026-01-01T00:00:00Z', end='2026-01-02T00:00:00Z'))
    write_json(ITEMS / 'edge.json', item(
        'edge', rect_ring(1, 1, 2, 2), [1, 1, 2, 2],
        dt='2026-01-02T00:00:00Z'))
    write_json(ITEMS / 'later.json', item(
        'later', rect_ring(179, 2, 180, 3), [179, 2, 180, 3],
        dt='2026-01-03T00:00:00Z'))
    # Two distinct items/identities whose asset bytes are identical content
    # (same sha256) but served from separate replica URLs; asset_key names
    # must never be merged across items.
    twin_ring = rect_ring(10, 10, 11, 11)
    write_json(ITEMS / 'twin-a.json', item(
        'twin-a', twin_ring, [10, 10, 11, 11], dt='2026-01-04T00:00:00Z',
        href='../../../replicas/twin-a.bin'))
    write_json(ITEMS / 'twin-b.json', item(
        'twin-b', twin_ring, [10, 10, 11, 11], dt='2026-01-04T00:00:00Z',
        href='../../../replicas/twin-b.bin'))
    write_json(ITEMS / 'bigrange.json', item(
        'bigrange', rect_ring(20, 20, 21, 21), [20, 20, 21, 21],
        dt='2026-01-05T00:00:00Z',
        href='../../../replicas/bigrange-range.bin'))

    # --- replica bytes ----------------------------------------------------
    dateline_good = b'synthetic:dateline-good-payload!!'   # 32 bytes
    stale = b'stale:old-version-of-dateline!!!!!'          # 31? make equal size
    stale = (stale + b'x' * (len(dateline_good) - len(stale)))[:len(dateline_good)]
    assert len(stale) == len(dateline_good)
    w(REP / 'dateline-current.bin', dateline_good)
    w(REP / 'dateline-stale.bin', stale)

    center = b'synthetic:center-item-v1-16!'[:16]
    w(REP / 'center.bin', center)

    edge = b'synthetic:edge-only-18-bytes'[:18]
    w(REP / 'edge-fullonly.bin', edge)
    w(REP / 'edge-fail.bin', b'X' * len(edge))   # wrong content, same size
    w(REP / 'edge-corrupt.bin', edge)            # good bytes; fault injected

    later = b'synthetic:later-range-15'[:15]
    w(REP / 'later-range.bin', later)

    twin = bytes((i * 7 + 3) & 0xFF for i in range(64))
    w(REP / 'twin-a.bin', twin)
    # Separate URL, identical bytes (hard link keeps one inode if supported).
    shutil.copyfile(REP / 'twin-a.bin', REP / 'twin-b.bin')

    big = bytes(i & 0xFF for i in range(3 * BLOCK))
    w(REP / 'bigrange-range.bin', big)
    w(REP / 'bigrange-full.bin', big)   # whole-file-only, same content

    # --- manifest ---------------------------------------------------------
    def rep_spec(url, data, logical_version, range_capable, declared=None):
        return {
            'url': url,
            'sha256': declared['sha256'] if declared else sha(data),
            'size': declared['size'] if declared else len(data),
            'logical_version': logical_version,
            'range_capable': range_capable,
        }

    assets = [
        {
            'collection': 'corp', 'item': 'dateline', 'asset_key': 'data',
            'content_sha256': sha(dateline_good),
            'size': len(dateline_good),
            'blocks': blocks_of(dateline_good),
            'replicas': [
                # stale first: wrong sha256 at the same size -> must be
                # detected by real GET verification and skipped
                rep_spec('replicas/dateline-stale.bin', stale, 'v0', True),
                rep_spec('replicas/dateline-current.bin', dateline_good,
                         'v1', True),
            ],
        },
        {
            'collection': 'corp', 'item': 'center', 'asset_key': 'data',
            'content_sha256': sha(center), 'size': len(center),
            'blocks': blocks_of(center),
            'replicas': [rep_spec('replicas/center.bin', center, 'v1', True)],
        },
        {
            'collection': 'corp', 'item': 'edge', 'asset_key': 'data',
            'content_sha256': sha(edge), 'size': len(edge),
            'blocks': blocks_of(edge),
            'replicas': [
                # first two fail at fetch time (missing / bad hash), third ok
                rep_spec('replicas/edge-missing.bin', edge, 'v1', False),
                rep_spec('replicas/edge-fail.bin', edge, 'v1', False,
                         declared={'sha256': sha(edge), 'size': len(edge)}),
                rep_spec('replicas/edge-fullonly.bin', edge, 'v1', False),
            ],
        },
        {
            'collection': 'corp', 'item': 'later', 'asset_key': 'data',
            'content_sha256': sha(later), 'size': len(later),
            'blocks': blocks_of(later),
            'replicas': [rep_spec('replicas/later-range.bin', later, 'v1',
                                  True)],
        },
        {
            'collection': 'corp', 'item': 'twin-a', 'asset_key': 'data',
            'content_sha256': sha(twin), 'size': len(twin),
            'blocks': blocks_of(twin),
            'replicas': [rep_spec('replicas/twin-a.bin', twin, 'v1', True)],
        },
        {
            'collection': 'corp', 'item': 'twin-b', 'asset_key': 'data',
            # Same content hash as twin-a: cache object is shared, but the
            # two identities stay distinct and both sources stay recorded.
            'content_sha256': sha(twin), 'size': len(twin),
            'blocks': blocks_of(twin),
            'replicas': [rep_spec('replicas/twin-b.bin', twin, 'v1', True)],
        },
        {
            'collection': 'corp', 'item': 'bigrange', 'asset_key': 'data',
            'content_sha256': sha(big), 'size': len(big),
            'blocks': blocks_of(big),
            'replicas': [
                rep_spec('replicas/bigrange-range.bin', big, 'v1', True),
                rep_spec('replicas/bigrange-full.bin', big, 'v1', False),
            ],
        },
    ]
    write_json(MAN / 'snapshot-v1.json',
               {'format': 'stac-replica-manifest/1',
                'snapshot': 'v1', 'assets': assets})

    print(json.dumps({'fixtures': str(FX),
                      'items': 7, 'replicas': len(list(REP.iterdir()))},
                     indent=2))


if __name__ == '__main__':
    main()
