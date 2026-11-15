"""inspect: reread produced artifacts (SQLite catalog and byte packages).

Inspection never opens the source STAC JSON or the original replica files;
it only reads the SQLite catalog database and the package ``manifest.json``
plus, optionally, re-hashes the packaged bytes.
"""

import hashlib
import json
import os

from store import Store


def inspect_catalog(db_path, build_id=None):
    """Return a full, JSON-serializable view of one build from SQLite."""
    with Store(db_path) as st:
        build_id = st.resolve_build(build_id)
        info = st.build_info(build_id)
        info['events'] = st.events(build_id)
        info['collections'] = [dict(r) for r in st.conn.execute(
            'SELECT * FROM collections WHERE build_id=? ORDER BY collection_id',
            (build_id,))]
        info['items'] = []
        for row in st.conn.execute(
                'SELECT * FROM items WHERE build_id=? ORDER BY item_id',
                (build_id,)):
            item = dict(row)
            item['parts'] = [dict(w=r['w'], s=r['s'], e=r['e'], n=r['n'])
                             for r in st.conn.execute(
                'SELECT * FROM item_parts WHERE build_id=? AND item_id=? '
                'ORDER BY part_index', (build_id, item['item_id']))]
            info['items'].append(item)
        info['assets'] = st.all_assets(build_id)
        return info


def inspect_package(package_dir, *, rehash=True):
    """Read a package manifest and verify its packaged bytes on disk."""
    manifest_path = os.path.join(package_dir, 'manifest.json')
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
    checks = []
    if rehash:
        for result in manifest['results']:
            if result['status'] != 'ok':
                continue
            obj_path = os.path.join(package_dir, result['object'])
            with open(obj_path, 'rb') as f:
                digest = hashlib.sha256(f.read()).hexdigest()
            entry = {'object': result['object'], 'sha256': digest}
            if not result.get('partial'):
                entry['matches_content_sha256'] = (
                    digest == result['identity'][3])
            else:
                entry['note'] = ('range object: verify via the block digests '
                                 'in the catalog, not the full content hash')
            checks.append(entry)
    manifest['on_disk_rehash'] = checks
    return manifest


def summarize(manifest):
    """Short human-readable summary used by the CLI and logs."""
    return {
        'plan_id': manifest['plan_id'],
        'build_id': manifest['build_id'],
        'complete': manifest['complete'],
        'ok': manifest['ok_count'],
        'failed': [f['identity'] for f in manifest['failed_identities']],
        'returned_bytes': manifest['access_totals']['returned_bytes'],
        'cache_footprint_bytes': manifest['cache_stats']['footprint_bytes'],
        'cache_hits': manifest['cache_stats']['hits'],
        'decisions': manifest['selection_decisions'],
    }
