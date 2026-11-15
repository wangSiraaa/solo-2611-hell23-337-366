"""Immutable, build-versioned SQLite STAC catalog.

A catalog database may hold several builds; every row is namespaced by
``build_id`` (a content hash of everything ingested).  Plans pin a
``build_id`` so publishing a new build never changes an old plan.  The
database is self-contained: after ``build`` the source JSON files are no
longer needed for query, plan, fetch or inspect.

Spatial design:

* ``item_parts`` holds every *real* rectangle part of each geometry with
  lon/lat columns and an index -- the candidate index.
* A query bbox (split into dateline pieces first) selects candidate parts
  with a cheap index overlap test.
* Exact membership is then re-checked in Python against the stored parts
  and the closed time interval.  A plain Item bbox is never more than a
  candidate.
"""

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone

from catalog import (CatalogError, enforce_limits, item_time_interval,
                     parse_time, validate_item, walk_stac)

SCHEMA = """
CREATE TABLE IF NOT EXISTS builds(
  build_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  root TEXT NOT NULL,
  manifest_sources TEXT NOT NULL,
  item_count INTEGER NOT NULL,
  asset_count INTEGER NOT NULL,
  canonical_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS collections(
  build_id TEXT NOT NULL, collection_id TEXT NOT NULL, doc_path TEXT NOT NULL,
  temporal_start TEXT, temporal_end TEXT,
  PRIMARY KEY(build_id, collection_id));
CREATE TABLE IF NOT EXISTS items(
  build_id TEXT NOT NULL, collection_id TEXT NOT NULL, item_id TEXT NOT NULL,
  doc_path TEXT NOT NULL,
  bbox_w REAL, bbox_s REAL, bbox_e REAL, bbox_n REAL,
  time_start TEXT NOT NULL, time_end TEXT NOT NULL,
  PRIMARY KEY(build_id, collection_id, item_id));
CREATE TABLE IF NOT EXISTS item_parts(
  build_id TEXT NOT NULL, collection_id TEXT NOT NULL, item_id TEXT NOT NULL,
  part_index INTEGER NOT NULL,
  w REAL NOT NULL, s REAL NOT NULL, e REAL NOT NULL, n REAL NOT NULL,
  PRIMARY KEY(build_id, collection_id, item_id, part_index));
CREATE INDEX IF NOT EXISTS idx_parts_spatial
  ON item_parts(build_id, w, e, s, n);
CREATE TABLE IF NOT EXISTS assets(
  build_id TEXT NOT NULL, collection_id TEXT NOT NULL, item_id TEXT NOT NULL,
  asset_key TEXT NOT NULL, content_sha256 TEXT NOT NULL, size INTEGER NOT NULL,
  manifest_source TEXT NOT NULL,
  PRIMARY KEY(build_id, collection_id, item_id, asset_key));
CREATE TABLE IF NOT EXISTS asset_blocks(
  build_id TEXT NOT NULL, collection_id TEXT NOT NULL, item_id TEXT NOT NULL,
  asset_key TEXT NOT NULL, block_index INTEGER NOT NULL,
  start INTEGER NOT NULL, end INTEGER NOT NULL, sha256 TEXT NOT NULL,
  PRIMARY KEY(build_id, collection_id, item_id, asset_key, block_index));
CREATE TABLE IF NOT EXISTS replicas(
  build_id TEXT NOT NULL, collection_id TEXT NOT NULL, item_id TEXT NOT NULL,
  asset_key TEXT NOT NULL, ordinal INTEGER NOT NULL,
  url TEXT NOT NULL, sha256 TEXT NOT NULL, size INTEGER NOT NULL,
  logical_version TEXT NOT NULL, range_capable INTEGER NOT NULL,
  verified INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(build_id, collection_id, item_id, asset_key, ordinal));
CREATE TABLE IF NOT EXISTS build_events(
  build_id TEXT NOT NULL, seq INTEGER NOT NULL, level TEXT NOT NULL,
  code TEXT NOT NULL, message TEXT NOT NULL,
  PRIMARY KEY(build_id, seq));
"""


def _iso(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


class Store:
    def __init__(self, db_path):
        self.db_path = db_path
        first = not os.path.exists(db_path)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('PRAGMA foreign_keys=ON')
        if first:
            self.conn.executescript(SCHEMA)
            self.conn.commit()
        else:
            # Reopened database: make sure it really is one of ours.
            names = {r[0] for r in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if 'builds' not in names:
                raise CatalogError(f'{db_path}: not a catalog database')

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- build -------------------------------------------------------------
    def build(self, stac_root, asset_specs, *, replica_root=None,
              verify_replicas='head', resolver=None):
        """Ingest one immutable snapshot and return its build_id.

        ``verify_replicas`` is 'head' (stat each backing file) or 'full'
        (read and hash every replica; mismatches are recorded as events and
        the replica is marked unverified, never silently trusted).
        """
        events = []

        def event(level, code, message):
            events.append((len(events), level, code, message))

        catalogs, collections, items, duplicate_paths = walk_stac(stac_root)
        collection_ids = {c['id'] for c in collections}
        if not collections:
            raise CatalogError('no STAC Collection found under root')

        # Structural validation + collection/Item relationship checks.
        item_by_id = {}
        item_records = []
        for item in items:
            if item['id'] in item_by_id:
                raise CatalogError(f"duplicate Item id {item['id']!r}")
            parts = validate_item(item, collection_ids)
            t0, t1 = item_time_interval(item)
            bw, bs, be, bn = (float(x) for x in item['bbox'])
            item_by_id[item['id']] = item
            item_records.append((item, parts, t0, t1, bw, bs, be, bn))

        for dup in duplicate_paths:
            event('warning', 'duplicate_item_link',
                  f'collection links the same item path more than once: {dup}')
        for coll_path in self._missing_link_targets(stac_root, collections):
            event('warning', 'missing_link_target', coll_path)

        # Asset identity join: manifest specs must reference real Item assets.
        asset_records = []
        seen_asset_keys = {}
        replicas_count = {}
        for spec in asset_specs:
            key = (spec.collection_id, spec.item_id, spec.asset_key)
            item = item_by_id.get(spec.item_id)
            if item is None or item.get('collection') != spec.collection_id:
                raise CatalogError(
                    f'manifest asset {key} has no matching catalog Item')
            if spec.asset_key not in item.get('assets', {}):
                raise CatalogError(
                    f"item {spec.item_id!r} has no asset key "
                    f"{spec.asset_key!r}")
            if key in seen_asset_keys:
                prev = seen_asset_keys[key]
                if prev != spec.content_sha256:
                    raise CatalogError(
                        f'asset identity conflict for {key}: content_sha256 '
                        f'{prev} vs {spec.content_sha256}')
                raise CatalogError(f'duplicate manifest record for {key}')
            seen_asset_keys[key] = spec.content_sha256
            replicas_count[key] = len(spec.replicas)
            asset_records.append(spec)

        enforce_limits(len(item_records), len(asset_records), replicas_count)

        # Replica verification against real local files.
        for spec in asset_records:
            for rep in spec.replicas:
                verified = 0
                try:
                    adapter = resolver.adapter(rep) if resolver else None
                    if adapter is None:
                        raise CatalogError('resolver required for build')
                    actual_size = adapter.head(rep.url)
                    if actual_size != rep.size:
                        event('error', 'replica_size_mismatch',
                              f'{rep.url}: declared {rep.size}, on disk '
                              f'{actual_size}')
                    elif verify_replicas == 'full':
                        data, _ = adapter.get(rep.url)
                        actual = hashlib.sha256(data).hexdigest()
                        if actual != rep.sha256:
                            event('error', 'replica_hash_mismatch',
                                  f'{rep.url}: actual {actual[:16]} vs '
                                  f'declared replica sha256 {rep.sha256[:16]}')
                        elif actual != spec.content_sha256:
                            # Replica is internally consistent but serves an
                            # old logical version of this asset.
                            event('error', 'stale_replica_content',
                                  f'{rep.url}: replica sha256 matches its own '
                                  f'declaration but differs from asset '
                                  f'content_sha256 (logical_version '
                                  f'{rep.logical_version})')
                        else:
                            verified = 1
                    else:
                        # HEAD-only build: existence/size proven, hash not.
                        verified = 0
                except Exception as exc:  # adapter errors are real failures
                    event('error', 'replica_unavailable',
                          f'{rep.url}: {exc}')
                rep._verified = verified
            if not spec.blocks and resolver is not None:
                try:
                    from manifest import derive_block_digests
                    spec.blocks = derive_block_digests(
                        resolver.root, spec)
                    event('info', 'blocks_derived',
                          f'{spec.identity}: {len(spec.blocks)} block digests '
                          'derived from local replica')
                except Exception as exc:
                    event('warning', 'blocks_unavailable',
                          f'{spec.identity}: {exc}')

        # Content-addressed build id.
        canonical = self._canonical_payload(stac_root, asset_specs,
                                            item_records)
        build_id = hashlib.sha256(canonical).hexdigest()
        if self.conn.execute('SELECT 1 FROM builds WHERE build_id=?',
                             (build_id,)).fetchone():
            event('info', 'build_idempotent',
                  'identical build already present; returning existing build')
            self._log_events(build_id, events)
            return build_id

        now = _iso(datetime.now(timezone.utc))
        manifest_sources = sorted({s.source for s in asset_specs})
        with self.conn:
            self.conn.execute(
                'INSERT INTO builds VALUES(?,?,?,?,?,?,?)',
                (build_id, now, os.path.abspath(os.fspath(stac_root)),
                 json.dumps(manifest_sources), len(item_records),
                 len(asset_records), build_id))
            for coll in collections:
                t0 = t1 = None
                intervals = coll.get('extent', {}).get(
                    'temporal', {}).get('interval', [])
                if intervals and intervals[0][0]:
                    t0, t1 = _iso(parse_time(intervals[0][0])), \
                             _iso(parse_time(intervals[0][1]))
                self.conn.execute(
                    'INSERT INTO collections VALUES(?,?,?,?,?)',
                    (build_id, coll['id'], coll.get('_path', ''), t0, t1))
            for item, parts, t0, t1, bw, bs, be, bn in item_records:
                self.conn.execute(
                    'INSERT INTO items VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (build_id, item['collection'], item['id'],
                     item.get('_path', ''), bw, bs, be, bn,
                     _iso(t0), _iso(t1)))
                for idx, (w, s, e, n) in enumerate(parts):
                    self.conn.execute(
                        'INSERT INTO item_parts VALUES(?,?,?,?,?,?,?,?)',
                        (build_id, item['collection'], item['id'],
                         idx, w, s, e, n))
            for spec in asset_records:
                self.conn.execute(
                    'INSERT INTO assets VALUES(?,?,?,?,?,?,?)',
                    (build_id, spec.collection_id, spec.item_id,
                     spec.asset_key, spec.content_sha256, spec.size,
                     spec.source))
                for idx, (start, end, digest) in enumerate(spec.blocks):
                    self.conn.execute(
                        'INSERT INTO asset_blocks VALUES(?,?,?,?,?,?,?,?)',
                        (build_id, spec.collection_id, spec.item_id,
                         spec.asset_key, idx, start, end, digest))
                for ordinal, rep in enumerate(spec.replicas):
                    self.conn.execute(
                        'INSERT INTO replicas VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                        (build_id, spec.collection_id, spec.item_id,
                         spec.asset_key, ordinal, rep.url, rep.sha256,
                         rep.size, rep.logical_version,
                         1 if rep.range_capable else 0,
                         getattr(rep, '_verified', 0)))
            self._log_events(build_id, events)
        return build_id

    def _log_events(self, build_id, events):
        with self.conn:
            for seq, level, code, message in events:
                self.conn.execute(
                    'INSERT OR REPLACE INTO build_events VALUES(?,?,?,?,?)',
                    (build_id, seq, level, code, message))

    def _missing_link_targets(self, root, collections):
        missing = []
        for coll in collections:
            cpath = coll.get('_path')
            if not cpath:
                continue
            for link in coll.get('links', []):
                if link.get('rel') != 'item':
                    continue
                target = os.path.normpath(
                    os.path.join(os.path.dirname(cpath), link['href']))
                if not os.path.exists(target):
                    missing.append(f"{coll['id']}: {link['href']}")
        return missing

    def _canonical_payload(self, root, asset_specs, item_records):
        items_payload = []
        for item, parts, t0, t1, bw, bs, be, bn in item_records:
            items_payload.append({
                'id': item['id'], 'collection': item['collection'],
                'bbox': [bw, bs, be, bn], 'parts': parts,
                'time': [_iso(t0), _iso(t1)],
                'asset_keys': sorted(item.get('assets', {}).keys()),
            })
        assets_payload = [{
            'identity': list(s.identity), 'size': s.size,
            'blocks': s.blocks, 'source': s.source,
            'replicas': [r.__dict__ for r in sorted(
                s.replicas, key=lambda r: r.url)],
        } for s in sorted(asset_specs, key=lambda s: s.identity)]
        return json.dumps(
            {'root': os.path.abspath(os.fspath(root)),
             'items': sorted(items_payload, key=lambda x: x['id']),
             'assets': assets_payload},
            sort_keys=True, separators=(',', ':')).encode('utf-8')

    # -- read side ---------------------------------------------------------
    def list_builds(self):
        return [dict(r) for r in self.conn.execute(
            'SELECT * FROM builds ORDER BY created_at').fetchall()]

    def latest_build_id(self):
        row = self.conn.execute(
            'SELECT build_id FROM builds ORDER BY created_at DESC LIMIT 1'
        ).fetchone()
        if row is None:
            raise CatalogError('catalog database has no builds')
        return row[0]

    def resolve_build(self, build_id):
        if build_id is None:
            return self.latest_build_id()
        row = self.conn.execute('SELECT 1 FROM builds WHERE build_id=?',
                                (build_id,)).fetchone()
        if row is None:
            raise CatalogError(f'unknown build_id {build_id}')
        return build_id

    def build_info(self, build_id):
        row = self.conn.execute('SELECT * FROM builds WHERE build_id=?',
                                (build_id,)).fetchone()
        return dict(row) if row else None

    def events(self, build_id):
        return [dict(r) for r in self.conn.execute(
            'SELECT * FROM build_events WHERE build_id=? ORDER BY seq',
            (build_id,))]

    def candidate_items(self, build_id, pieces):
        """Spatial candidate index: real parts overlapping any query piece."""
        hits = set()
        for w, s, e, n in pieces:
            rows = self.conn.execute(
                """SELECT DISTINCT collection_id, item_id FROM item_parts
                   WHERE build_id=? AND w <= ? AND e >= ?
                     AND s <= ? AND n >= ?""",
                (build_id, e, w, n, s)).fetchall()
            hits.update((r['collection_id'], r['item_id']) for r in rows)
        return sorted(hits)

    def load_item(self, build_id, collection_id, item_id):
        row = self.conn.execute(
            'SELECT * FROM items WHERE build_id=? AND collection_id=? '
            'AND item_id=?', (build_id, collection_id, item_id)).fetchone()
        if row is None:
            return None
        parts = [tuple(r) for r in self.conn.execute(
            'SELECT w,s,e,n FROM item_parts WHERE build_id=? AND '
            'collection_id=? AND item_id=? ORDER BY part_index',
            (build_id, collection_id, item_id))]
        d = dict(row)
        d['parts'] = [(w, s, e, n) for (w, s, e, n) in parts]
        d['time_start_dt'] = parse_time(d['time_start'] + 'Z'
                                        if not d['time_start'].endswith('Z')
                                        else d['time_start'])
        d['time_end_dt'] = parse_time(d['time_end'] + 'Z'
                                      if not d['time_end'].endswith('Z')
                                      else d['time_end'])
        return d

    def item_asset_keys(self, build_id, collection_id, item_id):
        return [r['asset_key'] for r in self.conn.execute(
            'SELECT asset_key FROM assets WHERE build_id=? AND collection_id=? '
            'AND item_id=? ORDER BY asset_key',
            (build_id, collection_id, item_id))]

    def load_asset(self, build_id, collection_id, item_id, asset_key):
        row = self.conn.execute(
            'SELECT * FROM assets WHERE build_id=? AND collection_id=? '
            'AND item_id=? AND asset_key=?',
            (build_id, collection_id, item_id, asset_key)).fetchone()
        if row is None:
            return None
        asset = dict(row)
        asset['blocks'] = [dict(r) for r in self.conn.execute(
            'SELECT block_index,start,end,sha256 FROM asset_blocks WHERE '
            'build_id=? AND collection_id=? AND item_id=? AND asset_key=? '
            'ORDER BY block_index',
            (build_id, collection_id, item_id, asset_key))]
        asset['replicas'] = [dict(r) for r in self.conn.execute(
            'SELECT * FROM replicas WHERE build_id=? AND collection_id=? '
            'AND item_id=? AND asset_key=? ORDER BY ordinal',
            (build_id, collection_id, item_id, asset_key))]
        return asset

    def all_assets(self, build_id):
        rows = self.conn.execute(
            'SELECT collection_id,item_id,asset_key FROM assets WHERE '
            'build_id=? ORDER BY collection_id,item_id,asset_key',
            (build_id,)).fetchall()
        return [self.load_asset(build_id, r['collection_id'],
                                r['item_id'], r['asset_key']) for r in rows]
