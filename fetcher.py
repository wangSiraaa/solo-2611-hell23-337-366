"""fetch-query: fetch and verify the bytes of a pinned plan.

Selection policy (documented and logged per asset):

* If the plan needs the whole asset and a replica is whole-file-only, the
  whole file is read and fully verified -- whole-file adapters are never
  made to look like ranged reads.
* If only a sub-range is requested, it is rounded out to whole 4096-byte
  blocks.  Range-capable replicas are then read per block and each block is
  verified against the catalog block digest; unread blocks are never
  claimed verified.
* When no range-capable replica exists, or the requested blocks cover at
  least ``full_file_threshold`` of the file, whole-file verification is
  chosen because it costs fewer returned bytes than many range GETs while
  verifying everything.
* Replicas are tried in catalog order; any real failure (HEAD mismatch,
  GET error, hash mismatch, short read) falls through to the next replica
  and is logged with the asset identity.  Exhausting all replicas marks the
  asset failed and the whole package is published as incomplete.

Caching:

* Whole objects are content-addressed by the asset content sha256; verified
  blocks are content-addressed by their catalog block digest.  Two queries
  sharing a block therefore re-read zero replica bytes for it.
* Only verified bytes are ever cached; temp files are private and promoted
  atomically.  Concurrent plans never see each other's half-written data.

Concurrency control points ``before_get``, ``after_get_before_verify`` and
``before_publish`` receive callables so tests can force deterministic
interleavings.
"""

import json
import os

from replica import ReplicaError, sha256_hex


class FetchFailure(Exception):
    pass


def _interlock(event, hooks, **kw):
    cb = hooks.get(event) if hooks else None
    if cb is not None:
        cb(event, **kw)


def _blocks_for_range(blocks, start, end):
    """Return catalog block records overlapping [start, end)."""
    return [b for b in blocks if b['end'] > start and b['start'] < end]


def _missing_coalesce(needed):
    """Coalesce adjacent block ranges into the fewest contiguous GET spans."""
    needed = sorted(needed, key=lambda b: b['start'])
    spans = []
    for b in needed:
        if spans and spans[-1][1] == b['start']:
            spans[-1][1] = b['end']
        else:
            spans.append([b['start'], b['end']])
    return [tuple(s) for s in spans]


class FetchSession:
    def __init__(self, plan, resolver, cache, package_dir, *,
                 plan_id=None, policy=None, hooks=None):
        self.plan = plan
        self.resolver = resolver
        self.cache = cache
        self.package_dir = os.path.abspath(os.fspath(package_dir))
        self.plan_id = plan_id or plan['plan_id']
        # coverage fraction at/above which a whole-file read wins.
        self.full_file_threshold = (policy or {}).get(
            'full_file_threshold', 0.5)
        self.hooks = hooks or {}
        self.results = []
        self.failed = []
        self.decisions = []

    # -- per-asset entry ---------------------------------------------------
    def run(self):
        os.makedirs(os.path.join(self.package_dir, 'objects'), exist_ok=True)
        os.makedirs(os.path.join(self.package_dir, 'ranges'), exist_ok=True)
        for hit in self.plan['hits']:
            for asset in hit['assets']:
                # Plans store the identity as a 4-tuple; surface its parts as
                # flat keys so the rest of the engine reads them naturally.
                cid, iid, key, content_sha = asset['identity']
                asset.setdefault('collection_id', cid)
                asset.setdefault('item_id', iid)
                asset.setdefault('asset_key', key)
                asset.setdefault('content_sha256', content_sha)
                try:
                    self._fetch_asset(asset)
                except FetchFailure as exc:
                    ident = list(asset['identity'])
                    self.failed.append({'identity': ident, 'reason': str(exc)})
                    self.results.append({'identity': ident, 'status': 'failed',
                                         'reason': str(exc)})
        return self._write_manifest()

    # -- HEAD gate ---------------------------------------------------------
    def _head_gate(self, rep):
        adapter = self.resolver.adapter(rep)
        size = adapter.head(rep['url'])
        if size != rep['size']:
            raise ReplicaError(
                f"HEAD size {size} disagrees with catalog size {rep['size']}")
        return adapter

    # -- decision ----------------------------------------------------------
    def _choose_mode(self, asset):
        want_whole = asset['range'][0] is None
        range_reps = [r for r in asset['replicas'] if r['range_capable']]
        blocks = asset['blocks']
        if want_whole:
            return 'full-file', 'whole asset requested'
        if not range_reps:
            return 'full-file', 'no range-capable replica available'
        wanted = _blocks_for_range(blocks, asset['range'][0],
                                   asset['range'][1])
        coverage = (sum(b['end'] - b['start'] for b in wanted)
                    / max(1, asset['size']))
        if coverage >= self.full_file_threshold or not blocks:
            return 'full-file', (
                f'coverage {coverage:.2f} >= threshold {self.full_file_threshold}')
        return 'block', (f'coverage {coverage:.2f}, {len(wanted)} blocks '
                         'fetched via range')

    def _usable_range_replicas(self, asset):
        """Range replicas that currently answer HEAD (real availability)."""
        usable = []
        for rep in asset['replicas']:
            if not rep['range_capable']:
                continue
            try:
                self._head_gate(rep)
                usable.append(rep)
            except ReplicaError:
                continue
        return usable

    def _publish_callback(self):
        """Adapt the unified hook to the cache's (key, tmp_path) callback."""
        cb = self.hooks.get('before_publish')
        if cb is None:
            return None

        def _cb(key, tmp_path):
            cb('before_publish', key=key, tmp_path=tmp_path)

        return _cb

    # -- fetching ----------------------------------------------------------
    def _fetch_asset(self, asset):
        identity = tuple(asset['identity'])
        mode, why = self._choose_mode(asset)
        self.decisions.append(
            {'identity': list(identity), 'mode': mode, 'rationale': why})
        if mode == 'full-file':
            self._full_file_path(asset, identity)
            return
        # Planned block mode, but the decision must reflect real availability:
        # if every range-capable replica is currently unusable, fall back to a
        # whole-file read instead of failing the asset outright.
        if not self._usable_range_replicas(asset):
            self.decisions.append({
                'identity': list(identity), 'mode': 'full-file',
                'rationale': 'planned block mode but no range replica is '
                             'currently usable; whole-file fallback'})
            self._full_file_path(asset, identity)
            return
        self._block_fetch(asset, identity)

    def _full_file_path(self, asset, identity):
        data, used_url = self._full_file(asset, identity)
        if asset['range'][0] is None:
            self._store_full(asset, data, used_url, 'full-file')
        else:
            # Whole-file read forced; every byte was verified, but the
            # package only carries the requested slice.  The full read is
            # honestly reflected in the access log.
            rstart, rend = asset['range']
            self._store_range(
                asset, data[rstart:rend], used_url, 'full-file',
                blob_start=rstart, verified_blocks=None,
                verified=('full-file sha256; whole file read, only the '
                          'requested slice packaged'),
                cached_blocks=[])

    def _full_file(self, asset, identity):
        errors = []
        for rep in asset['replicas']:
            try:
                adapter = self._head_gate(rep)
                _interlock('before_get', self.hooks)
                data, _ = adapter.get(rep['url'])
                _interlock('after_get_before_verify', self.hooks)
                if len(data) != asset['size']:
                    raise ReplicaError(
                        f'short/long read: {len(data)} != {asset["size"]}')
                actual = sha256_hex(data)
                if actual != asset['content_sha256']:
                    raise ReplicaError(
                        f'content hash mismatch: {actual[:12]} != '
                        f'{asset["content_sha256"][:12]}')
                if actual != rep['sha256']:
                    raise ReplicaError(
                        f'replica declared sha256 {rep["sha256"][:12]} but '
                        f'bytes hash to {actual[:12]} (stale replica)')
                return data, rep['url']
            except ReplicaError as exc:
                errors.append({'replica': rep['url'], 'error': str(exc)})
                continue
        raise FetchFailure(
            f'all {len(asset["replicas"])} replicas failed: {errors}')

    def _block_fetch(self, asset, identity):
        """Range fetch with per-block verification and a shared block cache.

        Each needed block is content-addressed by its catalog block digest.
        A block already verified (by this plan or a concurrent one) is served
        from the cache and costs zero replica bytes; only missing blocks
        trigger coalesced range GETs.  Unread blocks are never claimed
        verified.
        """
        wanted = _blocks_for_range(asset['blocks'],
                                   asset['range'][0], asset['range'][1])
        wanted_by_start = {b['start']: b for b in wanted}
        assembled = {}                       # start -> verified bytes
        cached_starts = set()
        used_url = None
        errors = []

        # 1) Serve whatever is already verified in the shared cache.
        for b in wanted:
            cached = self.cache.get(b['sha256'], self.plan_id)
            if cached is not None:
                if len(cached) != b['end'] - b['start']:
                    raise FetchFailure(
                        f'cached block {b["start"]} has wrong length')
                assembled[b['start']] = cached
                cached_starts.add(b['start'])

        # 2) Fetch the still-missing blocks, replica by replica.
        for rep in asset['replicas']:
            missing = [b for b in wanted if b['start'] not in assembled]
            if not missing:
                break
            if not rep['range_capable']:
                continue  # whole-file replica cannot serve per-block reads
            try:
                adapter = self._head_gate(rep)
                used_url = rep['url']
                for span_start, span_end in _missing_coalesce(missing):
                    _interlock('before_get', self.hooks)
                    data, _ = adapter.get(rep['url'], span_start, span_end)
                    _interlock('after_get_before_verify', self.hooks)
                    if len(data) != span_end - span_start:
                        raise ReplicaError(
                            f'short range read {span_start}:{span_end}: '
                            f'got {len(data)} bytes')
                    for b in missing:
                        if not (b['start'] >= span_start
                                and b['end'] <= span_end):
                            continue
                        off = b['start'] - span_start
                        chunk = data[off:off + (b['end'] - b['start'])]
                        if sha256_hex(chunk) != b['sha256']:
                            raise ReplicaError(
                                f"block {b['start']} digest mismatch on "
                                f'{rep["url"]}')
                        provenance = (self.plan['build_id'], identity,
                                      rep['url'])
                        with self.cache.lease(b['sha256'], self.plan_id):
                            if self.cache.contains(b['sha256']):
                                existing = self.cache.get(
                                    b['sha256'], self.plan_id)
                                if existing != chunk:
                                    raise FetchFailure(
                                        'block cache digest collision with '
                                        'different bytes')
                            else:
                                tmp = self.cache.new_temp(
                                    f'{asset["item_id"]}-blk{b["start"]}')
                                with open(tmp, 'wb') as f:
                                    f.write(chunk)
                                self.cache.publish(
                                    tmp, b['sha256'], chunk, provenance,
                                    self.plan_id,
                                    before_publish=self._publish_callback())
                        assembled[b['start']] = chunk
            except ReplicaError as exc:
                errors.append({'replica': rep['url'], 'error': str(exc)})
                continue
        if len(assembled) != len(wanted):
            missing_ids = sorted(set(wanted_by_start) - set(assembled))
            # Every range replica failed/corrupted.  A whole-file-only replica
            # is a legitimate fallback: read the whole file, fully verify it,
            # and package the requested slice.
            whole_reps = [r for r in asset['replicas']
                          if not r['range_capable']]
            if whole_reps:
                self.decisions.append({
                    'identity': list(identity), 'mode': 'full-file',
                    'rationale': (f'range replicas failed ({errors}); '
                                  'whole-file replica fallback')})
                full_asset = dict(asset)
                full_asset['replicas'] = whole_reps
                data, used_url = self._full_file(full_asset, identity)
                rstart, rend = asset['range']
                self._store_range(
                    asset, data[rstart:rend], used_url, 'full-file',
                    blob_start=rstart, verified_blocks=None,
                    verified=('full-file sha256 after range failure; whole '
                              'file read, only the requested slice packaged'),
                    cached_blocks=sorted(cached_starts))
                return
            raise FetchFailure(
                f'block fetch incomplete; missing blocks {missing_ids}; '
                f'replica errors {errors}')
        span_start = min(assembled)
        blob = b''.join(assembled[s] for s in sorted(assembled))
        self._store_range(
            asset, blob, used_url, 'block', blob_start=span_start,
            verified_blocks=sorted(assembled),
            verified=('per-block sha256 on read blocks only; '
                      f'{len(cached_starts)} block(s) served from cache'),
            cached_blocks=sorted(cached_starts))

    # -- packaging ---------------------------------------------------------
    def _store_full(self, asset, data, used_url, mode):
        identity = tuple(asset['identity'])
        content_key = asset['content_sha256']
        provenance = (self.plan['build_id'], identity, used_url)
        with self.cache.lease(content_key, self.plan_id):
            cached = self.cache.get(content_key, self.plan_id)
            if cached is not None:
                if cached != data:
                    # Same content key, different bytes: refuse to trust.
                    raise FetchFailure(
                        'cache object exists but bytes differ: possible '
                        'same-URL content swap')
                self.cache.note_provenance(content_key, provenance,
                                           self.plan_id)
                data_to_write = cached
            else:
                tmp = self.cache.new_temp(f'{asset["item_id"]}-full')
                with open(tmp, 'wb') as f:
                    f.write(data)
                self.cache.publish(tmp, content_key, data, provenance,
                                   self.plan_id,
                                   before_publish=self._publish_callback())
                data_to_write = data
        obj_rel = os.path.join('objects', content_key[:2], content_key)
        obj_path = os.path.join(self.package_dir, obj_rel)
        os.makedirs(os.path.dirname(obj_path), exist_ok=True)
        if not os.path.exists(obj_path):
            with open(obj_path, 'wb') as f:
                f.write(data_to_write)
        self.results.append({
            'identity': list(identity), 'status': 'ok', 'mode': mode,
            'partial': False, 'verified': 'full-file sha256',
            'replica_used': used_url,
            'object': obj_rel.replace(os.sep, '/'),
            'size': len(data_to_write)})

    def _store_range(self, asset, blob, used_url, mode, *, blob_start,
                     verified_blocks, verified, cached_blocks):
        identity = tuple(asset['identity'])
        rstart, rend = asset['range']
        safe = f'{asset["item_id"]}_{asset["asset_key"]}'
        pkg_name = f'{safe}__{rstart}-{rend}.bin'
        rel = os.path.join('ranges', pkg_name).replace(os.sep, '/')
        pkg_path = os.path.join(self.package_dir, rel)
        with open(pkg_path, 'wb') as f:
            f.write(blob)
        entry = {
            'identity': list(identity), 'status': 'ok', 'mode': mode,
            'partial': True, 'verified': verified,
            'unverified_claim': ('blocks outside the request were not read '
                                 'and are not claimed verified'),
            'replica_used': used_url,
            'requested_range': [rstart, rend],
            'verified_blob_range': [blob_start, blob_start + len(blob)],
            'object': rel, 'size': len(blob),
            'cached_blocks': sorted(cached_blocks),
        }
        if verified_blocks is not None:
            entry['verified_blocks'] = sorted(verified_blocks)
        self.results.append(entry)

    def _write_manifest(self):
        manifest = {
            'package_format': 'verified-asset-package/1',
            'plan_id': self.plan_id,
            'build_id': self.plan['build_id'],
            'complete': not self.failed,
            'asset_count': len(self.results),
            'ok_count': sum(1 for r in self.results if r['status'] == 'ok'),
            'failed_identities': self.failed,
            'selection_decisions': self.decisions,
            'access_log': self.resolver.access_log(),
            'access_totals': self.resolver.totals(),
            'cache_stats': self.cache.stats(),
            'results': self.results,
        }
        with open(os.path.join(self.package_dir, 'manifest.json'),
                  'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, sort_keys=True)
        return manifest
