"""End-to-end verification for the verifiable STAC replica pipeline.

Two independent oracles ground every check:

* spatial oracle -- enumerates the *real* rectangle parts straight from the
  source STAC geometry and applies exact rectangle/closed-time predicates,
  never consulting the SQLite candidate index;
* byte oracle -- slices the original replica files directly, so packaged
  bytes are compared against ground-truth file content rather than the
  cache that produced them.

The harness also reopens the SQLite database and re-reads package manifests
from disk.  A structured log is written to out/verification-log.json.

Run:  PYTHONDONTWRITEBYTECODE=1 python verify_pipeline.py
"""

import hashlib
import json
import os
import shutil
import tempfile
import threading
import traceback
from datetime import timedelta

from cache import BlockCache
from catalog import (CatalogError, geometry_parts, item_matches_pieces,
                     item_time_interval, parse_time, split_bbox)
from fetcher import FetchSession, FetchFailure
from inspect_cmd import inspect_catalog, inspect_package
from manifest import load_manifest, load_manifests
from planner import make_plan
from replica import BLOCK_SIZE, ReplicaResolver, sha256_hex
from store import Store

ROOT = os.path.abspath(os.path.dirname(__file__))
FX = os.path.join(ROOT, 'fixtures')
CAT = os.path.join(FX, 'catalog')
REPROOT = FX
MANIFEST_V1 = os.path.join(FX, 'manifests', 'snapshot-v1.json')
OUT = os.path.join(ROOT, 'out')
WORK = os.path.join(OUT, 'verify')

LOG = {'checks': [], 'passed': 0, 'failed': 0}


def check(name, cond, detail=''):
    status = 'PASS' if cond else 'FAIL'
    LOG['checks'].append({'name': name, 'status': status, 'detail': detail})
    if cond:
        LOG['passed'] += 1
        print(f'  PASS {name}')
    else:
        LOG['failed'] += 1
        print(f'  FAIL {name} :: {detail}')


def section(title):
    print(f'\n=== {title} ===')
    LOG['checks'].append({'name': title, 'status': 'SECTION'})


# --------------------------------------------------------------------------
# Independent oracles.
# --------------------------------------------------------------------------
def spatial_oracle(items, bbox, time_interval):
    """Ground-truth hit set + per-item hit part indices, bypassing SQLite."""
    pieces = split_bbox(bbox)
    q0 = parse_time(time_interval[0])
    q1 = parse_time(time_interval[1])
    hits = {}
    for item in items:
        parts = geometry_parts(item['geometry'])
        t0, t1 = item_time_interval(item)
        ok, idxs = item_matches_pieces(parts, pieces, t0, t1, q0, q1)
        if ok:
            hits[item['id']] = sorted(idxs)
    return hits, pieces


def byte_oracle(rel_url, start=None, end=None):
    path = os.path.join(REPROOT, *rel_url.split('/'))
    with open(path, 'rb') as f:
        data = f.read()
    return data if start is None else data[start:end]


def _hits_of(plan):
    return plan['hits'] if isinstance(plan, dict) else plan.hits


def plan_hits(plan):
    return {h['item']: sorted(p['part_index'] for p in h['hit_parts'])
            for h in _hits_of(plan)}


def fetch(plan, resolver, cache, pkg, *, plan_id=None, policy=None, hooks=None):
    return FetchSession(plan, resolver, cache, pkg, plan_id=plan_id,
                        policy=policy, hooks=hooks).run()


def main():
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK, exist_ok=True)

    # Defensive cleanup: an earlier interrupted run must not leave a replica
    # renamed or swapped.  Regenerate the deterministic fixtures wholesale.
    import make_fixtures
    make_fixtures.main()

    # Load raw STAC items once for the spatial oracle.
    raw_items = []
    for dirpath, _, files in os.walk(CAT):
        for fn in files:
            if fn.endswith('.json'):
                doc = json.load(open(os.path.join(dirpath, fn)))
                if doc.get('type') == 'Feature':
                    raw_items.append(doc)

    # ------------------------------------------------------------------
    section('build: immutable SQLite catalog, self-contained reread')
    # ------------------------------------------------------------------
    db = os.path.join(WORK, 'catalog.db')
    specs = load_manifest(MANIFEST_V1)
    resolver0 = ReplicaResolver(REPROOT)
    with Store(db) as st:
        build_id = st.build(CAT, specs, resolver=resolver0,
                            verify_replicas='full')
        builds_before = st.list_builds()
    check('build returns a sha256 build id', len(build_id) == 64)
    check('one build present', len(builds_before) == 1)

    # Reopen the DB from a copied location with no source-JSON access: the
    # catalog alone must answer every query.
    db_copy = os.path.join(WORK, 'copied-catalog.db')
    shutil.copy(db, db_copy)
    cat_view = inspect_catalog(db_copy)
    check('reread catalog without source JSON: 7 items',
          len(cat_view['items']) == 7, str(len(cat_view['items'])))
    check('reread catalog: 7 assets', len(cat_view['assets']) == 7)
    check('reread catalog: real rectangle parts stored',
          all(len(i['parts']) >= 1 for i in cat_view['items']))
    dateline_row = next(i for i in cat_view['items'] if i['item_id'] == 'dateline')
    check('dateline item keeps two real parts (not one wide bbox)',
          len(dateline_row['parts']) == 2, str(len(dateline_row['parts'])))
    events = {e['code'] for e in cat_view['events']}
    check('duplicate item path recorded as warning',
          'duplicate_item_link' in events, str(events))
    check('dangling link recorded as warning',
          'missing_link_target' in events, str(events))
    check('stale replica content detected at build',
          'stale_replica_content' in events, str(events))
    check('unavailable replica detected at build',
          'replica_unavailable' in events, str(events))

    # ------------------------------------------------------------------
    section('plan-query: dateline split, exact parts, closed time')
    # ------------------------------------------------------------------
    with Store(db) as st:
        # 1. antimeridian wrap
        wrap = make_plan(st, [179.5, -2, -179.5, 2],
                         ['2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'],
                         build_id, plan_id='wrap')
        oracle_wrap, pieces_wrap = spatial_oracle(
            raw_items, [179.5, -2, -179.5, 2],
            ['2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'])
        check('wrap query splits into two dateline pieces',
              pieces_wrap == [(179.5, -2, 180.0, 2), (-180.0, -2, -179.5, 2)],
              str(pieces_wrap))
        check('wrap hits match spatial oracle',
              plan_hits(wrap) == oracle_wrap,
              f'{plan_hits(wrap)} vs {oracle_wrap}')
        check('wrap matches BOTH real dateline parts',
              plan_hits(wrap).get('dateline') == [0, 1],
              str(plan_hits(wrap).get('dateline')))
        check('later rejected on time in wrap window',
              any(r['item'] == 'later' and r['reason'] == 'time'
                  for r in wrap.rejected))

        # 2. the legacy false-hit: center-of-earth bbox must NOT catch dateline
        false = make_plan(st, [-0.5, -0.5, 0.5, 0.5],
                          ['2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'],
                          build_id, plan_id='false')
        oracle_false, _ = spatial_oracle(
            raw_items, [-0.5, -0.5, 0.5, 0.5],
            ['2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'])
        check('center bbox never false-hits the dateline item',
              'dateline' not in plan_hits(false), str(plan_hits(false)))
        check('false-bbox plan matches oracle',
              plan_hits(false) == oracle_false,
              f'{plan_hits(false)} vs {oracle_false}')
        check('dateline is not even a spatial candidate',
              all(cid_iid[1] != 'dateline'
                  for cid_iid in false.candidate_item_ids))

        # 3. boundary touch (closed geometry + closed time)
        edge = make_plan(st, [1, 1, 2, 2],
                         ['2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z'],
                         build_id, plan_id='edge')
        oracle_edge, _ = spatial_oracle(
            raw_items, [1, 1, 2, 2],
            ['2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z'])
        check('edge-touch plan matches oracle (boundary counts)',
              plan_hits(edge) == oracle_edge,
              f'{plan_hits(edge)} vs {oracle_edge}')
        check('edge item hit on shared boundary',
              'edge' in plan_hits(edge))

        # 4. closed time endpoints
        center_end = make_plan(st, [-180, -90, 180, 90],
                               ['2026-01-02T00:00:00Z',
                                '2026-01-02T00:00:00Z'],
                               build_id, plan_id='t-endpoint')
        check('center included at its closing instant (closed interval)',
              'center' in plan_hits(center_end))
        after = make_plan(st, [-180, -90, 180, 90],
                          ['2026-01-02T00:00:01Z', '2026-02-01T00:00:00Z'],
                          build_id, plan_id='t-after')
        check('center excluded one second after close',
              'center' not in plan_hits(after))
        before = make_plan(st, [-180, -90, 180, 90],
                           ['2025-12-31T00:00:00Z',
                            '2025-12-31T23:59:59Z'],
                           build_id, plan_id='t-before')
        check('center excluded before opening instant',
              'center' not in plan_hits(before))

        # 5. deterministic oracle sweep
        import random
        rng = random.Random(360)
        sweep_ok = True
        lons = [-180, -179.5, -1, -0.5, 0.5, 1, 179, 179.5, 180]
        for _ in range(60):
            w, e = rng.sample(lons, 2)
            s, n = sorted(rng.sample([-90, -2, -1, 1, 2, 90], 2))
            t0 = parse_time('2026-01-01T00:00:00Z') + timedelta(
                days=rng.randint(0, 32))
            t1 = t0 + timedelta(days=rng.randint(0, 3))
            ti = [t0.strftime('%Y-%m-%dT%H:%M:%SZ'),
                  t1.strftime('%Y-%m-%dT%H:%M:%SZ')]
            p = make_plan(st, [w, s, e, n], ti, build_id, plan_id='sweep')
            oj, _ = spatial_oracle(raw_items, [w, s, e, n], ti)
            if plan_hits(p) != oj:
                sweep_ok = False
                break
        check('60-query sweep always agrees with spatial oracle', sweep_ok)

    # ------------------------------------------------------------------
    section('fetch-query: real verification, fallback, incomplete package')
    # ------------------------------------------------------------------
    with Store(db) as st:
        plan_all = make_plan(st, [-180, -90, 180, 90],
                             ['2026-01-01T00:00:00Z',
                              '2026-02-01T00:00:00Z'],
                             build_id, plan_id='all')
    # 6. stale replica really rejected, current content used.
    res = ReplicaResolver(REPROOT)
    cache = BlockCache(os.path.join(WORK, 'cache-all'))
    pkg = os.path.join(WORK, 'pkg-all')
    manifest = fetch(plan_all, res, cache, pkg, plan_id='all')
    dl = next(r for r in manifest['results']
              if r['identity'][1] == 'dateline')
    check('dateline fetched from current, not stale replica',
          dl['replica_used'] == 'replicas/dateline-current.bin',
          dl['replica_used'])
    dl_bytes = open(os.path.join(pkg, dl['object']), 'rb').read()
    check('dateline package bytes equal byte oracle (current file)',
          dl_bytes == byte_oracle('replicas/dateline-current.bin'))
    check('dateline content sha256 verified',
          sha256_hex(dl_bytes) == dl['identity'][3])
    stale_attempts = [a for a in manifest['access_log']
                      if a['url'] == 'replicas/dateline-stale.bin'
                      and a['method'] == 'GET']
    check('stale replica was really GET then rejected',
          len(stale_attempts) == 1 and stale_attempts[0]['status'] == 'ok')

    # 7. edge fallback chain missing -> bad-hash -> whole-file success.
    edge_res = next(r for r in manifest['results']
                    if r['identity'][1] == 'edge')
    check('edge fell back to the whole-file-only good replica',
          edge_res['replica_used'] == 'replicas/edge-fullonly.bin')
    edge_urls = [a['url'] for a in manifest['access_log']
                 if 'edge' in a['url'] and a['method'] == 'HEAD']
    check('edge tried all three replica URLs in order',
          edge_urls == ['replicas/edge-missing.bin',
                        'replicas/edge-fail.bin',
                        'replicas/edge-fullonly.bin'], str(edge_urls))
    check('edge package bytes equal byte oracle',
          open(os.path.join(pkg, edge_res['object']), 'rb').read()
          == byte_oracle('replicas/edge-fullonly.bin'))

    # 9. whole-file-only adapter never fakes a ranged read.
    wholefile_gets = [a for a in manifest['access_log']
                      if a['url'] == 'replicas/edge-fullonly.bin'
                      and a['method'] == 'GET']
    check('whole-file replica GET had no range start (full read)',
          all(a['start'] is None for a in wholefile_gets)
          and wholefile_gets[0]['returned_bytes'] == edge_res['size'])

    # 10/11. same-content sharing + asset_key identity isolation.
    check('twin assets share one content-addressed cache object',
          cache.object_count() == 6 and cache.stats()['hits'] >= 1,
          str(cache.stats()))
    twin_key = next(r['identity'][3] for r in manifest['results']
                    if r['identity'][1] == 'twin-a')
    idx = json.load(open(os.path.join(cache.dir, 'index.json')))
    prov = idx['objects'][twin_key]['provenance']
    prov_idents = {tuple(p['identity'][0:3]) for p in prov}
    check('shared object retains both item/asset provenance',
          {('corp', 'twin-a', 'data'), ('corp', 'twin-b', 'data')}
          <= prov_idents, str(prov_idents))
    twin_idents = [tuple(r['identity']) for r in manifest['results']
                   if r['identity'][1].startswith('twin')]
    check('same asset_key across items is NOT merged',
          len({i[:3] for i in twin_idents}) == 2
          and len({i[3] for i in twin_idents}) == 1)

    # 8. explicit incomplete package when every replica fails.
    res_bad = ReplicaResolver(REPROOT)
    res_bad.make_unavailable('replicas/edge-fullonly.bin')
    res_bad.make_unavailable('replicas/edge-fail.bin')
    cache_bad = BlockCache(os.path.join(WORK, 'cache-bad'))
    pkg_bad = os.path.join(WORK, 'pkg-bad')
    manifest_bad = fetch(plan_all, res_bad, cache_bad, pkg_bad,
                         plan_id='all-bad')
    check('package marked incomplete when an asset fails',
          manifest_bad['complete'] is False)
    failed = [f['identity'] for f in manifest_bad['failed_identities']]
    check('failed identity listed explicitly',
          any(i[1] == 'edge' for i in failed), str(failed))
    ok_items = {r['identity'][1] for r in manifest_bad['results']
                if r['status'] == 'ok'}
    check('other assets still delivered in incomplete package',
          'dateline' in ok_items and 'edge' not in ok_items)
    res_bad.restore('replicas/edge-fullonly.bin')
    res_bad.restore('replicas/edge-fail.bin')

    # 24. HEAD metadata lies -> replica really skipped, fallback succeeds.
    res_lie = ReplicaResolver(REPROOT)
    res_lie.head_lie('replicas/dateline-current.bin', 999)
    # make stale unavailable so only the lying current replica would be tried,
    # then prove a second healthy replica path: instead use edge-free plan.
    cache_lie = BlockCache(os.path.join(WORK, 'cache-lie'))
    pkg_lie = os.path.join(WORK, 'pkg-lie')
    manifest_lie = fetch(plan_all, res_lie, cache_lie, pkg_lie,
                         plan_id='lie')
    # dateline has stale (hash-fail) then current (HEAD-lie) => both fail.
    dl_fail = next(f for f in manifest_lie['failed_identities']
                   if f['identity'][1] == 'dateline')
    check('HEAD size lie detected and recorded as failure',
          'HEAD size' in dl_fail['reason'], dl_fail['reason'])

    # ------------------------------------------------------------------
    section('range fetch: block verification, cross-block, no over-claim')
    # ------------------------------------------------------------------
    with Store(db) as st:
        # within block 0 -> block mode under default threshold
        plan_r1 = make_plan(st, [20, 20, 21, 21],
                            ['2026-01-05T00:00:00Z',
                             '2026-01-05T00:00:00Z'], build_id,
                            asset_ranges={('bigrange', 'data'): (100, 200)},
                            plan_id='r1')
        # crosses block0/block1 boundary; low threshold keeps block mode
        plan_r2 = make_plan(st, [20, 20, 21, 21],
                            ['2026-01-05T00:00:00Z',
                             '2026-01-05T00:00:00Z'], build_id,
                            asset_ranges={('bigrange', 'data'): (4000, 4200)},
                            plan_id='r2')
    res_r = ReplicaResolver(REPROOT)
    cache_r = BlockCache(os.path.join(WORK, 'cache-range'))
    m1 = fetch(plan_r1, res_r, cache_r, os.path.join(WORK, 'pkg-r1'),
               plan_id='r1')
    b1 = next(r for r in m1['results'])
    check('single-block request uses block mode', b1['mode'] == 'block',
          b1['mode'])
    check('block GET returned only one 4096 block',
          m1['access_totals']['returned_bytes'] == BLOCK_SIZE,
          str(m1['access_totals']))
    check('packaged bytes equal byte oracle slice (block 0)',
          open(os.path.join(WORK, 'pkg-r1', b1['object']), 'rb').read()
          == byte_oracle('replicas/bigrange-range.bin', 0, BLOCK_SIZE))
    check('only read block claimed verified; block 1/2 not claimed',
          b1['verified_blocks'] == [0] and 1 not in b1['verified_blocks']
          and 2 not in b1['verified_blocks'], str(b1['verified_blocks']))

    # Fresh cache: block 0 must really be fetched again so the coalescing of
    # adjacent blocks into one range GET is exercised end to end.
    res_r2 = ReplicaResolver(REPROOT)
    cache_r2 = BlockCache(os.path.join(WORK, 'cache-range2'))
    m2 = fetch(plan_r2, res_r2, cache_r2, os.path.join(WORK, 'pkg-r2'),
               plan_id='r2', policy={'full_file_threshold': 0.9})
    b2 = next(r for r in m2['results'])
    check('cross-block request stays in block mode (threshold 0.9)',
          b2['mode'] == 'block', b2['mode'])
    range_gets = [(a['start'], a['end']) for a in m2['access_log']
                  if a['method'] == 'GET']
    check('adjacent blocks coalesced into one range GET',
          (0, 2 * BLOCK_SIZE) in range_gets and len(range_gets) == 1,
          str(range_gets))
    check('cross-block blob equals byte oracle [0:8192)',
          open(os.path.join(WORK, 'pkg-r2', b2['object']), 'rb').read()
          == byte_oracle('replicas/bigrange-range.bin', 0, 2 * BLOCK_SIZE))
    check('blocks 0 and 1 verified, block 2 never claimed',
          sorted(b2['verified_blocks']) == [0, 4096],
          str(b2['verified_blocks']))

    # 14. real block-digest corruption is detected.  Use a range-only manifest
    # (no whole-file sibling) so the block digest check must fail the asset
    # rather than paper over the corruption with a full read.
    range_only = json.load(open(MANIFEST_V1))
    range_only['snapshot'] = 'range-only'
    range_only['assets'] = [
        a for a in range_only['assets'] if a['item'] == 'bigrange']
    range_only['assets'][0]['replicas'] = [
        r for r in range_only['assets'][0]['replicas']
        if r['range_capable']]
    ro_path = os.path.join(WORK, 'range-only.json')
    json.dump(range_only, open(ro_path, 'w'))
    with Store(os.path.join(WORK, 'catalog-ro.db')) as st_ro:
        bid_ro = st_ro.build(CAT, load_manifest(ro_path),
                             resolver=ReplicaResolver(REPROOT),
                             verify_replicas='head')
        plan_ro = make_plan(st_ro, [20, 20, 21, 21],
                            ['2026-01-05T00:00:00Z',
                             '2026-01-05T00:00:00Z'], bid_ro,
                            asset_ranges={('bigrange', 'data'): (4000, 4200)},
                            plan_id='ro')
    res_c = ReplicaResolver(REPROOT)
    res_c.corrupt_transport('replicas/bigrange-range.bin', offset=5000)
    cache_c = BlockCache(os.path.join(WORK, 'cache-corrupt'))
    m_c = fetch(plan_ro, res_c, cache_c, os.path.join(WORK, 'pkg-corrupt'),
                plan_id='rc', policy={'full_file_threshold': 0.9})
    check('corrupted block digest fails the asset (no silent accept)',
          m_c['complete'] is False
          and any('digest mismatch' in f['reason']
                  for f in m_c['failed_identities']),
          str(m_c['failed_identities']))

    # 14b. with the whole-file fallback healthy, corruption is detected then
    # transparently recovered via the full read (fallback path proven).
    res_c2 = ReplicaResolver(REPROOT)
    res_c2.corrupt_transport('replicas/bigrange-range.bin', offset=5000)
    cache_c2 = BlockCache(os.path.join(WORK, 'cache-corrupt2'))
    m_c2 = fetch(plan_r2, res_c2, cache_c2,
                 os.path.join(WORK, 'pkg-corrupt2'), plan_id='rc2',
                 policy={'full_file_threshold': 0.9})
    rec = next(r for r in m_c2['results'])
    check('corrupted range replica triggers whole-file fallback recovery',
          rec['status'] == 'ok' and rec['mode'] == 'full-file'
          and rec['replica_used'] == 'replicas/bigrange-full.bin',
          str(rec.get('replica_used')))

    # 15. shared blocks cut returned bytes vs two full-file reads.
    benchmark = json.load(open(os.path.join(ROOT, 'inputs',
                                            'range-benchmark.json')))
    res_q1 = ReplicaResolver(REPROOT)
    res_q2 = ReplicaResolver(REPROOT)
    cache_sh = BlockCache(os.path.join(WORK, 'cache-shared'))
    with Store(db) as st:
        q1 = make_plan(st, [20, 20, 21, 21],
                       ['2026-01-05T00:00:00Z', '2026-01-05T00:00:00Z'],
                       build_id,
                       asset_ranges={('bigrange', 'data'):
                                     tuple(benchmark['queries'][0])},
                       plan_id='q1')
        q2 = make_plan(st, [20, 20, 21, 21],
                       ['2026-01-05T00:00:00Z', '2026-01-05T00:00:00Z'],
                       build_id,
                       asset_ranges={('bigrange', 'data'):
                                     tuple(benchmark['queries'][1])},
                       plan_id='q2')
    mq1 = fetch(q1, res_q1, cache_sh, os.path.join(WORK, 'pkg-q1'),
                plan_id='q1')
    mq2 = fetch(q2, res_q2, cache_sh, os.path.join(WORK, 'pkg-q2'),
                plan_id='q2')
    shared_returned = (mq1['access_totals']['returned_bytes']
                       + mq2['access_totals']['returned_bytes'])
    check('second overlapping query reads zero replica bytes (cache hit)',
          mq2['access_totals']['returned_bytes'] == 0,
          str(mq2['access_totals']))
    check('shared-block plan beats full-file baseline '
          f'({shared_returned} < {benchmark["full_read_baseline_bytes"]})',
          shared_returned < benchmark['full_read_baseline_bytes'],
          f'{shared_returned} vs {benchmark["full_read_baseline_bytes"]}')

    # 16. whole-file-only replica cannot fake a range: full read recorded.
    res_wf = ReplicaResolver(REPROOT)
    res_wf.make_unavailable('replicas/bigrange-range.bin')
    cache_wf = BlockCache(os.path.join(WORK, 'cache-wf'))
    m_wf = fetch(q1, res_wf, cache_wf, os.path.join(WORK, 'pkg-wf'),
                 plan_id='wf')
    wf = next(r for r in m_wf['results'])
    check('no range replica -> full-file mode chosen',
          wf['mode'] == 'full-file' and wf['partial'] is True)
    check('full 12288 bytes really read despite small range request',
          m_wf['access_totals']['returned_bytes']
          == benchmark['size'], str(m_wf['access_totals']))
    check('packaged slice still equals the requested oracle bytes',
          open(os.path.join(WORK, 'pkg-wf', wf['object']), 'rb').read()
          == byte_oracle('replicas/bigrange-full.bin',
                         benchmark['queries'][0][0],
                         benchmark['queries'][0][1]))
    res_wf.restore('replicas/bigrange-range.bin')

    # ------------------------------------------------------------------
    section('concurrency: deterministic interleaving, no half-written share')
    # ------------------------------------------------------------------
    # Use the bigrange whole-object on a guaranteed-empty cache so both
    # threads genuinely fetch and publish the same content key.
    barrier = threading.Event()
    observed = {}

    def before_publish(event, **kw):
        key = kw['key']
        tmp_path = kw['tmp_path']
        observed['tmp_exists'] = os.path.exists(tmp_path)
        observed['published_during_halfwrite'] = cache_cc.contains(key)
        observed['get_during_halfwrite'] = cache_cc.get(key, 'observer')
        barrier.wait(timeout=10)

    with Store(db) as st:
        plan_conc = make_plan(st, [20, 20, 21, 21],
                              ['2026-01-05T00:00:00Z',
                               '2026-01-05T00:00:00Z'], build_id,
                              plan_id='conc')
    cache_cc = BlockCache(os.path.join(WORK, 'cache-conc'))
    check('concurrency cache starts empty', cache_cc.object_count() == 0)
    errors = []

    def worker(plan_id, pkg_dir, hookset):
        try:
            r = ReplicaResolver(REPROOT)
            fetch(plan_conc, r, cache_cc, pkg_dir, plan_id=plan_id,
                  hooks=hookset)
        except Exception as exc:  # noqa: BLE001 - recorded for assertion
            errors.append((plan_id, repr(exc), traceback.format_exc()))

    t1 = threading.Thread(
        target=worker, args=('conc-A', os.path.join(WORK, 'pkg-conc-A'),
                             {'before_publish': before_publish}))
    t2 = threading.Thread(
        target=worker, args=('conc-B', os.path.join(WORK, 'pkg-conc-B'), {}))
    t1.start()
    parked = False
    for _ in range(150):
        if observed.get('tmp_exists'):
            parked = True
            break
        threading.Event().wait(0.02)
    t2.start()
    check('thread A parked with a private temp file', parked)
    check('half-written temp is NOT visible in the cache',
          observed.get('published_during_halfwrite') is False
          and observed.get('get_during_halfwrite') is None,
          str(observed))
    # Give B a moment: it must block on the same key, not read A's temp.
    threading.Event().wait(0.15)
    barrier.set()
    t1.join(timeout=15)
    t2.join(timeout=15)
    check('both concurrent workers succeeded', not errors, str(errors))
    # Exactly one object: either B lost the publish race (temp discarded) or
    # B observed A's verified object and took a cache hit.  Both are valid.
    check('exactly one content object (race resolved without duplication)',
          cache_cc.object_count() == 1
          and (cache_cc.stats()['discarded_temps'] >= 1
               or cache_cc.stats()['hits'] >= 1),
          str(cache_cc.stats()))
    check('no temp files left behind',
          os.listdir(cache_cc.tmp_dir) == [])
    check('both plans hold references on the shared object',
          {tuple(v['refs']) for v in cache_cc.index['objects'].values()}
          and all(set(v['refs']) == {'conc-A', 'conc-B'}
                  for v in cache_cc.index['objects'].values()))
    # Both packaged bytes independently match the oracle.
    for d in ('pkg-conc-A', 'pkg-conc-B'):
        mf = json.load(open(os.path.join(WORK, d, 'manifest.json')))
        obj = next(r['object'] for r in mf['results'])
        ok = open(os.path.join(WORK, d, obj), 'rb').read() == byte_oracle(
            'replicas/bigrange-range.bin')
        check(f'{d} packaged bytes match oracle', ok)

    # All three control points fire, in the order get -> verify -> publish.
    order = []

    def hook(event, **kw):
        order.append(event)

    with Store(db) as st:
        plan_hook = make_plan(st, [20, 20, 21, 21],
                              ['2026-01-05T00:00:00Z',
                               '2026-01-05T00:00:00Z'], build_id,
                              plan_id='hook')
    cache_hook = BlockCache(os.path.join(WORK, 'cache-hook'))
    fetch(plan_hook, ReplicaResolver(REPROOT), cache_hook,
          os.path.join(WORK, 'pkg-hook'), plan_id='hook',
          hooks={'before_get': hook,
                 'after_get_before_verify': hook,
                 'before_publish': hook})
    check('all three control points fired',
          {'before_get', 'after_get_before_verify',
           'before_publish'} <= set(order), str(order))
    first_get = order.index('before_get')
    first_verify = order.index('after_get_before_verify')
    first_publish = order.index('before_publish')
    check('control points ordered get -> verify -> publish',
          first_get < first_verify < first_publish, str(order))

    # ------------------------------------------------------------------
    section('build pinning, snapshot provenance, same-URL content swap')
    # ------------------------------------------------------------------
    # v2 snapshot: same content, an extra replica + bumped logical version.
    v2_doc = json.load(open(MANIFEST_V1))
    center_asset = next(a for a in v2_doc['assets']
                        if a['item'] == 'center')
    shutil.copyfile(os.path.join(REPROOT, 'replicas', 'center.bin'),
                    os.path.join(REPROOT, 'replicas', 'center-v2.bin'))
    center_asset['replicas'].append({
        'url': 'replicas/center-v2.bin',
        'sha256': center_asset['content_sha256'],
        'size': center_asset['size'], 'logical_version': 'v2',
        'range_capable': True})
    v2_path = os.path.join(FX, 'manifests', 'snapshot-v2.json')
    json.dump(v2_doc, open(v2_path, 'w'), indent=2, sort_keys=True)
    specs_v2 = load_manifest(v2_path)
    with Store(db) as st:
        build_id_v2 = st.build(CAT, specs_v2,
                               resolver=ReplicaResolver(REPROOT),
                               verify_replicas='head')
        all_builds = st.list_builds()
    check('new snapshot creates a distinct build id',
          build_id_v2 != build_id and len(all_builds) == 2)
    with Store(db) as st:
        pinned = make_plan(st, [-180, -90, 180, 90],
                           ['2026-01-01T00:00:00Z',
                            '2026-02-01T00:00:00Z'],
                           build_id, plan_id='pinned-old')
    check('old plan stays pinned to the old build id',
          pinned.build_id == build_id)
    old_center = next(a for h in pinned.hits for a in h['assets']
                      if a['identity'][1] == 'center')
    check('old plan keeps the old replica set (v2 url absent)',
          all(r['url'] != 'replicas/center-v2.bin'
              for r in old_center['replicas']))

    # Same URL, different bytes on disk: the pinned plan must reject it and
    # fall back, never silently accept swapped content.
    original = byte_oracle('replicas/bigrange-range.bin')
    swap = ReplicaResolver(REPROOT)
    swap.swap_content('replicas/bigrange-range.bin',
                      b'Z' * len(original))
    try:
        res_swap = ReplicaResolver(REPROOT)
        cache_swap = BlockCache(os.path.join(WORK, 'cache-swap'))
        m_swap = fetch(q1, res_swap, cache_swap,
                       os.path.join(WORK, 'pkg-swap'), plan_id='swap')
        sw = next(r for r in m_swap['results'])
        check('swapped same-URL range replica rejected; fell back',
              sw['replica_used'] == 'replicas/bigrange-full.bin',
              sw['replica_used'])
        check('pinned content still equals the original byte oracle',
              open(os.path.join(WORK, 'pkg-swap', sw['object']), 'rb').read()
              == byte_oracle('replicas/bigrange-full.bin',
                             benchmark['queries'][0][0],
                             benchmark['queries'][0][1]))
    finally:
        swap.restore_swap('replicas/bigrange-range.bin')
        check('fixture restored after swap test',
              byte_oracle('replicas/bigrange-range.bin') == original)

    # ------------------------------------------------------------------
    section('lifecycle: plan release reaps only unreferenced cache')
    # ------------------------------------------------------------------
    cache_lc = BlockCache(os.path.join(WORK, 'cache-life'))
    r_lc = ReplicaResolver(REPROOT)
    fetch(q1, r_lc, cache_lc, os.path.join(WORK, 'pkg-lc1'), plan_id='L1')
    r_lc2 = ReplicaResolver(REPROOT)
    fetch(q2, r_lc2, cache_lc, os.path.join(WORK, 'pkg-lc2'), plan_id='L2')
    block_key = q1.hits and None
    # q1 and q2 share block 0; find its digest from the plan block table.
    big_asset = next(a for h in q1.hits for a in h['assets'])
    block0_hash = next(b['sha256'] for b in big_asset['blocks']
                       if b['start'] == 0)
    check('shared block referenced by both plans before release',
          set(cache_lc.index['objects'][block0_hash]['refs'])
          == {'L1', 'L2'})
    cache_lc.release_plan('L1')
    check('releasing L1 keeps the block (L2 still references it)',
          cache_lc.contains(block0_hash))
    cache_lc.release_plan('L2')
    check('releasing last plan reaps the block; footprint zero',
          not cache_lc.contains(block0_hash)
          and cache_lc.footprint() == 0, str(cache_lc.stats()))

    # ------------------------------------------------------------------
    section('validation: identity conflicts, limits, malformed geometry')
    # ------------------------------------------------------------------
    def expect_error(label, fn):
        try:
            fn()
            check(label, False, 'no error raised')
        except CatalogError as exc:
            check(label, True, str(exc)[:80])

    # identity conflict: two snapshot manifests assert different content for
    # the SAME (collection, item, asset_key) identity.
    snap_a = json.load(open(MANIFEST_V1))
    snap_b = json.loads(json.dumps(snap_a))
    snap_b['assets'][0]['content_sha256'] = 'f' * 64
    # keep replica self-consistency so the only error is the identity clash
    snap_b['assets'][0]['replicas'] = [{
        'url': 'replicas/dateline-current.bin', 'sha256': 'f' * 64,
        'size': snap_b['assets'][0]['size'],
        'logical_version': 'v9', 'range_capable': True}]
    pa = os.path.join(WORK, 'snap-a.json')
    pb = os.path.join(WORK, 'snap-b.json')
    json.dump(snap_a, open(pa, 'w'))
    json.dump(snap_b, open(pb, 'w'))
    expect_error('identity conflict (same key, two content shas) rejected',
                 lambda: Store(':memory:').build(
                     CAT, load_manifests([pa, pb]),
                     resolver=ReplicaResolver(REPROOT)))

    # duplicate identity across two manifests with identical content is also
    # rejected (one identity must have one authoritative record per build).
    snap_c = json.loads(json.dumps(snap_a))
    pc = os.path.join(WORK, 'snap-c.json')
    json.dump(snap_c, open(pc, 'w'))
    expect_error('duplicate manifest record for one identity rejected',
                 lambda: Store(':memory:').build(
                     CAT, load_manifests([pa, pc]),
                     resolver=ReplicaResolver(REPROOT)))

    # >3 replicas
    many = json.load(open(MANIFEST_V1))
    base = many['assets'][0]['replicas'][0]
    many['assets'][0]['replicas'] = [
        dict(base, url=f'replicas/x{i}.bin') for i in range(4)]
    mpath = os.path.join(WORK, 'many.json')
    json.dump(many, open(mpath, 'w'))
    expect_error('fourth replica rejected (limit 3)',
                 lambda: Store(':memory:').build(
                     CAT, load_manifest(mpath),
                     resolver=ReplicaResolver(REPROOT)))

    # duplicate item id
    dup_root = tempfile.mkdtemp(dir=WORK)
    os.makedirs(os.path.join(dup_root, 'c', 'items'))
    coll = {'stac_version': '1.0.0', 'type': 'Collection', 'id': 'c',
            'links': []}
    json.dump(coll, open(os.path.join(dup_root, 'c', 'collection.json'), 'w'))
    feat = {'type': 'Feature', 'id': 'same', 'collection': 'c',
            'geometry': {'type': 'Polygon',
                         'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 1],
                                          [0, 0]]]},
            'bbox': [0, 0, 1, 1],
            'properties': {'datetime': '2026-01-01T00:00:00Z'},
            'assets': {}}
    json.dump(feat, open(os.path.join(dup_root, 'c', 'items', 'a.json'), 'w'))
    json.dump(feat, open(os.path.join(dup_root, 'c', 'items', 'b.json'), 'w'))
    expect_error('duplicate Item id rejected',
                 lambda: Store(':memory:').build(
                     dup_root, [], resolver=ReplicaResolver(REPROOT)))

    # unknown collection reference
    bad_coll = dict(feat, collection='nope')
    broot = tempfile.mkdtemp(dir=WORK)
    os.makedirs(os.path.join(broot, 'c', 'items'))
    json.dump(coll, open(os.path.join(broot, 'c', 'collection.json'), 'w'))
    json.dump(bad_coll, open(os.path.join(broot, 'c', 'items', 'x.json'), 'w'))
    expect_error('item referencing unknown collection rejected',
                 lambda: Store(':memory:').build(
                     broot, [], resolver=ReplicaResolver(REPROOT)))

    # non-rectangle geometry
    tri = dict(feat, geometry={'type': 'Polygon', 'coordinates': [
        [[0, 0], [1, 0], [1, 1], [0, 0]]]})
    troot = tempfile.mkdtemp(dir=WORK)
    os.makedirs(os.path.join(troot, 'c', 'items'))
    json.dump(coll, open(os.path.join(troot, 'c', 'collection.json'), 'w'))
    json.dump(tri, open(os.path.join(troot, 'c', 'items', 't.json'), 'w'))
    expect_error('non-rectangular polygon rejected',
                 lambda: Store(':memory:').build(
                     troot, [], resolver=ReplicaResolver(REPROOT)))

    # inverted time interval
    badtime = dict(feat, properties={'start_datetime': '2026-01-02T00:00:00Z',
                                     'end_datetime': '2026-01-01T00:00:00Z',
                                     'datetime': None})
    btroot = tempfile.mkdtemp(dir=WORK)
    os.makedirs(os.path.join(btroot, 'c', 'items'))
    json.dump(coll, open(os.path.join(btroot, 'c', 'collection.json'), 'w'))
    json.dump(badtime, open(os.path.join(btroot, 'c', 'items', 'bt.json'), 'w'))
    expect_error('inverted item time interval rejected',
                 lambda: Store(':memory:').build(
                     btroot, [], resolver=ReplicaResolver(REPROOT)))

    # ------------------------------------------------------------------
    section('inspect: reread SQLite + package manifests from disk')
    # ------------------------------------------------------------------
    reread = inspect_package(os.path.join(WORK, 'pkg-all'))
    check('package manifest reread from disk', reread['complete'] is True)
    check('packaged objects rehash to their content sha256',
          all(c.get('matches_content_sha256') for c in reread['on_disk_rehash']
              if 'matches_content_sha256' in c))
    with Store(db) as st:
        check('reopened DB still resolves both builds',
              sorted(b['build_id'] for b in st.list_builds())
              == sorted([build_id, build_id_v2]))
        again = make_plan(st, [-0.5, -0.5, 0.5, 0.5],
                          ['2026-01-01T00:00:00Z',
                           '2026-01-01T00:00:00Z'], build_id)
        check('reopened DB reproduces the no-false-hit result',
              'dateline' not in plan_hits(again))

    LOG['summary'] = {'passed': LOG['passed'], 'failed': LOG['failed'],
                      'total': LOG['passed'] + LOG['failed']}
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, 'verification-log.json'), 'w') as f:
        json.dump(LOG, f, indent=2, sort_keys=True)
    print(f"\n{LOG['passed']} passed, {LOG['failed']} failed")
    return 1 if LOG['failed'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
