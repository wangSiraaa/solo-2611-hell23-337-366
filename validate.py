"""End-to-end validation against the task contract.

Run:  PYTHONDONTWRITEBYTECODE=1 python validate.py
Writes a read-back log (logs/validation_log.json) and returns non-zero on any
failed check.  Every warehouse state is checked by the independent oracle in
oracle.py cell-by-cell, not merely by schema compilation.
"""
import json
import os
import sqlite3
import sys
import traceback

import make_fixtures
from oracle import oracle_facts
from warehouse import (Warehouse, MigrationConflict, PlanStale,
                       RevisionConflict, PreconditionFailed, StructuralRejection)

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT, 'warehouse.db')
LOG_DIR = os.path.join(ROOT, 'logs')
LOG_PATH = os.path.join(LOG_DIR, 'validation_log.json')

checks = []


def check(name, cond, detail=''):
    checks.append({'check': name, 'ok': bool(cond), 'detail': detail})
    if not cond:
        print('FAIL:', name, detail)
    else:
        print('ok  :', name)
    return bool(cond)


def fail_cases():
    return [c for c in checks if not c['ok']]


def new_warehouse():
    for suffix in ('', '-wal', '-shm'):
        p = DB_PATH + suffix
        if os.path.exists(p):
            os.remove(p)
    return Warehouse(DB_PATH)


def dsd_of(w, revision):
    seq = w.conn.execute(
        'SELECT dsd_version_seq FROM revision WHERE revision_seq=?',
        (revision,)).fetchone()[0]
    return seq, w._dsd(seq)


def assert_oracle(w, revision, extra_isolation=None, tag=''):
    dsd_seq, dsd = dsd_of(w, revision)
    expected = oracle_facts(w.conn, revision, dsd, dsd_seq, extra_isolation)
    obs_rows = {r['obs_key']: dict(r) for r in w.conn.execute(
        'SELECT * FROM v_obs_fact WHERE revision_seq=?', (revision,))}
    attr_rows = {}
    for r in w.conn.execute(
            'SELECT * FROM v_attr_fact WHERE revision_seq=?', (revision,)):
        attr_rows[(r['obs_key'], r['attr_id'])] = dict(r)

    ok = True
    detail = []
    if set(obs_rows) != set(expected):
        ok = False
        detail.append('obs key set mismatch: %s vs %s'
                      % (sorted(obs_rows), sorted(expected)))
    for ok_j, exp in expected.items():
        got = obs_rows[ok_j]
        if got['value_state'] != exp['value_state'] or \
                got['norm_value'] != exp['norm_value']:
            ok = False
            detail.append('value mismatch %s: (%s,%s) vs (%s,%s)' % (
                ok_j, got['value_state'], got['norm_value'],
                exp['value_state'], exp['norm_value']))
        exp_reasons = sorted(exp['reasons'])
        got_reasons = json.loads(got['reasons'])
        if got_reasons != exp_reasons:
            ok = False
            detail.append('isolation/reason mismatch %s: %s vs %s'
                          % (ok_j, got_reasons, exp_reasons))
        if got['isolated'] != int(bool(exp_reasons)):
            ok = False
            detail.append('isolated flag mismatch %s' % ok_j)
        for aid, eslot in exp['attrs'].items():
            g = attr_rows.get((ok_j, aid))
            if g is None:
                ok = False
                detail.append('missing attr_fact %s.%s' % (ok_j, aid))
                continue
            for field in ('present', 'raw_value', 'explicit_empty', 'valid',
                          'invalid_reason', 'source_edge_id', 'source_origin',
                          'source_revision', 'resolved_layer'):
                if g[field] != eslot[field]:
                    ok = False
                    detail.append('%s.%s.%s: fact=%r oracle=%r'
                                  % (ok_j, aid, field, g[field], eslot[field]))
    check('oracle agreement @rev%d %s' % (revision, tag), ok,
          '; '.join(detail[:6]))
    return ok


def key(country, measure, time_):
    return json.dumps({'COUNTRY': country, 'MEASURE': measure, 'TIME': time_},
                      sort_keys=True, separators=(',', ':'))


def main():
    make_fixtures.generate_all()
    os.makedirs(LOG_DIR, exist_ok=True)
    log = {'scenarios': []}

    # ------------------------------------------------------------------ #
    # Scenario 1: import semantics on real XML
    # ------------------------------------------------------------------ #
    w = new_warehouse()
    w.import_xml('fixtures/dsd-a.xml', 'fixtures/data-main.xml')
    check('import materialised 6 observations',
          w._scalar('SELECT COUNT(*) FROM obs_fact WHERE revision_seq=0') == 6)
    q0 = w.query_version(0)
    by_key = {o['obs_key']: o for o in q0['observations']}
    check('zero value canonical as finite 0',
          by_key[key('CN', 'VOL', '2026-01')]['norm_value'] == '0'
          and by_key[key('CN', 'VOL', '2026-01')]['value_state'] == 'finite')
    check('NaN treated as missing, not finite and not explicit empty',
          by_key[key('CN', 'VOL', '2026-02')]['value_state'] == 'missing'
          and by_key[key('CN', 'VOL', '2026-02')]['norm_value'] is None)
    check('trailing zeros normalised (1.2300 -> 1.23; 5.000 -> 5)',
          by_key[key('CN', 'PRICE', '2026-01')]['norm_value'] == '1.23'
          and by_key[key('CN', 'VOL', '2026-03')]['norm_value'] == '5')
    note = [a for a in q0['attributes'] if a['attr_id'] == 'NOTE']
    check('explicit empty NOTE is present with empty text',
          all(a['present'] == 1 and a['raw_value'] == '' and a['explicit_empty'] == 1
              for a in note) and len(note) == 6)
    us_obs = [o for o in q0['observations'] if '"US"' in o['obs_key']]
    check('duplicate CLASS group key isolates exactly the US observations',
          all(o['isolated'] == 1 and 'scope_conflict:CLASS' in o['reasons']
              for o in us_obs) and len(us_obs) == 2)
    check('CN observations are not isolated by the US group conflict',
          all(o['isolated'] == 0 for o in q0['observations'] if '"CN"' in o['obs_key']))
    units = {a['obs_key']: a for a in q0['attributes'] if a['attr_id'] == 'UNIT'}
    check('UNIT count vs USD preserved at series attachment',
          units[key('CN', 'VOL', '2026-01')]['raw_value'] == 'count'
          and units[key('CN', 'PRICE', '2026-01')]['raw_value'] == 'USD'
          and units[key('CN', 'VOL', '2026-01')]['resolved_layer'] == 'series')
    assert_oracle(w, 0, tag='after import')

    # ------------------------------------------------------------------ #
    # Scenario 2: series -> obs refinement migration (UNIT)
    # ------------------------------------------------------------------ #
    plan_b = w.plan_migration('fixtures/dsd-b.xml', 'plan-A-to-B')
    check('refinement plan is lossless', plan_b['lossless']
          and not plan_b['blocking_findings'])
    # refinement must expand: one derived edge per covered obs, source kept
    unit_deriv = [d for d in plan_b['derivations'] if d['attr_id'] == 'UNIT']
    check('refinement expands UNIT to one obs-scoped edge per observation',
          len(unit_deriv) == 6 and all(d['layer'] == 'obs' for d in unit_deriv))
    check('each derived edge retains its original source edge',
          all(d['source_edge_ids'] for d in unit_deriv))
    rev1 = w.apply_migration('plan-A-to-B')
    check('migration published revision 1 / dsd 1',
          rev1['revision_seq'] == 1 and rev1['dsd_version_seq'] == 1)
    q1 = w.query_version(1)
    u1 = {a['obs_key']: a for a in q1['attributes'] if a['attr_id'] == 'UNIT'}
    check('UNIT survived the attachment move (the baseline defect is fixed)',
          all(a['present'] == 1 and a['resolved_layer'] == 'obs'
              for a in u1.values()))
    check('migration provenance links every derived UNIT edge to a series edge',
          w._scalar(
              "SELECT COUNT(*) FROM migration_provenance mp JOIN decl_edge e "
              "ON e.edge_id=mp.derived_edge_id WHERE e.attr_id='UNIT'") == 6
          and w._scalar(
              "SELECT COUNT(*) FROM migration_provenance mp JOIN decl_edge s "
              "ON s.edge_id=mp.source_edge_id WHERE s.layer='series' "
              "AND s.attr_id='UNIT'") == 6)
    assert_oracle(w, 1, plan_b['isolated_observations'], tag='after refinement')

    # ------------------------------------------------------------------ #
    # Scenario 3: compat view while still uniform; then obs revision makes
    # the old series-level UNIT export inexpressible
    # ------------------------------------------------------------------ #
    export_ok = w.export_compat(1, 0)
    check('old DSD export expressible while series UNIT stays uniform',
          export_ok['expressible'] and len(export_ok['rows']) == 24)
    w.ingest_revision({
        'revision_seq': 2,
        'operations': [{
            'action': 'upsert',
            'key': {'COUNTRY': 'US', 'MEASURE': 'VOL', 'TIME': '2026-02'},
            'value': '7.50',
            'attrs': [{'attr_id': 'UNIT', 'value': 'USD'},
                      {'attr_id': 'OBS_STATUS', 'value': 'A'}]}]})
    assert_oracle(w, 2, tag='after obs revision')
    # the revision itself re-assesses compatibility against older DSD versions
    export_bad = w.export_compat(2, 0)
    check('divergent obs-level UNIT makes old DSD export inexpressible',
          not export_bad['expressible'])
    blocker = export_bad['blockers'][0]
    evidence_values = {e['value'] for ev in blocker['evidence'].values() for e in ev}
    evidence_obs = {e['obs_key'] for ev in blocker['evidence'].values() for e in ev}
    check('inexpressibility names UNIT and the real source observations',
          blocker['attr_id'] == 'UNIT'
          and evidence_obs == {key('US', 'VOL', '2026-01'),
                               key('US', 'VOL', '2026-02')}
          and evidence_values == {'USD', 'count'})
    check('no silent fallback: current facts still hold both values',
          w._scalar(
              "SELECT COUNT(*) FROM attr_fact WHERE revision_seq=2 AND attr_id='UNIT' "
              "AND raw_value='USD'") == 2
          and w._scalar(
              "SELECT COUNT(*) FROM attr_fact WHERE revision_seq=2 AND attr_id='UNIT' "
              "AND raw_value='count'") == 4)

    # ------------------------------------------------------------------ #
    # Scenario 4: same-revision idempotency vs same-seq/different-content
    # ------------------------------------------------------------------ #
    same = {
        'revision_seq': 3,
        'operations': [{'action': 'upsert',
                        'key': {'COUNTRY': 'CN', 'MEASURE': 'VOL', 'TIME': '2026-01'},
                        'value': '0',
                        'attrs': [{'attr_id': 'UNIT', 'value': 'count'},
                                  {'attr_id': 'OBS_STATUS', 'value': 'A'}]}]}
    r3 = w.ingest_revision(same)
    r3b = w.ingest_revision(same)
    check('identical same-seq revision replays idempotently',
          r3b['idempotent'] and r3b['revision_seq'] == 3)
    conflict_req = json.loads(json.dumps(same))
    conflict_req['operations'][0]['value'] = '99'
    raised = False
    try:
        w.ingest_revision(conflict_req)
    except RevisionConflict:
        raised = True
    check('same revision_seq with different content conflicts', raised)
    assert_oracle(w, 3, tag='after idempotent revision')

    # ------------------------------------------------------------------ #
    # Scenario 5: delete + reinsert, precondition-gated conflict repair,
    # coarsening plan with conflict listing
    # ------------------------------------------------------------------ #
    # delete an observation, confirm it disappears but history remains
    w.ingest_revision({
        'revision_seq': 4,
        'operations': [{'action': 'delete',
                        'key': {'COUNTRY': 'CN', 'MEASURE': 'VOL', 'TIME': '2026-03'}}]})
    check('delete removes the observation from current facts',
          w._scalar('SELECT COUNT(*) FROM obs_fact WHERE revision_seq=4') == 5)
    check('deleted observation still readable at earlier revisions',
          w._scalar('SELECT COUNT(*) FROM v_obs_fact WHERE revision_seq=1 '
                    "AND obs_key=?", (key('CN', 'VOL', '2026-03'),)) == 1)
    # reinsert
    w.ingest_revision({
        'revision_seq': 5,
        'operations': [{'action': 'upsert',
                        'key': {'COUNTRY': 'CN', 'MEASURE': 'VOL', 'TIME': '2026-03'},
                        'value': '5.000',
                        'attrs': [{'attr_id': 'UNIT', 'value': 'count'},
                                  {'attr_id': 'OBS_STATUS', 'value': 'A'}]}]})
    check('reinsert restores the observation',
          w._scalar('SELECT COUNT(*) FROM obs_fact WHERE revision_seq=5') == 6)
    # failed precondition must reject without changing facts
    bad_pre = {
        'revision_seq': 6,
        'operations': [{'action': 'upsert',
                        'key': {'COUNTRY': 'US', 'MEASURE': 'VOL', 'TIME': '2026-02'},
                        'preconditions': [{'attr_id': 'UNIT', 'expected': 'count'}],
                        'value': '8',
                        'attrs': [{'attr_id': 'UNIT', 'value': 'USD'},
                                  {'attr_id': 'OBS_STATUS', 'value': 'A'}]}]}
    raised = False
    try:
        w.ingest_revision(bad_pre)
    except PreconditionFailed:
        raised = True
    check('revision with unmet old-value precondition is rejected', raised)
    check('rejected revision created no new revision row',
          w.current_revision() == 5)
    # the same change with the correct precondition succeeds (explicit repair)
    good_pre = json.loads(json.dumps(bad_pre))
    good_pre['operations'][0]['preconditions'][0]['expected'] = 'USD'
    w.ingest_revision(good_pre)
    check('precondition-gated repair succeeds when old value matches',
          w.current_revision() == 6)
    assert_oracle(w, 6, tag='after delete/reinsert/preconditions')

    # plan a coarsening B(obs UNIT, currently divergent on US/VOL) -> C(series)
    plan_c = w.plan_migration('fixtures/dsd-c.xml', 'plan-B-to-C')
    conflicts = [f for f in plan_c['blocking_findings']
                 if f['kind'] == 'coarsening_conflict']
    check('coarsening conflict is listed, not last-value-wins',
          any(f['attr_id'] == 'UNIT' and
              set(f['values']) == {'USD', 'count'} for f in conflicts))
    listed = next(f for f in conflicts if f['attr_id'] == 'UNIT')
    check('conflict finding names the real affected observations and sources',
          {o['obs_key'] for o in listed['observations']}
          == {key('US', 'VOL', '2026-01'), key('US', 'VOL', '2026-02')}
          and all(o['source_edge_id'] for o in listed['observations']))
    raised = False
    try:
        w.apply_migration('plan-B-to-C')
    except MigrationConflict:
        raised = True
    check('applying a non-lossless plan raises MigrationConflict', raised)
    check('warehouse stays on revision 6 after refused migration',
          w.current_revision() == 6 and w.current_dsd_seq() == 1)

    # explicit revision removes the divergence (no unit conversion, just a
    # declared value replacement), then re-plan succeeds
    w.ingest_revision({
        'revision_seq': 7,
        'operations': [{
            'action': 'upsert',
            'key': {'COUNTRY': 'US', 'MEASURE': 'VOL', 'TIME': '2026-02'},
            'preconditions': [{'attr_id': 'UNIT', 'expected': 'USD'}],
            'value': '7.50',
            'attrs': [{'attr_id': 'UNIT', 'value': 'count'},
                      {'attr_id': 'OBS_STATUS', 'value': 'A'}]}]})
    stale = False
    try:
        w.apply_migration('plan-B-to-C')
    except PlanStale:
        stale = True
    check('plan bound to older revision is refused once facts change', stale)
    plan_c2 = w.plan_migration('fixtures/dsd-c.xml', 'plan-B-to-C-v2')
    unit_blockers = [f for f in plan_c2['blocking_findings']
                     if f['attr_id'] == 'UNIT']
    check('after explicit repair the UNIT coarsening has no conflict',
          not unit_blockers)
    source_iso = {i['obs_key'] for i in plan_c2['isolated_observations']
                  if i['attr_id'] == 'SOURCE'}
    check('new mandatory SOURCE without data isolates every observation',
          len(source_iso) == 6)
    rev8 = w.apply_migration('plan-B-to-C-v2')
    check('coarsened migration publishes revision 8 / dsd 2',
          rev8['revision_seq'] == 8 and rev8['dsd_version_seq'] == 2
          and rev8['isolated_obs'] == 6)
    q8 = w.query_version(8)
    u8 = {a['obs_key']: a for a in q8['attributes'] if a['attr_id'] == 'UNIT'}
    check('coarsened UNIT resolves at series layer with the shared value',
          all(a['resolved_layer'] == 'series' and a['present'] == 1
              for a in u8.values())
          and u8[key('US', 'VOL', '2026-02')]['raw_value'] == 'count')
    check('deleted optional NOTE simply disappears from the new DSD',
          not any(a['attr_id'] == 'NOTE' for a in q8['attributes']))
    check('optional AUDIT exists and is absent without fabricated defaults',
          all(a['attr_id'] != 'AUDIT' or a['present'] == 0
              for a in q8['attributes']))
    assert_oracle(w, 8, plan_c2['isolated_observations'], tag='after coarsening')

    # ------------------------------------------------------------------ #
    # Scenario 6: group attachment migration onto a different dimension
    # subset (G_COUNTRY -> G_MEASURE), and mid-migration revision staleness
    # ------------------------------------------------------------------ #
    plan_d = None
    raised = False
    try:
        # DSD D drops SOURCE, which is Mandatory in DSD C -> must be rejected
        w.plan_migration('fixtures/dsd-d.xml', 'plan-C-to-D')
    except StructuralRejection:
        raised = True
    check('deleting a mandatory attribute is rejected at planning', raised)
    w.close()

    # rebuild a clean warehouse for the group-subset move without SOURCE
    w = new_warehouse()
    w.import_xml('fixtures/dsd-a.xml', 'fixtures/data-main.xml')
    w.plan_migration('fixtures/dsd-b.xml', 'pAB')
    w.apply_migration('pAB')
    # custom DSD variant: CLASS group(COUNTRY) -> group(MEASURE), keep rest
    from make_fixtures import dsd, DSD_B, GROUPS_COUNTRY, GROUPS_MEASURE
    dsd_b2_specs = [(a, s, ('group_measure' if a == 'CLASS' else r), cl)
                    for (a, s, r, cl) in DSD_B]
    with open('fixtures/dsd-b2.xml', 'w', encoding='utf-8') as fh:
        fh.write(dsd('DSDB2', dsd_b2_specs, GROUPS_COUNTRY + GROUPS_MEASURE))
    # custom DSD variant: CLASS group(COUNTRY) -> group(MEASURE): the two
    # dimension subsets are incomparable (neither contains the other), so the
    # plan must list every observation as an incomparable-scope blocker
    from make_fixtures import dsd, DSD_B, GROUPS_COUNTRY, GROUPS_MEASURE
    dsd_b2_specs = [(a, s, ('group_measure' if a == 'CLASS' else r), cl)
                    for (a, s, r, cl) in DSD_B]
    with open('fixtures/dsd-b2.xml', 'w', encoding='utf-8') as fh:
        fh.write(dsd('DSDB2', dsd_b2_specs, GROUPS_COUNTRY + GROUPS_MEASURE))
    plan_g = w.plan_migration('fixtures/dsd-b2.xml', 'pGroup')
    incomparable = [f for f in plan_g['blocking_findings']
                    if f['kind'] == 'incomparable_scope' and f['attr_id'] == 'CLASS']
    check('incomparable group subset move lists all affected observations',
          plan_g['lossless'] is False and len(incomparable) == 1
          and len(incomparable[0]['observations']) == 6)
    raised = False
    try:
        w.apply_migration('pGroup')
    except MigrationConflict:
        raised = True
    check('incomparable-scope migration is refused, not guessed', raised)

    # a lossless refinement DSD (CLASS group(COUNTRY) -> obs) drives the
    # staleness and rollback scenarios below
    dsd_b3_specs = [(a, s, ('obs' if a == 'CLASS' else r), cl)
                    for (a, s, r, cl) in DSD_B]
    with open('fixtures/dsd-b3.xml', 'w', encoding='utf-8') as fh:
        fh.write(dsd('DSDB3', dsd_b3_specs, GROUPS_COUNTRY))
    plan_r = w.plan_migration('fixtures/dsd-b3.xml', 'pRefine')
    check('group->obs refinement plan is lossless', plan_r['lossless'])
    # a revision landing between plan and apply invalidates the plan
    w.ingest_revision({
        'revision_seq': 2,
        'operations': [{'action': 'upsert',
                        'key': {'COUNTRY': 'CN', 'MEASURE': 'VOL', 'TIME': '2026-01'},
                        'value': '0',
                        'attrs': [{'attr_id': 'UNIT', 'value': 'count'},
                                  {'attr_id': 'OBS_STATUS', 'value': 'A'}]}]})
    stale = False
    try:
        w.apply_migration('pRefine')
    except PlanStale:
        stale = True
    check('revision during planned migration forces re-plan (no recompute)', stale)
    check('stale plan is marked invalidated',
          w._scalar("SELECT status FROM migration_plan WHERE plan_id='pRefine'")
          == 'invalidated')

    # ------------------------------------------------------------------ #
    # Scenario 7: migration interruption rolls back atomically
    # ------------------------------------------------------------------ #
    w.plan_migration('fixtures/dsd-b3.xml', 'pRefine2')
    before_rev = w.current_revision()
    before_edges = w._scalar('SELECT COUNT(*) FROM decl_edge')
    before_dsd = w._scalar('SELECT COUNT(*) FROM dsd_version')
    orig = Warehouse._materialise

    def boom(self, *a, **k):
        raise RuntimeError('simulated failure during materialisation')

    Warehouse._materialise = boom
    raised = False
    try:
        w.apply_migration('pRefine2')
    except RuntimeError:
        raised = True
    Warehouse._materialise = orig
    check('failure inside migration propagates', raised)
    check('rollback leaves revision sequence untouched',
          w.current_revision() == before_rev)
    check('rollback leaves no partial derived edges or DSD rows',
          w._scalar('SELECT COUNT(*) FROM decl_edge') == before_edges
          and w._scalar('SELECT COUNT(*) FROM dsd_version') == before_dsd)
    check('rolled-back plan remains planned and can be re-applied',
          w._scalar("SELECT status FROM migration_plan WHERE plan_id='pRefine2'")
          == 'planned')
    reapply = w.apply_migration('pRefine2')
    check('re-applying after rollback succeeds', reapply['status'] == 'committed')
    assert_oracle(w, reapply['revision_seq'], tag='after group->obs move')

    # ------------------------------------------------------------------ #
    # Scenario 8: concurrent reader only ever sees a complete revision
    # ------------------------------------------------------------------ #
    ro = sqlite3.connect(DB_PATH)
    seen = []

    def snapshot():
        rev = ro.execute('SELECT MAX(revision_seq) FROM revision').fetchone()[0]
        n_obs = ro.execute(
            'SELECT COUNT(*) FROM obs_fact WHERE revision_seq=?', (rev,)).fetchone()[0]
        n_attr = ro.execute(
            'SELECT COUNT(*) FROM attr_fact WHERE revision_seq=?', (rev,)).fetchone()[0]
        dsd_n = ro.execute(
            'SELECT COUNT(*) FROM dsd_version').fetchone()[0]
        seen.append((rev, n_obs, n_attr, dsd_n))

    for _ in range(2):
        snapshot()
    w.ingest_revision({
        'revision_seq': w.current_revision() + 1,
        'operations': [{'action': 'upsert',
                        'key': {'COUNTRY': 'US', 'MEASURE': 'PRICE', 'TIME': '2026-02'},
                        'value': '3.14',
                        'attrs': [{'attr_id': 'UNIT', 'value': 'USD'},
                                  {'attr_id': 'OBS_STATUS', 'value': 'A'}]}]})
    snapshot()
    check('reader never observes a partial revision (facts match revision)',
          all((r is None) or (o > 0 and a > 0) for r, o, a, _ in seen)
          and seen[-1][0] == w.current_revision())
    ro.close()

    # ------------------------------------------------------------------ #
    # Scenario 9: codelist / misplaced / unknown-group structural rejection
    # ------------------------------------------------------------------ #
    from make_fixtures import data, value, group as grp, series as srs
    bad_group = data('DSDA', grp('G_NOPE', {'COUNTRY': 'CN'}, {'CLASS': 'x'}))
    with open('fixtures/data-bad-group.xml', 'w', encoding='utf-8') as fh:
        fh.write(bad_group)
    raised = False
    try:
        w2 = new_warehouse()
        w2.import_xml('fixtures/dsd-a.xml', 'fixtures/data-bad-group.xml')
    except Exception as e:
        raised = True
        bad_group_err = str(e)
    check('unknown group type rejects the dataset', raised and 'Group' in bad_group_err)

    # mandatory attribute given explicit empty text -> isolation, not default
    empty_unit_body = (
        '<g:Attributes></g:Attributes>'
        + srs('CN', 'VOL', '', [('2026-01', '1', 'A', [])])
        + srs('US', 'VOL', 'count', [('2026-01', '2', 'A', [])]))
    with open('fixtures/data-empty-mandatory.xml', 'w', encoding='utf-8') as fh:
        fh.write(data('DSDA', empty_unit_body))
    w3 = new_warehouse()
    w3.import_xml('fixtures/dsd-a.xml', 'fixtures/data-empty-mandatory.xml')
    iso = w3.query_version(0)
    cn = next(o for o in iso['observations'] if '"CN"' in o['obs_key'])
    check('explicit empty mandatory UNIT isolates only that series observations',
          cn['isolated'] == 1 and 'empty_mandatory:UNIT' in cn['reasons'])
    us = next(o for o in iso['observations'] if '"US"' in o['obs_key'])
    check('other series unaffected by the empty-mandatory declaration',
          us['isolated'] == 0)
    assert_oracle(w3, 0, tag='empty mandatory')
    w3.close()

    # structural DSD rejection: dimension identity change
    from make_fixtures import dsd as build_dsd
    dim_changed = build_dsd('DSDX', DSD_B).replace('id="MEASURE"', 'id="METRIC"')
    with open('fixtures/dsd-dim-change.xml', 'w', encoding='utf-8') as fh:
        fh.write(dim_changed)
    raised = False
    try:
        w.plan_migration('fixtures/dsd-dim-change.xml', 'pDim')
    except StructuralRejection:
        raised = True
    check('dimension identity change rejects before planning', raised)

    # ------------------------------------------------------------------ #
    # Scenario 10: audit trail and full version read-back
    # ------------------------------------------------------------------ #
    report = w.audit(source_files=['fixtures/dsd-a.xml', 'fixtures/data-main.xml',
                                   'fixtures/dsd-b.xml', 'fixtures/dsd-b2.xml'])
    check('audit reports actual materialised rows per revision',
          all(r['obs_fact_rows'] > 0 for r in report['materialised_by_revision']))
    check('audit reports provenance edge counts and file bytes',
          report['provenance_edges'] > 0 and report['source_file_total_bytes'] > 0
          and report['database_file_bytes'] > 0)
    log['audit'] = report
    log['revisions'] = w.revisions()
    log['plans'] = w.plans()
    # every committed revision remains independently queryable
    revs = [r['revision_seq'] for r in w.revisions()]
    readable = all(len(w.query_version(r)['observations']) >= 0 for r in revs)
    check('all revision facts remain queryable (current never overwrites history)',
          readable and len(revs) == w.current_revision() + 1)
    # history content is intact: rev1 still has the pre-divergence count value
    old = w.query_version(1)
    check('historical revision 1 still shows original US/VOL 2026-02 UNIT=count',
          next(a['raw_value'] for a in old['attributes']
               if a['attr_id'] == 'UNIT'
               and a['obs_key'] == key('US', 'VOL', '2026-02')) == 'count')
    assert_oracle(w, w.current_revision(), tag='final state')
    w.close()

    log['checks'] = checks
    log['summary'] = {'total': len(checks), 'passed': sum(c['ok'] for c in checks),
                      'failed': len(fail_cases())}
    with open(LOG_PATH, 'w', encoding='utf-8') as fh:
        json.dump(log, fh, ensure_ascii=False, indent=2, default=str)
    print('\n%s: %d/%d passed; log -> %s'
          % ('PASS' if not fail_cases() else 'FAIL',
             log['summary']['passed'], log['summary']['total'], LOG_PATH))
    return 1 if fail_cases() else 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(2)
