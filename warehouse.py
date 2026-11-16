"""SQLite statistical warehouse for SDMX DSD attachment migration.

Design (see design.md for the trade-off discussion):

* decl_edge / obs_decl store the *original hierarchical declarations* as
  append-only edges (raw text, explicit-empty flag, origin and provenance),
  never whole-XML copies.
* obs_fact / attr_fact are *canonical effective-attribute fact tables*
  materialised per (revision, DSD version): one row per observation and one row
  per observation x DSD attribute, with presence, value, isolation and the
  source edge id.  Coarsening validation is a GROUP BY over these facts and
  compatibility views are plain SQL projections of them.

Every publish (import / revision / migration) happens in one transaction so a
concurrent reader only ever observes a complete revision.
"""
import copy
import hashlib
import json
import os
import sqlite3
import datetime as _dt
from decimal import Decimal, InvalidOperation

from structure import load_dsd, scope_dims
from data_reader import parse_dataset

class WarehouseError(RuntimeError):
    pass


class MigrationConflict(WarehouseError):
    def __init__(self, plan_id, findings):
        super().__init__('migration %s not lossless: %d finding(s)' % (plan_id, len(findings)))
        self.plan_id = plan_id
        self.findings = findings


class PlanStale(WarehouseError):
    pass


class RevisionConflict(WarehouseError):
    pass


class PreconditionFailed(WarehouseError):
    pass


class StructuralRejection(WarehouseError):
    pass


# --------------------------------------------------------------------------- #
# value normalisation: finite decimal text vs missing vs explicit empty
# --------------------------------------------------------------------------- #
def normalize_value(raw):
    """Return (state, canonical_text_or_None).

    finite  -> ('finite', canonical decimal, trailing zeros stripped)
    missing -> ('missing', None)  (NaN/Inf/absent ObsValue; raw kept separately)
    empty   -> ('empty', None)    (explicit empty text)
    """
    if raw is None:
        return 'missing', None
    if raw == '':
        return 'empty', None
    try:
        d = Decimal(raw.strip())
    except (InvalidOperation, ValueError):
        return 'missing', None
    if not d.is_finite():
        return 'missing', None
    sign, digits, exp = d.as_tuple()
    digits = ''.join(str(x) for x in digits)
    if exp >= 0:
        intpart, frac = digits + '0' * exp, ''
    else:
        cut = len(digits) + exp
        if cut <= 0:
            intpart, frac = '0', '0' * (-cut) + digits
        else:
            intpart, frac = digits[:cut], digits[cut:]
    frac = frac.rstrip('0')
    return 'finite', ('-' if sign else '') + intpart + ('.' + frac if frac else '')


def _now():
    return _dt.datetime.utcnow().isoformat(timespec='seconds') + 'Z'


def _j(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


# --------------------------------------------------------------------------- #
# Warehouse
# --------------------------------------------------------------------------- #
class Warehouse:
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
    CREATE TABLE IF NOT EXISTS dsd_version(
      dsd_version_seq INTEGER PRIMARY KEY,
      dsd_id TEXT NOT NULL, descriptor_json TEXT NOT NULL,
      introduced_revision INTEGER NOT NULL, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS revision(
      revision_seq INTEGER PRIMARY KEY, kind TEXT NOT NULL,
      dsd_version_seq INTEGER NOT NULL, request_json TEXT,
      plan_id TEXT, status TEXT NOT NULL, message TEXT,
      content_hash TEXT NOT NULL, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS obs_decl(
      obs_decl_id INTEGER PRIMARY KEY AUTOINCREMENT,
      revision_seq INTEGER NOT NULL, origin TEXT NOT NULL, origin_ref TEXT,
      obs_key TEXT NOT NULL, raw_value TEXT, explicit_empty INTEGER NOT NULL,
      operation TEXT NOT NULL, supersedes_id INTEGER, revoked_revision INTEGER);
    CREATE TABLE IF NOT EXISTS decl_edge(
      edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
      dsd_version_seq INTEGER NOT NULL, revision_seq INTEGER NOT NULL,
      origin TEXT NOT NULL, origin_ref TEXT,
      derived_from_edge INTEGER, supersedes_edge INTEGER,
      revoked_revision INTEGER,
      attr_id TEXT NOT NULL, layer TEXT NOT NULL, group_type TEXT,
      scope_key TEXT NOT NULL, raw_value TEXT NOT NULL,
      explicit_empty INTEGER NOT NULL,
      valid INTEGER NOT NULL, invalid_reason TEXT);
    CREATE INDEX IF NOT EXISTS ix_edge_lookup
      ON decl_edge(dsd_version_seq, attr_id, layer, scope_key);
    CREATE TABLE IF NOT EXISTS migration_plan(
      plan_id TEXT PRIMARY KEY,
      from_dsd_version_seq INTEGER NOT NULL, to_dsd_version_seq INTEGER NOT NULL,
      bound_revision_seq INTEGER NOT NULL, bound_hash TEXT NOT NULL,
      status TEXT NOT NULL, plan_json TEXT NOT NULL,
      created_at TEXT NOT NULL, applied_revision INTEGER);
    CREATE TABLE IF NOT EXISTS migration_provenance(
      plan_id TEXT NOT NULL, revision_seq INTEGER NOT NULL,
      derived_edge_id INTEGER NOT NULL, source_edge_id INTEGER,
      obs_key TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS ix_prov_edge ON migration_provenance(derived_edge_id);
    CREATE TABLE IF NOT EXISTS obs_key_dim(
      obs_key_id INTEGER PRIMARY KEY,
      obs_key TEXT NOT NULL UNIQUE,
      country TEXT, measure TEXT, time_period TEXT);
    CREATE TABLE IF NOT EXISTS obs_fact(
      revision_seq INTEGER NOT NULL, obs_key_id INTEGER NOT NULL,
      value_state TEXT NOT NULL, raw_value TEXT, norm_value TEXT,
      isolated INTEGER NOT NULL, reasons TEXT NOT NULL,
      PRIMARY KEY(revision_seq, obs_key_id)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS attr_fact(
      revision_seq INTEGER NOT NULL, obs_key_id INTEGER NOT NULL,
      attr_id TEXT NOT NULL,
      present INTEGER NOT NULL, raw_value TEXT, explicit_empty INTEGER NOT NULL,
      resolved_layer TEXT NOT NULL, valid INTEGER NOT NULL, invalid_reason TEXT,
      source_edge_id INTEGER, source_origin TEXT, source_revision INTEGER,
      PRIMARY KEY(revision_seq, obs_key_id, attr_id)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS ix_attrfact_rev_attr ON attr_fact(revision_seq, attr_id);
    CREATE VIEW IF NOT EXISTS v_obs_fact AS
      SELECT f.revision_seq, d.obs_key, d.obs_key_id, d.country, d.measure,
             d.time_period, f.value_state, f.raw_value, f.norm_value,
             f.isolated, f.reasons
      FROM obs_fact f JOIN obs_key_dim d USING(obs_key_id);
    CREATE VIEW IF NOT EXISTS v_attr_fact AS
      SELECT f.revision_seq, d.obs_key, d.obs_key_id, f.attr_id, f.present,
             f.raw_value, f.explicit_empty, f.resolved_layer, f.valid,
             f.invalid_reason, f.source_edge_id, f.source_origin,
             f.source_revision
      FROM attr_fact f JOIN obs_key_dim d USING(obs_key_id);
    CREATE TABLE IF NOT EXISTS compat_assessment(
      revision_seq INTEGER NOT NULL, dsd_version_seq INTEGER NOT NULL,
      target_dsd_version_seq INTEGER NOT NULL, attr_id TEXT NOT NULL,
      expressible INTEGER NOT NULL, reason TEXT,
      affected_obs_count INTEGER NOT NULL, evidence TEXT NOT NULL,
      PRIMARY KEY(revision_seq, target_dsd_version_seq, attr_id)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS compat_view_registry(
      revision_seq INTEGER NOT NULL, target_dsd_version_seq INTEGER NOT NULL,
      view_name TEXT NOT NULL, PRIMARY KEY(revision_seq, target_dsd_version_seq))
      WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS audit_run(
      audit_seq INTEGER PRIMARY KEY AUTOINCREMENT, revision_seq INTEGER NOT NULL,
      report_json TEXT NOT NULL, created_at TEXT NOT NULL);
    """

    def __init__(self, db_path):
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('PRAGMA journal_mode=WAL')
        # keep the WAL bounded: checkpoint roughly every 16 MiB of frames
        self.conn.execute('PRAGMA wal_autocheckpoint=4000')
        self.conn.execute('PRAGMA foreign_keys=ON')
        self.conn.execute('PRAGMA synchronous=FULL')
        self.conn.execute('PRAGMA temp_store=MEMORY')
        self.conn.execute('PRAGMA cache_size=-65536')
        self.conn.executescript(self.SCHEMA)
        self.conn.commit()
        self._facts_cache = None

    def close(self):
        self.conn.close()

    def _invalidate_facts(self):
        self._facts_cache = None

    # ------------------------------------------------------------------ #
    # small helpers
    # ------------------------------------------------------------------ #
    def _scalar(self, sql, args=()):
        row = self.conn.execute(sql, args).fetchone()
        return None if row is None else row[0]

    def current_revision(self):
        return self._scalar('SELECT MAX(revision_seq) FROM revision')

    def current_dsd_seq(self, revision=None):
        if revision is None:
            revision = self.current_revision()
        return self._scalar(
            'SELECT dsd_version_seq FROM revision WHERE revision_seq=?', (revision,))

    def _dsd(self, seq):
        row = self.conn.execute(
            'SELECT descriptor_json FROM dsd_version WHERE dsd_version_seq=?',
            (seq,)).fetchone()
        if row is None:
            raise WarehouseError('unknown dsd version %s' % seq)
        return json.loads(row[0])

    def current_dsd(self):
        return self._dsd(self.current_dsd_seq())

    @staticmethod
    def _scope_key(dsd, attr, obs_key):
        dims = scope_dims(dsd, attr)
        if not dims:
            return '{}'
        return _j({d: obs_key[d] for d in sorted(dims)})

    # ------------------------------------------------------------------ #
    # initial XML import -> revision 0, dsd version 0
    # ------------------------------------------------------------------ #
    def import_xml(self, dsd_path, data_path):
        if self.current_revision() is not None:
            raise WarehouseError('warehouse already initialised')
        dsd = load_dsd(dsd_path)
        parsed = parse_dataset(data_path, dsd)
        if parsed['obs_count'] > 100_000:
            raise StructuralRejection('observation limit 100000 exceeded: %d'
                                      % parsed['obs_count'])
        try:
            self._invalidate_facts()
            with self.conn:  # atomic publish
                cur = self.conn
                cur.execute(
                    'INSERT INTO dsd_version(dsd_version_seq,dsd_id,descriptor_json,'
                    'introduced_revision,created_at) VALUES(0,?,?,0,?)',
                    (dsd['dsd_id'], _j(dsd), _now()))
                ref = os.path.basename(str(data_path))
                edge_rows, obs_rows = self._edges_from_xml(dsd, parsed, ref)
                cur.executemany(
                    'INSERT INTO obs_decl(revision_seq,origin,origin_ref,obs_key,'
                    'raw_value,explicit_empty,operation) VALUES(0,?,?,?,?,?,?)',
                    obs_rows)
                cur.executemany(
                    'INSERT INTO decl_edge(dsd_version_seq,revision_seq,origin,'
                    'origin_ref,attr_id,layer,group_type,scope_key,raw_value,'
                    'explicit_empty,valid,invalid_reason) '
                    'VALUES(0,0,?,?,?,?,?,?,?,?,?,?)', edge_rows)
                self._materialise(0, 0, dsd)
                content_hash = self._content_hash(0)
                cur.execute(
                    'INSERT INTO revision(revision_seq,kind,dsd_version_seq,'
                    'request_json,status,content_hash,created_at) '
                    "VALUES(0,'import',0,?,'committed',?,?)",
                    (_j({'dsd': str(dsd_path), 'data': str(data_path)}),
                     content_hash, _now()))
                self._publish_current_views(0)
        except Exception:
            self.conn.rollback()
            raise
        return self._revision_report(0)

    def _edges_from_xml(self, dsd, parsed, origin_ref):
        edge_rows, obs_rows = [], []
        attr_defs = dsd['attributes']

        def validate(attr_id, raw, declared_layer):
            if attr_id not in attr_defs:
                return 0, 'unknown_attribute:%s' % attr_id
            d = attr_defs[attr_id]
            if d['layer'] != declared_layer:
                return 0, 'misplaced:declared_%s_but_dsd_%s' % (declared_layer, d['layer'])
            if raw == '' and d['status'] == 'Mandatory':
                return 0, 'empty_mandatory'
            if d['codelist'] and raw not in dsd['codelists'][d['codelist']]:
                return 0, 'codelist_violation:%s' % d['codelist']
            return 1, None

        # dataset-level declarations cover every observation
        for attr_id, raw, _ in parsed['dataset_attrs']:
            valid, reason = validate(attr_id, raw, 'dataset')
            edge_rows.append(('xml', origin_ref, attr_id, 'dataset', None, '{}',
                              raw, int(raw == ''), valid, reason))
        # group declarations
        for g in parsed['groups']:
            gtype = g['type']
            skey = _j(g['key'])
            for attr_id, raw, _ in g['attrs']:
                valid, reason = validate(attr_id, raw, 'group')
                if valid and (attr_id not in attr_defs
                              or not attr_defs[attr_id]['group']
                              or attr_defs[attr_id]['group'] != gtype):
                    valid, reason = 0, 'misplaced:group_%s' % gtype
                edge_rows.append(('xml', origin_ref, attr_id, 'group', gtype, skey,
                                  raw, int(raw == ''), valid, reason))
        # series and obs declarations
        for s in parsed['series']:
            skey = _j(s['key'])
            for attr_id, raw, _ in s['attrs']:
                valid, reason = validate(attr_id, raw, 'series')
                if valid and attr_defs[attr_id]['layer'] not in ('series', 'dimset'):
                    valid, reason = 0, 'misplaced:declared_series_but_dsd_%s' % \
                        attr_defs[attr_id]['layer']
                edge_rows.append(('xml', origin_ref, attr_id, 'series', None, skey,
                                  raw, int(raw == ''), valid, reason))
            for o in s['obs']:
                okey = _j(o['key'])
                obs_rows.append(('xml', origin_ref, okey, o['raw_value'],
                                 int(o['raw_value'] == ''), 'upsert'))
                for attr_id, raw, _ in o['attrs']:
                    valid, reason = validate(attr_id, raw, 'obs')
                    edge_rows.append(('xml', origin_ref, attr_id, 'obs', None, okey,
                                      raw, int(raw == ''), valid, reason))
        return edge_rows, obs_rows

    # ------------------------------------------------------------------ #
    # revision ingest (update / delete / reinsert with preconditions)
    # ------------------------------------------------------------------ #
    def ingest_revision(self, request):
        if isinstance(request, (str, os.PathLike)):
            with open(request, encoding='utf-8') as fh:
                request = json.load(fh)
        req = copy.deepcopy(request)
        seq = req.get('revision_seq')
        cur_max = self.current_revision()
        if cur_max is None:
            raise WarehouseError('import_xml must run before revisions')
        # import occupies revision 0; the contract caps subsequent revisions
        # so the highest permitted revision_seq is 51
        if seq is None:
            seq = cur_max + 1
        if seq > 51:
            raise StructuralRejection('revision limit (50 post-import revisions) reached')
        prior = self.conn.execute(
            'SELECT request_json,status FROM revision WHERE revision_seq=?',
            (seq,)).fetchone()
        if prior is not None:
            # idempotent replay vs same-seq/different-content conflict
            if prior[1] != 'committed' or prior[0] != _j(req):
                raise RevisionConflict(
                    'revision_seq %d already exists with different content' % seq)
            return self._revision_report(seq, idempotent=True)
        if seq != cur_max + 1:
            raise RevisionConflict(
                'revision_seq %d is not the next sequence %d' % (seq, cur_max + 1))

        dsd_seq = self.current_dsd_seq()
        dsd = self.current_dsd()
        # validate request structurally against the current DSD
        self._validate_request(dsd, req)
        # preconditions are evaluated against current effective facts
        self._check_preconditions(dsd, seq, req)
        try:
            self._invalidate_facts()
            with self.conn:
                self._apply_request(dsd, dsd_seq, seq, req)
                self._materialise(seq, dsd_seq, dsd)
                h = self._content_hash(seq)
                self.conn.execute(
                    'INSERT INTO revision(revision_seq,kind,dsd_version_seq,'
                    'request_json,status,content_hash,created_at) '
                    "VALUES(?,'revision',?,?,'committed',?,?)",
                    (seq, dsd_seq, _j(req), h, _now()))
                self._publish_current_views(seq)
                # re-assess compatibility against every older DSD version so
                # obs-level divergence is reflected without a manual call
                older = [r[0] for r in self.conn.execute(
                    'SELECT dsd_version_seq FROM dsd_version '
                    'WHERE dsd_version_seq<? ORDER BY dsd_version_seq', (dsd_seq,))]
                for old_seq in older:
                    self._assess_compatibility(seq, dsd_seq, old_seq)
                # any un-applied plan is now invalidated by new facts
                self.conn.execute(
                    "UPDATE migration_plan SET status='invalidated' "
                    "WHERE status='planned' AND bound_revision_seq<?", (seq,))
        except Exception:
            self.conn.rollback()
            raise
        return self._revision_report(seq)

    def _validate_request(self, dsd, req):
        ops = req.get('operations')
        if not isinstance(ops, list) or not ops:
            raise StructuralRejection('revision needs a non-empty operations list')
        dim_ids = set(dimdef['id'] for dimdef in dsd['dimensions'])
        for op in ops:
            if op.get('action') not in ('upsert', 'delete'):
                raise StructuralRejection('bad operation action: %r' % op.get('action'))
            key = op.get('key')
            if not isinstance(key, dict) or set(key) != dim_ids:
                raise StructuralRejection('operation key must use the full dimension key')
            for dimdef in dsd['dimensions']:
                if dimdef['codelist'] and key[dimdef['id']] not in dsd['codelists'][dimdef['codelist']]:
                    raise StructuralRejection(
                        'key %s violates codelist %s' % (key, dimdef['codelist']))
            for a in op.get('attrs', []):
                aid = a.get('attr_id')
                if aid not in dsd['attributes']:
                    raise StructuralRejection('unknown attribute %s' % aid)
                if 'value' not in a and not a.get('remove'):
                    raise StructuralRejection('attr %s needs value or remove=true' % aid)
                if a.get('remove') and dsd['attributes'][aid]['status'] == 'Mandatory':
                    raise StructuralRejection('cannot remove mandatory attribute %s' % aid)
            for p in op.get('preconditions', []):
                if 'attr_id' in p:
                    if p['attr_id'] not in dsd['attributes']:
                        raise StructuralRejection(
                            'precondition unknown attribute %s' % p['attr_id'])
                elif 'value' not in p:
                    raise StructuralRejection('precondition needs attr_id or value')

    def _check_preconditions(self, dsd, seq, req):
        rev = self.current_revision()
        facts = {(r['obs_key'], r['attr_id']): r
                 for r in self.conn.execute(
                     'SELECT * FROM v_attr_fact WHERE revision_seq=?', (rev,))}
        obs = {r['obs_key']: r for r in self.conn.execute(
            'SELECT * FROM v_obs_fact WHERE revision_seq=?', (rev,))}
        for op in req['operations']:
            okey = _j(op['key'])
            for p in op.get('preconditions', []):
                if 'value' in p:
                    state, norm = normalize_value(p['value'])
                    cur = obs.get(okey)
                    if cur is None or cur['norm_value'] != norm or cur['value_state'] != state:
                        raise PreconditionFailed(
                            'value precondition failed for %s: expected %r got %r'
                            % (okey, p['value'], None if cur is None else cur['raw_value']))
                    continue
                f = facts.get((okey, p['attr_id']))
                if p.get('absent'):
                    if f is not None and f['present']:
                        raise PreconditionFailed(
                            'precondition failed: %s present on %s with %r'
                            % (p['attr_id'], okey, f['raw_value']))
                else:
                    if f is None or not f['present'] or f['raw_value'] != p['expected']:
                        raise PreconditionFailed(
                            'precondition failed for %s.%s: expected %r'
                            % (okey, p['attr_id'], p.get('expected')))

    def _apply_request(self, dsd, dsd_seq, seq, req):
        cur = self.conn
        for op in req['operations']:
            okey = _j(op['key'])
            if op['action'] == 'delete':
                # tombstone the observation and revoke its obs-scoped edges
                cur.execute(
                    'INSERT INTO obs_decl(revision_seq,origin,origin_ref,obs_key,'
                    'raw_value,explicit_empty,operation) VALUES(?,?,?,?,NULL,0,?)',
                    (seq, 'revision', str(seq), okey, 'delete'))
                cur.execute(
                    'UPDATE decl_edge SET revoked_revision=? WHERE dsd_version_seq=? '
                    "AND layer='obs' AND scope_key=? AND revoked_revision IS NULL",
                    (seq, dsd_seq, okey))
                continue
            raw = op.get('value')
            cur.execute(
                'INSERT INTO obs_decl(revision_seq,origin,origin_ref,obs_key,'
                'raw_value,explicit_empty,operation) VALUES(?,?,?,?,?,?,?)',
                (seq, 'revision', str(seq), okey, raw, int(raw == ''), 'upsert'))
            # reinserted/updated obs revokes prior obs-scoped edges first
            cur.execute(
                'UPDATE decl_edge SET revoked_revision=? WHERE dsd_version_seq=? '
                "AND layer='obs' AND scope_key=? AND revoked_revision IS NULL",
                (seq, dsd_seq, okey))
            for a in op.get('attrs', []):
                attr = dsd['attributes'][a['attr_id']]
                skey = self._scope_key(dsd, attr, op['key'])
                if a.get('remove'):
                    cur.execute(
                        'UPDATE decl_edge SET revoked_revision=? WHERE dsd_version_seq=? '
                        'AND attr_id=? AND layer=? AND scope_key=? '
                        'AND revoked_revision IS NULL',
                        (seq, dsd_seq, a['attr_id'], attr['layer'], skey))
                    continue
                prev = cur.execute(
                    'SELECT edge_id FROM decl_edge WHERE dsd_version_seq=? AND attr_id=? '
                    'AND layer=? AND scope_key=? AND revoked_revision IS NULL '
                    'ORDER BY edge_id DESC LIMIT 1',
                    (dsd_seq, a['attr_id'], attr['layer'], skey)).fetchone()
                valid, reason = 1, None
                if raw == '' and attr['status'] == 'Mandatory':
                    valid, reason = 0, 'empty_mandatory'
                if attr['codelist'] and a['value'] not in dsd['codelists'][attr['codelist']]:
                    valid, reason = 0, 'codelist_violation:%s' % attr['codelist']
                cur.execute(
                    'INSERT INTO decl_edge(dsd_version_seq,revision_seq,origin,'
                    'origin_ref,supersedes_edge,attr_id,layer,group_type,scope_key,'
                    'raw_value,explicit_empty,valid,invalid_reason) '
                    'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (dsd_seq, seq, 'revision', str(seq),
                     prev[0] if prev else None, a['attr_id'], attr['layer'],
                     attr['group'], skey, a['value'], int(a['value'] == ''),
                     valid, reason))
                if prev is not None:
                    cur.execute('UPDATE decl_edge SET revoked_revision=? WHERE edge_id=?',
                                (seq, prev[0]))

    # ------------------------------------------------------------------ #
    # migration planning
    # ------------------------------------------------------------------ #
    def plan_migration(self, to_dsd_path, plan_id=None):
        dsd_b = load_dsd(to_dsd_path)
        bound_rev = self.current_revision()
        if bound_rev is None:
            raise WarehouseError('no data loaded')
        dsd_a_seq = self.current_dsd_seq()
        dsd_a = self.current_dsd()
        if [d['id'] for d in dsd_a['dimensions']] != [d['id'] for d in dsd_b['dimensions']]:
            raise StructuralRejection('dimension identity must not change across DSD versions')
        # mandatory attributes may not be deleted
        for aid, a in dsd_a['attributes'].items():
            if a['status'] == 'Mandatory' and aid not in dsd_b['attributes']:
                raise StructuralRejection(
                    'mandatory attribute %s cannot be deleted' % aid)
        n_versions = self._scalar('SELECT COUNT(*) FROM dsd_version')
        if n_versions >= 4:
            raise StructuralRejection('DSD version limit (4) reached')

        facts = self._effective_facts(bound_rev, dsd_a, dsd_a_seq)
        plan = self._build_plan(dsd_a, dsd_a_seq, dsd_b, bound_rev, facts)
        if plan_id is None:
            plan_id = 'plan-r%d-v%d-to-v%d' % (
                bound_rev, dsd_a_seq, n_versions)
        plan['plan_id'] = plan_id
        plan['to_descriptor'] = dsd_b
        bound_hash = self._scalar(
            'SELECT content_hash FROM revision WHERE revision_seq=?', (bound_rev,))
        with self.conn:
            self.conn.execute(
                'INSERT INTO migration_plan(plan_id,from_dsd_version_seq,'
                'to_dsd_version_seq,bound_revision_seq,bound_hash,status,'
                'plan_json,created_at) VALUES(?,?,?,?,?,?,?,?)',
                (plan_id, dsd_a_seq, n_versions, bound_rev, bound_hash,
                 'planned', _j(plan), _now()))
        return plan

    def _build_plan(self, dsd_a, seq_a, dsd_b, bound_rev, facts):
        findings, derivations, isolated_obs = [], [], []
        obs_keys = sorted(facts.keys())
        b_scope = {aid: scope_dims(dsd_b, a) for aid, a in dsd_b['attributes'].items()}
        a_scope = {aid: scope_dims(dsd_a, a) for aid, a in dsd_a['attributes'].items()}

        for aid, attr_b in dsd_b['attributes'].items():
            if aid not in dsd_a['attributes']:
                if attr_b['status'] == 'Mandatory':
                    # no source anywhere: isolate, never fabricate a default
                    for ok in obs_keys:
                        isolated_obs.append({'obs_key': ok, 'attr_id': aid,
                                             'reason': 'new_mandatory_without_source'})
                continue
            attr_a = dsd_a['attributes'][aid]
            sa, sb = a_scope[aid], b_scope[aid]
            # codelist tightening: values that stop being legal under B
            if attr_b['codelist']:
                allowed = set(dsd_b['codelists'][attr_b['codelist']])
                for ok in obs_keys:
                    f = facts[ok]['attrs'].get(aid)
                    if f and f['present'] and f['raw_value'] not in allowed:
                        isolated_obs.append({'obs_key': ok, 'attr_id': aid,
                                             'reason': 'codelist_violation_under_new_dsd',
                                             'value': f['raw_value']})
            if sa == sb:
                for ok in obs_keys:
                    f = facts[ok]['attrs'][aid]
                    if f['present']:
                        derivations.append(self._deriv(attr_b, dsd_b, ok, f))
                    elif attr_b['status'] == 'Mandatory':
                        isolated_obs.append({'obs_key': ok, 'attr_id': aid,
                                             'reason': 'missing_mandatory_after_migration'})
                continue
            if sb > sa:  # refinement: expand, keep every original declaration source
                for ok in obs_keys:
                    f = facts[ok]['attrs'][aid]
                    if f['present']:
                        derivations.append(self._deriv(attr_b, dsd_b, ok, f))
                    elif attr_b['status'] == 'Mandatory':
                        isolated_obs.append({'obs_key': ok, 'attr_id': aid,
                                             'reason': 'missing_mandatory_after_migration'})
                continue
            if sb < sa:  # coarsening: uniformity + no required-missing required
                cells = {}
                for ok in obs_keys:
                    f = facts[ok]['attrs'][aid]
                    cell = self._scope_key(dsd_b, attr_b, json.loads(ok))
                    cells.setdefault(cell, []).append((ok, f))
                for cell, members in sorted(cells.items()):
                    vals = {(f['raw_value'], f['explicit_empty']) for _, f in members
                            if f['present']}
                    missing = [ok for ok, f in members if not f['present']]
                    if len(vals) > 1:
                        findings.append({
                            'kind': 'coarsening_conflict', 'attr_id': aid,
                            'new_scope_key': cell,
                            'values': sorted(v for v, _ in vals),
                            'observations': [
                                {'obs_key': ok, 'value': f['raw_value'],
                                 'source_edge_id': f['source_edge_id']}
                                for ok, f in members if f['present']]})
                    elif missing and attr_b['status'] == 'Mandatory':
                        findings.append({
                            'kind': 'required_missing_in_coarsened_scope',
                            'attr_id': aid, 'new_scope_key': cell,
                            'observations': missing})
                    elif len(vals) == 1:
                        val, empty = next(iter(vals))
                        srcs = [f['source_edge_id'] for _, f in members if f['present']]
                        derivations.append({
                            'attr_id': aid, 'layer': attr_b['layer'],
                            'group_type': attr_b['group'], 'scope_key': cell,
                            'raw_value': val, 'explicit_empty': empty,
                            'obs_keys': [ok for ok, _ in members],
                            'source_edge_ids': sorted(s for s in srcs if s)})
                continue
            # incomparable scopes: every cell conflicts
            findings.append({
                'kind': 'incomparable_scope', 'attr_id': aid,
                'old_scope': sorted(sa), 'new_scope': sorted(sb),
                'observations': obs_keys})

        # conditional -> mandatory tightening on retained attributes
        for aid, attr_b in dsd_b['attributes'].items():
            if aid in dsd_a['attributes'] and attr_b['status'] == 'Mandatory' \
                    and dsd_a['attributes'][aid]['status'] == 'Conditional':
                for ok in obs_keys:
                    f = facts[ok]['attrs'][aid]
                    if not f['present']:
                        isolated_obs.append({'obs_key': ok, 'attr_id': aid,
                                             'reason': 'newly_mandatory_missing'})

        blocking = [f for f in findings]
        return {
            'from_dsd_id': dsd_a['dsd_id'], 'to_dsd_id': dsd_b['dsd_id'],
            'bound_revision_seq': bound_rev,
            'observation_count': len(obs_keys),
            'derived_edge_count': len(derivations),
            'derivations': derivations,
            'blocking_findings': blocking,
            'isolated_observations': isolated_obs,
            'deleted_optional_attributes': [
                aid for aid in dsd_a['attributes'] if aid not in dsd_b['attributes']],
            'lossless': not blocking,
        }

    @staticmethod
    def _deriv(attr_b, dsd_b, obs_key_json, fact):
        key = json.loads(obs_key_json)
        return {
            'attr_id': attr_b['id'], 'layer': attr_b['layer'],
            'group_type': attr_b['group'],
            'scope_key': Warehouse._scope_key(dsd_b, attr_b, key),
            'raw_value': fact['raw_value'], 'explicit_empty': fact['explicit_empty'],
            'obs_keys': [obs_key_json],
            'source_edge_ids': [fact['source_edge_id']] if fact['source_edge_id'] else [],
        }

    # ------------------------------------------------------------------ #
    # migration apply (atomic; plan is bound and may go stale)
    # ------------------------------------------------------------------ #
    def apply_migration(self, plan_id):
        plan_row = self.conn.execute(
            'SELECT * FROM migration_plan WHERE plan_id=?', (plan_id,)).fetchone()
        if plan_row is None:
            raise WarehouseError('unknown plan %s' % plan_id)
        if plan_row['status'] == 'applied':
            return self._revision_report(plan_row['applied_revision'], idempotent=True)
        if plan_row['status'] != 'planned':
            raise PlanStale('plan %s is %s and cannot be applied'
                            % (plan_id, plan_row['status']))
        cur_rev = self.current_revision()
        if cur_rev != plan_row['bound_revision_seq']:
            raise PlanStale(
                'plan bound to revision %d but current revision is %d; re-plan required'
                % (plan_row['bound_revision_seq'], cur_rev))
        cur_hash = self._scalar(
            'SELECT content_hash FROM revision WHERE revision_seq=?', (cur_rev,))
        if cur_hash != plan_row['bound_hash']:
            raise PlanStale('plan %s content hash no longer matches' % plan_id)
        plan = json.loads(plan_row['plan_json'])
        if plan['blocking_findings']:
            raise MigrationConflict(plan_id, plan['blocking_findings'])

        new_rev = cur_rev + 1
        new_dsd_seq = plan_row['to_dsd_version_seq']
        dsd_b = plan['to_descriptor']
        try:
            self._invalidate_facts()
            with self.conn:
                self.conn.execute(
                    'INSERT INTO dsd_version(dsd_version_seq,dsd_id,descriptor_json,'
                    'introduced_revision,created_at) VALUES(?,?,?,?,?)',
                    (new_dsd_seq, dsd_b['dsd_id'], _j(dsd_b), new_rev, _now()))
                for d in plan['derivations']:
                    cur = self.conn.execute(
                        'INSERT INTO decl_edge(dsd_version_seq,revision_seq,origin,'
                        'origin_ref,derived_from_edge,attr_id,layer,group_type,'
                        'scope_key,raw_value,explicit_empty,valid,invalid_reason) '
                        'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        (new_dsd_seq, new_rev, 'migration', plan_id,
                         d['source_edge_ids'][0] if d['source_edge_ids'] else None,
                         d['attr_id'], d['layer'], d['group_type'], d['scope_key'],
                         d['raw_value'], d['explicit_empty'], 1, None)).lastrowid
                    for ok in d['obs_keys']:
                        for src in d['source_edge_ids']:
                            self.conn.execute(
                                'INSERT INTO migration_provenance(plan_id,revision_seq,'
                                'derived_edge_id,source_edge_id,obs_key) '
                                'VALUES(?,?,?,?,?)',
                                (plan_id, new_rev, cur, src, ok))
                self._materialise(new_rev, new_dsd_seq, dsd_b,
                                  extra_isolation=plan['isolated_observations'])
                h = self._content_hash(new_rev)
                self.conn.execute(
                    'INSERT INTO revision(revision_seq,kind,dsd_version_seq,'
                    'request_json,plan_id,status,content_hash,created_at) '
                    "VALUES(?, 'migration', ?, ?, ?, 'committed', ?, ?)",
                    (new_rev, new_dsd_seq, plan_row['plan_json'], plan_id, h, _now()))
                self.conn.execute(
                    "UPDATE migration_plan SET status='applied',applied_revision=? "
                    'WHERE plan_id=?', (new_rev, plan_id))
                self._publish_current_views(new_rev)
                self._assess_compatibility(new_rev, new_dsd_seq,
                                           plan_row['from_dsd_version_seq'])
        except Exception:
            self.conn.rollback()
            raise
        return self._revision_report(new_rev)

    # ------------------------------------------------------------------ #
    # effective fact resolution (the canonical semantic baseline)
    # ------------------------------------------------------------------ #
    def _active_edges(self, revision, dsd_seq):
        rows = self.conn.execute(
            'SELECT * FROM decl_edge WHERE dsd_version_seq=? AND revision_seq<=? '
            'AND (revoked_revision IS NULL OR revoked_revision>?)',
            (dsd_seq, revision, revision)).fetchall()
        grouped = {}
        for r in rows:
            grouped.setdefault((r['attr_id'], r['layer'], r['group_type'],
                                r['scope_key']), []).append(r)
        return grouped

    def _active_obs(self, revision):
        rows = self.conn.execute(
            'SELECT * FROM obs_decl WHERE revision_seq<=?', (revision,)).fetchall()
        latest = {}
        for r in rows:
            prev = latest.get(r['obs_key'])
            if prev is None or r['obs_decl_id'] > prev['obs_decl_id']:
                latest[r['obs_key']] = r
        return {k: r for k, r in latest.items() if r['operation'] != 'delete'}

    def _effective_facts(self, revision, dsd, dsd_seq=None, use_cache=True):
        """Pure-Python resolution used by planning and independently testable.

        Candidate declarations are collected per (observation, attribute), then
        adjudicated once: distinct valid values -> scope_conflict isolation;
        invalid edges (misplaced/codelist/duplicate-scope conflict) never shadow
        a valid value; mandatory absence is recorded as its own reason.
        """
        if use_cache:
            cached = getattr(self, '_facts_cache', None)
            if cached and cached[0] == revision and cached[1] == dsd_seq:
                return cached[2]
        if dsd_seq is None:
            dsd_seq = self.current_dsd_seq(revision)
        edges = self._active_edges(revision, dsd_seq)
        active_obs = self._active_obs(revision)
        facts = {}
        for okey_j, decl in active_obs.items():
            okey = json.loads(okey_j)
            state, norm = normalize_value(decl['raw_value'])
            facts[okey_j] = {'key': okey, 'value_state': state,
                             'raw_value': decl['raw_value'], 'norm_value': norm,
                             'reasons': [], 'attrs': {}}
        # dimension codelist checks
        for ok_j, f in facts.items():
            for d in dsd['dimensions']:
                if d['codelist'] and f['key'][d['id']] not in dsd['codelists'][d['codelist']]:
                    f['reasons'].append('bad_dimension_code:%s' % d['id'])
        # index edges once per attribute so candidate lookup per observation is
        # O(attachment shapes) instead of O(total edges)
        by_attr = {}
        for ident, elist in edges.items():
            by_attr.setdefault(ident[0], []).extend(elist)
        key_cache = {}

        def scoped_key(dims, ok_j, key):
            ck = (dims, ok_j)
            val = key_cache.get(ck)
            if val is None:
                val = _j({d: key[d] for d in dims})
                key_cache[ck] = val
            return val

        for aid, attr in dsd['attributes'].items():
            idx = self._build_edge_index(by_attr.get(aid, ()))
            for ok_j, f in facts.items():
                cand = self._candidates(idx, dsd, f['key'], scoped_key, ok_j)
                slot = {'present': False, 'raw_value': None, 'explicit_empty': 0,
                        'valid': True, 'invalid_reason': None, 'source_edge_id': None,
                        'source_origin': None, 'source_revision': None,
                        'resolved_layer': attr['layer']}
                valid_edges = [e for e in cand if e['valid']]
                invalid_edges = [e for e in cand if not e['valid']]
                distinct = {(e['raw_value'], e['explicit_empty']) for e in valid_edges}
                if len(distinct) > 1:
                    # same-scope key conflict: isolate the real affected obs
                    chosen = max(valid_edges, key=lambda e: e['edge_id'])
                    slot.update(present=True, raw_value=chosen['raw_value'],
                                explicit_empty=chosen['explicit_empty'],
                                valid=False, invalid_reason='scope_conflict',
                                source_edge_id=chosen['edge_id'],
                                source_origin=chosen['origin'],
                                source_revision=chosen['revision_seq'])
                    f['reasons'].append('scope_conflict:%s' % aid)
                elif len(distinct) == 1:
                    chosen = max(valid_edges, key=lambda e: e['edge_id'])
                    slot.update(present=True, raw_value=chosen['raw_value'],
                                explicit_empty=chosen['explicit_empty'],
                                source_edge_id=chosen['edge_id'],
                                source_origin=chosen['origin'],
                                source_revision=chosen['revision_seq'])
                elif invalid_edges:
                    chosen = max(invalid_edges, key=lambda e: e['edge_id'])
                    slot.update(present=True, raw_value=chosen['raw_value'],
                                explicit_empty=chosen['explicit_empty'],
                                valid=False, invalid_reason=chosen['invalid_reason'],
                                source_edge_id=chosen['edge_id'],
                                source_origin=chosen['origin'],
                                source_revision=chosen['revision_seq'])
                    f['reasons'].append('%s:%s' % (chosen['invalid_reason'], aid))
                elif attr['status'] == 'Mandatory':
                    f['reasons'].append('missing_mandatory:%s' % aid)
                f['attrs'][aid] = slot
        self._facts_cache = (revision, dsd_seq, facts)
        return facts

    @staticmethod
    def _build_edge_index(rows):
        """Index declaration rows by their own declared layer/scope key."""
        idx = {'dataset': [], 'group': {}, 'scoped': {}}
        for r in rows:
            if r['layer'] == 'dataset':
                idx['dataset'].append(r)
            elif r['layer'] == 'group':
                idx['group'].setdefault(r['group_type'], {}) \
                    .setdefault(r['scope_key'], []).append(r)
            else:
                dims = frozenset(json.loads(r['scope_key']).keys())
                idx['scoped'].setdefault(dims, {}) \
                    .setdefault(r['scope_key'], []).append(r)
        return idx

    @staticmethod
    def _candidates(idx, dsd, key, resolve=None, ok_j=None):
        out = list(idx['dataset'])
        for gtype, buckets in idx['group'].items():
            dims = frozenset(dsd['groups'][gtype])
            if resolve is not None:
                skey = resolve(dims, ok_j, key)
            else:
                skey = _j({d: key[d] for d in dims})
            out.extend(buckets.get(skey, ()))
        for dims, buckets in idx['scoped'].items():
            if resolve is not None:
                skey = resolve(dims, ok_j, key)
            else:
                skey = _j({d: key[d] for d in dims})
            out.extend(buckets.get(skey, ()))
        return out

    def _materialise(self, revision, dsd_seq, dsd, extra_isolation=None):
        facts = self._effective_facts(revision, dsd, dsd_seq)
        extra = {}
        for item in extra_isolation or []:
            extra.setdefault(item['obs_key'], []).append(
                '%s:%s' % (item['reason'], item['attr_id']))
        # register every observation key once; facts store the integer proxy
        for ok_j in sorted(facts):
            key = facts[ok_j]['key']
            self.conn.execute(
                'INSERT OR IGNORE INTO obs_key_dim(obs_key,country,measure,'
                'time_period) VALUES(?,?,?,?)',
                (ok_j, key.get('COUNTRY'), key.get('MEASURE'),
                 key.get(dsd['time_dimension'])))
        id_map = {r['obs_key']: r['obs_key_id'] for r in self.conn.execute(
            'SELECT obs_key_id,obs_key FROM obs_key_dim')}
        obs_rows, attr_rows = [], []
        for ok_j in sorted(facts):
            f = facts[ok_j]
            reasons = sorted(set(f['reasons']) | set(extra.get(ok_j, [])))
            obs_rows.append((revision, id_map[ok_j], f['value_state'],
                             f['raw_value'], f['norm_value'],
                             int(bool(reasons)), _j(reasons)))
            for aid in sorted(dsd['attributes']):
                a = f['attrs'][aid]
                attr_rows.append((revision, id_map[ok_j], aid, int(a['present']),
                                  a['raw_value'], a['explicit_empty'],
                                  a['resolved_layer'], int(a['valid']),
                                  a['invalid_reason'], a['source_edge_id'],
                                  a['source_origin'], a['source_revision']))
        self.conn.execute('DELETE FROM obs_fact WHERE revision_seq=?', (revision,))
        self.conn.execute('DELETE FROM attr_fact WHERE revision_seq=?', (revision,))
        self.conn.executemany(
            'INSERT INTO obs_fact(revision_seq,obs_key_id,value_state,raw_value,'
            'norm_value,isolated,reasons) VALUES(?,?,?,?,?,?,?)', obs_rows)
        # chunk for large warehouses
        for i in range(0, len(attr_rows), 5000):
            self.conn.executemany(
                'INSERT INTO attr_fact(revision_seq,obs_key_id,attr_id,present,'
                'raw_value,explicit_empty,resolved_layer,valid,invalid_reason,'
                'source_edge_id,source_origin,source_revision) '
                'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', attr_rows[i:i + 5000])

    # ------------------------------------------------------------------ #
    # current views + compatibility assessment / views
    # ------------------------------------------------------------------ #
    def _publish_current_views(self, revision):
        self.conn.execute('DROP VIEW IF EXISTS current_obs_fact')
        self.conn.execute('DROP VIEW IF EXISTS current_attr_fact')
        self.conn.execute(
            'CREATE VIEW current_obs_fact AS SELECT * FROM v_obs_fact '
            'WHERE revision_seq=(SELECT MAX(revision_seq) FROM revision)')
        self.conn.execute(
            'CREATE VIEW current_attr_fact AS SELECT * FROM v_attr_fact '
            'WHERE revision_seq=(SELECT MAX(revision_seq) FROM revision)')

    def _assess_compatibility(self, revision, new_dsd_seq, old_dsd_seq):
        dsd_new = self._dsd(new_dsd_seq)
        dsd_old = self._dsd(old_dsd_seq)
        facts = self._effective_facts(revision, dsd_new, new_dsd_seq)
        # re-assessment replaces any previous result for this revision pair
        old_view = self._scalar(
            'SELECT view_name FROM compat_view_registry WHERE revision_seq=? '
            'AND target_dsd_version_seq=?', (revision, old_dsd_seq))
        if old_view:
            self.conn.execute('DROP VIEW IF EXISTS %s' % old_view)
        self.conn.execute(
            'DELETE FROM compat_view_registry WHERE revision_seq=? '
            'AND target_dsd_version_seq=?', (revision, old_dsd_seq))
        self.conn.execute(
            'DELETE FROM compat_assessment WHERE revision_seq=? '
            'AND target_dsd_version_seq=?', (revision, old_dsd_seq))
        all_ok = True
        for aid, attr_old in dsd_old['attributes'].items():
            if aid not in dsd_new['attributes']:
                all_ok = False
                self.conn.execute(
                    'INSERT INTO compat_assessment(revision_seq,dsd_version_seq,'
                    'target_dsd_version_seq,attr_id,expressible,reason,'
                    'affected_obs_count,evidence) VALUES(?,?,?,?,0,?,?,?)',
                    (revision, new_dsd_seq, old_dsd_seq, aid,
                     'attribute_deleted_in_new_dsd', len(facts),
                     _j(sorted(facts.keys()))))
                continue
            attr_new = dsd_new['attributes'][aid]
            so, sn = scope_dims(dsd_old, attr_old), scope_dims(dsd_new, attr_new)
            blockers, evidence = [], {}
            if sn <= so:
                # new scope coarser/equal: broadcast is always representable
                pass
            else:
                # new scope finer: every old-scope cell must be uniform in
                # presence and value
                cells = {}
                for ok_j, f in facts.items():
                    key = f['key']
                    cell = _j({d: key[d] for d in sorted(so)})
                    cells.setdefault(cell, set()).add(
                        (f['attrs'][aid]['present'],
                         f['attrs'][aid]['raw_value'],
                         f['attrs'][aid]['explicit_empty']))
                for cell, sig in sorted(cells.items()):
                    if len(sig) > 1:
                        blockers.append(cell)
                        evidence[cell] = [
                            {'obs_key': ok,
                             'value': facts[ok]['attrs'][aid]['raw_value'],
                             'present': facts[ok]['attrs'][aid]['present'],
                             'source_edge_id': facts[ok]['attrs'][aid]['source_edge_id']}
                            for ok in sorted(facts)
                            if _j({d: facts[ok]['key'][d] for d in sorted(so)}) == cell]
            if blockers:
                all_ok = False
                self.conn.execute(
                    'INSERT INTO compat_assessment(revision_seq,dsd_version_seq,'
                    'target_dsd_version_seq,attr_id,expressible,reason,'
                    'affected_obs_count,evidence) VALUES(?,?,?,?,0,?,?,?)',
                    (revision, new_dsd_seq, old_dsd_seq, aid,
                     'divergence_within_old_scope',
                     sum(len(v) for v in evidence.values()), _j(evidence)))
            else:
                self.conn.execute(
                    'INSERT INTO compat_assessment(revision_seq,dsd_version_seq,'
                    'target_dsd_version_seq,attr_id,expressible,reason,'
                    'affected_obs_count,evidence) VALUES(?,?,?,?,1,NULL,0,?)',
                    (revision, new_dsd_seq, old_dsd_seq, aid, '{}'))
        if all_ok:
            view = 'compat_r%d_dsd%d' % (revision, old_dsd_seq)
            self.conn.execute('DROP VIEW IF EXISTS %s' % view)
            old_attrs = ','.join("'%s'" % a for a in sorted(dsd_old['attributes']))
            self.conn.execute(
                'CREATE VIEW %s AS SELECT o.revision_seq,o.obs_key,o.country,'
                'o.measure,o.time_period,o.value_state,o.raw_value,o.norm_value,'
                'o.isolated,o.reasons,a.attr_id,a.present,a.raw_value AS attr_value,'
                'a.explicit_empty,a.source_edge_id,a.source_origin,a.source_revision '
                'FROM v_obs_fact o JOIN v_attr_fact a ON a.revision_seq=o.revision_seq '
                'AND a.obs_key=o.obs_key WHERE o.revision_seq=%d AND a.attr_id IN (%s)'
                % (view, revision, old_attrs))
            self.conn.execute(
                'INSERT INTO compat_view_registry(revision_seq,target_dsd_version_seq,'
                'view_name) VALUES(?,?,?)', (revision, old_dsd_seq, view))

    def export_compat(self, revision, target_dsd_seq):
        """Return old-DSD shaped export, or an explicit inexpressibility report."""
        rows = self.conn.execute(
            'SELECT * FROM compat_assessment WHERE revision_seq=? '
            'AND target_dsd_version_seq=?', (revision, target_dsd_seq)).fetchall()
        if not rows:
            raise WarehouseError('no compatibility assessment for revision %d / dsd %d'
                                 % (revision, target_dsd_seq))
        bad = [dict(r) for r in rows if not r['expressible']]
        if bad:
            for b in bad:
                b['evidence'] = json.loads(b['evidence'])
            return {'expressible': False, 'revision_seq': revision,
                    'target_dsd_version_seq': target_dsd_seq,
                    'blockers': bad,
                    'note': 'old DSD export refused: facts cannot be represented; '
                            'current facts are not rolled back to stale data'}
        view = self._scalar(
            'SELECT view_name FROM compat_view_registry WHERE revision_seq=? '
            'AND target_dsd_version_seq=?', (revision, target_dsd_seq))
        data = [dict(r) for r in self.conn.execute('SELECT * FROM %s' % view)]
        return {'expressible': True, 'revision_seq': revision,
                'target_dsd_version_seq': target_dsd_seq,
                'view_name': view, 'rows': data}

    # ------------------------------------------------------------------ #
    # queries
    # ------------------------------------------------------------------ #
    def query_version(self, revision=None):
        if revision is None:
            revision = self.current_revision()
        rev = self.conn.execute(
            'SELECT * FROM revision WHERE revision_seq=?', (revision,)).fetchone()
        if rev is None:
            raise WarehouseError('unknown revision %d' % revision)
        dsd = self._dsd(rev['dsd_version_seq'])
        obs = [dict(r) for r in self.conn.execute(
            'SELECT * FROM v_obs_fact WHERE revision_seq=? ORDER BY obs_key',
            (revision,))]
        attrs = [dict(r) for r in self.conn.execute(
            'SELECT * FROM v_attr_fact WHERE revision_seq=? ORDER BY obs_key,attr_id',
            (revision,))]
        for r in obs:
            r['reasons'] = json.loads(r['reasons'])
        return {'revision_seq': revision, 'kind': rev['kind'],
                'dsd_version_seq': rev['dsd_version_seq'],
                'dsd_id': dsd['dsd_id'],
                'observations': obs, 'attributes': attrs}

    def revisions(self):
        return [dict(r) for r in self.conn.execute(
            'SELECT revision_seq,kind,dsd_version_seq,status,plan_id,'
            'content_hash,created_at FROM revision ORDER BY revision_seq')]

    def plans(self):
        return [dict(r) for r in self.conn.execute(
            'SELECT plan_id,from_dsd_version_seq,to_dsd_version_seq,'
            'bound_revision_seq,status,applied_revision FROM migration_plan '
            'ORDER BY created_at')]

    # ------------------------------------------------------------------ #
    # content hash + audit
    # ------------------------------------------------------------------ #
    def _content_hash(self, revision):
        dsd_seq = self.current_dsd_seq(revision)
        h = hashlib.sha256()
        # streaming cursors: never materialise the full history in memory
        for r in self.conn.execute(
                'SELECT obs_key,raw_value,explicit_empty,operation FROM obs_decl '
                'WHERE revision_seq<=? ORDER BY obs_decl_id', (revision,)):
            h.update(_j(dict(r)).encode())
        for r in self.conn.execute(
                'SELECT attr_id,layer,group_type,scope_key,raw_value,'
                'explicit_empty,revoked_revision FROM decl_edge '
                'WHERE dsd_version_seq=? AND revision_seq<=? ORDER BY edge_id',
                (dsd_seq, revision)):
            h.update(_j(dict(r)).encode())
        return h.hexdigest()

    def audit(self, source_files=None):
        rev = self.current_revision()
        # fold the WAL into the main database so byte counts reflect the
        # committed content rather than pending frames
        self.conn.commit()
        self.conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        rep = {'revision_seq': rev, 'database_file': os.path.abspath(self.db_path)}
        rep['database_file_bytes'] = os.path.getsize(self.db_path)
        wal = self.db_path + '-wal'
        rep['wal_file_bytes'] = os.path.getsize(wal) if os.path.exists(wal) else 0
        rep['source_files'] = {}
        for p in source_files or []:
            rep['source_files'][str(p)] = os.path.getsize(p)
        rep['source_file_total_bytes'] = sum(rep['source_files'].values())
        for table in ('dsd_version', 'revision', 'obs_decl', 'decl_edge',
                      'migration_plan', 'migration_provenance', 'obs_key_dim',
                      'obs_fact', 'attr_fact', 'compat_assessment',
                      'compat_view_registry'):
            rep['rows_' + table] = self._scalar(
                'SELECT COUNT(*) FROM %s' % table)
        rep['active_decl_edges'] = self._scalar(
            'SELECT COUNT(*) FROM decl_edge WHERE revoked_revision IS NULL')
        rep['materialised_by_revision'] = [dict(r) for r in self.conn.execute(
            'SELECT revision_seq, COUNT(*) AS obs_fact_rows, '
            '(SELECT COUNT(*) FROM attr_fact a WHERE a.revision_seq=o.revision_seq) '
            'AS attr_fact_rows, SUM(isolated) AS isolated_obs FROM obs_fact o '
            'GROUP BY revision_seq ORDER BY revision_seq')]
        rep['provenance_edges'] = self._scalar(
            'SELECT COUNT(*) FROM migration_provenance')
        rep['provenance_by_plan'] = [dict(r) for r in self.conn.execute(
            'SELECT plan_id, COUNT(*) AS edges FROM migration_provenance '
            'GROUP BY plan_id')]
        with self.conn:
            self.conn.execute(
                'INSERT INTO audit_run(revision_seq,report_json,created_at) '
                'VALUES(?,?,?)', (rev, _j(rep), _now()))
        return rep

    def _revision_report(self, seq, idempotent=False):
        row = self.conn.execute(
            'SELECT kind,dsd_version_seq,status,content_hash FROM revision '
            'WHERE revision_seq=?', (seq,)).fetchone()
        return {'revision_seq': seq, 'kind': row['kind'],
                'dsd_version_seq': row['dsd_version_seq'], 'status': row['status'],
                'idempotent': idempotent, 'content_hash': row['content_hash'],
                'obs_fact_rows': self._scalar(
                    'SELECT COUNT(*) FROM obs_fact WHERE revision_seq=?', (seq,)),
                'isolated_obs': self._scalar(
                    'SELECT COUNT(*) FROM obs_fact WHERE revision_seq=? AND isolated=1',
                    (seq,))}
