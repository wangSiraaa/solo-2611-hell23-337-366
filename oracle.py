"""Independent oracle for effective attribute facts.

This module deliberately does not import or call any Warehouse resolution code.
It reads only the raw declaration tables (obs_decl / decl_edge) plus the DSD
descriptor and re-derives, for every observation and every DSD attribute:
presence, value, explicit-empty flag, validity, isolation reason and source
edge.  The validation suite compares its output cell-by-cell with the
materialised attr_fact / obs_fact tables.
"""
import json
from decimal import Decimal, InvalidOperation


def norm(raw):
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


def _active_obs_decls(conn, revision):
    rows = conn.execute(
        'SELECT * FROM obs_decl WHERE revision_seq<=? ORDER BY obs_decl_id',
        (revision,)).fetchall()
    latest = {}
    for r in rows:
        if r['obs_key'] not in latest or r['obs_decl_id'] > latest[r['obs_key']]['obs_decl_id']:
            latest[r['obs_key']] = dict(r)
    return {k: r for k, r in latest.items() if r['operation'] != 'delete'}


def _active_edges(conn, revision, dsd_seq):
    rows = conn.execute(
        'SELECT * FROM decl_edge WHERE dsd_version_seq=? AND revision_seq<=? '
        'AND (revoked_revision IS NULL OR revoked_revision>?) ORDER BY edge_id',
        (dsd_seq, revision, revision)).fetchall()
    return [dict(r) for r in rows]


def _covers(edge, dsd, obs_key):
    if edge['layer'] == 'dataset':
        return True
    want = json.loads(edge['scope_key'])
    if edge['layer'] == 'group':
        gdims = dsd['groups'][edge['group_type']]
        return all(obs_key[d] == want[d] for d in gdims)
    return all(obs_key[d] == want[d] for d in want)


def _j(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def _build_index(rows):
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


def oracle_facts(conn, revision, dsd, dsd_seq, extra_isolation=None):
    """Return {obs_key: {value_state, norm_value, reasons, attrs: {aid: {...}}}}."""
    obs_decls = _active_obs_decls(conn, revision)
    edges = _active_edges(conn, revision, dsd_seq)
    facts = {}
    for ok_j, decl in obs_decls.items():
        key = json.loads(ok_j)
        state, canonical = norm(decl['raw_value'])
        facts[ok_j] = {'key': key, 'value_state': state,
                       'raw_value': decl['raw_value'], 'norm_value': canonical,
                       'reasons': set(), 'attrs': {}}
        for d in dsd['dimensions']:
            if d['codelist'] and key[d['id']] not in dsd['codelists'][d['codelist']]:
                facts[ok_j]['reasons'].add('bad_dimension_code:' + d['id'])

    by_attr = {}
    for e in edges:
        by_attr.setdefault(e['attr_id'], []).append(e)
    key_cache = {}

    def scoped_key(dims, ok_j, key):
        ck = (dims, ok_j)
        val = key_cache.get(ck)
        if val is None:
            val = _j({d: key[d] for d in dims})
            key_cache[ck] = val
        return val

    for aid, attr in dsd['attributes'].items():
        idx = _build_index(by_attr.get(aid, ()))
        for ok_j, f in facts.items():
            cand = _candidates(idx, dsd, f['key'], scoped_key, ok_j)
            valid = [e for e in cand if e['valid']]
            invalid = [e for e in cand if not e['valid']]
            distinct = {(e['raw_value'], e['explicit_empty']) for e in valid}
            slot = {'present': 0, 'raw_value': None, 'explicit_empty': 0,
                    'valid': 1, 'invalid_reason': None, 'source_edge_id': None,
                    'source_origin': None, 'source_revision': None,
                    'resolved_layer': attr['layer']}
            if len(distinct) > 1:
                chosen = max(valid, key=lambda e: e['edge_id'])
                slot.update(present=1, raw_value=chosen['raw_value'],
                            explicit_empty=chosen['explicit_empty'], valid=0,
                            invalid_reason='scope_conflict',
                            source_edge_id=chosen['edge_id'],
                            source_origin=chosen['origin'],
                            source_revision=chosen['revision_seq'])
                f['reasons'].add('scope_conflict:' + aid)
            elif len(distinct) == 1:
                chosen = max(valid, key=lambda e: e['edge_id'])
                slot.update(present=1, raw_value=chosen['raw_value'],
                            explicit_empty=chosen['explicit_empty'],
                            source_edge_id=chosen['edge_id'],
                            source_origin=chosen['origin'],
                            source_revision=chosen['revision_seq'])
            elif invalid:
                chosen = max(invalid, key=lambda e: e['edge_id'])
                slot.update(present=1, raw_value=chosen['raw_value'],
                            explicit_empty=chosen['explicit_empty'], valid=0,
                            invalid_reason=chosen['invalid_reason'],
                            source_edge_id=chosen['edge_id'],
                            source_origin=chosen['origin'],
                            source_revision=chosen['revision_seq'])
                f['reasons'].add('%s:%s' % (chosen['invalid_reason'], aid))
            elif attr['status'] == 'Mandatory':
                f['reasons'].add('missing_mandatory:' + aid)
            f['attrs'][aid] = slot

    for item in extra_isolation or []:
        facts[item['obs_key']]['reasons'].add(
            '%s:%s' % (item['reason'], item['attr_id']))
    return facts


def plan_extra_isolation(plan_json):
    return json.loads(plan_json).get('isolated_observations', []) \
        if isinstance(plan_json, str) else plan_json.get('isolated_observations', [])
