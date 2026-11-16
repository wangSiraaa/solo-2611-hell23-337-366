"""SDMX 2.1 GenericData reader (declared subset).

read(path) keeps the legacy flat row shape used by baseline_demo.
parse_dataset(path, dsd) returns every raw declaration with its declared layer
and explicit-empty vs absent distinction, plus structural validation of group
coverage.  No semantic repair is performed here: misplaced/invalid declarations
are kept with a validity reason so the warehouse can isolate exactly the
observations they affect.
"""
import xml.etree.ElementTree as ET


class DataError(ValueError):
    """Malformed GenericData that cannot be imported."""


def _local(tag):
    return tag.split('}', 1)[-1]


def values(node):
    if node is None:
        return {}
    return {x.get('id'): x.get('value') for x in node.findall('{*}Value')}


def read(path):
    """Legacy flat reader: one dict per observation."""
    root = ET.parse(path).getroot()
    rows = []
    for s in root.findall('.//{*}Series'):
        key = values(s.find('{*}SeriesKey'))
        attrs = values(s.find('{*}Attributes'))
        for o in s.findall('{*}Obs'):
            rows.append({
                'key': dict(key, TIME=o.find('{*}ObsDimension').get('value')),
                'value': o.find('{*}ObsValue').get('value'),
                'series_attrs': attrs,
                'obs_attrs': values(o.find('{*}Attributes')),
            })
    return rows


def _attrs(scope_node):
    """Return [(id, value, declared_present)] distinguishing explicit empty text."""
    out = []
    if scope_node is None:
        return out
    for v in scope_node.findall('{*}Value'):
        out.append((v.get('id'), v.get('value'), True))
    return out


def _key_values(node):
    if node is None:
        return {}
    return {v.get('id'): v.get('value') for v in node.findall('{*}Value')}


def parse_dataset(path, dsd):
    """Parse one GenericData message against a compiled DSD.

    Returns dict with:
      structure_ref, dataset_attrs, groups, series, obs_count
    Every attribute declaration carries (attr_id, raw_value) where raw_value is
    the original XML text ('' means an explicit empty value; absent simply means
    no declaration).
    """
    root = ET.parse(path).getroot()
    ds = root.find('.//{*}DataSet')
    if ds is None:
        raise DataError('no DataSet')
    structure_ref = ds.get('structureRef')
    if dsd['dsd_id'] is not None and structure_ref != dsd['dsd_id']:
        raise DataError('structureRef %s != DSD id %s' % (structure_ref, dsd['dsd_id']))

    all_dims = [d['id'] for d in dsd['dimensions']]

    dataset_attrs = _attrs(ds.find('{*}Attributes'))

    groups = []
    for g in ds.findall('{*}Group'):
        gtype = g.get('type')
        if gtype not in dsd['groups']:
            raise DataError('unknown Group type %r: coverage cannot be determined' % gtype)
        gkey = _key_values(g.find('{*}GroupKey'))
        wanted = set(dsd['groups'][gtype])
        if set(gkey) != wanted:
            raise DataError('Group %s key %r does not match its dimensions %r'
                            % (gtype, gkey, sorted(wanted)))
        groups.append({'type': gtype, 'key': gkey,
                       'attrs': _attrs(g.find('{*}Attributes'))})

    series_list = []
    seen_series_keys = set()
    for s in ds.findall('{*}Series'):
        skey = _key_values(s.find('{*}SeriesKey'))
        if set(skey) != set(dsd['non_time_dims']):
            raise DataError('SeriesKey %r does not cover non-time dimensions %r'
                            % (skey, dsd['non_time_dims']))
        sig = tuple(skey[d] for d in dsd['non_time_dims'])
        if sig in seen_series_keys:
            raise DataError('duplicate SeriesKey %r' % (skey,))
        seen_series_keys.add(sig)
        obs_list = []
        seen_time = set()
        for o in s.findall('{*}Obs'):
            od = o.find('{*}ObsDimension')
            if od is None or od.get('value') is None:
                raise DataError('Obs without ObsDimension value')
            tval = od.get('value')
            if tval in seen_time:
                raise DataError('duplicate TIME %s in series %r' % (tval, skey))
            seen_time.add(tval)
            ov = o.find('{*}ObsValue')
            raw_value = ov.get('value') if ov is not None else None
            obs_list.append({
                'key': dict(skey, **{dsd['time_dimension']: tval}),
                'raw_value': raw_value,
                'attrs': _attrs(o.find('{*}Attributes')),
            })
        series_list.append({
            'key': skey,
            'attrs': _attrs(s.find('{*}Attributes')),
            'obs': obs_list,
        })

    return {
        'structure_ref': structure_ref,
        'all_dims': all_dims,
        'dataset_attrs': dataset_attrs,
        'groups': groups,
        'series': series_list,
        'obs_count': sum(len(s['obs']) for s in series_list),
    }
