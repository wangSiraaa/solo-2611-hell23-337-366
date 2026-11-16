"""SDMX 2.1 subset DSD compiler.

compile(path) keeps the legacy thin mapping (id -> relationship tag list) used by
the original baseline.  load_dsd(path) is the full compiler used by the warehouse:
dimensions, groups, codelists, attribute attachment layers and assignment status,
with structural validation that rejects before any data import.
"""
import json
import xml.etree.ElementTree as ET


class StructureError(ValueError):
    """Structural DSD error: must reject before import."""


def _local(tag):
    return tag.split('}', 1)[-1]


def _children(node):
    return list(node) if node is not None else []


def compile(path):
    """Legacy mapping: attribute id -> list of relationship element local tags."""
    root = ET.parse(path).getroot()
    out = {}
    for a in root.findall('.//{*}Attribute'):
        rel = a.find('{*}AttributeRelationship')
        out[a.get('id')] = [_local(x.tag) for x in _children(rel)]
    return out


def _find1(node, name):
    hits = node.findall('{*}' + name)
    if len(hits) != 1:
        raise StructureError('expected exactly one <%s>, got %d' % (name, len(hits)))
    return hits[0]


def _ref_id(node):
    ref = node.find('{*}Ref')
    if ref is None:
        raise StructureError('missing Ref')
    rid = ref.get('id')
    if not rid:
        raise StructureError('Ref without id')
    return rid


def _enum_codelist(attr_node):
    lr = attr_node.find('{*}LocalRepresentation')
    if lr is None:
        return None
    en = lr.find('{*}Enumeration')
    if en is None:
        return None
    return _ref_id(en)


def load_dsd(path):
    """Compile a DSD XML file into a plain JSON-serialisable descriptor.

    Layers:
      dataset  : AttributeRelationship/None
      obs      : PrimaryMeasure
      series   : Dimension refs covering every non-time dimension
      dimset   : Dimension refs covering a proper non-time subset
      group    : Group ref to a declared Group
    """
    root = ET.parse(path).getroot()
    dsc = root.find('.//{*}DataStructureComponents')
    if dsc is None:
        raise StructureError('no DataStructureComponents')

    codelists = {}
    for cl in root.findall('.//{*}Codelists/{*}Codelist'):
        cid = cl.get('id')
        if not cid or cid in codelists:
            raise StructureError('bad/duplicate codelist id: %r' % cid)
        codes = set()
        for code in cl.findall('{*}Code'):
            code_id = code.get('id')
            if not code_id or code_id in codes:
                raise StructureError('bad/duplicate Code id in %s' % cid)
            codes.add(code_id)
        codelists[cid] = sorted(codes)

    dim_list = _find1(dsc, 'DimensionList')
    dimensions = []
    seen_dim_ids = set()
    time_dimension = None
    for d in dim_list:
        kind = _local(d.tag)
        if kind not in ('Dimension', 'TimeDimension'):
            continue
        did = d.get('id')
        if not did or did in seen_dim_ids:
            raise StructureError('bad/duplicate dimension id: %r' % did)
        seen_dim_ids.add(did)
        pos = d.get('position')
        cl = None
        lr = d.find('{*}LocalRepresentation')
        if lr is not None:
            en = lr.find('{*}Enumeration')
            if en is not None:
                cl = _ref_id(en)
                if cl not in codelists:
                    raise StructureError('dimension %s references unknown codelist %s' % (did, cl))
        dimensions.append({'id': did, 'position': int(pos) if pos else len(dimensions) + 1,
                           'kind': kind, 'codelist': cl})
        if kind == 'TimeDimension':
            if time_dimension is not None:
                raise StructureError('more than one TimeDimension')
            time_dimension = did
    if time_dimension is None:
        raise StructureError('missing TimeDimension')
    dimensions.sort(key=lambda x: x['position'])
    non_time = [d['id'] for d in dimensions if d['kind'] == 'Dimension']

    groups = {}
    for g in dsc.findall('{*}Group'):
        gid = g.get('id')
        if not gid or gid in groups:
            raise StructureError('bad/duplicate Group id: %r' % gid)
        dims = []
        for gd in g.findall('{*}GroupDimension'):
            dr = _find1(gd, 'DimensionReference')
            rid = _ref_id(dr)
            if rid not in non_time or rid in dims:
                raise StructureError('group %s references bad dimension %s' % (gid, rid))
            dims.append(rid)
        if not dims:
            raise StructureError('group %s has no dimensions' % gid)
        groups[gid] = dims

    ml = dsc.find('{*}MeasureList')
    measure = None
    if ml is not None:
        pm = ml.find('{*}PrimaryMeasure')
        if pm is not None:
            measure = pm.get('id')
    if not measure:
        raise StructureError('missing PrimaryMeasure')

    attributes = {}
    attr_list = dsc.find('{*}AttributeList')
    if attr_list is None:
        raise StructureError('missing AttributeList')
    for a in attr_list.findall('{*}Attribute'):
        aid = a.get('id')
        if not aid or aid in attributes:
            raise StructureError('bad/duplicate Attribute id: %r' % aid)
        status = a.get('assignmentStatus', 'Conditional')
        if status not in ('Mandatory', 'Conditional'):
            raise StructureError('attribute %s bad assignmentStatus %s' % (aid, status))
        rel = _find1(a, 'AttributeRelationship')
        kids = _children(rel)
        if not kids:
            raise StructureError('attribute %s has empty relationship' % aid)
        rtags = {_local(n.tag) for n in kids}
        if len(rtags) != 1:
            raise StructureError('attribute %s mixes relationship types' % aid)
        rnode = kids[0]
        rtag = next(iter(rtags))
        if rtag == 'None':
            layer, group_id, dims = 'dataset', None, []
        elif rtag == 'PrimaryMeasure':
            layer, group_id, dims = 'obs', None, list(non_time) + [time_dimension]
        elif rtag == 'Group':
            layer = 'group'
            group_id = _ref_id(rnode)
            if group_id not in groups:
                raise StructureError('attribute %s references unknown Group %s' % (aid, group_id))
            dims = groups[group_id]
        elif rtag == 'Dimension':
            dims = []
            for dn in rel.findall('{*}Dimension'):
                rid = _ref_id(dn)
                if rid not in non_time or rid in dims:
                    raise StructureError('attribute %s bad dimension ref %s' % (aid, rid))
                dims.append(rid)
            if set(dims) == set(non_time):
                layer, group_id = 'series', None
            else:
                layer, group_id = 'dimset', None
        else:
            raise StructureError('attribute %s unsupported relationship %s' % (aid, rtag))
        cl = _enum_codelist(a)
        if cl is not None and cl not in codelists:
            raise StructureError('attribute %s references unknown codelist %s' % (aid, cl))
        attributes[aid] = {
            'id': aid, 'status': status, 'layer': layer,
            'group': group_id, 'dims': dims, 'codelist': cl,
        }

    ds = root.find('.//{*}DataStructure')
    return {
        'dsd_id': ds.get('id') if ds is not None else None,
        'dimensions': dimensions,
        'time_dimension': time_dimension,
        'non_time_dims': non_time,
        'groups': groups,
        'primary_measure': measure,
        'codelists': codelists,
        'attributes': attributes,
    }


def scope_dims(dsd, attr):
    """Dimension set that identifies one attachment scope of an attribute."""
    if attr['layer'] == 'dataset':
        return frozenset()
    if attr['layer'] == 'obs':
        return frozenset(dsd['non_time_dims'] + [dsd['time_dimension']])
    return frozenset(attr['dims'])


def dumps(dsd):
    return json.dumps(dsd, sort_keys=True, ensure_ascii=False)
