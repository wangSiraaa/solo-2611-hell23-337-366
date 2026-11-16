"""Generate DSD/GenericData XML fixtures for the migration validation suite.

All fixtures stay inside the declared subset: COUNTRY/MEASURE/TIME, local
codelists, dataset/group(any non-time dimension subset)/series/observation
attachment layers.  Files are written under fixtures/ and are real XML parsed
through the same structure.py/data_reader.py path as legacy inputs.
"""
import os

ROOT = os.path.join(os.path.dirname(__file__), 'fixtures')

DIMS = (
    '<s:DimensionList>'
    '<s:Dimension id="COUNTRY" position="1"><s:LocalRepresentation>'
    '<s:Enumeration><s:Ref id="CL_COUNTRY"/></s:Enumeration>'
    '</s:LocalRepresentation></s:Dimension>'
    '<s:Dimension id="MEASURE" position="2"><s:LocalRepresentation>'
    '<s:Enumeration><s:Ref id="CL_MEASURE"/></s:Enumeration>'
    '</s:LocalRepresentation></s:Dimension>'
    '<s:TimeDimension id="TIME" position="3" />'
    '</s:DimensionList>'
)

GROUPS_COUNTRY = (
    '<s:Group id="G_COUNTRY"><s:GroupDimension>'
    '<s:DimensionReference><s:Ref id="COUNTRY"/></s:DimensionReference>'
    '</s:GroupDimension></s:Group>'
)

GROUPS_MEASURE = (
    '<s:Group id="G_MEASURE"><s:GroupDimension>'
    '<s:DimensionReference><s:Ref id="MEASURE"/></s:DimensionReference>'
    '</s:GroupDimension></s:Group>'
)

CODELISTS = (
    '<s:Codelists>'
    '<s:Codelist id="CL_COUNTRY"><s:Code id="CN" /><s:Code id="US" /></s:Codelist>'
    '<s:Codelist id="CL_MEASURE"><s:Code id="VOL" /><s:Code id="PRICE" /></s:Codelist>'
    '<s:Codelist id="CL_STATUS"><s:Code id="A" /><s:Code id="P" /></s:Codelist>'
    '<s:Codelist id="CL_UNIT"><s:Code id="count" /><s:Code id="USD" />'
    '<s:Code id="thousand_count" /></s:Codelist>'
    '</s:Codelists>'
)

ENVELOPE = (
    "<?xml version='1.0' encoding='utf-8'?>"
    '<m:Structure xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
    'xmlns:s="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">'
    '<m:Structures>{codelists}<s:DataStructures><s:DataStructure id="{dsd_id}">'
    '<s:DataStructureComponents>{dims}{groups}'
    '<s:MeasureList><s:PrimaryMeasure id="OBS_VALUE" /></s:MeasureList>'
    '<s:AttributeList>{attrs}</s:AttributeList>'
    '</s:DataStructureComponents></s:DataStructure></s:DataStructures>'
    '</m:Structures></m:Structure>'
)


def attr(aid, status, relationship, codelist=None):
    lr = ('<s:LocalRepresentation><s:Enumeration><s:Ref id="%s" /></s:Enumeration>'
          '</s:LocalRepresentation>' % codelist) if codelist else ''
    return ('<s:Attribute id="%s" assignmentStatus="%s">%s'
            '<s:AttributeRelationship>%s</s:AttributeRelationship></s:Attribute>'
            % (aid, status, lr, relationship))


REL = {
    'dataset': '<s:None />',
    'obs': '<s:PrimaryMeasure />',
    'series': '<s:Dimension><s:Ref id="COUNTRY" /></s:Dimension>'
              '<s:Dimension><s:Ref id="MEASURE" /></s:Dimension>',
    'group_country': '<s:Group><s:Ref id="G_COUNTRY" /></s:Group>',
    'group_measure': '<s:Group><s:Ref id="G_MEASURE" /></s:Group>',
}


def dsd(dsd_id, attr_specs, groups=GROUPS_COUNTRY):
    attrs = ''.join(attr(a, s, REL[r], cl) for a, s, r, cl in attr_specs)
    return ENVELOPE.format(codelists=CODELISTS, dsd_id=dsd_id, dims=DIMS,
                           groups=groups, attrs=attrs)


# DSD A: UNIT on series (full non-time dims), CLASS group(COUNTRY),
# NOTE dataset, OBS_STATUS obs.
DSD_A = [
    ('NOTE', 'Conditional', 'dataset', None),
    ('UNIT', 'Mandatory', 'series', 'CL_UNIT'),
    ('CLASS', 'Conditional', 'group_country', None),
    ('OBS_STATUS', 'Mandatory', 'obs', 'CL_STATUS'),
]
# DSD B: UNIT refined to observation; everything else unchanged.
DSD_B = [
    ('NOTE', 'Conditional', 'dataset', None),
    ('UNIT', 'Mandatory', 'obs', 'CL_UNIT'),
    ('CLASS', 'Conditional', 'group_country', None),
    ('OBS_STATUS', 'Mandatory', 'obs', 'CL_STATUS'),
]
# DSD C: UNIT coarsened back to series; new mandatory SOURCE (no data source
# exists for it); optional AUDIT added; NOTE deleted.
DSD_C = [
    ('UNIT', 'Mandatory', 'series', 'CL_UNIT'),
    ('CLASS', 'Conditional', 'group_country', None),
    ('OBS_STATUS', 'Mandatory', 'obs', 'CL_STATUS'),
    ('SOURCE', 'Mandatory', 'dataset', None),
    ('AUDIT', 'Conditional', 'obs', None),
]
# DSD D: CLASS moves onto a different non-time dimension subset G_MEASURE.
DSD_D = [
    ('NOTE', 'Conditional', 'dataset', None),
    ('UNIT', 'Mandatory', 'obs', 'CL_UNIT'),
    ('CLASS', 'Conditional', 'group_measure', None),
    ('OBS_STATUS', 'Mandatory', 'obs', 'CL_STATUS'),
]


def value(aid, val):
    return '<g:Value id="%s" value="%s" />' % (aid, val)


def series(country, measure, unit, obs_list, series_extra=''):
    obs_xml = ''
    for time_val, raw, status, obs_attrs in obs_list:
        oa = value('OBS_STATUS', status) + ''.join(value(k, v) for k, v in obs_attrs)
        obs_xml += ('<g:Obs><g:ObsDimension value="%s" />'
                    '<g:ObsValue value="%s" /><g:Attributes>%s</g:Attributes></g:Obs>'
                    % (time_val, raw, oa))
    return ('<g:Series><g:SeriesKey>%s%s</g:SeriesKey>'
            '<g:Attributes>%s%s</g:Attributes>%s</g:Series>' % (
                value('COUNTRY', country), value('MEASURE', measure),
                value('UNIT', unit), series_extra, obs_xml))


def group(gtype, key_vals, attrs):
    kv = ''.join(value(k, v) for k, v in key_vals.items())
    av = ''.join(value(k, v) for k, v in attrs.items())
    return ('<g:Group type="%s"><g:GroupKey>%s</g:GroupKey>'
            '<g:Attributes>%s</g:Attributes></g:Group>' % (gtype, kv, av))


DATA_ENVELOPE = (
    "<?xml version='1.0' encoding='utf-8'?>"
    '<m:GenericData xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
    'xmlns:g="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic">'
    '<m:DataSet structureRef="{dsd_id}">{body}</m:DataSet></m:GenericData>'
)


def data(dsd_id, body):
    return DATA_ENVELOPE.format(dsd_id=dsd_id, body=body)


# Dataset used by the main scenario:
#  - explicit empty NOTE at dataset level
#  - CLASS group declarations, including a duplicate same-key conflict on US
#  - UNIT count on CN/VOL, USD on CN/PRICE -> later obs-level split on US/VOL
#  - one zero value and one missing (NaN) value kept distinct
MAIN_BODY = (
    '<g:Attributes>' + value('NOTE', '') + '</g:Attributes>'
    + group('G_COUNTRY', {'COUNTRY': 'CN'}, {'CLASS': 'east'})
    + group('G_COUNTRY', {'COUNTRY': 'US'}, {'CLASS': 'west'})
    + group('G_COUNTRY', {'COUNTRY': 'US'}, {'CLASS': 'conflict'})
    + series('CN', 'VOL', 'count', [
        ('2026-01', '0', 'A', []),
        ('2026-02', 'NaN', 'P', []),
        ('2026-03', '5.000', 'A', [])])
    + series('CN', 'PRICE', 'USD', [
        ('2026-01', '1.2300', 'A', [])])
    + series('US', 'VOL', 'count', [
        ('2026-01', '9', 'A', []),
        ('2026-02', '7.50', 'A', [])])
)

# Same data but every UNIT declared per observation (used to exercise
# obs->series coarsening with a real conflict inside one series).
OBS_UNIT_BODY = (
    '<g:Attributes>' + value('NOTE', 'note-x') + '</g:Attributes>'
    + group('G_COUNTRY', {'COUNTRY': 'CN'}, {'CLASS': 'east'})
    + group('G_COUNTRY', {'COUNTRY': 'US'}, {'CLASS': 'west'})
    + '<g:Series><g:SeriesKey>' + value('COUNTRY', 'CN') + value('MEASURE', 'VOL')
    + '</g:SeriesKey><g:Attributes></g:Attributes>'
    + ('<g:Obs><g:ObsDimension value="2026-01" /><g:ObsValue value="1" />'
       '<g:Attributes>' + value('OBS_STATUS', 'A') + value('UNIT', 'count')
       + '</g:Attributes></g:Obs>')
    + ('<g:Obs><g:ObsDimension value="2026-02" /><g:ObsValue value="2" />'
       '<g:Attributes>' + value('OBS_STATUS', 'A') + value('UNIT', 'USD')
       + '</g:Attributes></g:Obs>')
    + '</g:Series>'
)


def write(name, content):
    os.makedirs(ROOT, exist_ok=True)
    path = os.path.join(ROOT, name)
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(content)
    return path


def generate_all():
    paths = []
    paths.append(write('dsd-a.xml', dsd('DSDA', DSD_A)))
    paths.append(write('dsd-b.xml', dsd('DSDB', DSD_B)))
    paths.append(write('dsd-c.xml', dsd('DSDC', DSD_C)))
    paths.append(write('dsd-d.xml', dsd(
        'DSDD', DSD_D, GROUPS_COUNTRY + GROUPS_MEASURE)))
    paths.append(write('data-main.xml', data('DSDA', MAIN_BODY)))
    paths.append(write('data-obs-unit.xml', data('DSDB', OBS_UNIT_BODY)))
    return paths


if __name__ == '__main__':
    for p in generate_all():
        print(p)
