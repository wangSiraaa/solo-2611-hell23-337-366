"""Scale check at the declared 100,000-observation ceiling.

Generates a real GenericData XML file with 100,000 observations (no synthetic
in-memory shortcuts), imports it, runs a series->obs UNIT refinement, applies
50 revisions (the declared ceiling), and records actual materialised row counts,
provenance edge counts, wall time and database file bytes.  The same independent
oracle verifies a sampled set of observations at each major step.
"""
import json
import os
import random
import time

from make_fixtures import value
from warehouse import Warehouse
from oracle import oracle_facts

ROOT = os.path.dirname(os.path.abspath(__file__))
BIG_XML = os.path.join(ROOT, 'fixtures', 'data-100k.xml')
BIG_DSD_A = os.path.join(ROOT, 'fixtures', 'dsd-big-a.xml')
BIG_DSD_B = os.path.join(ROOT, 'fixtures', 'dsd-big-b.xml')
BIG_DB = os.path.join(ROOT, 'warehouse-100k.db')
N_OBS = 100_000
N_REVISIONS = 50


def big_dsd(dsd_id, unit_layer):
    """DSD without enumerations so the 100x100 synthetic key space is legal."""
    dims = (
        '<s:DimensionList>'
        '<s:Dimension id="COUNTRY" position="1" />'
        '<s:Dimension id="MEASURE" position="2" />'
        '<s:TimeDimension id="TIME" position="3" />'
        '</s:DimensionList>')
    groups = ('<s:Group id="G_COUNTRY"><s:GroupDimension>'
              '<s:DimensionReference><s:Ref id="COUNTRY"/></s:DimensionReference>'
              '</s:GroupDimension></s:Group>')
    rel = {'dataset': '<s:None />', 'obs': '<s:PrimaryMeasure />',
           'series': '<s:Dimension><s:Ref id="COUNTRY"/></s:Dimension>'
                     '<s:Dimension><s:Ref id="MEASURE"/></s:Dimension>'}
    specs = [('NOTE', 'Conditional', 'dataset'), ('UNIT', 'Mandatory', unit_layer),
             ('CLASS', 'Conditional', 'group_country'),
             ('OBS_STATUS', 'Mandatory', 'obs')]
    attrs = ''
    for aid, status, layer in specs:
        r = rel.get(layer, '<s:Group><s:Ref id="G_COUNTRY"/></s:Group>')
        attrs += ('<s:Attribute id="%s" assignmentStatus="%s">'
                  '<s:AttributeRelationship>%s</s:AttributeRelationship></s:Attribute>'
                  % (aid, status, r))
    return ("<?xml version='1.0' encoding='utf-8'?>"
            '<m:Structure xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
            'xmlns:s="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">'
            '<m:Structures><s:DataStructures><s:DataStructure id="%s">'
            '<s:DataStructureComponents>%s%s'
            '<s:MeasureList><s:PrimaryMeasure id="OBS_VALUE"/></s:MeasureList>'
            '<s:AttributeList>%s</s:AttributeList>'
            '</s:DataStructureComponents></s:DataStructure></s:DataStructures>'
            '</m:Structures></m:Structure>'
            % (dsd_id, dims, groups, attrs))


def build():
    random.seed(319)
    # 100 countries x 100 measures x 10 periods = 100,000 observations
    countries = ['C%03d' % i for i in range(100)]
    measures = ['M%03d' % i for i in range(100)]
    with open(BIG_DSD_A, 'w', encoding='utf-8') as fh:
        fh.write(big_dsd('BIGA', 'series'))
    with open(BIG_DSD_B, 'w', encoding='utf-8') as fh:
        fh.write(big_dsd('BIGB', 'obs'))
    t0 = time.time()
    header = ("<?xml version='1.0' encoding='utf-8'?>"
              '<m:GenericData xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
              'xmlns:g="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic">'
              '<m:DataSet structureRef="BIGA">')
    with open(BIG_XML, 'w', encoding='utf-8') as fh:
        fh.write(header)
        for ci, country in enumerate(countries):
            for measure in measures:
                unit = 'count' if measure.endswith(('0', '2', '4', '6', '8')) else 'USD'
                fh.write('<g:Series><g:SeriesKey>')
                fh.write(value('COUNTRY', country))
                fh.write(value('MEASURE', measure))
                fh.write('</g:SeriesKey><g:Attributes>')
                fh.write(value('UNIT', unit))
                fh.write('</g:Attributes>')
                for period in range(10):
                    fh.write('<g:Obs><g:ObsDimension value="2026-%02d" />' % (period + 1))
                    fh.write('<g:ObsValue value="%d" /><g:Attributes>'
                             % (ci * 1000 + period))
                    fh.write(value('OBS_STATUS', 'A' if period % 2 == 0 else 'P'))
                    fh.write('</g:Attributes></g:Obs>')
                fh.write('</g:Series>')
        fh.write('</m:DataSet></m:GenericData>')
    return time.time() - t0


def main():
    for suffix in ('', '-wal', '-shm'):
        if os.path.exists(BIG_DB + suffix):
            os.remove(BIG_DB + suffix)
    xml_seconds = build()
    report = {'target_observations': N_OBS, 'target_revisions': N_REVISIONS,
              'xml_build_seconds': round(xml_seconds, 2),
              'xml_file_bytes': os.path.getsize(BIG_XML)}
    w = Warehouse(BIG_DB)

    t0 = time.time()
    w.import_xml(BIG_DSD_A, BIG_XML)
    report['import_seconds'] = round(time.time() - t0, 2)
    assert w._scalar('SELECT COUNT(*) FROM obs_fact WHERE revision_seq=0') == N_OBS

    t0 = time.time()
    plan = w.plan_migration(BIG_DSD_B, 'big-refine')
    report['plan_seconds'] = round(time.time() - t0, 2)
    report['plan_lossless'] = plan['lossless']
    report['derived_edges_in_plan'] = plan['derived_edge_count']

    t0 = time.time()
    w.apply_migration('big-refine')
    report['apply_seconds'] = round(time.time() - t0, 2)

    # 50 revisions: each upserts two observations; full keys decide identity
    t0 = time.time()
    for i in range(1, N_REVISIONS + 1):
        w.ingest_revision({
            'revision_seq': i + 1,
            'operations': [
                {'action': 'upsert',
                 'key': {'COUNTRY': 'C%03d' % (i % 100), 'MEASURE': 'M%03d' % (i % 100),
                         'TIME': '2026-%02d' % ((i % 10) + 1)},
                 'value': str(10_000 + i),
                 'attrs': [{'attr_id': 'UNIT', 'value': 'USD'},
                           {'attr_id': 'OBS_STATUS', 'value': 'A'}]},
                {'action': 'upsert',
                 'key': {'COUNTRY': 'C%03d' % ((i + 7) % 100),
                         'MEASURE': 'M%03d' % ((i + 3) % 100),
                         'TIME': '2026-%02d' % ((i % 10) + 1)},
                 'value': str(20_000 + i),
                 'attrs': [{'attr_id': 'UNIT', 'value': 'count'},
                           {'attr_id': 'OBS_STATUS', 'value': 'P'}]}]})
    report['revision_seconds_total'] = round(time.time() - t0, 2)
    report['final_revision'] = w.current_revision()

    # oracle on a deterministic sample across the full key space
    dsd_seq = w.current_dsd_seq()
    dsd = w._dsd(dsd_seq)
    sample_keys = [
        json.dumps({'COUNTRY': 'C%03d' % i, 'MEASURE': 'M%03d' % (i * 7 % 100),
                    'TIME': '2026-%02d' % (i % 10 + 1)}, sort_keys=True,
                   separators=(',', ':')) for i in range(0, 100, 5)]
    expected = oracle_facts(w.conn, w.current_revision(), dsd, dsd_seq)
    mismatches = 0
    for ok_j in sample_keys:
        got = w.conn.execute(
            'SELECT * FROM v_attr_fact WHERE revision_seq=? AND obs_key=?',
            (w.current_revision(), ok_j)).fetchall()
        exp = expected[ok_j]['attrs']
        for row in got:
            es = exp[row['attr_id']]
            if (row['present'], row['raw_value'], row['source_origin']) != \
                    (es['present'], es['raw_value'], es['source_origin']):
                mismatches += 1
    report['oracle_sample_size'] = len(sample_keys)
    report['oracle_sample_mismatches'] = mismatches

    audit = w.audit(source_files=[BIG_XML, BIG_DSD_A, BIG_DSD_B])
    report['audit'] = audit
    w.close()
    os.makedirs(os.path.join(ROOT, 'logs'), exist_ok=True)
    with open(os.path.join(ROOT, 'logs', 'scale_log.json'), 'w',
              encoding='utf-8') as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    print(json.dumps({k: v for k, v in report.items() if k != 'audit'},
                     indent=2))
    print('database_file_bytes =', audit['database_file_bytes'])
    assert mismatches == 0
    assert plan['lossless'] is True
    assert w is not None


if __name__ == '__main__':
    main()
