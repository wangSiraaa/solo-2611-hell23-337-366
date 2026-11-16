"""Build the delivered demo warehouse from the legacy inputs and capture real
SQLite query read-back into logs/query_readback.log.

Workflow exercised on the actual legacy files:
  import DSD1 + GenericData (revision 0)
  -> plan/apply DSD2 UNIT series->obs refinement (revision 1)
  -> precondition-gated obs revision that splits a series UNIT (revision 2)
  -> old-DSD compatibility export is now explicitly inexpressible
  -> every revision remains queryable; audit numbers recorded
"""
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(ROOT, 'warehouse.db')
LOG = os.path.join(ROOT, 'logs', 'query_readback.log')

sys.path.insert(0, ROOT)
from warehouse import Warehouse  # noqa: E402


def section(fh, title):
    fh.write('\n' + '=' * 78 + '\n' + title + '\n' + '=' * 78 + '\n')


def run_queries(db_path, sql_path):
    """Execute the numbered statements in queries.sql through Python's sqlite3
    and render their result sets in a readable tabular form."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    text = open(sql_path, encoding='utf-8').read()
    # strip dot-commands, then split on statement separators
    statements = [s for s in
                  '\n'.join(line for line in text.splitlines()
                            if not line.startswith('.')).split(';')
                  if s.strip()]
    chunks = []
    for stmt in statements:
        cur = conn.execute(stmt)
        rows = cur.fetchall()
        title = stmt.strip().splitlines()[-1].strip()
        chunks.append('SQL: %s' % title)
        if not rows:
            chunks.append('  (no rows)')
            continue
        cols = rows[0].keys()
        widths = [max(len(str(c)), *(len(str(r[c])) for r in rows)) for c in cols]
        chunks.append('  ' + ' | '.join(str(c).ljust(w) for c, w in zip(cols, widths)))
        chunks.append('  ' + '-+-'.join('-' * w for w in widths))
        for r in rows:
            chunks.append('  ' + ' | '.join(
                str('' if r[c] is None else r[c]).ljust(w)
                for c, w in zip(cols, widths)))
        chunks.append('  (%d row%s)' % (len(rows), '' if len(rows) == 1 else 's'))
        chunks.append('')
    conn.close()
    return '\n'.join(chunks)


def main():
    for suffix in ('', '-wal', '-shm'):
        if os.path.exists(DB + suffix):
            os.remove(DB + suffix)
    os.makedirs(os.path.join(ROOT, 'logs'), exist_ok=True)

    w = Warehouse(DB)
    w.import_xml(os.path.join(ROOT, 'legacy/structure.xml'),
                 os.path.join(ROOT, 'legacy/data.xml'))
    plan = w.plan_migration(os.path.join(ROOT, 'inputs/structure-v2.xml'),
                            'legacy-to-v2')
    w.apply_migration('legacy-to-v2')

    # revision 2: re-insert a second observation into the US/VOL series so the
    # series genuinely contains two obs-level UNIT values after the next change
    w.ingest_revision({
        'revision_seq': 2,
        'operations': [{
            'action': 'upsert',
            'key': {'COUNTRY': 'US', 'MEASURE': 'VOL', 'TIME': '2026-02'},
            'value': '7.50',
            'attrs': [{'attr_id': 'UNIT', 'value': 'count'},
                      {'attr_id': 'OBS_STATUS', 'value': 'A'}]}]})
    # revision 3: precondition-gated obs change splits the series UNIT (count vs USD)
    w.ingest_revision({
        'revision_seq': 3,
        'operations': [{
            'action': 'upsert',
            'key': {'COUNTRY': 'US', 'MEASURE': 'VOL', 'TIME': '2026-01'},
            'preconditions': [{'attr_id': 'UNIT', 'expected': 'count'}],
            'value': '9',
            'attrs': [{'attr_id': 'UNIT', 'value': 'USD'},
                      {'attr_id': 'OBS_STATUS', 'value': 'A'}]}]})
    audit = w.audit(source_files=[
        os.path.join(ROOT, 'legacy/structure.xml'),
        os.path.join(ROOT, 'legacy/data.xml'),
        os.path.join(ROOT, 'inputs/structure-v2.xml')])
    w.close()

    raw = run_queries(DB, os.path.join(ROOT, 'queries.sql'))
    compat = Warehouse(DB)
    export_rev1 = compat.export_compat(1, 0)
    export_rev3 = compat.export_compat(3, 0)
    versions = [compat.query_version(r) for r in
                sorted(r['revision_seq'] for r in compat.revisions())]
    plans = compat.plans()
    compat.close()

    with open(LOG, 'w', encoding='utf-8') as fh:
        section(fh, 'migration plan (lossless refinement, derived edges)')
        fh.write(json.dumps({k: v for k, v in plan.items()
                             if k != 'derivations' and k != 'to_descriptor'},
                            ensure_ascii=False, indent=2))
        section(fh, 'compatibility export at revision 1 (old DSD1)')
        fh.write('expressible=%s view=%s rows=%d\n' % (
            export_rev1['expressible'], export_rev1.get('view_name'),
            len(export_rev1.get('rows', []))))
        section(fh, 'compatibility export at revision 3 after obs UNIT split')
        fh.write(json.dumps(export_rev3, ensure_ascii=False, indent=2))
        section(fh, 'revisions queryable (observation counts)')
        for v in versions:
            fh.write('revision %d dsd_seq=%d dsd_id=%s observations=%d '
                     'attr_facts=%d\n' % (
                         v['revision_seq'], v['dsd_version_seq'], v['dsd_id'],
                         len(v['observations']), len(v['attributes'])))
        section(fh, 'plans')
        fh.write(json.dumps(plans, ensure_ascii=False, indent=2))
        section(fh, 'audit (actual row/edge/byte counts)')
        fh.write(json.dumps(audit, ensure_ascii=False, indent=2))
        section(fh, 'live sqlite3 output of queries.sql')
        fh.write(raw)
    print('read-back log written to', LOG)


if __name__ == '__main__':
    main()
