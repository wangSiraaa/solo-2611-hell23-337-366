"""Baseline reproduction plus the fixed behaviour.

The first half reproduces the original engineering-start defect exactly:
structure.compile / data_reader.read still return the legacy in-memory shapes,
and a schema-only swap loses every UNIT value when the attachment moves from
series to observation.  The second half runs the same input through the real
SQLite warehouse and shows the attachment migration now preserves every value
with provenance and an old-DSD compatibility view.
"""
import json
import sys
import tempfile
from pathlib import Path

M = Path(__file__).resolve().parent
sys.path.insert(0, str(M))

# ---- legacy thin modules still expose their original entry points -------- #
from structure import compile
from data_reader import read
from warehouse import Warehouse


class SchemaOnlyWarehouse:
    """The original baseline behaviour: swap schema, never migrate facts."""

    def __init__(self, dsd, data):
        self.schema = compile(dsd)
        self.rows = read(data)

    def migrate(self, dsd):
        self.schema = compile(dsd)

    def unit(self, row):
        return row['obs_attrs'].get('UNIT') if self.schema['UNIT'] == ['PrimaryMeasure'] \
            else row['series_attrs'].get('UNIT')


def main():
    old = SchemaOnlyWarehouse(M / 'legacy/structure.xml', M / 'legacy/data.xml')
    before = [old.unit(r) for r in old.rows]
    old.migrate(M / 'inputs/structure-v2.xml')
    after = [old.unit(r) for r in old.rows]
    assert before == ['count', 'count', 'USD', 'count'] and after == [None] * 4

    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / 'warehouse.db'
        w = Warehouse(db)
        w.import_xml(M / 'legacy/structure.xml', M / 'legacy/data.xml')
        plan = w.plan_migration(M / 'inputs/structure-v2.xml', 'baseline-plan')
        w.apply_migration('baseline-plan')
        migrated = w.query_version(1)
        units = {a['obs_key']: a['raw_value']
                 for a in migrated['attributes'] if a['attr_id'] == 'UNIT'}
        provenance = w.conn.execute(
            'SELECT COUNT(*) FROM migration_provenance').fetchone()[0]
        compat = w.export_compat(1, 0)
        w.close()

    # compare semantically on the full observation key, independent of order
    expected_by_key = {
        json.dumps(row['key'], sort_keys=True, separators=(',', ':')): unit
        for row, unit in zip(old.rows, before)}
    data_loss_fixed = units == expected_by_key

    result = {
        'actual_xml_rows': len(old.rows),
        'before_units': before,
        'after_schema_only_migration': after,
        'attachment_migration_data_loss_reproduced': True,
        'fixed_warehouse_units_after_migration': units,
        'fixed_warehouse_provenance_edges': provenance,
        'fixed_warehouse_legacy_dsd_export_expressible': compat['expressible'],
        'data_loss_fixed': data_loss_fixed,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    assert result['data_loss_fixed']
    assert result['fixed_warehouse_legacy_dsd_export_expressible'] is True


if __name__ == '__main__':
    main()
