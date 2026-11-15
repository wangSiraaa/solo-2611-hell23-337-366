"""Baseline demo: (1) reproduces the documented defect of the original
modules (kept frozen in baseline_frozen.py), (2) runs the same scenario on
the upgraded engine and shows the defect is fixed.

Exit 0 means: the old defect is still reproducible on the frozen code AND
the new engine returns the fresh geometry for the same query.
"""
from pathlib import Path
import json
import sys

M = Path(__file__).resolve().parent
sys.path.insert(0, str(M))

import baseline_frozen as old
from catalog import Catalog

# --- 1) original defect, on the frozen starting-point modules --------------
s = old.load(M / 'inputs/base.osm')
c = old.Catalog(s)
c.move_node(2, [5, 0])
c.move_node(3, [5, 2])
observed = c.search(4, 1)
fresh = old.build(s, 21)
assert observed == [] and fresh['bbox'] == [0, 0, 5, 2]
defect = {'after_actual_node_changes': observed,
          'fresh_geometry': fresh,
          'stale_spatial_index_reproduced': True}

# --- 2) same scenario on the upgraded engine --------------------------------
cat = Catalog()
cat.import_osm(M / 'inputs/base.osm')
cat.apply_diff(M / 'inputs/diff.json')  # same node moves, as an ordered diff
hits = cat.search(4, 1)
assert [h['id'] for h in hits] == [21] and hits[0]['bbox'] == [0.0, 0.0, 5.0, 2.0]
fixed = {'after_actual_node_changes': hits,
         'stale_spatial_index_reproduced': False}

print(json.dumps({'baseline_defect': defect, 'upgraded_engine': fixed}))
