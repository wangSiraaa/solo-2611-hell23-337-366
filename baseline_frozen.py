"""Frozen verbatim copy of the original (pre-upgrade) modules.

Kept so baseline_demo.py can still reproduce the documented defect of the
starting point exactly as it was: move_node only mutates the source dict,
while Catalog.shapes and its bbox index are never invalidated, so queries
keep returning the stale extent.  Do not reuse this code; the upgraded
engine lives in osm_source.py / geometry.py / catalog.py.
"""
import xml.etree.ElementTree as ET


def load(path):
    r = ET.parse(path).getroot()
    nodes = {int(n.get('id')): [float(n.get('lon')), float(n.get('lat'))] for n in r.findall('node')}
    ways = {int(w.get('id')): [int(x.get('ref')) for x in w.findall('nd')] for w in r.findall('way')}
    rels = {int(a.get('id')): [int(x.get('ref')) for x in a.findall('member') if x.get('type') == 'way'] for a in r.findall('relation')}
    return {'nodes': nodes, 'ways': ways, 'relations': rels}


def build(source, rid):
    ids = source['ways'][source['relations'][rid][0]]
    points = [source['nodes'][i] for i in ids]
    return {'ring': points, 'bbox': [min(p[0] for p in points), min(p[1] for p in points),
                                     max(p[0] for p in points), max(p[1] for p in points)]}


class Catalog:
    def __init__(self, source):
        self.source = source
        self.shapes = {k: build(source, k) for k in source['relations']}

    def move_node(self, n, point):
        self.source['nodes'][n] = point

    def search(self, x, y):
        return [rid for rid, g in self.shapes.items()
                if g['bbox'][0] <= x <= g['bbox'][2] and g['bbox'][1] <= y <= g['bbox'][3]]
