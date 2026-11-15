import xml.etree.ElementTree as ET
def load(path):
 r=ET.parse(path).getroot();nodes={int(n.get('id')):[float(n.get('lon')),float(n.get('lat'))] for n in r.findall('node')}
 ways={int(w.get('id')):[int(x.get('ref')) for x in w.findall('nd')] for w in r.findall('way')}
 rels={int(a.get('id')):[int(x.get('ref')) for x in a.findall('member') if x.get('type')=='way'] for a in r.findall('relation')}
 return {'nodes':nodes,'ways':ways,'relations':rels}
