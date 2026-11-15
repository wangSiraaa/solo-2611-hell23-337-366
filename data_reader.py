import xml.etree.ElementTree as ET
def values(node):return {x.get('id'):x.get('value') for x in node.findall('{*}Value')} if node is not None else {}
def read(path):
 r=ET.parse(path).getroot();rows=[]
 for s in r.findall('.//{*}Series'):
  key=values(s.find('{*}SeriesKey'));attrs=values(s.find('{*}Attributes'))
  for o in s.findall('{*}Obs'):rows.append({'key':dict(key,TIME=o.find('{*}ObsDimension').get('value')),'value':o.find('{*}ObsValue').get('value'),'series_attrs':attrs,'obs_attrs':values(o.find('{*}Attributes'))})
 return rows
