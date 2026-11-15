import xml.etree.ElementTree as ET
def compile(path):
 r=ET.parse(path).getroot();out={}
 for a in r.findall('.//{*}Attribute'):
  rel=a.find('{*}AttributeRelationship');children=list(rel)
  out[a.get('id')]=[x.tag.split('}')[-1] for x in children]
 return out
