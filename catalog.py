from geometry import build
class Catalog:
 def __init__(self,source):self.source=source;self.shapes={k:build(source,k) for k in source['relations']}
 def move_node(self,n,point):self.source['nodes'][n]=point
 def search(self,x,y):return [rid for rid,g in self.shapes.items() if g['bbox'][0]<=x<=g['bbox'][2] and g['bbox'][1]<=y<=g['bbox'][3]]
