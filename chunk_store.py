import json
class Reader:
 def __init__(self,root):self.root=root;self.reads=[]
 def read(self,name):
  p=self.root/name;raw=p.read_bytes();v=json.loads(raw);self.reads.append({'name':name,'bytes':len(raw),'cells':len(v)})
  return v
