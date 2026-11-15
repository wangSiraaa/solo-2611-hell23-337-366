import hashlib
class Origin:
 def __init__(self,root):self.root=root;self.log=[]
 def get(self,path,start,end):
  with (self.root/path).open('rb') as f:f.seek(start);b=f.read(end-start)
  self.log.append({'path':path,'start':start,'end':end,'bytes':len(b)})
  return b
