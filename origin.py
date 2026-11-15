from pathlib import Path
import hashlib,json
class Origin:
 def __init__(self,root):self.root=Path(root);self.bytes=0;self.log=[]
 def get(self,url,revision):
  p=self.root/revision/url;b=p.read_bytes();self.bytes+=len(b)
  self.log.append({'url':url,'revision':revision,'bytes':len(b)})
  return b,hashlib.sha256(b).hexdigest()
