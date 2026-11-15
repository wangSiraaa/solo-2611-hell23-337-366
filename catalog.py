import json
def discover(root):
 out=[]
 for p in root.rglob('*.json'):
  d=json.load(open(p))
  if d.get('type')=='Feature':out.append(d)
 return out
def search(items,bbox):
 w,s,e,n=bbox
 return [i for i in items if i['bbox'][0]<=e and i['bbox'][2]>=w and i['bbox'][1]<=n and i['bbox'][3]>=s]
