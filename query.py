from catalog import search
def query(items,bbox,assets,origin):
 hits=search(items,bbox);out=[]
 for item in hits:
  a=assets[item['id']];b=origin.get(a['replicas'][0],0,a['size']);out.append({'id':item['id'],'data':b.hex()})
 return out
