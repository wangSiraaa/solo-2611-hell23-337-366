import json
from pathlib import Path
from manifest import load
def collect(manifest,origin,cache,target):
 rev,rows=load(manifest);cache=Path(cache);cache.mkdir(exist_ok=True)
 result=[]
 for r in rows:
  for url in [r['init'],r['url']]:
   p=cache/url.replace('/','_')
   if not p.exists():p.write_bytes(origin.get(url,rev)[0])
  result.append(r)
 Path(target).write_text(json.dumps({'revision':rev,'segments':result}))
 return result
