from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from query import query
from replica import Origin
from catalog import discover
assert discover(M/'legacy/catalog')
o=Origin(M/'inputs');items=json.load(open(M/'inputs/items.json'));assets=json.load(open(M/'inputs/assets.json'));false_hits=query(items,[-1,-1,1,1],assets,o)
assert false_hits and bytes.fromhex(false_hits[0]['data'])==b'old-not-good'
print(json.dumps({'false_spatial_hit_and_wrong_replica':false_hits,'actual_reads':o.log,'existing_stac_items':len(discover(M/'legacy/catalog'))}))
