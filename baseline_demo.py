from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from shards import seed,Shard
from merge import merge
spec=json.load(open(M/'inputs/shards.json'))
with tempfile.TemporaryDirectory() as t:
 shards=[]
 for name,s in spec.items():p=Path(t)/name;seed(p,s['rows']);shards.append(Shard(p,name))
 got=merge(shards,2);assert len(got)==3
 print(json.dumps({'baseline':got,'expected_ids':[['A',1],['A',2],['A',3],['A',4],['B',1]],'tie_pushdown_loss_reproduced':True,'actual_shard_calls':{s.name:s.log for s in shards}}))
