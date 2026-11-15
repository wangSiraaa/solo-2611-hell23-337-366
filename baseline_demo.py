from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from materializer import Store
events=json.load(open(M/'inputs/events.json'));r1=json.load(open(M/'inputs/r1.json'));r2=json.load(open(M/'inputs/r2.json'));s=Store(events,r1);s.update_rules(r2);correct=Store(events,r2)
assert s.pairs==[] and correct.pairs==[['u','t']]
print(json.dumps({'stale_pairs':s.pairs,'new_rule_recompute':correct.pairs,'utc_tai_offsets':[[e['id'],e['tai']] for e in correct.events],'cross_module_staleness_reproduced':True}))
