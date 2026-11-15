from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from index import build,overlap
from patcher import apply
from annotation import read
rows=json.load(open(M/'inputs/graph.json')); parsed=read(M/'legacy/features.gff3');assert parsed
with tempfile.TemporaryDirectory() as t:
 db=Path(t)/'graph.db';build(db,rows);before=overlap(db,20,21);apply(db,json.load(open(M/'inputs/patch.json')));after=overlap(db,20,21)
 assert 't1' in after and 'e' in after
 print(json.dumps({'parsed_legacy_entities':len(parsed),'before':before,'after_delete_t1':after,'stale_index_and_dangling_child_reproduced':True}))
