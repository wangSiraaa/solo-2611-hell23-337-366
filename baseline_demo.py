from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from chunk_store import Reader
from executor import combine
meta=json.load(open(M/'inputs/meta.json'));r=Reader(M/'inputs');got=combine(meta,r,'A','B');expected=[41,None,33,14]
assert got!=expected and len(r.reads)==4
print(json.dumps({'baseline':got,'label_oracle':expected,'actual_reads':r.reads,'alignment_failure_reproduced':True,'resident_cells_required_by_baseline':8}))
