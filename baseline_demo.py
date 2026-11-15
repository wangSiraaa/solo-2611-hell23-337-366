from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from grib_reader import read
from field_catalog import build
r=read(M/'inputs/windows.grib2');c=build(r)
assert len(r)==4 and len(c)<len(r) and len(r[0]['cells'])>0
assert r[1]['cells'][0][2]==5 and r[2]['cells'][0][2]==8
print(json.dumps({'messages':[dict((k,v) for k,v in x.items() if k!='cells') for x in r],'decoded_first_cell_values':[x['cells'][0][2] for x in r],'catalog_rows':len(c),'window_and_cycle_collision_reproduced':True}))
