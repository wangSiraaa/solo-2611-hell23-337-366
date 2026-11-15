from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from osm_source import load
from catalog import Catalog
from geometry import build
s=load(M/'inputs/base.osm');c=Catalog(s);c.move_node(2,[5,0]);c.move_node(3,[5,2]);observed=c.search(4,1);fresh=build(s,21)
assert observed==[] and fresh['bbox']==[0,0,5,2]
print(json.dumps({'after_actual_node_changes':observed,'fresh_geometry':fresh,'stale_spatial_index_reproduced':True}))
