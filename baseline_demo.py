from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from delta import apply
from range_reader import fetch
from source_image import read
d=json.load(open(M/'inputs/delta.json'))
with tempfile.TemporaryDirectory() as t:
 target=Path(t)/'mirror.img';shutil.copy2(M/'inputs/g1.img',target);apply(M/'inputs/g2.img',target,d['ops'],d['new_size']);got=fetch(target,0,4);expected=read(M/'inputs/g2.img',0,4)
 assert bytes.fromhex(got['data'])==b'AAAA' and expected==bytes(4)
 print(json.dumps({'range_result':got,'expected':expected.hex(),'actual_input_sizes':[(p.name,p.stat().st_size,p.stat().st_blocks*512) for p in (M/'inputs').glob('*.img')],'data_to_hole_failure_reproduced':True}))
