from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from collector import collect
from origin import Origin
with tempfile.TemporaryDirectory() as t:
 o=Origin(M/'origin');collect(M/'inputs/r1.json',o,Path(t)/'cache',Path(t)/'manifest.json')
 before=o.bytes;collect(M/'inputs/r2.json',o,Path(t)/'cache',Path(t)/'manifest.json')
 stale=(Path(t)/'cache/init.bin').read_bytes()!= (M/'origin/r2/init.bin').read_bytes()
 assert stale and o.bytes==before
 assert (M/'origin/r1/low-0.bin').read_bytes()!=(M/'origin/r2/low-0.bin').read_bytes()
 print(json.dumps({'baseline_runs':2,'actual_origin_reads':o.log,'stale_cache_reproduced':stale,'budget':40,'low_bundle_bytes':sum(len((M/'origin/r1'/x).read_bytes()) for x in ['init.bin','low-0.bin','low-1.bin'])}))
