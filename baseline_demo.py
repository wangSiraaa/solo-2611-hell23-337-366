from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from pipeline import run
edits=json.load(open(M/'inputs/edits.json'));x=run(M/'inputs',edits)
assert x['sequence']=='TTAACCGGNACGT'
# f1 original bases CC/AC survive at 4:6 and 9:11; strand - => GTGG.
assert x['features']['f1']!='GTGG'
print(json.dumps({'baseline':x,'surviving_feature_expected':'GTGG','coordinate_drift_reproduced':True}))
