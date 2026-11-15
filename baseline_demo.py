from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from database import seed,connect
from retention import plan,execute
from auditor import remaining
with tempfile.TemporaryDirectory() as t:
 p=Path(t)/'db';seed(p);proposal=plan(p,1);c=connect(p);c.execute('INSERT INTO event VALUES(12,1,9)');c.commit();c.close();execute(p,proposal);result=remaining(p)
 assert proposal['events']==[11] and result['event']==[]
 print(json.dumps({'plan':proposal,'concurrent_insert':12,'result':result,'unreviewed_cascade_reproduced':True}))
