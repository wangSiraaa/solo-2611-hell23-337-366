from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from source import seed,revoke
from pager import page
with tempfile.TemporaryDirectory() as t:
 p=Path(t)/'x.db';seed(p);a,cursor=page(p,'u',1);revoke(p);b,_=page(p,'u',1,cursor)
 assert a[0][1]=='a' and b[0][1]=='a'
 print(json.dumps({'page1':a,'after_revocation_page2':b,'authorization_leak_reproduced':True}))
