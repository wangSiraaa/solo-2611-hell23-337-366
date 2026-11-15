import sqlite3,json,base64
from auth import allowed
def page(path,user,size,cursor=None):
 c=sqlite3.connect(path)
 if cursor:
  state=json.loads(base64.urlsafe_b64decode(cursor));tenants=state['tenants'];last=state['last']
 else:tenants=allowed(c,user);last=10**9
 marks=','.join('?' for _ in tenants) or 'NULL'
 rows=c.execute('SELECT id,tenant,score,body FROM row_data WHERE tenant IN ('+marks+') AND score<? ORDER BY score DESC,id ASC LIMIT ?',tenants+[last,size]).fetchall();c.close()
 token=base64.urlsafe_b64encode(json.dumps({'tenants':tenants,'last':rows[-1][2] if rows else last}).encode()).decode()
 return rows,token
