from database import connect
def plan(path,account):
 c=connect(path);ids=[x[0] for x in c.execute('SELECT id FROM event WHERE account_id=?',(account,))];c.close();return {'account':account,'events':ids}
def execute(path,p):
 c=connect(path)
 try:c.execute('DELETE FROM account WHERE id=?',(p['account'],));c.commit()
 finally:c.close()
