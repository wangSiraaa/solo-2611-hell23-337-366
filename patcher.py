import sqlite3,json
def apply(path,patch):
 c=sqlite3.connect(path)
 for op in patch:
  if op['op']=='delete':c.execute('DELETE FROM entity WHERE id=?',(op['id'],))
  else:c.execute('INSERT OR REPLACE INTO entity VALUES (?,?)',(op['id'],json.dumps(op['entity'])))
 c.commit();c.close()
