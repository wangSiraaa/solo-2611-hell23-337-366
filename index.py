import sqlite3,json
def build(path,rows):
 c=sqlite3.connect(path);c.executescript('DROP TABLE IF EXISTS entity;DROP TABLE IF EXISTS segment;CREATE TABLE entity(id PRIMARY KEY,body);CREATE TABLE segment(id,lo,hi);')
 for k,r in rows.items():
  c.execute('INSERT INTO entity VALUES (?,?)',(k,json.dumps(r)))
  c.executemany('INSERT INTO segment VALUES (?,?,?)',[(k,a,b) for a,b in r['segments']])
 c.commit();c.close()
def overlap(path,lo,hi):
 c=sqlite3.connect(path);out=[x[0] for x in c.execute('SELECT DISTINCT id FROM segment WHERE lo<? AND hi>? ORDER BY id',(hi,lo))];c.close();return out
