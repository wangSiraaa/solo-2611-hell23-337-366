import sqlite3
def seed(path,rows):
 c=sqlite3.connect(path);c.execute('CREATE TABLE fact(id INTEGER PRIMARY KEY,score INTEGER,payload TEXT)');c.executemany('INSERT INTO fact VALUES(?,?,?)',rows);c.commit();c.close()
class Shard:
 def __init__(self,path,name):self.path=path;self.name=name;self.log=[]
 def top(self,n):
  c=sqlite3.connect(self.path);r=c.execute('SELECT id,score,payload FROM fact ORDER BY score DESC,id LIMIT ?',(n,)).fetchall();c.close();self.log.append({'limit':n,'rows':len(r)});return r
