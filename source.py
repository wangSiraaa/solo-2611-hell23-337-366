import sqlite3
def seed(path):
 c=sqlite3.connect(path);c.executescript('CREATE TABLE row_data(id INTEGER PRIMARY KEY,tenant TEXT,score INTEGER,body TEXT);CREATE TABLE grant_data(user TEXT,tenant TEXT);CREATE TABLE policy(epoch INTEGER);INSERT INTO policy VALUES(1);')
 c.executemany('INSERT INTO row_data VALUES(?,?,?,?)',[(1,'a',90,'A1'),(2,'b',85,'B1'),(3,'a',80,'A2'),(4,'b',70,'B2')]);c.execute('INSERT INTO grant_data VALUES(?,?)',('u','a'));c.commit();c.close()
def revoke(path):
 c=sqlite3.connect(path);c.execute('DELETE FROM grant_data');c.execute('UPDATE policy SET epoch=epoch+1');c.commit();c.close()
