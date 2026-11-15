import sqlite3
def connect(path):
 c=sqlite3.connect(path);c.execute('PRAGMA foreign_keys=ON');return c
def seed(path):
 c=connect(path);c.executescript('CREATE TABLE account(id INTEGER PRIMARY KEY);CREATE TABLE event(id INTEGER PRIMARY KEY,account_id REFERENCES account(id) ON DELETE CASCADE,created INTEGER);CREATE TABLE legal_hold(event_id INTEGER PRIMARY KEY REFERENCES event(id) ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED);INSERT INTO account VALUES(1);INSERT INTO event VALUES(11,1,5);');c.commit();c.close()
