import sqlite3
from vcd import parse
def rebuild(raw,db):
    c=sqlite3.connect(db);c.executescript('DROP TABLE IF EXISTS events; CREATE TABLE events(tick INTEGER,key TEXT,value TEXT,observable INTEGER,seq INTEGER);')
    es=parse(raw);c.executemany('INSERT INTO events VALUES(:tick,:key,:value,:observable,:sequence)',es);c.commit();c.close();return len(es)
def value_at(db,key,tick):
    c=sqlite3.connect(db);r=c.execute('SELECT value,observable FROM events WHERE key=? AND tick<=? ORDER BY tick DESC,seq DESC LIMIT 1',(key,tick)).fetchone();c.close();return r
