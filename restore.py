import sqlite3
from lexer import commands
def restore(text,path,stop_after=None):
    c=sqlite3.connect(path);c.execute('PRAGMA foreign_keys=ON');last=0
    for cmd in commands(text):
        if cmd['kind']=='sql':
            sql=cmd['sql']
            if sql.upper() in ('BEGIN;','COMMIT;'):last=cmd['line_after'];continue
            c.execute(sql)
        else:
            for row in cmd['rows']:c.execute('INSERT INTO '+cmd['table']+' VALUES('+','.join('?' for _ in row)+')',row)
        c.commit();last=cmd['line_after']
        if stop_after is not None and last>=stop_after:break
    c.close();return last
