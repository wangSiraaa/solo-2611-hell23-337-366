from pathlib import Path
import sqlite3,hashlib,json
def create(source,dest,token):
 dest=Path(dest)
 if dest.exists():raise ValueError('snapshot already exists')
 src=sqlite3.connect('file:'+str(Path(source).resolve())+'?mode=ro',uri=True);out=sqlite3.connect(dest)
 try:src.backup(out)
 finally:out.close();src.close()
 return {'token':token,'path':str(dest.resolve()),'sha256':hashlib.sha256(dest.read_bytes()).hexdigest()}
def open_snapshot(binding):
 p=Path(binding['path'])
 if not p.exists() or hashlib.sha256(p.read_bytes()).hexdigest()!=binding['sha256']:raise ValueError('snapshot unavailable or changed')
 return sqlite3.connect('file:'+str(p.resolve())+'?mode=ro',uri=True)
