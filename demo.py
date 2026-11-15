import tempfile,json,sqlite3
from pathlib import Path
from restore import restore
with tempfile.TemporaryDirectory() as d:
    off=restore(Path('fixtures/export.sql').read_text(),d+'/x.db');c=sqlite3.connect(d+'/x.db');print(json.dumps({'line_after':off,'parents':c.execute('SELECT * FROM parent').fetchall(),'children':c.execute('SELECT * FROM child').fetchall()}))
