import tempfile,json
from pathlib import Path
from index import rebuild,value_at
with tempfile.TemporaryDirectory() as d:
    n=rebuild(Path('fixtures/signals.vcd').read_bytes(),d+'/index.db');print(json.dumps({'events':n,'at5':value_at(d+'/index.db','#',5)}))
