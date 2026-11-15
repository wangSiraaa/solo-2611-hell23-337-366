import json,hashlib
from pathlib import Path
from archive import get
raw=get(Path('fixtures/archive.frames').read_bytes(),99);print(json.dumps({'message':99,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}))
