import json
from pathlib import Path
from receiver import Receiver
r=Receiver();print(json.dumps(r.accept('fixtures/a.arrow',json.loads(Path('fixtures/a.schema.json').read_text()))))
