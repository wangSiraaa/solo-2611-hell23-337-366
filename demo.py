import json
from graph import load,Store
s=Store(load('fixtures/shared.cbor'));print(json.dumps(s.get(1)))
