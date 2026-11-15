import json
from range_server import Source
from query import query
s=Source(['fixtures/a.fits']);url=s.start()
try:print(json.dumps({'result':query(url,[7,83]),'body_bytes':s.sent}))
finally:s.close()
