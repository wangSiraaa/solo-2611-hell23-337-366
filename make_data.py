import cbor2,json
from pathlib import Path
shared=[10,20];root={1:shared,'1':shared};root['cycle']=root
Path('fixtures').mkdir(exist_ok=True);Path('fixtures/shared.cbor').write_bytes(cbor2.dumps(root,value_sharing=True));Path('fixtures/patches.json').write_text(json.dumps([{'base':1,'node':'1','index':0,'value':{'scalar':99}},{'drop_version':1},{'reopen_version':2}]))
