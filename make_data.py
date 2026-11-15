import zlib,json,hashlib
from pathlib import Path
from frames import frame
c=zlib.compressobj(wbits=-15);wire=bytearray();manifest=[];history=b''
for i in range(100):
    data=(bytes(range(256))*16)[:-8]+i.to_bytes(8,'big')
    payload=c.compress(data)+c.flush(zlib.Z_SYNC_FLUSH);assert payload[-4:]==b'\x00\x00\xff\xff';payload=payload[:-4];mid=len(payload)//2
    start=len(wire);wire+=frame(payload[:mid],fin=False,rsv1=True);wire+=frame(b'ping',opcode=9);wire+=frame(payload[mid:],opcode=0)
    manifest.append({'message':i,'start':start,'end':len(wire),'sha256':hashlib.sha256(data).hexdigest(),'uncompressed_bytes':len(data)});history+=data
Path('fixtures').mkdir(exist_ok=True);Path('fixtures/archive.frames').write_bytes(wire);Path('fixtures/source-truth.json').write_text(json.dumps(manifest,indent=2));Path('fixtures/negotiation.json').write_text(json.dumps({'permessage_deflate':True,'server_no_context_takeover':False,'server_max_window_bits':15}))
