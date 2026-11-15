import zlib
from frames import frames
def decode_all(data):
    dec=zlib.decompressobj(-15);messages=[];pending=bytearray();compressed=False
    for f in frames(data):
        if f['opcode'] in (9,10):continue
        if f['opcode']==2:compressed=f['compressed'];pending=bytearray()
        pending.extend(f['body'])
        if f['fin']:
            raw=bytes(pending);messages.append(dec.decompress(raw+b'\x00\x00\xff\xff') if compressed else raw)
    return messages
def get(data,index):return decode_all(data)[index]
