import struct
def frame(payload,opcode=2,fin=True,rsv1=False):
    n=len(payload);head=bytes([(128 if fin else 0)|(64 if rsv1 else 0)|opcode]);return head+(bytes([n]) if n<126 else b'\x7e'+struct.pack('>H',n))+payload
def frames(data):
    p=0
    while p<len(data):
        start=p;b0,b1=data[p:p+2];p+=2;n=b1&127
        if n==126:n=struct.unpack('>H',data[p:p+2])[0];p+=2
        if b1&128:raise ValueError('fixture is unmasked server direction')
        body=data[p:p+n]
        if len(body)!=n:raise ValueError('truncated frame')
        p+=n;yield {'offset':start,'end':p,'fin':bool(b0&128),'compressed':bool(b0&64),'opcode':b0&15,'body':body}
