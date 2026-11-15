import hashlib
def fetch(path,start,end):
 with open(path,'rb') as f:f.seek(start);b=f.read(end-start)
 return {'data':b.hex(),'sha256':hashlib.sha256(b).hexdigest(),'read_bytes':len(b)}
