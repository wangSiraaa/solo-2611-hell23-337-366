#!/usr/bin/env python3
from pathlib import Path
import os,sys,json
out=Path(sys.argv[1]);out.mkdir(parents=True,exist_ok=True)
p=out/'sample.bin'
with p.open('xb') as f:
 f.truncate(8*1024*1024+17)
 f.seek(0);f.write(b'HEAD'+b'A'*4092)
 f.seek(2*1024*1024);f.write(b'MIDDLE'+b'B'*4090)
 f.seek(4*1024*1024);f.write(b'\0'*4096)
 f.seek(8*1024*1024);f.write(b'TAIL-17-BYTES!!!!')
with (out/'empty.bin').open('xb'):pass
print(json.dumps({'sample':str(p),'size':p.stat().st_size,'empty':str(out/'empty.bin')}))
