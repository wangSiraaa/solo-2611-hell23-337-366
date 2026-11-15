from pathlib import Path
def make(path,size,writes):
 with open(path,'wb') as f:
  for start,data in writes:f.seek(start);f.write(data)
  f.truncate(size)
def read(path,start,end):
 with open(path,'rb') as f:f.seek(start);return f.read(end-start)
