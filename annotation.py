from urllib.parse import unquote
def read(path):
 rows={}
 for line in open(path):
  if line.startswith('#') or not line.strip():continue
  f=line.rstrip().split('	');attrs=dict(x.split('=',1) for x in f[8].split(';') if '=' in x)
  key=unquote(attrs.get('ID','line-'+str(len(rows))))
  r=rows.setdefault(key,{'id':key,'kind':f[2],'parents':[unquote(p) for p in attrs.get('Parent','').split(',') if p],'segments':[]})
  r['segments'].append([int(f[3])-1,int(f[4])])
 return rows
