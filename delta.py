def apply(source,target,ops,new_size):
 with open(source,'rb') as src,open(target,'r+b') as dst:
  for op in ops:
   if op['kind']=='data':src.seek(op['start']);b=src.read(op['end']-op['start']);dst.seek(op['start']);dst.write(b)
  dst.truncate(new_size)
