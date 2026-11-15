from planner import plan
def combine(meta,reader,left,right):
 p=plan(meta,left,right);a=sum((reader.read(n) for n in p['left']),[]);b=sum((reader.read(n) for n in p['right']),[])
 return [None if x is None or y is None else x+y for x,y in zip(a,b)]
