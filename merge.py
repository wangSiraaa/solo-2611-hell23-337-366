def merge(shards,n):
 rows=[]
 for s in shards:rows.extend((r[1],s.name,r[0],r[2]) for r in s.top(n))
 rows.sort(key=lambda x:(-x[0],x[1],x[2]));boundary=rows[min(n,len(rows))-1][0] if rows and n else None
 return [r for i,r in enumerate(rows) if i<n or r[0]==boundary]
