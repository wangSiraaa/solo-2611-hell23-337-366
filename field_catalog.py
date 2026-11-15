def build(rows):
 out={}
 for r in rows:out[(r['shortName'],r['endStep'])]=r
 return out
