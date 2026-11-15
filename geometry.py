def build(source,rid):
 ids=source['ways'][source['relations'][rid][0]];points=[source['nodes'][i] for i in ids]
 return {'ring':points,'bbox':[min(p[0] for p in points),min(p[1] for p in points),max(p[0] for p in points),max(p[1] for p in points)]}
