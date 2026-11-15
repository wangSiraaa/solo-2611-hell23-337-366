import json
def key(row):return json.dumps(row,sort_keys=True,separators=(',',':'))
def evaluate(node,sources,path='root'):
    if node['op']=='scan':return [{'row':r,'witness':f'{path}:{node["source"]}:{i}'} for i,r in enumerate(sources[node['source']])]
    a=evaluate(node['left'],sources,path+'.L');b=evaluate(node['right'],sources,path+'.R')
    if node['op']=='union_all':return a+b
    counts={}
    for x in b:counts[key(x['row'])]=counts.get(key(x['row']),0)+1
    out=[]
    for x in a:
        k=key(x['row']);hit=counts.get(k,0)>0
        if hit:counts[k]-=1
        if (node['op']=='except_all' and not hit) or (node['op']=='intersect_all' and hit):out.append(x)
    return out
