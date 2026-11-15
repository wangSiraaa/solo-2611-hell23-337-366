import cbor2,copy
def load(path):
    with open(path,'rb') as f:root=cbor2.load(f)
    seen={};nodes={}
    def visit(x):
        if not isinstance(x,(dict,list)):return {'scalar':x}
        if id(x) in seen:return {'ref':seen[id(x)]}
        k=str(len(seen));seen[id(x)]=k;nodes[k]={'kind':'map' if isinstance(x,dict) else 'array','items':[]}
        if isinstance(x,dict):nodes[k]['items']=[[{'type':type(a).__name__,'value':a},visit(b)] for a,b in x.items()]
        else:nodes[k]['items']=[visit(b) for b in x]
        return {'ref':k}
    return {'root':visit(root)['ref'],'nodes':nodes}
class Store:
    def __init__(self,graph):self.versions={1:graph}
    def edit(self,version,node,index,new_value):
        g=self.versions[version];n=max(self.versions)+1;self.versions[n]=g;g['nodes'][node]['items'][index]=new_value;return n
    def get(self,v):return copy.deepcopy(self.versions[v])
