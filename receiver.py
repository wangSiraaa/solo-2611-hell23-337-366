from union_reader import load,rows
class Receiver:
    def __init__(self):self.mapping=None;self.output=[]
    def accept(self,path,schema):
        if self.mapping is None:self.mapping={int(k):v for k,v in schema['type_codes'].items()}
        batch=rows(load(path),self.mapping);self.output.extend(batch);return batch
