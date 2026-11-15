import json
from sequence_store import load,apply
from features import extract
def run(root,edits):
 seq=apply(load(root/'reference.fa'),edits)
 features=json.load(open(root/'features.json'))
 return {'sequence':seq,'features':{f['id']:extract(seq,f) for f in features}}
