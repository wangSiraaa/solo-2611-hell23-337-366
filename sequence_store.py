def load(path):return ''.join(x.strip() for x in open(path) if not x.startswith('>'))
def apply(sequence,edits):
 out=[];pos=0
 for e in sorted(edits,key=lambda x:x['start']):out.extend([sequence[pos:e['start']],e['insert']]);pos=e['end']
 return ''.join(out)+sequence[pos:]
