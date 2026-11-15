def extract(sequence,feature):
 s=''.join(sequence[a:b] for a,b in feature['segments'])
 return s.translate(str.maketrans('ACGTN','TGCAN'))[::-1] if feature['strand']=='-' else s
