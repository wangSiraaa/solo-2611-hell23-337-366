from fractions import Fraction
import json
def load(path):
 d=json.load(open(path));out=[]
 for rep in d['representations']:
  for n,seg in enumerate(rep['segments']):
   out.append(dict(rep=rep['id'],number=n,start=str(Fraction(seg['t']-rep['pto'],rep['scale'])),end=str(Fraction(seg['t']+seg['d']-rep['pto'],rep['scale'])),url=seg['url'],init=rep['init'],bytes=seg['bytes']))
 return d['revision'],out
