from nmea import decode
class Collector:
    def __init__(self):self.pending={};self.output=[]
    def accept(self,receiver,generation,line):
        d=decode(line);key=(receiver,d['talker'])
        if d['part']==1:self.pending[key]={}
        parts=self.pending.setdefault(key,{})
        parts[d['part']]=d
        if set(parts)==set(range(1,d['total']+1)):
            result={'receiver':receiver,'generation':generation,'talker':d['talker'],'parts':[parts[i] for i in sorted(parts)]};self.output.append(result);self.pending.pop(key);return result
