import json,tempfile
from nmea import sentence
from collector import Collector
from sink import Sink
c=Collector()
with tempfile.TemporaryDirectory() as d:
    s=Sink(d+'/s.db')
    for i in [1,2]:
        x=c.accept('R1',1,sentence(2,i,10+i))
        if x:s.send(x)
    print(json.dumps(s.rows()))
