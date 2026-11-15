import json
from pathlib import Path
from nmea import sentence
events=[{'kind':'data','receiver':'R1','generation':1,'seq':1,'wire':sentence(2,1,11)},{'kind':'disconnect','receiver':'R1','generation':1},{'kind':'data','receiver':'R1','generation':2,'seq':1,'wire':sentence(2,2,22)},{'kind':'data','receiver':'R2','generation':4,'seq':1,'wire':sentence(1,1,31)},{'kind':'data','receiver':'R1','generation':2,'seq':2,'wire':sentence(2,1,41)},{'kind':'data','receiver':'R1','generation':2,'seq':3,'wire':sentence(2,2,42)}]
Path('fixtures').mkdir(exist_ok=True);Path('fixtures/events.json').write_text(json.dumps(events,indent=2));Path('fixtures/sink-schedule.json').write_text(json.dumps(['pause','receive','commit','lose_ack','retry','resume']))
