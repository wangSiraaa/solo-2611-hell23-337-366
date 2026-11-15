import re
def parse(raw):
    text=raw.decode('ascii');body=text.split('$enddefinitions $end',1)[1];tick=0;events=[];observable=True;control=None
    for line in body.splitlines():
        line=line.strip()
        if not line:continue
        if line.startswith('#'):tick=int(line[1:]);continue
        if line in ('$dumpvars','$dumpoff','$dumpon'):
            control=line
            if line=='$dumpoff':observable=False
            if line=='$dumpon':observable=True
            continue
        if line=='$end':control=None;continue
        if line.startswith('b'):value,key=line[1:].split()
        elif line[0] in '01xz':value,key=line[0],line[1:]
        else:raise ValueError(line)
        events.append({'tick':tick,'key':key,'value':value,'observable':observable,'sequence':len(events)})
    return events
