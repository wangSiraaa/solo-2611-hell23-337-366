def commands(text):
    # Existing line-oriented subset; quoted COPY payload is deliberately a different mode.
    lines=text.splitlines();i=0
    while i<len(lines):
        line=lines[i];i+=1
        if line.startswith('COPY '):
            table=line.split()[1];rows=[]
            while i<len(lines) and lines[i]!='\\.':rows.append(lines[i].split('\t'));i+=1
            if i==len(lines):raise ValueError('truncated COPY')
            i+=1;yield {'kind':'copy','table':table,'rows':rows,'line_after':i}
        elif line.strip():yield {'kind':'sql','sql':line,'line_after':i}
