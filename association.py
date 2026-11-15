def pairs(events):
 return sorted([[a['id'],b['id']] for a in events for b in events if a['source']=='A' and b['source']=='B' and a['key']==b['key'] and abs(a['tai']-b['tai'])<=a.get('tolerance',0)])
