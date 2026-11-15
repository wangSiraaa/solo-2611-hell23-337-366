from datetime import datetime,timezone
def seconds(s):return int(datetime.fromisoformat(s.replace('Z','+00:00')).replace(tzinfo=timezone.utc).timestamp())
def normalize(event,table):
 if event['scale']=='TAI':return seconds(event['value'])
 t=seconds(event['value']);offset=table['initial_offset']
 for e in table['changes']:
  if t>=seconds(e['effective']):offset=e['offset']
 return t+offset
