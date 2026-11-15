from structure import compile
from data_reader import read
class Warehouse:
 def __init__(self,dsd,data):self.schema=compile(dsd);self.rows=read(data)
 def migrate(self,dsd):self.schema=compile(dsd)
 def unit(self,row):return row['obs_attrs'].get('UNIT') if self.schema['UNIT']==['PrimaryMeasure'] else row['series_attrs'].get('UNIT')
