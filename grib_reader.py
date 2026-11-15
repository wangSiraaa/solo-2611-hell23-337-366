import eccodes as ec
def read(path):
 rows=[]
 with open(path,'rb') as f:
  while True:
   offset=f.tell();h=ec.codes_grib_new_from_file(f)
   if h is None:break
   try:
    r={k:ec.codes_get(h,k) for k in ['dataDate','dataTime','stepType','startStep','endStep','shortName']}
    r['offset']=offset;r['cells']=sorted([(float(x['lat']),float(x['lon']),float(x['value'])) for x in ec.codes_grib_get_data(h)]);rows.append(r)
   finally:ec.codes_release(h)
 return rows
