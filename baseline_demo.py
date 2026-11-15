from pathlib import Path
import json,sys,tempfile,shutil,subprocess
M=Path(__file__).resolve().parent
sys.path.insert(0,str(M))
from warehouse import Warehouse
w=Warehouse(M/'legacy/structure.xml',M/'legacy/data.xml');before=[w.unit(r) for r in w.rows];w.migrate(M/'inputs/structure-v2.xml');after=[w.unit(r) for r in w.rows]
assert before==['count','count','USD','count'] and after==[None]*4
print(json.dumps({'actual_xml_rows':len(w.rows),'before_units':before,'after_schema_only_migration':after,'attachment_migration_data_loss_reproduced':True}))
