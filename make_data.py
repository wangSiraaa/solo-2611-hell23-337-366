from decimal import Decimal
from pathlib import Path
import pyarrow as pa,pyarrow.parquet as pq,json
Path('fixtures').mkdir(exist_ok=True)
for name,rows in [('a',['0.0150','0.0150',None]),('b',['-0.0250','9999.9950']),('safe',['1.0000','2.0000'])]:
    pq.write_table(pa.table({'id':list(range(len(rows))),'amount':pa.array([Decimal(x) if x is not None else None for x in rows],type=pa.decimal128(8,4))}),'fixtures/'+name+'.parquet')
Path('fixtures/plan.json').write_text(json.dumps({'from':{'precision':8,'scale':4},'to':{'precision':6,'scale':2},'rounding':'half_even','overflow':'reject_shard','old_reader_scale':4,'target_reader_semantics':'convert each row then sum','shards':['a','b'],'interruptions':['after_output_before_manifest','after_manifest_before_old_reader_release']},indent=2))
