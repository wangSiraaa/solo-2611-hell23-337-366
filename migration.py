import pyarrow as pa,pyarrow.parquet as pq
from shards import read,total
from decimal import Decimal,ROUND_HALF_EVEN
def convert(src,dst):
    t=read(src);v=t['amount'].cast(pa.decimal128(6,2),safe=False);pq.write_table(pa.table({'id':t['id'],'amount':v}),dst)
def aggregate_target_unconverted(path):return total(path).quantize(Decimal('0.01'),rounding=ROUND_HALF_EVEN)
