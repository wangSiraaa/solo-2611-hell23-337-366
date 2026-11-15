import pyarrow.parquet as pq
from decimal import Decimal
def read(path):return pq.read_table(path)
def total(path):return sum((x for x in read(path)['amount'].to_pylist() if x is not None),Decimal(0))
