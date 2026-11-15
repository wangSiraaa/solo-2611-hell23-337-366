import json,tempfile
from migration import convert
from shards import total
with tempfile.TemporaryDirectory() as d:
    convert('fixtures/safe.parquet',d+'/converted.parquet');print(json.dumps({'sum':str(total(d+'/converted.parquet'))}))
