import pyarrow as pa,json
from pathlib import Path
Path('fixtures').mkdir(exist_ok=True)
for name,codes,types,offsets,intvals,textvals in [('a',[3,7],[3,7,3],[0,0,1],[10,None],['x']),('b',[7,3],[7,3,7],[0,0,1],[20,21],['y'])]:
    u=pa.UnionArray.from_dense(pa.array(types,pa.int8()),pa.array(offsets,pa.int32()),[pa.array(intvals,pa.int64()),pa.array(textvals,pa.string())],field_names=['number','text'],type_codes=codes);t=pa.table({'value':u})
    with pa.OSFile('fixtures/'+name+'.arrow','wb') as sink:
        with pa.ipc.new_file(sink,t.schema) as w:w.write_table(t)
    Path('fixtures/'+name+'.schema.json').write_text(json.dumps({'schema_revision':1 if name=='a' else 2,'type_codes':{str(codes[0]):'branch-number',str(codes[1]):'branch-text'},'fields':{'branch-number':'int64','branch-text':'text'}},indent=2))
record=pa.array([{'qty':5},{'qty':999},None],type=pa.struct([pa.field('qty',pa.int64())]));missing=pa.nulls(3)
u=pa.UnionArray.from_sparse(pa.array([1,2,1],pa.int8()),[record,missing],field_names=['record','missing'],type_codes=[1,2]);t=pa.table({'value':u})
with pa.OSFile('fixtures/struct-v1.arrow','wb') as sink:
    with pa.ipc.new_file(sink,t.schema) as w:w.write_table(t)
record=pa.array([{'count':7,'note':'added'}],type=pa.struct([pa.field('count',pa.int64()),pa.field('note',pa.string())]));u=pa.UnionArray.from_dense(pa.array([5],pa.int8()),pa.array([0],pa.int32()),[record],field_names=['renamed_record'],type_codes=[5]);t=pa.table({'value':u})
with pa.OSFile('fixtures/struct-v2.arrow','wb') as sink:
    with pa.ipc.new_file(sink,t.schema) as w:w.write_table(t)
Path('fixtures/struct-evolution.json').write_text(json.dumps({'v1':{'type_codes':{'1':'branch-record','2':'branch-missing'},'branch-record':{'qty':'field-quantity'}},'v2':{'type_codes':{'5':'branch-record'},'branch-record':{'count':'field-quantity','note':'field-note'}},'recreated':{'count':'field-new-quantity','note':'field-note'},'expected_old_nulls':[{'parent_row':1,'branch':'branch-missing'},{'parent_row':2,'branch':'branch-record','child':None}]},indent=2))
Path('fixtures/schedule.json').write_text(json.dumps([{'producer':'P1','schema':'a.schema.json','batch':'a.arrow'},{'receiver':'pause'},{'producer':'P2','schema':'b.schema.json','batch':'b.arrow'},{'receiver':'accept_schema_revision_2'},{'receiver':'resume'}]))
