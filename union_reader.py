import pyarrow as pa
def load(path):
    with pa.memory_map(str(path),'r') as f:t=pa.ipc.open_file(f).read_all()
    return t.column(0).chunk(0)
def rows(array,code_to_branch):
    out=[];codes=list(array.type.type_codes)
    for i,code in enumerate(array.type_codes.to_pylist()):
        field=codes.index(code);child_index=array.offsets[i].as_py();out.append({'branch':code_to_branch[code],'value':array.field(field)[child_index].as_py()})
    return out
