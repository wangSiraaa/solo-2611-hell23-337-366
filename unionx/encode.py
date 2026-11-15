"""局部 type-code 解码 + canonical 重编码。

所有结构性校验都在这里发生；任何一项失败都抛 IncompatibleSchema，
调用方保证不会把半批数据写入输出（原子拒绝）。

转换缓冲披露：实现使用一次 child.to_pylist() 取出最多 1000 行的 Python 标量，
这是题目允许的"为完成一个正在处理批额外一个 <=1000 行转换 buffer"，
其字节数在 ReencodedBatch.conversion_buffer_bytes 中披露并计入内存预算。
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from .protocol import IPC_METADATA_KEY, Envelope, ProtocolError
from .schema import (
    KIND_INT64,
    KIND_STRUCT,
    KIND_TEXT,
    LIMITS,
    MISSING_BRANCH_ID,
    CanonicalSchema,
    IncompatibleSchema,
)


@dataclass
class ReencodedBatch:
    table: pa.Table                 # value: canonical dense union + provenance
    rows: list[dict]                # 规范化后的 python 行（含 branch_id/value/provenance）
    input_bytes: int                # 原始 IPC 字节
    conversion_buffer_bytes: int    # 披露：to_pylist 转换缓冲估算
    output_bytes: int               # 重编码后 Arrow 字节估算
    input_mode: str

    @property
    def nbytes(self) -> int:
        return self.input_bytes + self.conversion_buffer_bytes + self.output_bytes


def _check(cond: bool, msg: str) -> None:
    if not cond:
        raise IncompatibleSchema(msg)


def _validate_array_against_descriptor(union: pa.UnionArray, env: Envelope) -> str:
    desc = env.descriptor
    mode = union.type.mode
    _check(mode in ("dense", "sparse"), f"不支持的 union mode: {mode}")
    declared = dict(desc.branch_codes)
    arrow_codes = list(union.type.type_codes)
    _check(
        set(arrow_codes) == set(declared),
        f"Arrow type_codes {arrow_codes} 与 descriptor {sorted(declared)} 不一致",
    )
    _check(union.type.num_fields == len(desc.branches), "union child 数量与 descriptor 不符")

    type_ids = union.type_codes.to_pylist()
    _check(len(type_ids) <= LIMITS["max_rows_per_batch"], "批行数超过 1000")
    offsets = union.offsets.to_pylist() if mode == "dense" else None

    for arrow_index, code in enumerate(arrow_codes):
        branch = desc.branch(declared[code])
        _check(branch is not None, f"type-code {code} 无法映射到 branch_id")
        child = union.field(arrow_index)
        ctype = child.type
        if branch.branch_id == MISSING_BRANCH_ID:
            _check(pa.types.is_null(ctype), f"missing 分支 child 必须是 null 类型: {ctype}")
        elif branch.kind == KIND_INT64:
            _check(pa.types.is_int64(ctype), f"分支 {branch.branch_id} 期望 int64，实际 {ctype}")
        elif branch.kind == KIND_TEXT:
            _check(
                pa.types.is_string(ctype) or pa.types.is_large_string(ctype),
                f"分支 {branch.branch_id} 期望 string，实际 {ctype}",
            )
        elif branch.kind == KIND_STRUCT:
            _check(pa.types.is_struct(ctype), f"分支 {branch.branch_id} 期望 struct，实际 {ctype}")
            _check(
                ctype.num_fields == len(branch.fields),
                f"struct {branch.branch_id} 字段数不符: {ctype.num_fields} vs {len(branch.fields)}",
            )
            arrow_names = [ctype.field(i).name for i in range(ctype.num_fields)]
            # 外部 IPC 的 Arrow 字段名可能是显示名，按信封映射还原为 field_id
            resolved = {
                env.arrow_name_to_field_id(branch.branch_id, an) for an in arrow_names
            }
            _check(
                resolved == {f.field_id for f in branch.fields},
                f"struct {branch.branch_id} Arrow 字段无法映射到 field_id: {arrow_names}",
            )
            for i in range(ctype.num_fields):
                af = ctype.field(i)
                fid = env.arrow_name_to_field_id(branch.branch_id, af.name)
                spec = branch.field_by_id(fid)
                want = pa.int64() if spec.kind == KIND_INT64 else pa.string()
                _check(
                    af.type == want,
                    f"struct 字段 {fid} 类型不符: {af.type} vs {want}",
                )

    # 逐行：code 合法、dense offset 越界检查、非空字段约束
    selected: dict[str, set[int]] = {b.branch_id: set() for b in desc.branches}
    for parent_row, code in enumerate(type_ids):
        _check(code in declared, f"第 {parent_row} 行出现未声明 type-code {code}")
        bid = declared[code]
        if mode == "dense":
            off = offsets[parent_row]
            arrow_index = arrow_codes.index(code)
            _check(
                0 <= off < len(union.field(arrow_index)),
                f"第 {parent_row} 行 dense offset {off} 越界",
            )
            selected[bid].add(off)
        else:
            selected[bid].add(parent_row)  # sparse：child 槽位 == 父行号

    for branch in desc.branches:
        if branch.kind != KIND_STRUCT:
            continue
        arrow_index = arrow_codes.index(
            next(c for c, bid in declared.items() if bid == branch.branch_id)
        )
        pylist = union.field(arrow_index).to_pylist()
        for slot in sorted(selected[branch.branch_id]):
            val = pylist[slot]
            if val is None:
                continue  # 整 child NULL：分支仍存在（与 missing 严格区分），
                # 不触发 struct 内部的非空字段约束
            for f in branch.fields:
                # 旧 IPC dict 键可能是 Arrow 显示名，统一按 field_id 取值
                key = f.field_id
                arrow_val = None
                for k, v in val.items():
                    if env.arrow_name_to_field_id(branch.branch_id, k) == key:
                        arrow_val = v
                        break
                if not f.nullable and arrow_val is None:
                    raise IncompatibleSchema(
                        f"非空字段 {branch.branch_id}/{f.field_id} 在行 {slot} 为 NULL"
                    )
    return mode


def _canonical_struct_type(branch) -> pa.StructType:
    fields = [
        pa.field(
            f.field_id,
            pa.int64() if f.kind == KIND_INT64 else pa.string(),
            nullable=True,  # canonical 统一可空：旧行在新增字段上必然为 NULL
        )
        for f in branch.fields
    ]
    return pa.struct(fields)


def _canonical_arrow_type(cs: CanonicalSchema) -> pa.UnionType:
    fields, codes = [], []
    for b in cs.branches:
        code = cs.canonical_code(b.branch_id)
        if b.kind == KIND_INT64:
            at = pa.int64()
        elif b.kind == KIND_TEXT:
            at = pa.string()
        elif b.kind == KIND_STRUCT:
            at = _canonical_struct_type(b)
        else:
            at = pa.null()
        fields.append(pa.field(b.branch_id, at))
        codes.append(code)
    # dense union，canonical code 即 receiver 私有稳定编号
    return pa.dense_union(fields, type_codes=codes)


def provenance_struct_type() -> pa.StructType:
    return pa.struct(
        [
            pa.field("producer", pa.string()),
            pa.field("seq", pa.int64()),
            pa.field("parent_row", pa.int64()),
            pa.field("original_child_offset", pa.int64()),
            pa.field("input_mode", pa.string()),
        ]
    )


def reencode(env: Envelope, canonical: CanonicalSchema) -> ReencodedBatch:
    """把一个信封重编码进给定 canonical 快照。失败抛异常，不产生任何输出。"""
    # 0. IPC 自描述 metadata 与显式信封头必须一致
    table = env.read_table()
    meta = table.schema.metadata or {}
    raw = meta.get(IPC_METADATA_KEY)
    if raw is None:
        raise ProtocolError("IPC 缺少 unionx metadata")
    import json

    embedded = json.loads(raw.decode("utf-8"))
    h = embedded["header"]
    _check(
        h["producer_id"] == env.producer_id
        and int(h["schema_revision"]) == env.schema_revision
        and int(h["batch_seq"]) == env.batch_seq,
        "IPC metadata 批头与信封不一致",
    )
    _check(table.num_columns == 1, "信封 IPC 必须恰好包含一列 Union")
    union = table.column(0)
    _check(pa.types.is_union(union.type), "第 0 列必须是 Union")

    desc = env.descriptor
    _check(
        {int(k): v for k, v in h["branch_codes"].items()} == dict(env.branch_codes),
        "批内 branch_codes 与信封不一致",
    )

    # 0 行批：没有任何 chunk，直接产出符合 canonical 类型的空表
    if table.num_rows == 0:
        empty_children, empty_names, empty_codes = [], [], []
        for b in canonical.branches:
            if b.kind == KIND_INT64:
                arr = pa.array([], type=pa.int64())
            elif b.kind == KIND_TEXT:
                arr = pa.array([], type=pa.string())
            elif b.kind == KIND_STRUCT:
                arr = pa.array([], type=_canonical_struct_type(b))
            else:
                arr = pa.nulls(0, type=pa.null())
            empty_children.append(arr)
            empty_names.append(b.branch_id)
            empty_codes.append(canonical.canonical_code(b.branch_id))
        empty_value = pa.UnionArray.from_dense(
            pa.array([], type=pa.int8()), pa.array([], type=pa.int32()),
            empty_children, field_names=empty_names, type_codes=empty_codes,
        )
        empty_prov = pa.array([], type=provenance_struct_type())
        empty_table = pa.table({"value": empty_value, "provenance": empty_prov})
        return ReencodedBatch(
            table=empty_table, rows=[], input_bytes=len(env.ipc),
            conversion_buffer_bytes=0, output_bytes=empty_table.nbytes,
            input_mode=union.type.mode,
        )

    union = union.chunk(0)
    mode = _validate_array_against_descriptor(union, env)

    # 1. 描述身份必须都已存在于 canonical（屏障保证；这里是防御性断言）
    for b in desc.branches:
        _check(
            canonical.get_branch(b.branch_id) is not None,
            f"分支 {b.branch_id} 尚未进入 canonical，屏障失效",
        )

    arrow_codes = list(union.type.type_codes)
    type_ids = union.type_codes.to_pylist()
    offsets = union.offsets.to_pylist() if mode == "dense" else None
    declared = dict(desc.branch_codes)

    # 2. 取 child python 值（唯一的转换缓冲，披露字节数）
    child_py: dict[str, list] = {}
    conversion_bytes = 0
    for arrow_index, code in enumerate(arrow_codes):
        bid = declared[code]
        pl = union.field(arrow_index).to_pylist()
        child_py[bid] = pl
        conversion_bytes += len(union.field(arrow_index).buffers()) and union.field(
            arrow_index
        ).nbytes

    # 3. 按 canonical 身份逐行归位
    per_branch_values: dict[str, list] = {b.branch_id: [] for b in canonical.branches}
    canonical_ids: list[int] = []
    dense_offsets: list[int] = []
    prov_producer: list[str] = []
    prov_seq: list[int] = []
    prov_parent: list[int] = []
    prov_child_off: list[int] = []
    prov_mode: list[str] = []
    rows_out: list[dict] = []

    for parent_row, code in enumerate(type_ids):
        bid = declared[code]
        if mode == "dense":
            child_off = offsets[parent_row]
        else:
            child_off = parent_row  # sparse：child 与父等长，选中槽位即父行号
        raw_value = child_py[bid][child_off]
        cbranch = canonical.get_branch(bid)

        if bid == MISSING_BRANCH_ID:
            value = None
        elif cbranch.kind == KIND_STRUCT:
            if raw_value is None:
                value = None  # 分支存在、整 child NULL —— 不是缺席
            else:
                # 外部 IPC 的 dict 键可能是 Arrow 显示名，先规范化为 field_id，
                # 再按身份投影到 canonical 字段集合；未知/已删字段丢弃，新增字段缺键 => NULL
                normalized = {
                    env.arrow_name_to_field_id(bid, k): v for k, v in raw_value.items()
                }
                value = {
                    f.field_id: normalized.get(f.field_id) for f in cbranch.fields
                }
        else:
            value = raw_value

        canonical_ids.append(canonical.canonical_code(bid))
        dense_offsets.append(len(per_branch_values[bid]))
        per_branch_values[bid].append(value)

        prov_producer.append(env.producer_id)
        prov_seq.append(env.batch_seq)
        prov_parent.append(parent_row)
        prov_child_off.append(child_off)
        prov_mode.append(mode)
        rows_out.append(
            {
                "branch_id": bid,
                "value": value,
                "provenance": {
                    "producer": env.producer_id,
                    "seq": env.batch_seq,
                    "parent_row": parent_row,
                    "original_child_offset": child_off,
                    "input_mode": mode,
                },
            }
        )

    # 4. 构造 canonical dense union（所有 canonical 分支都带 child，未选中的为空数组）
    children, field_names, child_codes = [], [], []
    for b in canonical.branches:
        vals = per_branch_values[b.branch_id]
        if b.kind == KIND_INT64:
            arr = pa.array(vals, type=pa.int64())
        elif b.kind == KIND_TEXT:
            arr = pa.array(vals, type=pa.string())
        elif b.kind == KIND_STRUCT:
            arr = pa.array(vals, type=_canonical_struct_type(b))
        else:
            arr = pa.nulls(len(vals), type=pa.null())
        children.append(arr)
        field_names.append(b.branch_id)
        child_codes.append(canonical.canonical_code(b.branch_id))

    canonical_union = pa.UnionArray.from_dense(
        pa.array(canonical_ids, type=pa.int8()),
        pa.array(dense_offsets, type=pa.int32()),
        children,
        field_names=field_names,
        type_codes=child_codes,
    )
    provenance = pa.StructArray.from_arrays(
        [
            pa.array(prov_producer, type=pa.string()),
            pa.array(prov_seq, type=pa.int64()),
            pa.array(prov_parent, type=pa.int64()),
            pa.array(prov_child_off, type=pa.int64()),
            pa.array(prov_mode, type=pa.string()),
        ],
        names=["producer", "seq", "parent_row", "original_child_offset", "input_mode"],
    )
    out_table = pa.table({"value": canonical_union, "provenance": provenance})
    return ReencodedBatch(
        table=out_table,
        rows=rows_out,
        input_bytes=len(env.ipc),
        conversion_buffer_bytes=conversion_bytes,
        output_bytes=out_table.nbytes,
        input_mode=mode,
    )
