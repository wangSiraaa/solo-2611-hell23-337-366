"""生产者：按自己的局部 type-code 构造真实 Arrow dense/sparse Union IPC。

type-code 可合法任意选择（int8 范围内 Arrow 允许的值），并随 revision 变化。
每个信封的 IPC metadata 内嵌完整 descriptor + 批头，保证可独立读回。
"""

from __future__ import annotations

from typing import Optional, Sequence

import pyarrow as pa

from .protocol import IPC_METADATA_KEY, Envelope
from .schema import (
    KIND_INT64,
    KIND_STRUCT,
    KIND_TEXT,
    LIMITS,
    MISSING_BRANCH_ID,
    BranchSpec,
    IncompatibleSchema,
    ProducerDescriptor,
)


class Producer:
    def __init__(
        self,
        producer_id: str,
        descriptor: ProducerDescriptor,
        *,
        mode: str = "dense",
    ):
        if producer_id != descriptor.producer_id:
            raise IncompatibleSchema("producer_id 与 descriptor 不一致")
        descriptor.validate()
        if mode not in ("dense", "sparse"):
            raise ValueError("mode 只能是 dense / sparse")
        self.producer_id = producer_id
        self.descriptor = descriptor
        self.mode = mode
        self._seq = 0

    # ---- Arrow 子数组构造 -------------------------------------------------

    def _child_array(self, branch: BranchSpec, values: list):
        if branch.kind == KIND_INT64:
            return pa.array(values, type=pa.int64())
        if branch.kind == KIND_TEXT:
            return pa.array(values, type=pa.string())
        if branch.kind == "null":
            return pa.nulls(len(values), type=pa.null())
        # struct：按 field_id 身份组织输入，缺失键 => 字段 NULL；
        # 整行 None => 顶层 struct NULL（分支仍存在，区别于 missing 缺席）
        struct_fields = [
            pa.field(
                f.field_id,
                pa.int64() if f.kind == KIND_INT64 else pa.string(),
                nullable=True,
            )
            for f in branch.fields
        ]
        stype = pa.struct(struct_fields)
        normalized: list[Optional[dict]] = []
        valid_ids = {f.field_id for f in branch.fields}
        for row in values:
            if row is None:
                normalized.append(None)
                continue
            if not isinstance(row, dict):
                raise IncompatibleSchema(
                    f"{self.producer_id}: struct 行必须是 dict(field_id=值) 或 None"
                )
            unknown = set(row) - valid_ids
            if unknown:
                raise IncompatibleSchema(
                    f"{self.producer_id}: struct 出现未知 field_id {unknown}"
                )
            # 缺键由 Arrow 按 struct 类型补 NULL
            normalized.append({fid: row.get(fid) for fid in valid_ids})
        try:
            return pa.array(normalized, type=stype)
        except (pa.ArrowTypeError, pa.ArrowInvalid) as exc:
            raise IncompatibleSchema(
                f"{self.producer_id}: struct 分支 {branch.branch_id} 值与声明类型不兼容"
            ) from exc

    # ---- 批构造 -----------------------------------------------------------

    def build_batch(
        self,
        rows: Sequence[dict],
        *,
        mode: Optional[str] = None,
        seq: Optional[int] = None,
    ) -> Envelope:
        """rows: [{'branch_id': ..., 'value': ...}, ...]；缺席行用 missing 分支。

        value 语义：
        * int64/text 分支：标量或 None（None = 分支存在但 child NULL）；
        * struct 分支：dict(field_id -> 值) 或 None（整 child NULL）；
        * missing 分支：value 必须为 None，表示逻辑缺席。

        seq 显式指定时使用该序号（跨 descriptor 重建 Producer 时保持每 producer 连续），
        否则使用内部自增计数。
        """
        mode = mode or self.mode
        if len(rows) > LIMITS["max_rows_per_batch"]:
            raise IncompatibleSchema(
                f"批行数 {len(rows)} 超过上限 {LIMITS['max_rows_per_batch']}"
            )
        desc = self.descriptor
        code_for = {bid: c for c, bid in desc.branch_codes.items()}
        # Arrow Union 的 field 顺序按 type-code 升序最稳妥，codes 与之对齐
        ordered = sorted(desc.branches, key=lambda b: code_for[b.branch_id])
        field_names = [b.branch_id for b in ordered]
        type_codes = [code_for[b.branch_id] for b in ordered]
        if any(not (0 <= c <= 127) for c in type_codes):
            raise IncompatibleSchema(
                "Arrow Union type-id 经 IPC 往返只支持 [0,127]（非负 int8）"
            )

        per_branch: dict[str, list] = {b.branch_id: [] for b in ordered}
        type_ids: list[int] = []
        dense_offsets: list[int] = []
        for row in rows:
            bid = row["branch_id"]
            branch = desc.branch(bid)
            if branch is None:
                raise IncompatibleSchema(
                    f"{self.producer_id}: 批引用了 descriptor 中不存在的分支 {bid}"
                )
            val = row.get("value")
            if bid == MISSING_BRANCH_ID and val is not None:
                raise IncompatibleSchema("missing 分支不能携带值")
            if bid != MISSING_BRANCH_ID and branch.kind == KIND_STRUCT:
                if val is not None and not isinstance(val, dict):
                    raise IncompatibleSchema(f"struct 分支 {bid} 的值必须是 dict 或 None")
            type_ids.append(code_for[bid])
            if mode == "dense":
                dense_offsets.append(len(per_branch[bid]))
            per_branch[bid].append(val)

        children = [
            self._child_array(b, per_branch[b.branch_id]) for b in ordered
        ]
        if mode == "dense":
            union = pa.UnionArray.from_dense(
                pa.array(type_ids, type=pa.int8()),
                pa.array(dense_offsets, type=pa.int32()),
                children,
                field_names=field_names,
                type_codes=type_codes,
            )
        else:
            # sparse：每个 child 长度必须等于父行数；非选中槽位按 Arrow 约定填充
            sparse_children = []
            for b, child in zip(ordered, children):
                n = len(rows)
                if b.kind == "null":
                    sparse_children.append(pa.nulls(n, type=pa.null()))
                    continue
                fill = [None] * n
                slot = 0
                for i, tid in enumerate(type_ids):
                    if tid == code_for[b.branch_id]:
                        fill[i] = per_branch[b.branch_id][slot]
                        slot += 1
                sparse_children.append(
                    pa.array(fill, type=child.type)
                )
            union = pa.UnionArray.from_sparse(
                pa.array(type_ids, type=pa.int8()),
                sparse_children,
                field_names=field_names,
                type_codes=type_codes,
            )

        table = pa.table({"value": union})
        if seq is not None:
            self._seq = seq
        else:
            self._seq += 1
        env = Envelope(
            producer_id=self.producer_id,
            schema_revision=desc.schema_revision,
            batch_seq=self._seq,
            branch_codes=dict(desc.branch_codes),
            descriptor=desc,
            ipc=b"",
        )
        schema = table.schema.with_metadata(
            {**(table.schema.metadata or {}), IPC_METADATA_KEY: env.metadata_bytes()}
        )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, schema) as writer:
            writer.write_table(pa.table({"value": union}, schema=schema))
        env.ipc = sink.getvalue().to_pybytes()
        return env
