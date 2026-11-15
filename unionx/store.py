"""可重新打开的 Arrow 分片存储 + manifest。

工程选择（见 experiments/ 和 README）：**只增不改的分片 + 读时按身份对齐**。
每个分片绑定写入时刻的 canonical schema 快照；新增 nullable 字段 / 新分支只开新片，
旧片永不重编码。查询时按 field_id / branch_id 身份把旧片对齐到最新视图，
旧行在新字段上天然为 NULL。
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyarrow as pa

from .schema import (
    KIND_STRUCT,
    MISSING_BRANCH_ID,
    CanonicalSchema,
    IncompatibleSchema,
)

MANIFEST_NAME = "manifest.json"
SHARD_DIR = "shards"
FORMAT_VERSION = 1


@dataclass
class _OpenShard:
    shard_id: int
    path: Path
    sink: pa.OSFile
    writer: pa.ipc.RecordBatchFileWriter
    canonical_revision: int
    fingerprint: str
    rows: int


class ShardStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.shards_dir = self.root / SHARD_DIR
        self.manifest_path = self.root / MANIFEST_NAME
        self.shards_dir.mkdir(parents=True, exist_ok=True)
        self._open: Optional[_OpenShard] = None
        self.manifest = {
            "format": FORMAT_VERSION,
            "shards": [],          # 每个分片绑定的 canonical 快照
            "canonical_history": [],
            "producers": {},       # producer_id -> {last_seq, last_revision, rows}
            "total_rows": 0,
            "closed": False,
        }

    # ---------- 写入 ----------

    def _write_manifest_atomic(self) -> None:
        tmp = self.manifest_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.manifest, indent=2, ensure_ascii=False))
        os.replace(tmp, self.manifest_path)

    def append(
        self,
        table: pa.Table,
        snapshot: CanonicalSchema,
        producer_id: str,
        batch_seq: int,
        schema_revision: int,
    ) -> int:
        """追加一个已提交批。canonical revision 变化 => 滚动新分片（旧片不可变）。"""
        if self._open is None or self._open.canonical_revision != snapshot.revision:
            self._rotate(snapshot)
        shard = self._open
        shard.writer.write_table(table)
        shard.rows += table.num_rows
        self.manifest["total_rows"] += table.num_rows

        pstate = self.manifest["producers"].setdefault(
            producer_id, {"last_seq": 0, "last_revision": 0, "rows": 0}
        )
        if batch_seq != pstate["last_seq"] + 1:
            raise IncompatibleSchema(
                f"落盘 seq 不连续: {producer_id} {pstate['last_seq']} -> {batch_seq}"
            )
        pstate["last_seq"] = batch_seq
        pstate["last_revision"] = schema_revision
        pstate["rows"] += table.num_rows
        self._write_manifest_atomic()
        return shard.shard_id

    def _rotate(self, snapshot: CanonicalSchema) -> None:
        if self._open is not None:
            self._open.writer.close()
            self._open.sink.close()
            self._open = None
        shard_id = len(self.manifest["shards"])
        path = self.shards_dir / f"shard-{shard_id:05d}.arrow"
        sink = pa.OSFile(str(path), "wb")
        # 分片 schema 即 canonical 快照，并把快照指纹写入 Arrow metadata
        arrow_meta = {
            b"x-unionx-canonical-revision": str(snapshot.revision).encode(),
            b"x-unionx-fingerprint": snapshot.fingerprint().encode(),
        }
        writer = pa.ipc.new_file(
            sink,
            pa.schema(
                [
                    pa.field("value", table_type_value(snapshot)),
                    pa.field("provenance", provenance_type()),
                ],
                metadata=arrow_meta,
            ),
        )
        self._open = _OpenShard(
            shard_id=shard_id,
            path=path,
            sink=sink,
            writer=writer,
            canonical_revision=snapshot.revision,
            fingerprint=snapshot.fingerprint(),
            rows=0,
        )
        self.manifest["shards"].append(
            {
                "shard_id": shard_id,
                "file": f"{SHARD_DIR}/shard-{shard_id:05d}.arrow",
                "canonical_revision": snapshot.revision,
                "fingerprint": snapshot.fingerprint(),
                "rows": 0,
            }
        )
        self.manifest["canonical_history"].append(snapshot.to_dict())
        self._write_manifest_atomic()

    def close(self) -> None:
        if self._open is not None:
            self._open.writer.close()
            self._open.sink.close()
            self._open = None
        # 用实际文件行数校正 manifest
        for s in self.manifest["shards"]:
            with pa.memory_map(str(self.root / s["file"]), "r") as f:
                s["rows"] = pa.ipc.open_file(f).read_all().num_rows
        self.manifest["closed"] = True
        self._write_manifest_atomic()

    # ---------- 重开与查询 ----------

    @classmethod
    def reopen(cls, root: str | Path) -> "ShardStore":
        root = Path(root)
        store = cls.__new__(cls)
        store.root = root
        store.shards_dir = root / SHARD_DIR
        store.manifest_path = root / MANIFEST_NAME
        store._open = None
        store.manifest = json.loads((root / MANIFEST_NAME).read_text())
        if store.manifest.get("format") != FORMAT_VERSION:
            raise IncompatibleSchema("manifest 格式版本不支持")
        if not store.manifest.get("closed"):
            raise IncompatibleSchema("该存储未正常 close，不能重开")
        return store

    def snapshots(self) -> list[CanonicalSchema]:
        return [
            CanonicalSchema.from_dict(d) for d in self.manifest["canonical_history"]
        ]

    def query(
        self,
        branch_id: Optional[str] = None,
        field_id: Optional[str] = None,
    ) -> list[dict]:
        """按稳定身份查询。

        * 不指定 branch：返回所有"分支存在"的行（missing 缺席行单独用 query_missing）；
        * 指定 branch：只返回该 branch 被选中的行；child NULL 照样返回（present=True,
          value=None），与逻辑缺席严格区分；
        * 再指定 field_id：从 struct 中按 field_id 投影，旧片没有该字段时 value=None。
        """
        out: list[dict] = []
        for shard_meta in self.manifest["shards"]:
            snap = CanonicalSchema.from_dict(
                self.manifest["canonical_history"][
                    self._history_index(shard_meta["canonical_revision"])
                ]
            )
            with pa.memory_map(str(self.root / shard_meta["file"]), "r") as f:
                table = pa.ipc.open_file(f).read_all()
            self._scan_shard(table, snap, shard_meta["shard_id"], branch_id, field_id, out)
        return out

    def query_missing(self) -> list[dict]:
        """逻辑缺席行：missing 分支被显式选中的行。"""
        return self._query_branch(MISSING_BRANCH_ID)

    def _query_branch(self, branch_id: str) -> list[dict]:
        return self.query(branch_id=branch_id)

    def _history_index(self, revision: int) -> int:
        for i, d in enumerate(self.manifest["canonical_history"]):
            if d["canonical_revision"] == revision:
                return i
        raise IncompatibleSchema(f"manifest 中找不到 canonical revision {revision}")

    def _scan_shard(self, table, snap, shard_id, branch_id, field_id, out) -> None:
        union = table.column(0)
        prov = table.column(1).to_pylist()
        base = 0  # chunk 之间的全局行偏移；provenance 是合并后的 ChunkedArray
        for chunk_index in range(union.num_chunks):
            chunk = union.chunk(chunk_index)
            type_codes = list(chunk.type.type_codes)
            ids = chunk.type_codes.to_pylist()
            dense = chunk.offsets.to_pylist()
            # 每个 child 只物化为 python list 一次，避免逐行重复转换（O(n^2) -> O(n)）
            child_pylist = {
                arrow_index: chunk.field(arrow_index).to_pylist()
                for arrow_index in range(chunk.type.num_fields)
            }
            for parent, code in enumerate(ids):
                arrow_index = type_codes.index(code)
                bid = chunk.type.field(arrow_index).name
                if branch_id is not None and bid != branch_id:
                    continue
                child_off = dense[parent]
                raw = child_pylist[arrow_index][child_off]
                present = True
                if bid == MISSING_BRANCH_ID:
                    value = None
                    present = False
                elif snap.get_branch(bid) and snap.get_branch(bid).kind == KIND_STRUCT:
                    if raw is None:
                        value = None  # 分支存在但整 child NULL
                    elif field_id is not None:
                        value = raw.get(field_id)  # 旧片无此字段 => None
                    else:
                        value = raw
                else:
                    value = raw
                p = prov[base + parent]
                out.append(
                    {
                        "shard": shard_id,
                        "row_index": base + parent,
                        "branch_id": bid,
                        "field_id": field_id,
                        "value": value,
                        "present": present,
                        "producer": p["producer"],
                        "seq": p["seq"],
                        "parent_row": p["parent_row"],
                        "original_child_offset": p["original_child_offset"],
                        "input_mode": p["input_mode"],
                    }
                )
            base += len(ids)

    def count(self) -> int:
        return self.manifest["total_rows"]


def provenance_type() -> pa.StructType:
    return pa.struct(
        [
            pa.field("producer", pa.string()),
            pa.field("seq", pa.int64()),
            pa.field("parent_row", pa.int64()),
            pa.field("original_child_offset", pa.int64()),
            pa.field("input_mode", pa.string()),
        ]
    )


def table_type_value(snapshot: CanonicalSchema) -> pa.UnionType:
    fields, codes = [], []
    for b in snapshot.branches:
        if b.kind == "int64":
            at = pa.int64()
        elif b.kind == "text":
            at = pa.string()
        elif b.kind == "null":
            at = pa.null()
        else:
            at = pa.struct(
                [
                    pa.field(
                        f.field_id,
                        pa.int64() if f.kind == "int64" else pa.string(),
                        nullable=True,
                    )
                    for f in b.fields
                ]
            )
        fields.append(pa.field(b.branch_id, at))
        codes.append(snapshot.canonical_code(b.branch_id))
    return pa.dense_union(fields, type_codes=codes)
