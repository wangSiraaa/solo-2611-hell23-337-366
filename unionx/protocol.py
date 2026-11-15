"""协议对象：IPC 信封、确认与背压。

信封 = 显式头（producer_id / schema_revision / batch_seq / branch_id 映射）
     + 真实 Arrow IPC 字节（同一映射也冗余写入 IPC schema metadata，
       保证文件单独被读回时仍然自描述）。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import pyarrow as pa

from .schema import ProducerDescriptor

IPC_METADATA_KEY = b"x-unionx-envelope"


class ProtocolError(Exception):
    """信封/传输层面的畸形消息（区别于 schema 不兼容）。"""


@dataclass(frozen=True)
class Ack:
    """提交/提案被接受。committed=False 表示被屏障缓存，尚未进入输出。"""

    producer_id: str
    schema_revision: int
    batch_seq: int
    committed: bool
    detail: str = ""


@dataclass(frozen=True)
class Backpressure:
    """显式背压：缓冲已满，调用方必须稍后重投；该消息不会被保留，也没有提交确认。"""

    producer_id: str
    batch_seq: int
    reason: str
    resident_per_producer: int
    resident_global: int


@dataclass
class Envelope:
    producer_id: str
    schema_revision: int
    batch_seq: int
    # 该批 IPC 内局部 type-code -> branch_id（与 descriptor.branch_codes 一致，随批携带）
    branch_codes: dict[int, str]
    descriptor: ProducerDescriptor
    ipc: bytes
    # 适配外部 IPC：struct 分支的 Arrow child 字段名 -> field_id。
    # 本系统 Producer 产出的批 Arrow 名即 field_id（映射为恒等）；
    # 旧 fixture 用显示名（如 qty），由适配层注入映射。
    struct_arrow_names: dict | None = None

    def header(self) -> dict:
        return {
            "producer_id": self.producer_id,
            "schema_revision": self.schema_revision,
            "batch_seq": self.batch_seq,
            "branch_codes": {str(k): v for k, v in self.branch_codes.items()},
            "struct_arrow_names": self.struct_arrow_names or {},
        }

    def metadata_bytes(self) -> bytes:
        payload = {
            "header": self.header(),
            "descriptor": self.descriptor.to_dict(),
        }
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def read_table(self) -> pa.Table:
        """真实 IPC 读回（内存中的 bytes 走 BufferReader，落盘文件走同样的 open_file）。"""
        reader = pa.ipc.open_file(pa.BufferReader(self.ipc))
        table = reader.read_all()
        return table

    @staticmethod
    def from_ipc_bytes(ipc: bytes) -> "Envelope":
        """从自描述 IPC 字节单独重建信封（落盘/重开场景）。"""
        table = pa.ipc.open_file(pa.BufferReader(ipc)).read_all()
        meta = table.schema.metadata or {}
        raw = meta.get(IPC_METADATA_KEY)
        if raw is None:
            raise ProtocolError("IPC 缺少 unionx 信封 metadata，无法自描述读回")
        payload = json.loads(raw.decode("utf-8"))
        desc = ProducerDescriptor.from_dict(payload["descriptor"])
        h = payload["header"]
        san = h.get("struct_arrow_names") or {}
        # JSON 键被字符串化，还原嵌套结构 branch_id -> {arrow_name: field_id}
        struct_arrow_names = {
            bid: {an: fid for an, fid in mapping.items()} for bid, mapping in san.items()
        }
        return Envelope(
            producer_id=h["producer_id"],
            schema_revision=int(h["schema_revision"]),
            batch_seq=int(h["batch_seq"]),
            branch_codes={int(k): v for k, v in h["branch_codes"].items()},
            descriptor=desc,
            ipc=ipc,
            struct_arrow_names=struct_arrow_names or None,
        )

    def arrow_name_to_field_id(self, branch_id: str, arrow_name: str) -> str:
        """struct child 的 Arrow 字段名 -> 稳定 field_id。"""
        if self.struct_arrow_names and branch_id in self.struct_arrow_names:
            mapping = self.struct_arrow_names[branch_id]
            return mapping.get(arrow_name, arrow_name)
        return arrow_name  # 本系统产出：Arrow 名即 field_id
