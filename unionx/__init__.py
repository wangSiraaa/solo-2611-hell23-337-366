"""unionx —— 演进 Union 列的协商式数据交换。

模块划分：
  schema    稳定身份（branch_id / field_id）、descriptor、兼容性裁决、canonical schema
  protocol  IPC 信封、结果对象、异常
  producer  两个生产者之一，构造真实 Arrow IPC dense/sparse Union
  encode    局部 type-code 解码 + canonical 重编码（含全部校验，失败原子）
  store     可重开的 Arrow 分片 + manifest，按稳定身份查询
  exchange  提案/接受/提交分阶段协调器、屏障缓存、背压、内存记账
"""

from .schema import (
    MISSING_BRANCH_ID,
    BranchSpec,
    FieldSpec,
    ProducerDescriptor,
    CanonicalSchema,
    IncompatibleSchema,
    LIMITS,
)
from .protocol import Ack, Backpressure, Envelope
from .producer import Producer
from .exchange import Exchange
from .store import ShardStore

__all__ = [
    "MISSING_BRANCH_ID",
    "BranchSpec",
    "FieldSpec",
    "ProducerDescriptor",
    "CanonicalSchema",
    "IncompatibleSchema",
    "LIMITS",
    "Ack",
    "Backpressure",
    "Envelope",
    "Producer",
    "Exchange",
    "ShardStore",
]
