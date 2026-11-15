"""可修订共享/循环对象数据仓。

模块划分：
- cbor_codec: 子集 CBOR 的自研解析/编码，保留类型化键与 tag28/29 共享语义
- store:      SQLite 持久化、版本覆盖层、原子 patch、追踪式回收
- cli:        命令行入口
"""

from .cbor_codec import (
    CBORError,
    NodeArray,
    Ref,
    TypedKey,
    TypedMap,
    decode_cbor,
    encode_cbor,
)

__all__ = [
    "CBORError",
    "NodeArray",
    "Ref",
    "TypedKey",
    "TypedMap",
    "decode_cbor",
    "encode_cbor",
]


def __getattr__(name):
    # store 依赖较重，按需导入，避免仅使用编解码时拉起 sqlite
    if name in ("Store", "StoreError", "ValidationError", "ConflictError"):
        from . import store
        return getattr(store, name)
    raise AttributeError(name)
