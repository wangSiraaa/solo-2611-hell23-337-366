"""子集 CBOR 的自研解析与编码。

支持子集：int、text、bytes、bool、null、数组、映射；
映射键限 int/text/bytes/bool 且按 (类型, 值) 判等；
共享值使用标准 tag 28（可共享标记）/ tag 29（共享引用）。

为什么不用 cbor2 的入口：cbor2 6.x 的 C 扩展在把映射交给任何
Python object_hook 之前就用原生 dict 合并键，而 Python 中 1 == True，
typed-keys.cbor 里的 int(1) 与 bool(true) 两个键在入口处即丢失
（实测 cbor2.loads 只返回 {1: 'b'}）。这里自行做递归下降解析，
先保留类型再建表，避免“成熟库先丢信息、事后再补类型标注”。
"""

from dataclasses import dataclass

# 导入上限（题目边界）
MAX_NODES = 10_000
MAX_EDGES = 100_000
MAX_DEPTH = 100


class CBORError(ValueError):
    pass


class NodeArray:
    """带对象身份的数组容器。"""

    __slots__ = ("items",)

    def __init__(self, items=None):
        self.items = items if items is not None else []

    def __repr__(self):
        return f"NodeArray({self.items!r})"


class TypedMap:
    """类型化键映射：键按 (类型, 值) 判等，保留 Python 等价但类型不同的键。"""

    __slots__ = ("entries",)

    def __init__(self, entries=None):
        # entries: list[(TypedKey, value)]
        self.entries = entries if entries is not None else []

    def get(self, key_type, key_value):
        for k, v in self.entries:
            if k.type == key_type and k.value == key_value:
                return v
        raise KeyError((key_type, key_value))

    def __repr__(self):
        return f"TypedMap({self.entries!r})"


@dataclass(frozen=True)
class TypedKey:
    type: str  # 'int' | 'text' | 'bytes' | 'bool'
    value: object


@dataclass(frozen=True)
class Ref:
    """tag 29 共享引用占位，解析完成后替换为实际对象。"""

    index: int


@dataclass
class _Counters:
    nodes: int = 0
    edges: int = 0
    depth: int = 0


def _head(major: int, n: int) -> bytes:
    if n < 24:
        return bytes([(major << 5) | n])
    if n <= 0xFF:
        return bytes([(major << 5) | 24, n])
    if n <= 0xFFFF:
        return bytes([(major << 5) | 25]) + n.to_bytes(2, "big")
    if n <= 0xFFFFFFFF:
        return bytes([(major << 5) | 26]) + n.to_bytes(4, "big")
    return bytes([(major << 5) | 27]) + n.to_bytes(8, "big")


# ----------------------------- 解码 -----------------------------

class _Decoder:
    """显式栈迭代解析器：深度只受 MAX_DEPTH 控制，不吃 Python 调用栈。"""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0
        self.shared: list = []  # tag28 登记表，下标即共享索引
        self.ctr = _Counters()
        self._stack: list = []
        self._pending_key = None

    def _read(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise CBORError("CBOR 数据被截断")
        chunk = self.data[self.pos:self.pos + n]
        self.pos += n
        return chunk

    def _arg(self, ai: int) -> int:
        if ai < 24:
            return ai
        if ai == 24:
            return self._read(1)[0]
        if ai == 25:
            return int.from_bytes(self._read(2), "big")
        if ai == 26:
            return int.from_bytes(self._read(4), "big")
        if ai == 27:
            return int.from_bytes(self._read(8), "big")
        raise CBORError(f"不支持的附加信息位: {ai}")

    def decode(self):
        root = self._parse_root()
        if self.pos != len(self.data):
            raise CBORError("CBOR 根值之后存在多余字节")
        for i, obj in enumerate(self.shared):
            if obj is None:
                raise CBORError(f"共享索引 {i} 被引用但从未定义（tag28 缺失）")
        return self._resolve_refs(root)

    # ---- 词法：读一个 CBOR 项的头部，返回描述符 ----
    def _read_head(self):
        initial = self._read(1)[0]
        major, ai = initial >> 5, initial & 0x1F
        if ai == 31:
            raise CBORError("不支持不定长容器（子集仅允许定长）")
        return major, ai

    def _read_scalar(self, major, ai):
        if major == 0:
            return self._arg(ai)
        if major == 1:
            return -1 - self._arg(ai)
        if major == 2:
            return bytes(self._read(self._arg(ai)))
        if major == 3:
            n = self._arg(ai)
            try:
                return self._read(n).decode("utf-8")
            except UnicodeDecodeError as e:
                raise CBORError(f"非法 UTF-8 文本: {e}") from None
        if major == 7:
            if ai == 20:
                return False
            if ai == 21:
                return True
            if ai == 22:
                return None
            raise CBORError(f"不支持的 simple/float 值，附加位 {ai}")
        raise CBORError(f"非法 CBOR 主类型 {major}")

    def _read_key(self) -> TypedKey:
        major, ai = self._read_head()
        if major == 0:
            return TypedKey("int", self._arg(ai))
        if major == 1:
            return TypedKey("int", -1 - self._arg(ai))
        if major == 2:
            n = self._arg(ai)
            return TypedKey("bytes", bytes(self._read(n)))
        if major == 3:
            n = self._arg(ai)
            try:
                return TypedKey("text", self._read(n).decode("utf-8"))
            except UnicodeDecodeError as e:
                raise CBORError(f"非法 UTF-8 键: {e}") from None
        if major == 7 and ai in (20, 21):
            return TypedKey("bool", ai == 21)
        raise CBORError("映射键只允许 int/text/bytes/bool")

    def _parse_root(self):
        """迭代下降解析。

        栈顶是当前正在填充的容器帧：
          ('arr', arr, remaining)
          ('map', mp, remaining, expect_key)
        每个容器先在调用处创建并压栈，随后逐项读取；读到子容器时同样
        先建对象压栈，内层帧结束后该对象已被原地填充，父帧直接持有它。
        tag28 在登记共享表后继续解析其包裹值；tag29 产出 Ref 占位。
        """
        root = self._read_value()
        # 根是标量/Ref：没有容器帧
        if not self._stack:
            return root

        while self._stack:
            frame = self._stack[-1]
            if frame[0] == "arr":
                _, arr, remaining = frame
                if remaining == 0:
                    self.ctr.depth -= 1
                    self._stack.pop()
                    continue
                self._count_edge()
                arr.items.append(self._read_value())
                frame[2] -= 1
            else:
                _, mp, remaining, expect_key = frame
                if remaining == 0:
                    self.ctr.depth -= 1
                    self._stack.pop()
                    continue
                if expect_key:
                    frame[3] = False
                    self._pending_key = self._read_key()
                    continue
                self._count_edge()
                mp.entries.append((self._pending_key, self._read_value()))
                self._pending_key = None
                frame[2] -= 1
                frame[3] = True
        return root

    def _count_edge(self):
        if self.ctr.edges >= MAX_EDGES:
            raise CBORError(f"边数超过上限 {MAX_EDGES}")
        self.ctr.edges += 1

    def _enter_container(self):
        if self.ctr.nodes >= MAX_NODES:
            raise CBORError(f"容器节点数超过上限 {MAX_NODES}")
        if self.ctr.depth >= MAX_DEPTH:
            raise CBORError(f"嵌套深度超过上限 {MAX_DEPTH}")
        self.ctr.nodes += 1
        self.ctr.depth += 1

    def _read_value(self):
        major, ai = self._read_head()
        if major == 6:
            tag = self._arg(ai)
            if tag == 29:
                return Ref(self._arg(ai))
            if tag != 28:
                raise CBORError(f"不支持的 CBOR tag: {tag}（子集仅允许 28/29）")
            index = len(self.shared)
            self.shared.append(None)  # 先占位，环内前向引用可解析
            value = self._read_value()
            self.shared[index] = value
            return value
        if major == 4:  # 数组
            self._enter_container()
            length = self._arg(ai)
            arr = NodeArray()
            self._stack.append(["arr", arr, length])
            return arr
        if major == 5:  # 映射
            self._enter_container()
            length = self._arg(ai)
            mp = TypedMap()
            self._stack.append(["map", mp, length, True])
            return mp
        return self._read_scalar(major, ai)

    def _deref(self, ref: "Ref"):
        if not (0 <= ref.index < len(self.shared)) or \
                self.shared[ref.index] is None:
            raise CBORError(
                f"tag29 共享引用 {ref.index} 无对应的 tag28 定义")
        return self.shared[ref.index]

    def _resolve_refs(self, root):
        """非递归把 Ref 占位替换为共享表中的真实对象，支持环。"""
        if isinstance(root, Ref):
            return self._deref(root)
        stack = [root]
        seen = set()
        while stack:
            cur = stack.pop()
            if isinstance(cur, NodeArray):
                if id(cur) in seen:
                    continue
                seen.add(id(cur))
                for i, item in enumerate(cur.items):
                    if isinstance(item, Ref):
                        cur.items[i] = self._deref(item)
                    elif isinstance(item, (NodeArray, TypedMap)):
                        stack.append(item)
            elif isinstance(cur, TypedMap):
                if id(cur) in seen:
                    continue
                seen.add(id(cur))
                for i, (key, value) in enumerate(cur.entries):
                    if isinstance(value, Ref):
                        cur.entries[i] = (key, self._deref(value))
                    elif isinstance(value, (NodeArray, TypedMap)):
                        stack.append(value)
        return root


def decode_cbor(data: bytes):
    """解析 CBOR 字节，返回 (根对象, 容器节点数, 边数)。"""
    dec = _Decoder(data)
    root = dec.decode()
    return root, dec.ctr.nodes, dec.ctr.edges


# ----------------------------- 编码 -----------------------------

def _encode_scalar(buf: bytearray, value):
    if value is None:
        buf.append(0xF6)
    elif value is True:
        buf.append(0xF5)
    elif value is False:
        buf.append(0xF4)
    elif isinstance(value, bool):  # 兜底
        buf.append(0xF5 if value else 0xF4)
    elif isinstance(value, int):
        if value >= 0:
            buf += _head(0, value)
        else:
            buf += _head(1, -1 - value)
    elif isinstance(value, str):
        raw = value.encode("utf-8")
        buf += _head(3, len(raw)) + raw
    elif isinstance(value, bytes):
        buf += _head(2, len(value)) + value
    else:
        raise CBORError(f"不可编码的标量类型: {type(value).__name__}")


def _encode_key(buf: bytearray, key: TypedKey):
    if key.type == "int":
        v = key.value
        if v >= 0:
            buf += _head(0, v)
        else:
            buf += _head(1, -1 - v)
    elif key.type == "text":
        raw = key.value.encode("utf-8")
        buf += _head(3, len(raw)) + raw
    elif key.type == "bytes":
        buf += _head(2, len(key.value)) + key.value
    elif key.type == "bool":
        buf.append(0xF5 if key.value else 0xF4)
    else:
        raise CBORError(f"非法映射键类型: {key.type}")


def _plan_shareable(root) -> set:
    """标记需要 tag28 的容器：在边结构中出现多次（共享别名或环回指）。

    环上的容器必然被自身后代再次进入，因此计数 >= 2；不共享也不在环上的
    容器计数为 1，不打 tag，保持与常规 CBOR 一致的紧凑编码。
    """
    counts: dict[int, int] = {}
    stack = [root]
    seen = set()
    while stack:
        cur = stack.pop()
        if isinstance(cur, NodeArray):
            if id(cur) in seen:
                counts[id(cur)] += 1
                continue
            seen.add(id(cur))
            counts[id(cur)] = 1
            stack.extend(it for it in cur.items
                         if isinstance(it, (NodeArray, TypedMap)))
        elif isinstance(cur, TypedMap):
            if id(cur) in seen:
                counts[id(cur)] += 1
                continue
            seen.add(id(cur))
            counts[id(cur)] = 1
            stack.extend(v for _, v in cur.entries
                         if isinstance(v, (NodeArray, TypedMap)))
    return {ident for ident, c in counts.items() if c > 1}


class _Encoder:
    """按对象身份分配共享索引：仅规划为共享/环上的容器首次出现打 tag28，
    之后发 tag29；其余容器按普通数组/映射编码。全程显式栈、无递归。"""

    def __init__(self):
        self.buf = bytearray()
        self.index: dict[int, int] = {}
        self.shareable: set = set()

    def encode(self, root) -> bytes:
        self.shareable = _plan_shareable(root)
        # 工作项：('v', value) 发任意值；('h', key) 发映射键
        stack = [("v", root)]
        while stack:
            kind, payload = stack.pop()
            if kind == "h":
                _encode_key(self.buf, payload)
                continue
            value = payload
            if isinstance(value, NodeArray):
                self._emit_array(value, stack)
            elif isinstance(value, TypedMap):
                self._emit_map(value, stack)
            else:
                _encode_scalar(self.buf, value)
        return bytes(self.buf)

    def _emit_array(self, arr: NodeArray, stack):
        ident = id(arr)
        if ident in self.index:
            self.buf += _head(6, 29) + _head(0, self.index[ident])
            return
        if ident in self.shareable:
            self.index[ident] = len(self.index)
            self.buf += _head(6, 28)
        self.buf += _head(4, len(arr.items))
        # 逆序压栈，保证编码顺序与 items 一致
        for item in reversed(arr.items):
            stack.append(("v", item))

    def _emit_map(self, mp: TypedMap, stack):
        ident = id(mp)
        if ident in self.index:
            self.buf += _head(6, 29) + _head(0, self.index[ident])
            return
        if ident in self.shareable:
            self.index[ident] = len(self.index)
            self.buf += _head(6, 28)
        self.buf += _head(5, len(mp.entries))
        for key, value in reversed(mp.entries):
            stack.append(("v", value))
            stack.append(("h", key))


def encode_cbor(root) -> bytes:
    """把 NodeArray/TypedMap/标量对象图编码为带 tag28/29 的真实 CBOR。"""
    return _Encoder().encode(root)
