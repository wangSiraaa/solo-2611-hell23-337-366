"""SQLite 持久化的可修订共享对象存储。

设计要点
========
1. 版本覆盖层（overlay），不是整树复制：
   - 每个容器节点有稳定逻辑 id（lid），其内容按修订（rev）不可变追加；
   - 版本只记录“根 + 父版本”，读取某 lid 在某版本下的内容时沿
     node_content 查该 lid 最大且 rev <= 版本的修订；
   - 修改共享节点 = 为该 lid 追加一条新修订，新版本的所有别名都看到
     新内容，旧版本仍解析到旧修订（修复基线 Store 改写共享字典的问题）。

2. 句柄（handle）= "L<lid>:R<rev>"，指向不可变内容修订而非物理行。
   物理位置（SQLite rowid/页）被 GC 删除并复用后，旧 (lid,rev) 在表里
   查不到，解析显式报错，绝不会落到新对象上。

3. 内容与引用分离存储：
   - node_content 是“对象内容记录”，每条只存 kind 与序号，不存整节点
     载荷（题目计数对象）；
   - edge 表是引用/边记录，标量值也在 edge 内联；
   - versions 是版本索引。整节点内容不会藏进任何索引 blob。

4. 回收采用追踪式 mark-sweep（显式栈，按 lid 身份遍历，天然处理环），
   未采用引用计数：循环子图的引用计数永不为 0，无法回收。

5. 单写者；所有写操作在单个 SQLite 事务内提交，崩溃后事务回滚或
   GC 状态可续跑，版本结果不变。
"""

import json
import os
import sqlite3
from collections.abc import Mapping, Sequence

from .cbor_codec import (
    NodeArray,
    TypedKey,
    TypedMap,
    decode_cbor,
    encode_cbor,
)

MAX_RETAINED_VERSIONS = 5
MAX_OPS_PER_BATCH = 20
MAX_CONTENT_ROWS_PER_VERSION = 100

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS versions(
    vid          INTEGER PRIMARY KEY,
    parent_vid   INTEGER,
    root_kind    TEXT NOT NULL,              -- 'node' | 'scalar'
    root_lid     INTEGER,                    -- root_kind='node' 时有效
    root_scalar  BLOB,                       -- root_kind='scalar' 时内联
    created_from TEXT,                       -- 来源：import / patch
    retired      INTEGER NOT NULL DEFAULT 0  -- 超出保留窗口，GC 不再视为根
);
CREATE TABLE IF NOT EXISTS node_content(
    vid        INTEGER NOT NULL,             -- 该修订由哪个版本写入
    lid        INTEGER NOT NULL,
    rev        INTEGER NOT NULL,
    kind       TEXT NOT NULL,                -- 'array' | 'map'
    PRIMARY KEY(lid, rev)
);
CREATE TABLE IF NOT EXISTS edge(
    lid        INTEGER NOT NULL,
    rev        INTEGER NOT NULL,
    position   INTEGER NOT NULL,             -- 数组下标 / 映射项序号
    key_type   TEXT,                         -- 映射键类型；数组为 NULL
    key_value  BLOB,                         -- 映射键的 CBOR 编码值
    val_kind   TEXT NOT NULL,                -- 'node' | 'scalar'
    ref_lid    INTEGER,                      -- val_kind='node' 时
    scalar     BLOB                         -- val_kind='scalar' 时内联 CBOR
);
CREATE INDEX IF NOT EXISTS idx_edge_content ON edge(lid, rev);
CREATE INDEX IF NOT EXISTS idx_content_vid ON node_content(vid);
CREATE TABLE IF NOT EXISTS gc_state(
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    phase      TEXT NOT NULL,                -- 'mark' | 'sweep'
    live       TEXT NOT NULL                 -- JSON 编码的 (lid,rev) 列表
);
"""


class StoreError(Exception):
    pass


class ValidationError(StoreError):
    pass


class ConflictError(StoreError):
    pass


def make_handle(lid: int, rev: int) -> str:
    return f"L{lid}:R{rev}"


def parse_handle(handle: str):
    try:
        lpart, rpart = handle.split(":")
        assert lpart.startswith("L") and rpart.startswith("R")
        return int(lpart[1:]), int(rpart[1:])
    except (ValueError, AssertionError) as e:
        raise ValidationError(f"非法节点句柄: {handle!r}") from None


def _scalar_type_tag(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, str):
        return "text"
    if isinstance(value, bytes):
        return "bytes"
    raise ValidationError(f"不支持的标量类型: {type(value).__name__}")


def _encode_scalar_blob(value) -> bytes:
    # 复用 CBOR 标量编码做自描述内联存储
    return encode_cbor(value)


def _decode_scalar_blob(blob: bytes):
    value, _, _ = decode_cbor(blob)
    return value


def _encode_key_blob(key: TypedKey) -> bytes:
    # 键值用 CBOR 标量编码内联，键类型另存 key_type，二者共同判等
    return encode_cbor(key.value)


class Store:
    """持久化数据仓。一个目录对应一个单写者数据库文件。"""

    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)) or ".",
                        exist_ok=True)
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        # 崩溃注入钩子：'pre_commit'/'post_commit'（patch），
        # 'post_mark'/'pre_sweep'（gc）。测试置为可调用对象。
        self.crash_hook = None
        self._next_lid = self._load_next_lid()
        # 版本 -> 祖先（含自身）vid 集合的缓存；任何写操作后清空
        self._ancestor_cache: dict = {}

    def close(self):
        self.conn.close()

    # ------------------------- 基础工具 -------------------------

    def _load_next_lid(self) -> int:
        row = self.conn.execute("SELECT value FROM meta WHERE key='next_lid'").fetchone()
        return int(row["value"]) if row else 1

    def _alloc_lid(self) -> int:
        lid = self._next_lid
        self._next_lid += 1
        return lid

    def _new_vid(self) -> int:
        row = self.conn.execute("SELECT COALESCE(MAX(vid),0)+1 AS v FROM versions").fetchone()
        return row["v"]

    def list_versions(self, include_retired=False):
        sql = "SELECT vid, parent_vid, root_kind, retired FROM versions"
        if not include_retired:
            sql += " WHERE retired=0"
        sql += " ORDER BY vid"
        return [dict(r) for r in self.conn.execute(sql)]

    def _active_roots(self):
        """保留根：所有未退役版本的根。"""
        rows = self.conn.execute(
            "SELECT vid, root_kind, root_lid FROM versions WHERE retired=0"
        ).fetchall()
        return [(r["vid"], r["root_kind"], r["root_lid"]) for r in rows]

    def _ancestors_of(self, vid: int) -> set:
        """版本 vid 的祖先（含自身）vid 集合，带缓存。"""
        cached = self._ancestor_cache.get(vid)
        if cached is not None:
            return cached
        result = set()
        cur = vid
        while cur is not None and cur not in result:
            result.add(cur)
            row = self.conn.execute(
                "SELECT parent_vid FROM versions WHERE vid=?", (cur,)
            ).fetchone()
            if row is None:
                break
            cur = row["parent_vid"]
        self._ancestor_cache[vid] = result
        return result

    def _is_ancestor_or_self(self, ancestor_vid: int, vid: int) -> bool:
        """ancestor_vid 是否在 vid 的父链上（含自身）。

        覆盖层可见性必须按版本谱系判断，不能用 vid 大小：reopen 产生的
        兄弟分支 vid 更大，但其提交的修订对旧版本不可见。
        """
        return ancestor_vid in self._ancestors_of(vid)

    def _resolve_rev(self, lid: int, vid: int):
        """lid 在版本 vid 谱系下可见的最大修订。

        一条修订可见当且仅当写入它的版本是 vid 的祖先（或 vid 自身）。
        不可见则抛错（旧句柄失效 / 兄弟分支隔离语义）。
        """
        ancestors = self._ancestors_of(vid)
        rows = self.conn.execute(
            "SELECT rev, vid FROM node_content WHERE lid=? ORDER BY rev DESC",
            (lid,)).fetchall()
        for r in rows:
            if r["vid"] in ancestors:
                return r["rev"]
        raise ConflictError(
            f"节点 L{lid} 在版本 {vid} 不存在或已被回收，句柄无法解析")

    def _content_kind(self, lid: int, rev: int) -> str:
        row = self.conn.execute(
            "SELECT kind FROM node_content WHERE lid=? AND rev=?", (lid, rev)
        ).fetchone()
        if row is None:
            raise ConflictError(f"内容修订 L{lid}:R{rev} 不存在（可能已被回收）")
        return row["kind"]

    def _edges(self, lid: int, rev: int):
        return self.conn.execute(
            "SELECT * FROM edge WHERE lid=? AND rev=? ORDER BY position",
            (lid, rev),
        ).fetchall()

    # ------------------------- 导入 -------------------------

    def _rollback_safely(self):
        # 崩溃可能发生在 COMMIT 之后（事务已结束），ROLLBACK 需容错
        try:
            self.conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass

    def _invalidate_gc_state_locked(self):
        """任何新写入都使未完成的 GC 标记与谱系缓存失效。"""
        self.conn.execute("DELETE FROM gc_state WHERE id=1")
        self._ancestor_cache.clear()

    def import_cbor(self, data: bytes, label: str = "import") -> int:
        """导入真实 CBOR，保留容器身份、共享别名与环。返回新版本号。"""
        root, node_count, edge_count = decode_cbor(data)
        vid = self._new_vid()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            self._invalidate_gc_state_locked()
            lid_by_id: dict[int, int] = {}
            # 非递归遍历：先登记所有容器的 lid 与首修订，再写边，
            # 这样环内前向引用也有确定的 lid 可指向。
            stack = [root]
            seen = set()
            while stack:
                cur = stack.pop()
                if not isinstance(cur, (NodeArray, TypedMap)) or id(cur) in seen:
                    continue
                seen.add(id(cur))
                lid_by_id[id(cur)] = self._alloc_lid()
                children = (cur.items if isinstance(cur, NodeArray)
                            else [v for _, v in cur.entries])
                stack.extend(children)

            # 建 id->对象 的反查（遍历顺序不影响身份）
            obj_by_id = {}
            stack = [root]
            seen.clear()
            while stack:
                cur = stack.pop()
                if not isinstance(cur, (NodeArray, TypedMap)) or id(cur) in seen:
                    continue
                seen.add(id(cur))
                obj_by_id[id(cur)] = cur
                children = (cur.items if isinstance(cur, NodeArray)
                            else [v for _, v in cur.entries])
                stack.extend(children)

            for obj_id, lid in lid_by_id.items():
                obj = obj_by_id[obj_id]
                kind = "array" if isinstance(obj, NodeArray) else "map"
                self.conn.execute(
                    "INSERT INTO node_content(vid,lid,rev,kind) VALUES(?,?,1,?)",
                    (vid, lid, kind))
                self._write_edges(vid, lid, 1, obj, lid_by_id)

            if isinstance(root, (NodeArray, TypedMap)):
                self.conn.execute(
                    "INSERT INTO versions(vid,parent_vid,root_kind,root_lid,"
                    "root_scalar,created_from) VALUES(?,NULL,'node',?,NULL,?)",
                    (vid, lid_by_id[id(root)], label))
            else:
                self.conn.execute(
                    "INSERT INTO versions(vid,parent_vid,root_kind,root_lid,"
                    "root_scalar,created_from) VALUES(?,NULL,'scalar',NULL,?,?)",
                    (vid, _encode_scalar_blob(root), label))
            self._write_meta_locked()
            self._retain_window_locked()
            self._hook("pre_commit", "import")
            self.conn.execute("COMMIT")
            self._hook("post_commit", "import")
        except BaseException:
            self._rollback_safely()
            raise
        return vid

    def _write_edges(self, vid, lid, rev, obj, lid_by_id):
        if isinstance(obj, NodeArray):
            for pos, item in enumerate(obj.items):
                self._insert_edge(vid, lid, rev, pos, None, None, item, lid_by_id)
        else:
            for pos, (key, value) in enumerate(obj.entries):
                self._insert_edge(vid, lid, rev, pos, key, None, value, lid_by_id)

    def _insert_edge(self, vid, lid, rev, pos, key, _unused, value, lid_by_id):
        if isinstance(value, (NodeArray, TypedMap)):
            ref_lid = lid_by_id.get(id(value))
            if ref_lid is None:
                raise ValidationError("内部错误：子容器未分配 lid")
            self.conn.execute(
                "INSERT INTO edge(lid,rev,position,key_type,key_value,val_kind,"
                "ref_lid,scalar) VALUES(?,?,?,?,?, 'node',?,NULL)",
                (lid, rev, pos,
                 key.type if key is not None else None,
                 _encode_key_blob(key) if key is not None else None,
                 ref_lid))
        else:
            _scalar_type_tag(value)  # 类型校验
            self.conn.execute(
                "INSERT INTO edge(lid,rev,position,key_type,key_value,val_kind,"
                "ref_lid,scalar) VALUES(?,?,?,?,?, 'scalar',NULL,?)",
                (lid, rev, pos,
                 key.type if key is not None else None,
                 _encode_key_blob(key) if key is not None else None,
                 _encode_scalar_blob(value)))

    def _write_meta_locked(self):
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES('next_lid',?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(self._next_lid),))

    # ------------------------- 读取 / 物化 -------------------------

    def root_handle(self, vid: int) -> str:
        row = self.conn.execute(
            "SELECT root_kind, root_lid FROM versions WHERE vid=?", (vid,)
        ).fetchone()
        if row is None:
            raise ConflictError(f"版本 {vid} 不存在")
        if row["root_kind"] != "node":
            raise ConflictError(f"版本 {vid} 的根是标量，没有节点句柄")
        rev = self._resolve_rev(row["root_lid"], vid)
        return make_handle(row["root_lid"], rev)

    def materialize_index(self, vid: int) -> dict:
        """返回 {lid: 物化容器对象}，便于按稳定身份定位节点（非递归）。"""
        vrow = self.conn.execute(
            "SELECT root_kind, root_lid, root_scalar FROM versions WHERE vid=?",
            (vid,)).fetchone()
        if vrow is None:
            raise ConflictError(f"版本 {vid} 不存在")
        if vrow["root_kind"] == "scalar":
            return {}
        root_rev = self._resolve_rev(vrow["root_lid"], vid)
        built: dict[int, object] = {}
        stack = [(vrow["root_lid"], root_rev)]
        while stack:
            lid, rev = stack.pop()
            if lid in built:
                continue
            kind = self._content_kind(lid, rev)
            built[lid] = NodeArray() if kind == "array" else TypedMap()
            for e in self._edges(lid, rev):
                if e["val_kind"] == "node":
                    stack.append((e["ref_lid"],
                                  self._resolve_rev(e["ref_lid"], vid)))
        for lid, obj in list(built.items()):
            rev = self._resolve_rev(lid, vid)
            edges = self._edges(lid, rev)
            if isinstance(obj, NodeArray):
                obj.items = [self._edge_value(e, vid, built) for e in edges]
            else:
                obj.entries = [
                    (TypedKey(e["key_type"],
                              _decode_scalar_blob(e["key_value"])),
                     self._edge_value(e, vid, built))
                    for e in edges
                ]
        return built

    def materialize(self, vid: int):
        """把版本 vid 物化为带身份的 NodeArray/TypedMap/标量对象图。

        非递归；同一 lid 只构造一次对象，因此共享别名与环都被还原。
        """
        vrow = self.conn.execute(
            "SELECT root_kind, root_lid, root_scalar FROM versions WHERE vid=?",
            (vid,)).fetchone()
        if vrow is None:
            raise ConflictError(f"版本 {vid} 不存在")
        if vrow["root_kind"] == "scalar":
            return _decode_scalar_blob(vrow["root_scalar"])
        built = self.materialize_index(vid)
        return built[vrow["root_lid"]]

    def _edge_value(self, edge, vid, built):
        if edge["val_kind"] == "node":
            child = built.get(edge["ref_lid"])
            if child is None:
                # 理论上第一轮已占位；防御性兜底
                child_rev = self._resolve_rev(edge["ref_lid"], vid)
                kind = self._content_kind(edge["ref_lid"], child_rev)
                child = NodeArray() if kind == "array" else TypedMap()
                built[edge["ref_lid"]] = child
            return child
        return _decode_scalar_blob(edge["scalar"])

    def export_cbor(self, vid: int) -> bytes:
        return encode_cbor(self.materialize(vid))

    # ------------------------- patch -------------------------

    def patch(self, base_vid: int, operations: Sequence[Mapping]) -> int:
        """在 base_vid 上原子应用一批操作，返回新版本号。

        操作形式（op 字段）：
        - array_set:  {handle, index, value}
        - array_insert: {handle, index, value}
        - array_delete: {handle, index}
        - map_set:    {handle, key:{type,value}, value}
        - map_delete: {handle, key:{type,value}}
        - set_root:   {value}（标量或节点句柄字符串）
        value 为 {'scalar': ...} 或 {'ref': 'L..:R..'} 或嵌套内联容器。
        """
        if not operations:
            raise ValidationError("空 patch 不产生新版本")
        if len(operations) > MAX_OPS_PER_BATCH:
            raise ValidationError(
                f"单批最多 {MAX_OPS_PER_BATCH} 个节点修改，收到 {len(operations)}")
        base = self.conn.execute(
            "SELECT vid, root_kind, root_lid FROM versions WHERE vid=?",
            (base_vid,)).fetchone()
        if base is None:
            raise ConflictError(f"基版本 {base_vid} 不存在")

        new_vid = self._new_vid()
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            self._invalidate_gc_state_locked()
            # 受影响 lid -> 新修订号 的映射（同一批内共享节点只复制一次内容）
            new_rev: dict[int, int] = {}
            touched_kinds: dict[int, str] = {}
            content_rows = 0

            def ensure_mutable(lid: int) -> int:
                if lid in new_rev:
                    return new_rev[lid]
                old_rev = self._resolve_rev(lid, base_vid)
                kind = self._content_kind(lid, old_rev)
                # 修订号按 lid 全局单调，取全表最大 rev+1 而不是
                # old_rev+1：reopen 出的兄弟分支都从同一旧修订分叉，
                # 用 old_rev+1 会撞 (lid,rev) 主键。
                max_row = self.conn.execute(
                    "SELECT COALESCE(MAX(rev),0) AS m FROM node_content "
                    "WHERE lid=?", (lid,)).fetchone()
                rev = max_row["m"] + 1
                new_rev[lid] = rev
                touched_kinds[lid] = kind
                return rev

            # 先校验全部操作并构建变更后的边集合（在内存中），再落库：
            # 任何一步校验失败整批回滚。
            working: dict[int, list] = {}

            def load_working(lid: int):
                if lid in working:
                    return working[lid]
                rev = self._resolve_rev(lid, base_vid)
                kind = self._content_kind(lid, rev)
                rows = [dict(e) for e in self._edges(lid, rev)]
                working[lid] = rows
                return rows

            root_override = None  # (kind, payload)
            for op in operations:
                name = op.get("op")
                if name == "set_root":
                    root_override = self._validate_value(op.get("value"), base_vid)
                    continue
                handle = op.get("handle")
                lid, _hrev = parse_handle(handle)
                # 句柄修订必须在基版本可见，且与基版本当前修订一致
                visible_rev = self._resolve_rev(lid, base_vid)
                _h_lid, h_rev = parse_handle(handle)
                if h_rev != visible_rev:
                    raise ConflictError(
                        f"句柄 {handle} 不是版本 {base_vid} 的当前修订"
                        f"（当前 R{visible_rev}），请基于新版本重新获取句柄")
                kind = self._content_kind(lid, visible_rev)
                rows = load_working(lid)
                ensure_mutable(lid)
                if name in ("array_set", "array_insert", "array_delete"):
                    if kind != "array":
                        raise ValidationError(f"{name} 只能作用于数组节点，L{lid} 是 {kind}")
                    self._apply_array_op(name, op, rows, base_vid)
                elif name in ("map_set", "map_delete"):
                    if kind != "map":
                        raise ValidationError(f"{name} 只能作用于映射节点，L{lid} 是 {kind}")
                    self._apply_map_op(name, op, rows, base_vid)
                else:
                    raise ValidationError(f"未知 patch 操作: {name!r}")

            # 落库所有受影响节点的新修订（内容 + 边）
            for lid, rev in new_rev.items():
                self.conn.execute(
                    "INSERT INTO node_content(vid,lid,rev,kind) VALUES(?,?,?,?)",
                    (new_vid, lid, rev, touched_kinds[lid]))
                content_rows += 1
                for e in working[lid]:
                    self.conn.execute(
                        "INSERT INTO edge(lid,rev,position,key_type,key_value,"
                        "val_kind,ref_lid,scalar) VALUES(?,?,?,?,?,?,?,?)",
                        (lid, rev, e["position"], e["key_type"], e["key_value"],
                         e["val_kind"], e["ref_lid"], e["scalar"]))

            if content_rows > MAX_CONTENT_ROWS_PER_VERSION:
                raise ValidationError(
                    f"每版最多 {MAX_CONTENT_ROWS_PER_VERSION} 条内容记录，"
                    f"本批需要 {content_rows} 条")

            # 根引用
            if root_override is not None:
                rkind, payload = root_override
                if rkind == "node":
                    self.conn.execute(
                        "INSERT INTO versions(vid,parent_vid,root_kind,root_lid,"
                        "root_scalar,created_from) VALUES(?,?,'node',?,NULL,'patch')",
                        (new_vid, base_vid, payload))
                else:
                    self.conn.execute(
                        "INSERT INTO versions(vid,parent_vid,root_kind,root_lid,"
                        "root_scalar,created_from) VALUES(?,?,'scalar',NULL,?,'patch')",
                        (new_vid, base_vid, payload))
            else:
                self.conn.execute(
                    "INSERT INTO versions(vid,parent_vid,root_kind,root_lid,"
                    "root_scalar,created_from) "
                    "SELECT ?,?, root_kind, root_lid, root_scalar, 'patch' "
                    "FROM versions WHERE vid=?",
                    (new_vid, base_vid, base_vid))

            # 引用完整性：新版本根及其可达子图不得指向不存在的节点修订
            self._check_reachability_locked(new_vid)
            self._write_meta_locked()
            self._retain_window_locked()
            self._hook("pre_commit", "patch")
            self.conn.execute("COMMIT")
            self._hook("post_commit", "patch")
        except BaseException:
            self._rollback_safely()
            raise
        return new_vid

    def _validate_value(self, value, vid):
        """把 patch 输入值规范化为 ('node', lid) 或 ('scalar', blob)。"""
        if not isinstance(value, Mapping):
            raise ValidationError(f"非法 value 描述: {value!r}")
        if "scalar" in value:
            scalar = value["scalar"]
            _scalar_type_tag(scalar)
            return "scalar", _encode_scalar_blob(scalar)
        if "ref" in value:
            lid, rev = parse_handle(value["ref"])
            visible = self._resolve_rev(lid, vid)
            if rev != visible:
                raise ConflictError(
                    f"引用句柄 {value['ref']} 不是版本 {vid} 的当前修订 R{visible}")
            return "node", lid
        raise ValidationError("value 必须是 {'scalar': ...} 或 {'ref': handle}")

    def _apply_array_op(self, name, op, rows, vid):
        index = op.get("index")
        if not isinstance(index, int) or isinstance(index, bool) or index < 0:
            raise ValidationError(f"数组下标必须是非负整数，得到 {index!r}")
        if name == "array_set":
            if index >= len(rows):
                raise ValidationError(f"数组下标 {index} 越界（长度 {len(rows)}）")
            kind, payload = self._validate_value(op.get("value"), vid)
            rows[index] = self._edge_row(index, None, kind, payload)
        elif name == "array_insert":
            if index > len(rows):
                raise ValidationError(f"插入位置 {index} 越界（长度 {len(rows)}）")
            kind, payload = self._validate_value(op.get("value"), vid)
            rows.insert(index, self._edge_row(index, None, kind, payload))
            self._reindex(rows)
        else:  # array_delete
            if index >= len(rows):
                raise ValidationError(f"删除下标 {index} 越界（长度 {len(rows)}）")
            del rows[index]
            self._reindex(rows)

    def _apply_map_op(self, name, op, rows, vid):
        key_desc = op.get("key")
        if not isinstance(key_desc, Mapping) or "type" not in key_desc or "value" not in key_desc:
            raise ValidationError("映射操作需要 key:{type,value}")
        ktype, kvalue = key_desc["type"], key_desc["value"]
        expected = {"int": int, "text": str, "bytes": bytes, "bool": bool}
        if ktype not in expected:
            raise ValidationError(f"非法映射键类型: {ktype}")
        if not isinstance(kvalue, expected[ktype]):
            raise ValidationError(f"键值 {kvalue!r} 与声明类型 {ktype} 不符")
        if ktype == "int" and isinstance(kvalue, bool):
            raise ValidationError("bool 值不能声明为 int 键")
        # 构造 TypedKey 统一编码路径
        tkey = TypedKey(ktype, kvalue)
        key_blob = _encode_key_blob(tkey)
        pos = self._find_map_entry(rows, ktype, key_blob)
        if name == "map_set":
            kind, payload = self._validate_value(op.get("value"), vid)
            new_row = self._edge_row(pos if pos is not None else len(rows),
                                     (ktype, key_blob), kind, payload)
            if pos is None:
                rows.append(new_row)
            else:
                rows[pos] = new_row
        else:  # map_delete
            if pos is None:
                raise ConflictError(
                    f"映射中不存在键 ({ktype},{kvalue!r})，无法删除")
            del rows[pos]
            self._reindex(rows)

    @staticmethod
    def _find_map_entry(rows, ktype, key_blob):
        for i, e in enumerate(rows):
            if e["key_type"] == ktype and e["key_value"] == key_blob:
                return i
        return None

    @staticmethod
    def _edge_row(position, key, kind, payload):
        key_type = key[0] if key else None
        key_blob = key[1] if key else None
        if kind == "node":
            return {"position": position, "key_type": key_type,
                    "key_value": key_blob, "val_kind": "node",
                    "ref_lid": payload, "scalar": None}
        return {"position": position, "key_type": key_type,
                "key_value": key_blob, "val_kind": "scalar",
                "ref_lid": None, "scalar": payload}

    @staticmethod
    def _reindex(rows):
        for i, e in enumerate(rows):
            e["position"] = i

    def _check_reachability_locked(self, vid):
        """显式栈验证：从新版本根出发，所有 node 边都能解析到内容修订。

        允许子图不可达（那只是不再被访问），但禁止指向不存在的节点。
        """
        vrow = self.conn.execute(
            "SELECT root_kind, root_lid FROM versions WHERE vid=?", (vid,)
        ).fetchone()
        if vrow["root_kind"] == "scalar":
            return
        visited = set()
        stack = [(vrow["root_lid"], self._resolve_rev(vrow["root_lid"], vid))]
        while stack:
            lid, rev = stack.pop()
            if (lid, rev) in visited:
                continue
            visited.add((lid, rev))
            for e in self._edges(lid, rev):
                if e["val_kind"] == "node":
                    child_rev = self._resolve_rev(e["ref_lid"], vid)
                    stack.append((e["ref_lid"], child_rev))

    # ------------------------- 版本保留与回收 -------------------------

    def _retain_window_locked(self):
        """保留至多 MAX_RETAINED_VERSIONS 个未退役根，其余标记退役。"""
        rows = self.conn.execute(
            "SELECT vid FROM versions WHERE retired=0 ORDER BY vid DESC"
        ).fetchall()
        retire = [r["vid"] for r in rows[MAX_RETAINED_VERSIONS:]]
        if retire:
            self.conn.executemany(
                "UPDATE versions SET retired=1 WHERE vid=?",
                [(v,) for v in retire])

    def reopen_version(self, vid: int) -> int:
        """从（可能较旧的）版本拉出一个分支新版本。

        覆盖层下这只是新增一条版本索引、根引用与原版本相同，不复制任何
        节点内容；后续在新版本上 patch 会产生独立修订线，不影响其他分支。
        """
        row = self.conn.execute(
            "SELECT root_kind, root_lid, root_scalar FROM versions WHERE vid=?",
            (vid,)).fetchone()
        if row is None:
            raise ConflictError(f"版本 {vid} 不存在")
        new_vid = self._new_vid()
        self.conn.execute("BEGIN IMMEDIATE")
        self._invalidate_gc_state_locked()
        try:
            self.conn.execute(
                "INSERT INTO versions(vid,parent_vid,root_kind,root_lid,"
                "root_scalar,created_from) VALUES(?,?,?,?,?,'reopen')",
                (new_vid, vid, row["root_kind"], row["root_lid"],
                 row["root_scalar"]))
            self._retain_window_locked()
            self.conn.execute("COMMIT")
        except BaseException:
            self._rollback_safely()
            raise
        return new_vid

    def drop_version(self, vid: int) -> bool:
        """逻辑删除一个版本根。内容不立即删除，由 gc 按可达性回收。"""
        row = self.conn.execute(
            "SELECT retired FROM versions WHERE vid=?", (vid,)).fetchone()
        if row is None:
            raise ConflictError(f"版本 {vid} 不存在")
        self.conn.execute("BEGIN IMMEDIATE")
        self._invalidate_gc_state_locked()
        try:
            self.conn.execute(
                "UPDATE versions SET retired=1 WHERE vid=?", (vid,))
            self.conn.execute("COMMIT")
        except BaseException:
            self._rollback_safely()
            raise
        return True

    def _mark_live_locked(self):
        """从所有保留根出发做追踪标记，返回 live 的 (lid,rev) 集合。

        按 lid 身份遍历（不是按路径），环不会导致无限循环。
        """
        live: set = set()
        stack = []
        for _vid, root_kind, root_lid in self._active_roots():
            if root_kind == "node":
                rev = self._resolve_rev(root_lid, _vid)
                stack.append((root_lid, rev, _vid))
        while stack:
            lid, rev, vid = stack.pop()
            key = (lid, rev)
            if key in live:
                continue
            live.add(key)
            for e in self._edges(lid, rev):
                if e["val_kind"] == "node":
                    child_rev = self._resolve_rev(e["ref_lid"], vid)
                    stack.append((e["ref_lid"], child_rev, vid))
        return live

    def gc(self):
        """回收所有保留根都不可达的内容修订及其边。可在中断后续跑。

        分两个独立事务：
        - 标记事务：计算 live 集合并把 gc_state 落盘后提交，因此在
          “标记后/删除前”崩溃，标记结果已持久化，重开可直接进入删除；
        - 删除事务：依据已持久化的 live 集合 sweep，结束后清掉 gc_state。
        任何写入都会先使 gc_state 失效，避免按过期标记误删新内容。
        """
        # 第一事务：标记（或恢复已持久化的标记）
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            state = self.conn.execute(
                "SELECT phase, live FROM gc_state WHERE id=1").fetchone()
            if state is None:
                live = self._mark_live_locked()
                self.conn.execute(
                    "INSERT INTO gc_state(id,phase,live) VALUES(1,'sweep',?)",
                    (json.dumps(sorted(live)),))
                self.conn.execute("COMMIT")
                # 标记已持久化后再注入崩溃：模拟“标记后/删除前”断电
                self._hook("post_mark", "gc")
            else:
                live = {tuple(x) for x in json.loads(state["live"])}
                self.conn.execute("COMMIT")
        except BaseException:
            self._rollback_safely()
            raise

        # 第二事务：删除。崩溃后重跑会从已提交的 gc_state 恢复 live。
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self._hook("pre_sweep", "gc")
            all_content = self.conn.execute(
                "SELECT lid, rev FROM node_content").fetchall()
            dead = [(r["lid"], r["rev"]) for r in all_content
                    if (r["lid"], r["rev"]) not in live]
            removed_edges = 0
            for lid, rev in dead:
                cur = self.conn.execute(
                    "DELETE FROM edge WHERE lid=? AND rev=?", (lid, rev))
                removed_edges += cur.rowcount
                self.conn.execute(
                    "DELETE FROM node_content WHERE lid=? AND rev=?",
                    (lid, rev))
            self.conn.execute("DELETE FROM gc_state WHERE id=1")
            self.conn.execute("COMMIT")
            return {
                "live_content": len(live),
                "removed_content": len(dead),
                "removed_edges": removed_edges,
            }
        except BaseException:
            self._rollback_safely()
            raise

    # ------------------------- 统计 -------------------------

    def storage_counts(self):
        """以实际持久记录计数：内容记录 / 边引用 / 版本索引分开披露。"""
        c = self.conn.execute
        return {
            "content_rows": c("SELECT COUNT(*) n FROM node_content").fetchone()["n"],
            "edge_rows": c("SELECT COUNT(*) n FROM edge").fetchone()["n"],
            "version_rows": c("SELECT COUNT(*) n FROM versions").fetchone()["n"],
            "active_versions": c(
                "SELECT COUNT(*) n FROM versions WHERE retired=0").fetchone()["n"],
        }

    def content_rows_for_version(self, vid: int) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) n FROM node_content WHERE vid=?", (vid,)
        ).fetchone()["n"]

    def _hook(self, name, operation=None):
        """崩溃注入钩子。钩子签名为 hook(point, operation)，可按操作过滤。"""
        if self.crash_hook is not None:
            self.crash_hook(name, operation)
