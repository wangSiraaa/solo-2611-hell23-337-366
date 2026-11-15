"""可审阅的外键删除计划：plan / apply（事务内 verify）。

语义裁决全部在 Python 侧静态推演完成，plan 阶段绝不通过“先删一遍”冒充推演；
真实外键动作只在 apply 的单事务中由 SQLite 引擎执行，并在同一事务内逐表核对
预测写集。外键全程开启，不存在禁用外键的执行路径。

计划文档格式见 DESIGN.md「计划格式」一节，版本字段 format_version=1。
"""

import hashlib
import json
import re
import time
import types
from dataclasses import dataclass, field

from database import connect

PLAN_FORMAT_VERSION = 1

# 裁决/执行状态（明确的失败结果，不抛掉、不静默）。
ST_READY = "READY"            # 计划可执行
ST_BLOCKED = "BLOCKED"        # 计划被 hold / NOT NULL / 幸存 NO ACTION 阻断
ST_COMMITTED = "COMMITTED"
ST_STALE = "STALE_PLAN"       # 计划后发生影响写集/裁决的并发修改
ST_BUSY = "BUSY"              # 锁竞争，一次显式重试后仍失败
ST_REJECTED = "REJECTED"      # 计划本身不被接受（id 不符、阻断态申请执行等）
ST_MISMATCH = "VERIFY_MISMATCH"
ST_EXEC_FAIL = "EXEC_CONSTRAINT"
ST_COMMIT_BUSY = "COMMIT_BUSY"


class RetentionError(Exception):
    """配置/目录类硬错误（schema 不满足题定约束、配置未知表等）。"""


# --------------------------------------------------------------------------- #
# 序列化与哈希工具
# --------------------------------------------------------------------------- #
def canonical(obj):
    """确定性 JSON 字节：指纹与 plan_id 的唯一序列化口径。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=_json_default).encode("utf-8")


def _json_default(o):
    if isinstance(o, set):
        return sorted(o)
    if isinstance(o, (tuple,)):
        return list(o)
    raise TypeError(f"cannot canonicalize {type(o)!r}")


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    need = ("version", "roles", "root_predicate", "retained_tables")
    missing = [k for k in need if k not in cfg]
    if missing:
        raise RetentionError(f"config missing keys: {missing}")
    if cfg["root_predicate"]["op"] != "<=":
        raise RetentionError("only root predicate op '<=' is supported")
    return cfg


def save_plan(plan_doc, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(plan_doc, f, ensure_ascii=False, indent=2, sort_keys=True)


def load_plan(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# --------------------------------------------------------------------------- #
# 目录（schema/FK 目录）加载与校验
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Edge:
    child: str
    child_col: str
    parent: str
    parent_col: str
    on_delete: str
    on_update: str
    deferred: bool

    def as_dict(self):
        return {"child": self.child, "child_col": self.child_col,
                "parent": self.parent, "parent_col": self.parent_col,
                "on_delete": self.on_delete, "on_update": self.on_update,
                "deferred": self.deferred}


@dataclass
class Table:
    name: str
    columns: list                      # [(name, type, notnull, pk_pos)]
    pk_col: str
    col_index: dict = field(default_factory=dict)
    edges: list = field(default_factory=list)   # 本表作为子表的出边

    def as_dict(self):
        return {"columns": [{"name": n, "type": t, "notnull": nn, "pk": pk}
                            for (n, t, nn, pk) in self.columns],
                "pk_col": self.pk_col,
                "edges": [e.as_dict() for e in self.edges]}


@dataclass
class Catalog:
    tables: dict
    config: dict
    root_table: str
    retained: dict                     # table -> cfg
    edges: list

    def fingerprint(self):
        body = {
            "tables": {name: t.as_dict() for name, t in sorted(self.tables.items())},
            "roles": self.config["roles"],
            "root_predicate": self.config["root_predicate"],
            "retained_tables": self.config["retained_tables"],
        }
        return sha256_hex(canonical(body))


def _split_top_level(body):
    """按顶层逗号切分 CREATE TABLE 括号内文本（忽略嵌套括号内逗号）。"""
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(body):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(body[start:i])
            start = i + 1
    parts.append(body[start:])
    return parts


def _fk_deferrability(create_sql):
    """从 CREATE TABLE 文本解析每条 FK 是否 DEFERRABLE INITIALLY DEFERRED。

    返回键集合 {(from_col, parent_table, parent_col): deferred_bool}。
    只支持单列 FK（目录校验阶段已拒绝多列）。
    """
    m = re.search(r"\((?P<body>.*)\)\s*$", create_sql, re.S | re.I)
    if not m:
        return {}
    table_fk_re = re.compile(
        r'FOREIGN\s+KEY\s*\(\s*"?(?P<fc>\w+)"?\s*\)', re.I)
    ref_re = re.compile(
        r'REFERENCES\s+"?(?P<pt>\w+)"?\s*\(\s*"?(?P<pc>\w+)"?\s*\)'
        r'(?P<tail>[^,)]*)', re.I | re.S)
    out = {}
    for chunk in _split_top_level(m.group("body")):
        up = chunk.upper()
        if "REFERENCES" not in up:
            continue
        mm = table_fk_re.search(chunk)
        if mm:
            frm = mm.group("fc")
        else:
            # 列内联约束：该片段第一个标识符即列名。
            cm = re.match(r'\s*"?(?P<ic>\w+)"?', chunk)
            if not cm:
                continue
            frm = cm.group("ic")
        rm = ref_re.search(chunk)
        if not rm:
            continue
        tail = rm.group("tail")
        not_defer = re.search(r"NOT\s+DEFERRABLE", tail, re.I)
        deferred = (not not_defer
                    and bool(re.search(r"DEFERRABLE\s+INITIALLY\s+DEFERRED",
                                       tail, re.I)))
        out[(frm, rm.group("pt"), rm.group("pc"))] = deferred
    return out


def load_catalog(conn, config):
    masters = conn.execute(
        "SELECT type,name,sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    bad_types = sorted({t for (t, _n, _s) in masters if t not in ("table", "index")})
    if bad_types:
        raise RetentionError(f"unsupported schema objects (views/triggers forbidden): "
                             f"{bad_types}")
    table_rows = [(n, s) for (t, n, s) in masters if t == "table"]
    if not table_rows:
        raise RetentionError("database has no user tables")
    if len(table_rows) > config.get("max_tables", 12):
        raise RetentionError(f"table count {len(table_rows)} exceeds limit")

    roles = config["roles"]
    unknown = [n for (n, _s) in table_rows if n not in roles]
    if unknown:
        raise RetentionError(f"tables without role in config: {unknown}")
    root_tables = [n for n in roles if roles[n] == "root"]
    if len(root_tables) != 1:
        raise RetentionError(f"exactly one root table required, got {root_tables}")
    root_table = root_tables[0]
    if root_table not in dict(table_rows):
        raise RetentionError(f"root table {root_table!r} missing in database")

    tables = {}
    for name, create_sql in table_rows:
        info = conn.execute(f'PRAGMA table_info("{name}")').fetchall()
        pk_cols = [r for r in info if r[5] > 0]
        if len(pk_cols) != 1 or pk_cols[0][2].upper() != "INTEGER":
            raise RetentionError(
                f"table {name!r}: only single-column INTEGER PRIMARY KEY supported")
        pk_col = pk_cols[0][1]
        cols = [(r[1], r[2].upper(), bool(r[3]), r[5]) for r in info]
        tables[name] = Table(name=name, columns=cols, pk_col=pk_col,
                             col_index={r[1]: i for i, r in enumerate(info)})

    deferred_map = {n: _fk_deferrability(sql) for (n, sql) in table_rows}
    all_edges = []
    for name, _sql in table_rows:
        fks = conn.execute(f'PRAGMA foreign_key_list("{name}")').fetchall()
        grouped = {}
        for row in fks:
            # (id, seq, parent, from, to, on_update, on_delete, match)
            grouped.setdefault(row[0], []).append(row)
        for fid, rows in grouped.items():
            rows.sort(key=lambda r: r[1])
            if len(rows) != 1:
                raise RetentionError(
                    f"table {name!r} fk#{fid}: only single-column FK supported")
            (_id, _seq, parent, frm, to, on_up, on_del, _match) = rows[0]
            on_del = (on_del or "NO ACTION").upper()
            if on_del not in ("CASCADE", "SET NULL", "NO ACTION"):
                raise RetentionError(
                    f"table {name!r}.{frm}: unsupported ON DELETE {on_del!r}")
            if parent not in tables:
                raise RetentionError(
                    f"table {name!r}.{frm}: unknown parent table {parent!r}")
            if to != tables[parent].pk_col:
                raise RetentionError(
                    f"table {name!r}.{frm}: FK must target parent single PK "
                    f"{tables[parent].pk_col!r}, targets {to!r}")
            if frm not in tables[name].col_index:
                raise RetentionError(f"table {name!r}: FK column {frm!r} missing")
            deferred = deferred_map[name].get((frm, parent, to), False)
            if on_del == "NO ACTION" and not deferred:
                raise RetentionError(
                    f"table {name!r}.{frm}: NO ACTION must be DEFERRABLE INITIALLY "
                    f"DEFERRED; immediate RESTRICT semantics are out of scope")
            edge = Edge(child=name, child_col=frm, parent=parent, parent_col=to,
                        on_delete=on_del, on_update=(on_up or "NO ACTION").upper(),
                        deferred=deferred)
            tables[name].edges.append(edge)
            all_edges.append(edge)

    retained = config["retained_tables"]
    for tname, rcfg in retained.items():
        if tname not in tables or roles.get(tname) != "retained":
            raise RetentionError(f"retained table {tname!r} missing or mis-role'd")
        t = tables[tname]
        for key in ("protected_column", "expires_at_column"):
            if rcfg[key] not in t.col_index:
                raise RetentionError(
                    f"retained table {tname!r}: column {rcfg[key]!r} ({key}) missing")
        if rcfg["protected_parent_table"] not in tables:
            raise RetentionError(
                f"retained table {tname!r}: protected parent "
                f"{rcfg['protected_parent_table']!r} unknown")
    rp = config["root_predicate"]
    if rp["table"] != root_table or rp["column"] not in tables[root_table].col_index:
        raise RetentionError("root_predicate must reference a column of root table")

    total_rows = sum(conn.execute(
        f'SELECT COUNT(*) FROM "{n}"').fetchone()[0] for n in tables)
    if total_rows > config.get("max_rows", 30000):
        raise RetentionError(f"row count {total_rows} exceeds limit")
    return Catalog(tables=tables, config=config, root_table=root_table,
                   retained=retained, edges=all_edges)


# --------------------------------------------------------------------------- #
# 静态语义推演
# --------------------------------------------------------------------------- #
def hold_active(expires_at, clock):
    """逻辑时钟下的 hold 有效性：NULL=永久；expires_at>clock 才有效，== 已过期。"""
    if expires_at is None:
        return True
    return expires_at > clock


def _load_rows(conn, table):
    """返回 {pk: 完整行元组}，列顺序按 PRAGMA table_info。"""
    cols = ", ".join(f'"{c[0]}"' for c in table.columns)
    pk_idx = table.col_index[table.pk_col]
    return {row[pk_idx]: row
            for row in conn.execute(f'SELECT {cols} FROM "{table.name}"')}


def analyze(conn, catalog, explicit_roots, cutoff, clock):
    """只读静态推演：CASCADE 固定点 -> 幸存 SET NULL/NO ACTION -> hold 裁决。

    返回 dict：status、candidate_roots、delete_rows、null_updates、blockers、
    reasons、clock、cutoff、rows_read。
    """
    rows_read = 0
    rows = {}
    for tname, t in catalog.tables.items():
        rows[tname] = _load_rows(conn, t)
        rows_read += len(rows[tname])

    # 父表 -> 出边索引（边挂在子表上，按 parent 反查）。
    by_parent = {}
    for e in catalog.edges:
        by_parent.setdefault(e.parent, []).append(e)

    # 每条边建 {fk_value: [child_pk,...]} 索引，使固定点与幸存扫描为线性，
    # 避免 30000 行规模下“每个被删父行全表扫子表”的二次方复杂度。
    edge_children = {}
    for e in catalog.edges:
        child_t = catalog.tables[e.child]
        ci = child_t.col_index[e.child_col]
        idx = {}
        for cpk, crow in rows[e.child].items():
            fkval = crow[ci]
            if fkval is not None:
                idx.setdefault(fkval, []).append(cpk)
        edge_children[e] = idx

    blockers = []
    reasons = {}   # (table, pk) -> [reason,...]；null 项键为 (table, pk, col)

    def mark(key, why):
        reasons.setdefault(key, [])
        if why not in reasons[key]:
            reasons[key].append(why)

    # --- 候选根：显式根 ∪ cutoff 谓词根 -----------------------------------
    root_t = catalog.tables[catalog.root_table]
    candidates = {}
    for item in explicit_roots or []:
        tname, pk = item["table"], item["pk"]
        if tname != catalog.root_table:
            blockers.append({"kind": "INVALID_ROOT", "table": tname, "pk": pk,
                             "reason": f"explicit roots must target root table "
                                       f"{catalog.root_table!r}"})
            continue
        if pk not in rows[tname]:
            blockers.append({"kind": "MISSING_ROOT", "table": tname, "pk": pk,
                             "reason": "explicit root row does not exist"})
            continue
        candidates[(tname, pk)] = "root:explicit"
    if cutoff is not None:
        rp = catalog.config["root_predicate"]
        ci = root_t.col_index[rp["column"]]
        for pk, row in rows[catalog.root_table].items():
            if row[ci] is not None and row[ci] <= cutoff:
                candidates.setdefault((catalog.root_table, pk), "root:cutoff")

    # --- 第一步：CASCADE 固定点（先于一切 SET NULL/NO ACTION/hold 判断）---
    delete_set = {}
    work = list(candidates.items())
    for key, why in work:
        delete_set[key] = why
        mark(key, why)
    while work:
        (pt, ppk), _why = work.pop(0)
        for e in by_parent.get(pt, ()):
            if e.on_delete != "CASCADE":
                continue
            for cpk in edge_children[e].get(ppk, ()):
                # 已在删除集中的行不重复入队（环安全）；NULL 不入索引故天然跳过。
                if (e.child, cpk) in delete_set:
                    continue
                why = (f"cascade:{e.child}.{e.child_col}->"
                       f"{e.parent}({ppk})")
                delete_set[(e.child, cpk)] = why
                mark((e.child, cpk), why)
                work.append(((e.child, cpk), why))

    # --- 第二步：幸存子行的 SET NULL / 延期 NO ACTION ---------------------
    null_updates = {}     # (table,pk,col) -> reason list
    for e in catalog.edges:
        if e.on_delete not in ("SET NULL", "NO ACTION"):
            continue
        child_t = catalog.tables[e.child]
        ci = child_t.col_index[e.child_col]
        notnull = next(nn for (n, _ty, nn, _pk) in child_t.columns
                       if n == e.child_col)
        # 只遍历“指向被删父行”的子行（经边索引），与全表规模解耦。
        for fkval, child_pks in edge_children[e].items():
            if (e.parent, fkval) not in delete_set:
                continue
            for cpk in child_pks:
                if (e.child, cpk) in delete_set:
                    continue   # 多路径：删除优先，不再置空
                why = (f"{e.on_delete.lower().replace(' ', '_')}:"
                       f"{e.child}.{e.child_col}->{e.parent}({fkval})")
                if e.on_delete == "SET NULL":
                    if notnull:
                        blockers.append({
                            "kind": "SET_NULL_NOT_NULL", "table": e.child,
                            "pk": cpk, "column": e.child_col,
                            "parent_table": e.parent, "parent_pk": fkval,
                            "reason": f"SET NULL would write NULL into NOT NULL "
                                      f"column {e.child}.{e.child_col}"})
                        mark((e.child, cpk), "blocked:" + why)
                    else:
                        key = (e.child, cpk, e.child_col)
                        null_updates.setdefault(key, [])
                        if why not in null_updates[key]:
                            null_updates[key].append(why)
                        mark(key, why)
                else:
                    # 延期 NO ACTION：同批删除的子行已在上面跳过；幸存引用即阻断。
                    blockers.append({
                        "kind": "SURVIVING_NO_ACTION", "table": e.child,
                        "pk": cpk, "column": e.child_col,
                        "parent_table": e.parent, "parent_pk": fkval,
                        "reason": f"deferred NO ACTION: surviving row "
                                  f"{e.child}({cpk}) still references deleted "
                                  f"{e.parent}({fkval}) at commit time"})
                    mark((e.child, cpk), "blocked:" + why)

    # --- 第三步：有效 hold 裁决（不能靠直接删 hold 解锁）------------------
    for tname, rcfg in catalog.retained.items():
        t = catalog.tables[tname]
        pi = t.col_index[t.pk_col]
        ei = t.col_index[rcfg["protected_column"]]
        xi = t.col_index[rcfg["expires_at_column"]]
        parent_t = rcfg["protected_parent_table"]
        for hpk, hrow in rows[tname].items():
            if not hold_active(hrow[xi], clock):
                mark((tname, hrow[pi]),
                     f"hold-expired:expires_at={hrow[xi]} clock={clock}")
                continue
            mark((tname, hrow[pi]), f"hold-active:clock={clock}")
            # 顺序：先判 hold 行自身是否在删除闭包（不能直接删 hold 解锁），
            # 再判它保护的父行；NULL 保护列只参与前一项判断。
            if (tname, hpk) in delete_set:
                blockers.append({
                    "kind": "HOLD_ROW_DELETED", "table": tname, "pk": hpk,
                    "reason": "an active retained/hold row is inside the delete "
                              "closure; deleting the hold itself cannot unlock it"})
                continue
            target = hrow[ei]
            if target is not None and (parent_t, target) in delete_set:
                blockers.append({
                    "kind": "HOLD_PROTECTED_DELETED", "table": tname, "pk": hpk,
                    "protected_table": parent_t, "protected_pk": target,
                    "reason": f"active hold {tname}({hpk}) protects "
                              f"{parent_t}({target}) which is inside the delete "
                              f"closure"})

    delete_rows = {}
    for (tname, pk) in sorted(delete_set):
        delete_rows.setdefault(tname, []).append(pk)
    nulls = {}
    for (tname, pk, col) in sorted(null_updates):
        nulls.setdefault(tname, []).append(
            {"pk": pk, "column": col, "reasons": null_updates[(tname, pk, col)]})
    blocker_keys = sorted((b["kind"], b.get("table"), b.get("pk"),
                           b.get("column")) for b in blockers)
    status = ST_BLOCKED if blockers else ST_READY
    return {
        "status": status,
        "clock": clock,
        "cutoff": cutoff,
        "candidate_roots": [{"table": t, "pk": pk}
                            for (t, pk) in sorted(candidates)],
        "delete_rows": {t: sorted(v) for t, v in sorted(delete_rows.items())},
        "null_updates": {t: sorted(v, key=lambda x: (x["pk"], x["column"]))
                         for t, v in sorted(nulls.items())},
        "blockers": sorted(blockers, key=lambda b: blocker_keys),
        "reasons": {("|".join(map(str, k)) if isinstance(k, tuple) else k): v
                    for k, v in sorted(reasons.items(), key=lambda kv: str(kv[0]))},
        "rows_read": rows_read,
    }


# --------------------------------------------------------------------------- #
# 数据指纹：只覆盖闭包/谓词相关行，无关行修改不失效
# --------------------------------------------------------------------------- #
def data_fingerprint(conn, catalog, analysis):
    """按批准写集的结构范围计算每表行指纹。

    - root 表 / retained 表：整表（根数据与所有 hold 都参与裁决）；
    - ordinary 表：批准键集合（删除/置空/阻断行）∪ 当前所有“非空 FK 指向
      批准删除父行”的行 —— 后者用结构谓词在 apply 时重算，能捕获幻影行，
      而与闭包无关的行（NULL FK、指向未删父行）永不入指纹。
    """
    fp = {}
    del_keys = {t: set(pks) for t, pks in analysis["delete_rows"].items()}
    null_keys = {}
    blocker_keys = {}
    for t, items in analysis["null_updates"].items():
        null_keys.setdefault(t, set()).update(i["pk"] for i in items)
    for b in analysis["blockers"]:
        if b.get("table"):
            blocker_keys.setdefault(b["table"], set()).add(b["pk"])
    delete_parents = set((t, pk) for t, pks in del_keys.items() for pk in pks)

    for tname, t in catalog.tables.items():
        rows = _load_rows(conn, t)
        if catalog.config["roles"][tname] in ("root", "retained"):
            chosen = set(rows)
        else:
            chosen = (del_keys.get(tname, set())
                      | null_keys.get(tname, set())
                      | blocker_keys.get(tname, set()))
            # 结构谓词：本表作为子表，任一出边指向批准删除父行（NULL 不算引用）。
            for e in t.edges:
                ci = t.col_index[e.child_col]
                for cpk, crow in rows.items():
                    v = crow[ci]
                    if v is not None and (e.parent, v) in delete_parents:
                        chosen.add(cpk)
        digest = sha256_hex(canonical(
            {pk: list(rows[pk]) for pk in sorted(chosen) if pk in rows}))
        fp[tname] = {"sha256": digest, "row_count": len(chosen)}
    return sha256_hex(canonical(fp)), fp


# --------------------------------------------------------------------------- #
# plan
# --------------------------------------------------------------------------- #
def plan(path, config, explicit_roots, cutoff, plan_clock):
    """只读规划：返回可审阅、可批准的计划文档（不落库、不执行任何写）。"""
    conn = connect(path)
    try:
        catalog = load_catalog(conn, config)
        catalog_fp = catalog.fingerprint()
        analysis = analyze(conn, catalog, explicit_roots, cutoff, plan_clock)
        data_fp, data_fp_tables = data_fingerprint(conn, catalog, analysis)
    finally:
        conn.close()

    doc = {
        "format_version": PLAN_FORMAT_VERSION,
        "database_path": str(path),
        "embedded_config": config,
        "catalog_fingerprint": catalog_fp,
        "data_fingerprint": data_fp,
        "data_fingerprint_tables": data_fp_tables,
        "request": {
            "explicit_roots": explicit_roots or [],
            "cutoff": cutoff,
            "plan_clock": plan_clock,
        },
        "verdict": {k: analysis[k] for k in (
            "status", "candidate_roots", "delete_rows", "null_updates",
            "blockers", "reasons", "clock", "cutoff")},
        "stats": {"rows_read": analysis["rows_read"]},
    }
    doc["plan_id"] = _plan_id(doc)
    return doc


def _plan_id(doc):
    body = {k: v for k, v in doc.items() if k != "plan_id"}
    return sha256_hex(canonical(body))


# --------------------------------------------------------------------------- #
# apply：锁内重验 + 单事务真实外键动作 + 逐表核对
# --------------------------------------------------------------------------- #
def _revalidate(conn, catalog, doc, exec_clock):
    """锁内权威重验。返回 (ok, stale_detail, fresh_analysis, fresh_fp_tables)。"""
    if catalog.fingerprint() != doc["catalog_fingerprint"]:
        return False, {"cause": "catalog_changed",
                       "hint": "FK catalog / columns / roles changed after plan"}, None, None
    fresh = analyze(conn, catalog,
                    doc["request"]["explicit_roots"],
                    doc["request"]["cutoff"], exec_clock)
    fresh_fp, fresh_tables = data_fingerprint(conn, catalog, fresh)
    if fresh_fp != doc["data_fingerprint"]:
        return False, {"cause": "data_changed",
                       "hint": "rows inside the closure/predicate scope changed "
                               "after plan"}, fresh, fresh_tables
    approved = doc["verdict"]
    for key in ("status", "candidate_roots", "delete_rows", "null_updates",
                "blockers"):
        if canonical(fresh[key]) != canonical(approved[key]):
            return False, {"cause": "verdict_changed", "key": key,
                           "approved": approved[key], "fresh": fresh[key]}, \
                   fresh, fresh_tables
    return True, None, fresh, fresh_tables


def apply(path, plan_doc, exec_clock, audit, hooks=None,
          busy_retry_delay=0.05):
    """按批准计划执行。

    hooks: 可选三控制点回调 {"before_lock","after_revalidation","before_commit"}，
    每个回调接收 SimpleNamespace(path=..., connection=...)，自行开连接尝试写入。
    锁策略：busy_timeout=0 显式感知竞争；BEGIN IMMEDIATE 最多两次尝试（一次明确
    BUSY 重试），失败即返回 ST_BUSY，不做无饥饿保证。
    """
    hooks = hooks or {}
    result = {"plan_id": plan_doc.get("plan_id"), "lock_retries": 0,
              "status": None, "detail": None, "affected": None,
              "stats": {"rows_read": 0, "rows_written": 0}}
    audit.event("apply_begin", {"path": str(path),
                                "plan_id": plan_doc.get("plan_id"),
                                "exec_clock": exec_clock})

    if _plan_id(plan_doc) != plan_doc.get("plan_id"):
        result.update(status=ST_REJECTED,
                      detail={"cause": "plan_id_mismatch",
                              "hint": "plan document tampered after approval"})
        audit.event("apply_rejected", result["detail"])
        return result
    if plan_doc["format_version"] != PLAN_FORMAT_VERSION:
        result.update(status=ST_REJECTED,
                      detail={"cause": "unsupported_format_version"})
        audit.event("apply_rejected", result["detail"])
        return result
    if plan_doc["verdict"]["status"] == ST_BLOCKED:
        result.update(status=ST_REJECTED,
                      detail={"cause": "blocked_plan_not_applicable",
                              "blockers": plan_doc["verdict"]["blockers"]})
        audit.event("apply_rejected", result["detail"])
        return result

    ctx = types.SimpleNamespace(path=str(path), connection=None)

    def _open():
        c = connect(path)
        c.execute("PRAGMA busy_timeout=0")   # 显式 BUSY，自己控制重试
        return c

    if "before_lock" in hooks:
        audit.event("hook", {"point": "before_lock"})
        hooks["before_lock"](ctx)

    conn = _open()
    ctx.connection = conn
    try:
        for attempt in (1, 2):
            try:
                conn.execute("BEGIN IMMEDIATE")
                break
            except Exception as exc:                      # noqa: BLE001
                if "locked" not in str(exc).lower() or attempt == 2:
                    result.update(status=ST_BUSY,
                                  detail={"cause": "begin_immediate_locked",
                                          "message": str(exc),
                                          "attempts": attempt})
                    audit.event("apply_busy", result["detail"])
                    return result
                result["lock_retries"] += 1
                audit.event("lock_retry", {"attempt": attempt, "message": str(exc)})
                time.sleep(busy_retry_delay)
        audit.event("lock_acquired", {"retries": result["lock_retries"]})

        # --- 锁内重验：目录指纹 -> 数据指纹 -> 全量语义再裁决 -------------
        # 计划内嵌完整配置，重验不依赖外部配置文件是否被改动。目录被并发改成
        # 不再满足题定约束（未知表、FK 动作变化等）同样是 STALE，绝不能放行。
        try:
            catalog = _load_with_config(conn, plan_doc)
        except RetentionError as exc:
            conn.rollback()
            detail = {"cause": "catalog_changed", "message": str(exc)}
            audit.event("apply_stale", detail)
            result.update(status=ST_STALE, detail=detail)
            return result
        ok, detail, fresh, _fp = _revalidate(conn, catalog, plan_doc, exec_clock)
        audit.event("revalidation", {"ok": ok, "detail": detail})
        if not ok:
            conn.rollback()
            # 重验本身真实读了行（catalog_changed 时 fresh 可能为 None）。
            if fresh is not None:
                result["stats"]["rows_read"] += fresh["rows_read"]
            result.update(status=ST_STALE, detail=detail)
            audit.event("apply_stale", detail)
            return result
        result["stats"]["rows_read"] += fresh["rows_read"]

        if "after_revalidation" in hooks:
            audit.event("hook", {"point": "after_revalidation"})
            hooks["after_revalidation"](ctx)

        # --- 单事务真实执行：只删候选根，引擎触发全部 FK 动作 -------------
        snapshot = {t: _load_rows(conn, catalog.tables[t])
                    for t in catalog.tables}
        result["stats"]["rows_read"] += sum(len(v) for v in snapshot.values())
        before_changes = conn.total_changes
        try:
            root_t = catalog.root_table
            root_pks = [r["pk"] for r in fresh["candidate_roots"]
                        if r["table"] == root_t]
            for i in range(0, len(root_pks), 500):
                chunk = root_pks[i:i + 500]
                marks = ",".join("?" for _ in chunk)
                conn.execute(
                    f'DELETE FROM "{root_t}" WHERE "{catalog.tables[root_t].pk_col}" '
                    f'IN ({marks})', chunk)
        except Exception as exc:                      # noqa: BLE001
            conn.rollback()
            msg = str(exc)
            # SQLite 引擎自身的级联触发递归上限（约 1000 层深链）：这是引擎
            # 硬限制，直接执行同样语句也会失败，如实分类而非伪装成计划错误。
            cause = ("engine_trigger_recursion_limit"
                     if "trigger recursion" in msg.lower()
                     else "engine_delete_failed")
            result.update(status=ST_EXEC_FAIL,
                          detail={"cause": cause, "message": msg})
            audit.event("apply_exec_failed", result["detail"])
            return result
        written = conn.total_changes - before_changes
        result["stats"]["rows_written"] = written

        # --- 同事务逐表核对：删除集 / 置空集 / 不得有任何额外差异 ----------
        mismatch = _check_writeset(conn, catalog, snapshot, fresh)
        fkviol = conn.execute("PRAGMA foreign_key_check").fetchall()
        if mismatch or fkviol:
            conn.rollback()
            result.update(status=ST_MISMATCH,
                          detail={"writeset_mismatch": mismatch,
                                  "foreign_key_check": [list(v) for v in fkviol]})
            audit.event("apply_verify_mismatch", result["detail"])
            return result

        if "before_commit" in hooks:
            audit.event("hook", {"point": "before_commit"})
            hooks["before_commit"](ctx)

        try:
            conn.commit()
        except Exception as exc:                      # noqa: BLE001
            conn.rollback()
            result.update(status=ST_COMMIT_BUSY,
                          detail={"cause": "commit_failed", "message": str(exc)})
            audit.event("apply_commit_busy", result["detail"])
            return result

        affected = {
            "deletes": fresh["delete_rows"],
            "null_updates": fresh["null_updates"],
            "rows_written_engine_counter": written,
        }
        result.update(status=ST_COMMITTED, detail=None, affected=affected)
        audit.event("apply_committed",
                    {"affected": affected, "stats": result["stats"],
                     "lock_retries": result["lock_retries"]})
        return result
    finally:
        ctx.connection = None
        conn.close()


def _load_with_config(conn, doc):
    """apply 时从计划内嵌的配置重建目录（计划绑定完整语义）。"""
    cfg = doc["embedded_config"]
    return load_catalog(conn, cfg)


def _check_writeset(conn, catalog, snapshot, analysis):
    """事务内核对引擎实际写集与批准预测；无差异返回 []，否则返回明细列表。"""
    problems = []
    pred_del = {t: set(pks) for t, pks in analysis["delete_rows"].items()}
    pred_null = {}
    for t, items in analysis["null_updates"].items():
        for it in items:
            pred_null.setdefault((t, it["pk"]), []).append(it["column"])

    for tname, t in catalog.tables.items():
        current = _load_rows(conn, t)
        old = snapshot[tname]
        actual_deleted = set(old) - set(current)
        if actual_deleted != pred_del.get(tname, set()):
            problems.append({
                "table": tname, "kind": "delete_set_mismatch",
                "predicted": sorted(pred_del.get(tname, set())),
                "actual": sorted(actual_deleted)})
        for pk, new_row in current.items():
            old_row = old.get(pk)
            if old_row is None:
                problems.append({"table": tname, "kind": "unpredicted_insert",
                                 "pk": pk})
                continue
            null_cols = pred_null.get((tname, pk))
            if null_cols:
                for ci, (ov, nv) in enumerate(zip(old_row, new_row)):
                    col = t.columns[ci][0]
                    if col in null_cols:
                        if nv is not None:
                            problems.append({
                                "table": tname, "pk": pk, "column": col,
                                "kind": "expected_null_got_value", "value": nv})
                    elif ov != nv:
                        problems.append({
                            "table": tname, "pk": pk, "column": col,
                            "kind": "unpredicted_change",
                            "old": ov, "new": nv})
            elif old_row != new_row:
                problems.append({"table": tname, "pk": pk,
                                 "kind": "unpredicted_row_change",
                                 "old": list(old_row), "new": list(new_row)})
    return problems
