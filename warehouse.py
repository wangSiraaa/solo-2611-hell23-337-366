"""分片十进制列仓的在线语义迁移核心库。

设计要点（详见 README.md）：
* catalog.json 记录 schema 版本、每个分片的物理版本、读者与迁移计划；
* 逐行十进制转换函数在“迁移路径”和“读时转换路径”间共享，保证两条路径
  得到完全相同的目标语义（先逐行 convert，再参与 sum/count/min/max）；
* 迁移先写 staging，校验（对账）通过后才原子发布；目录从不引用半成品；
* begin_read/end_read 给读者绑定 (schema 版本, 目录快照) 并钉住所用文件；
* 磁盘配额按真实文件字节计量，不足时返回可重试背压，绝不删除活读者依赖。
"""
from __future__ import annotations

import fcntl
import json
import os
import shutil
import time
import uuid
from contextlib import contextmanager
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_EVEN, localcontext
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------- 常量

ROUNDING_MODES = {"half_even": ROUND_HALF_EVEN, "toward_zero": ROUND_DOWN}
MAX_PRECISION = 18
MAX_SCALE = 8
MAX_SHARDS = 8
MAX_ROWS_PER_SHARD = 10_000
MAX_READERS = 2
DECIMAL_PREC = 38  # decimal128 上限，所有精确计算在此上下文进行

CRASH_POINTS = (
    "after_output_before_manifest",        # 新 Parquet 写完未发布
    "after_manifest_before_old_reader_release",  # 发布后旧读者未释放
)


# ---------------------------------------------------------------- 异常

class WarehouseError(Exception):
    """可预期的仓内错误基类。"""


class Backpressure(WarehouseError):
    """磁盘配额不足；调用方可稍后重试（不删除任何活读者依赖）。"""
    retryable = True

    def __init__(self, message, needed_bytes=None, available_bytes=None):
        super().__init__(message)
        self.needed_bytes = needed_bytes
        self.available_bytes = available_bytes


class MigrationFailed(WarehouseError):
    """目标 schema 查询遇到迁移失败的分片：明确失败，不给部分成功金额。"""

    def __init__(self, shard, detail):
        super().__init__(f"shard {shard!r}: 目标 schema 迁移失败: {detail}")
        self.shard = shard
        self.detail = detail


class SchemaRetired(WarehouseError):
    """旧逻辑 schema 的物理文件已回收，无法再做该版本的查询。"""


class MigrationInProgress(WarehouseError):
    pass


class MaxReadersExceeded(WarehouseError):
    pass


class PlanInvalid(WarehouseError):
    pass


class UnknownShard(WarehouseError):
    pass


class UnknownSchema(WarehouseError):
    pass


class UnknownReader(WarehouseError):
    pass


class CorruptCatalog(WarehouseError):
    pass


class SimulatedCrash(Exception):
    """测试钩子：在命名中断点模拟进程崩溃。"""


# ---------------------------------------------------------------- 工具

def _fsync_file(path: Path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def convert_value(x: Decimal, precision: int, scale: int, rounding: str):
    """把一个非 NULL 的 Decimal 转换到 decimal(precision, scale)。

    返回 (converted, loss, fits)：
    * converted 按 plan 的 rounding 量化到 scale；
    * loss = x - converted，精确 Decimal（可正可负）；
    * fits 为 False 表示转换后超出 precision（溢出，整个分片应被拒绝）。
    """
    with localcontext() as ctx:
        ctx.prec = DECIMAL_PREC
        quantum = Decimal(1).scaleb(-scale)
        converted = x.quantize(quantum, rounding=ROUNDING_MODES[rounding])
        loss = x - converted
        fits = len(converted.as_tuple().digits) <= precision
    return converted, loss, fits


def validate_plan(precision: int, scale: int, rounding: str):
    if not isinstance(precision, int) or not 1 <= precision <= MAX_PRECISION:
        raise PlanInvalid(f"precision 须为 1..{MAX_PRECISION}，得到 {precision!r}")
    if not isinstance(scale, int) or not 0 <= scale <= MAX_SCALE:
        raise PlanInvalid(f"scale 须为 0..{MAX_SCALE}，得到 {scale!r}")
    if scale > precision:
        raise PlanInvalid(f"scale({scale}) 不能大于 precision({precision})")
    if rounding not in ROUNDING_MODES:
        raise PlanInvalid(f"rounding 须为 {sorted(ROUNDING_MODES)}，得到 {rounding!r}")


def _aggregate(values):
    """对 Decimal|None 序列求 rows/non_null/sum/min/max（sum 等对全 NULL 为 None）。"""
    rows = len(values)
    non_null = 0
    total = Decimal(0)
    mn = mx = None
    with localcontext() as ctx:
        ctx.prec = DECIMAL_PREC
        for v in values:
            if v is None:
                continue
            non_null += 1
            total += v
            mn = v if mn is None or v < mn else mn
            mx = v if mx is None or v > mx else mx
    return {
        "rows": rows,
        "non_null": non_null,
        "sum": total if non_null else None,
        "min": mn,
        "max": mx,
    }


def _summary_to_json(agg, loss_sum=Decimal(0), source_sum=Decimal(0)):
    return {
        "rows": agg["rows"],
        "non_null": agg["non_null"],
        "sum": None if agg["sum"] is None else str(agg["sum"]),
        "min": None if agg["min"] is None else str(agg["min"]),
        "max": None if agg["max"] is None else str(agg["max"]),
        "loss_sum": str(loss_sum),
        "source_sum": str(source_sum),
    }


def _summary_from_json(s):
    return {
        "rows": s["rows"],
        "non_null": s["non_null"],
        "sum": None if s["sum"] is None else Decimal(s["sum"]),
        "min": None if s["min"] is None else Decimal(s["min"]),
        "max": None if s["max"] is None else Decimal(s["max"]),
    }


def _convert_values(values, target):
    """对一列值逐行转换（迁移路径与读时路径共用同一实现）。

    返回 (converted, losses, overflows, per_row)；per_row 记录每行的
    old/new/loss/fits，供对账与失败明细使用。
    """
    p, s, r = target["precision"], target["scale"], target["rounding"]
    converted, losses, overflows, per_row = [], [], [], []
    for idx, x in enumerate(values):
        if x is None:
            converted.append(None)
            per_row.append({"row": idx, "old": None, "new": None, "loss": None})
            continue
        c, loss, fits = convert_value(x, p, s, r)
        converted.append(c)
        losses.append(loss)
        per_row.append({
            "row": idx, "old": str(x), "new": str(c), "loss": str(loss), "fits": fits,
        })
        if not fits:
            overflows.append({
                "row": idx,
                "value": str(x),
                "converted": str(c),
                "reason": f"转换后精度 {len(c.as_tuple().digits)} 位超过 p={p}",
            })
    return converted, losses, overflows, per_row


# ---------------------------------------------------------------- 仓

class Warehouse:
    """一个目录即一个仓：catalog.json + shards/ + staging/ + meta/。"""

    def __init__(self, root: Path):
        self.root = Path(root)

    # ---------------- 初始化 / 打开 / 恢复 ----------------

    @classmethod
    def init(cls, root, sources: dict):
        """从只读源 Parquet 建仓。源文件被逐字节复制为 v1，源本身绝不写入。"""
        root = Path(root)
        if (root / "catalog.json").exists():
            raise WarehouseError(f"{root} 已是仓")
        if len(sources) > MAX_SHARDS:
            raise WarehouseError(f"分片数 {len(sources)} 超过上限 {MAX_SHARDS}")
        for d in ("shards", "staging", "meta"):
            (root / d).mkdir(parents=True, exist_ok=True)

        schema1 = None
        shards = {}
        startup_bytes = 0
        for name, src in sources.items():
            if not name or not all(c.isalnum() or c in "_-" for c in name):
                raise WarehouseError(f"非法分片名 {name!r}")
            t = pq.read_table(src)
            Warehouse._validate_source_table(name, t)
            amt = t.schema.field("amount").type
            if schema1 is None:
                schema1 = {"precision": amt.precision, "scale": amt.scale, "rounding": None}
            elif (amt.precision, amt.scale) != (schema1["precision"], schema1["scale"]):
                raise WarehouseError("所有分片源 schema 必须一致")
            dst = root / "shards" / f"{name}__v1.parquet"
            shutil.copyfile(src, dst)  # 源只读；仓内副本计入配额
            nbytes = dst.stat().st_size
            startup_bytes += nbytes
            agg = _aggregate(t.column("amount").to_pylist())
            shards[name] = {
                "active": 1,
                "versions": {"1": {
                    "schema_version": 1,
                    "file": f"shards/{name}__v1.parquet",
                    "bytes": nbytes,
                    "summary": _summary_to_json(agg),
                    "recon": None,
                }},
                "failed": {},
            }

        cat = {
            "format": 1,
            "catalog_version": 0,
            "schema_versions": {"1": dict(schema1, kind="source")},
            "plan": None,
            "shards": shards,
            "readers": {},
            "migration_lock": None,
            "startup_bytes": startup_bytes,
            "max_new_file_bytes": 0,
        }
        tmp = cls(root)
        tmp._store_catalog(cat)
        return cls.open(root)

    @staticmethod
    def _validate_source_table(name, t):
        cols = {f.name: f.type for f in t.schema}
        if set(cols) != {"id", "amount"}:
            raise WarehouseError(f"{name}: 只处理 id+amount 两列，得到 {sorted(cols)}")
        if not pa.types.is_int64(cols["id"]):
            raise WarehouseError(f"{name}: id 须为 int64")
        if not pa.types.is_decimal128(cols["amount"]):
            raise WarehouseError(f"{name}: amount 须为 decimal128")
        amt = cols["amount"]
        if not (1 <= amt.precision <= MAX_PRECISION and 0 <= amt.scale <= MAX_SCALE):
            raise WarehouseError(f"{name}: 源 decimal({amt.precision},{amt.scale}) 超出输入族范围")
        if t.num_rows > MAX_ROWS_PER_SHARD:
            raise WarehouseError(f"{name}: 行数 {t.num_rows} 超过 {MAX_ROWS_PER_SHARD}")

    @classmethod
    def open(cls, root):
        wh = cls(Path(root))
        if not (wh.root / "catalog.json").exists():
            raise CorruptCatalog(f"{root} 不是仓（缺 catalog.json）")
        wh.recover(clear_lock=False)
        return wh

    def recover(self, clear_lock=False):
        """启动恢复：清 staging 孤儿、清目录未引用的碎片文件、核对引用完整。

        目录从不引用半文件：staging 里的一切都可回收；shards/ 里未被目录
        引用的文件是“rename 后、目录提交前”崩溃的遗留，同样回收。
        """
        with self._locked() as cat:
            staging_removed = []
            for f in (self.root / "staging").iterdir():
                if f.is_file():
                    f.unlink()
                    staging_removed.append(f.name)
            referenced = {
                v["file"] for sh in cat["shards"].values() for v in sh["versions"].values()
            }
            shards_removed = []
            for f in (self.root / "shards").iterdir():
                rel = f"shards/{f.name}"
                if f.is_file() and rel not in referenced:
                    f.unlink()
                    shards_removed.append(rel)
            missing = [rel for rel in sorted(referenced) if not (self.root / rel).exists()]
            if missing:
                raise CorruptCatalog(f"目录引用了不存在的文件: {missing}")
            meta_refs = set()
            for sh in cat["shards"].values():
                for v in sh["versions"].values():
                    if v.get("recon"):
                        meta_refs.add(v["recon"])
                for fa in sh["failed"].values():
                    if fa.get("recon"):
                        meta_refs.add(fa["recon"])
            meta_removed = []
            for f in (self.root / "meta").iterdir():
                rel = f"meta/{f.name}"
                if f.is_file() and rel not in meta_refs:
                    f.unlink()
                    meta_removed.append(rel)
            if clear_lock and cat["migration_lock"] is not None:
                cat["migration_lock"] = None
            return {
                "staging_removed": staging_removed,
                "shards_removed": shards_removed,
                "meta_removed": meta_removed,
                "lock_cleared": bool(clear_lock),
            }

    # ---------------- 目录持久化 ----------------

    def _load_catalog(self):
        with open(self.root / "catalog.json") as fh:
            return json.load(fh)

    def _store_catalog(self, cat):
        cat["catalog_version"] += 1
        tmp = self.root / "catalog.json.tmp"
        tmp.write_text(json.dumps(cat, indent=2, sort_keys=True))
        _fsync_file(tmp)
        os.replace(tmp, self.root / "catalog.json")
        _fsync_dir(self.root)

    @contextmanager
    def _locked(self):
        """flock 串行化目录读-改-写；无论是否抛错都提交（各变更点均设计为可提交）。"""
        fd = os.open(self.root / "catalog.lock", os.O_RDWR | os.O_CREAT)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            cat = self._load_catalog()
            try:
                yield cat
            finally:
                self._store_catalog(cat)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    # ---------------- 计量 ----------------

    def data_bytes(self):
        """真实测量：shards/ 与 staging/ 下物理 Parquet 总字节。"""
        total = 0
        for d in ("shards", "staging"):
            dd = self.root / d
            if dd.exists():
                for f in dd.iterdir():
                    if f.is_file():
                        total += f.stat().st_size
        return total

    def meta_bytes(self):
        """目录元数据单列统计：catalog.json + meta/ 对账文件。"""
        total = (self.root / "catalog.json").stat().st_size
        md = self.root / "meta"
        if md.exists():
            for f in md.iterdir():
                if f.is_file():
                    total += f.stat().st_size
        return total

    @staticmethod
    def quota_bytes(cat):
        """配额 = 启动时已有分片总字节 + 2 × 最大单片新文件字节。"""
        return cat["startup_bytes"] + 2 * cat["max_new_file_bytes"]

    # ---------------- 计划 ----------------

    def set_plan(self, precision: int, scale: int, rounding: str):
        validate_plan(precision, scale, rounding)
        with self._locked() as cat:
            for vid, s in cat["schema_versions"].items():
                if (s["precision"], s["scale"], s["rounding"]) == (precision, scale, rounding):
                    target = int(vid)
                    break
            else:
                target = max(int(v) for v in cat["schema_versions"]) + 1
                cat["schema_versions"][str(target)] = {
                    "precision": precision, "scale": scale,
                    "rounding": rounding, "kind": "target",
                }
            cat["plan"] = {"precision": precision, "scale": scale,
                           "rounding": rounding, "schema_version": target}
            return target

    # ---------------- 读者 ----------------

    def begin_read(self, schema_version: int):
        """绑定 (schema 版本, 目录快照)；钉住每个分片将用到的物理文件。"""
        with self._locked() as cat:
            if str(schema_version) not in cat["schema_versions"]:
                raise UnknownSchema(f"未知 schema 版本 {schema_version}")
            if len(cat["readers"]) >= MAX_READERS:
                raise MaxReadersExceeded(f"活动读者上限 {MAX_READERS}")
            rid = f"r{uuid.uuid4().hex[:8]}"
            resolutions = {}
            for name, sh in cat["shards"].items():
                resolutions[name] = self._resolve(cat, name, sh, schema_version)
            cat["readers"][rid] = {
                "id": rid,
                "schema_version": schema_version,
                "snapshot": cat["catalog_version"],
                "created": time.time(),
                "shards": resolutions,
            }
            return rid

    @staticmethod
    def _resolve(cat, name, sh, target_schema):
        """决定读者对某分片的取数路径：物理新片 / 读时转换 / 明确失败 / 已退役。"""
        for vnum, v in sh["versions"].items():
            if v["schema_version"] == target_schema:
                return {"kind": "physical", "version": int(vnum),
                        "file": v["file"], "summary": v["summary"]}
        failed = sh["failed"].get(str(target_schema))
        if failed is not None:
            return {"kind": "failed", "error": failed}
        active_v = sh["versions"][str(sh["active"])]
        if active_v["schema_version"] < target_schema:
            return {"kind": "read_time", "version": sh["active"], "file": active_v["file"]}
        raise SchemaRetired(
            f"shard {name!r}: schema v{target_schema} 的物理数据已回收"
            f"（当前活动 schema v{active_v['schema_version']}）"
        )

    def end_read(self, reader_id: str):
        """释放读者。文件自此“可回收”，但删除只发生在配额压力或显式 gc 时。"""
        with self._locked() as cat:
            if reader_id not in cat["readers"]:
                raise UnknownReader(reader_id)
            del cat["readers"][reader_id]
            return {"released": reader_id}

    def _reclaim_locked(self, cat):
        pinned = set()
        for r in cat["readers"].values():
            for shname, info in r["shards"].items():
                if info["kind"] in ("physical", "read_time"):
                    pinned.add((shname, info["version"]))
        reclaimed = []
        for name, sh in cat["shards"].items():
            for vnum in list(sh["versions"]):
                vnum_i = int(vnum)
                if vnum_i == sh["active"] or (name, vnum_i) in pinned:
                    continue
                v = sh["versions"].pop(vnum)
                fp = self.root / v["file"]
                if fp.exists():
                    fp.unlink()
                reclaimed.append({"shard": name, "version": vnum_i,
                                  "file": v["file"], "bytes": v["bytes"]})
        return reclaimed

    def gc(self):
        with self._locked() as cat:
            return {"reclaimed": self._reclaim_locked(cat)}

    # ---------------- 查询 ----------------

    def query(self, reader_id=None, schema_version=None, use_summary=True):
        """按读者绑定的 (schema, 快照) 聚合 sum/count/min/max/rows。

        已发布分片用校验过的预计算摘要（可选回读物理文件），未迁移分片
        读时逐行转换；失败分片明确抛 MigrationFailed，绝不混入部分金额。
        """
        if reader_id is None:
            if schema_version is None:
                raise WarehouseError("query 需要 reader_id 或 schema_version")
            rid = self.begin_read(schema_version)
            try:
                return self.query(reader_id=rid, use_summary=use_summary)
            finally:
                self.end_read(rid)
        cat = self._load_catalog()
        if reader_id not in cat["readers"]:
            raise UnknownReader(reader_id)
        reader = cat["readers"][reader_id]
        target = cat["schema_versions"][str(reader["schema_version"])]
        combined = {"rows": 0, "non_null": 0, "sum": Decimal(0),
                    "any": False, "min": None, "max": None}
        shard_paths = {}
        for name, res in reader["shards"].items():
            agg = self._shard_aggregate(name, res, target, use_summary)
            shard_paths[name] = {k: res[k] for k in ("kind",) if k in res}
            if "version" in res:
                shard_paths[name]["version"] = res["version"]
            combined["rows"] += agg["rows"]
            combined["non_null"] += agg["non_null"]
            if agg["sum"] is not None:
                combined["sum"] += agg["sum"]
                combined["any"] = True
            if agg["min"] is not None:
                combined["min"] = agg["min"] if combined["min"] is None or agg["min"] < combined["min"] else combined["min"]
            if agg["max"] is not None:
                combined["max"] = agg["max"] if combined["max"] is None or agg["max"] > combined["max"] else combined["max"]
        return {
            "schema_version": reader["schema_version"],
            "snapshot": reader["snapshot"],
            "rows": combined["rows"],
            "count": combined["non_null"],
            "sum": str(combined["sum"]) if combined["any"] else None,
            "min": None if combined["min"] is None else str(combined["min"]),
            "max": None if combined["max"] is None else str(combined["max"]),
            "shards": shard_paths,
        }

    def _shard_aggregate(self, name, res, target, use_summary):
        kind = res["kind"]
        if kind == "failed":
            raise MigrationFailed(name, res["error"].get("reason", "failed"))
        if kind == "physical":
            if use_summary:
                return _summary_from_json(res["summary"])
            t = pq.read_table(self.root / res["file"])
            return _aggregate(t.column("amount").to_pylist())
        # read_time：与迁移路径共用同一逐行转换
        t = pq.read_table(self.root / res["file"])
        values = t.column("amount").to_pylist()
        converted, _, overflows, _ = _convert_values(values, target)
        if overflows:
            raise MigrationFailed(name,
                                  f"读时转换溢出 {len(overflows)} 行，首行: {overflows[0]}")
        return _aggregate(converted)

    def scan(self, shard, schema_version=None, reader_id=None):
        """行级读取：返回 [{id, amount(, loss)}]，amount 已是目标 schema 语义。"""
        cat = self._load_catalog()
        if shard not in cat["shards"]:
            raise UnknownShard(shard)
        if reader_id is not None:
            if reader_id not in cat["readers"]:
                raise UnknownReader(reader_id)
            reader = cat["readers"][reader_id]
            if shard not in reader["shards"]:
                raise UnknownShard(shard)
            res = reader["shards"][shard]
            target = cat["schema_versions"][str(reader["schema_version"])]
        else:
            if schema_version is None:
                raise WarehouseError("scan 需要 reader_id 或 schema_version")
            res = self._resolve(cat, shard, cat["shards"][shard], schema_version)
            target = cat["schema_versions"][str(schema_version)]
        if res["kind"] == "failed":
            raise MigrationFailed(shard, res["error"].get("reason", "failed"))
        t = pq.read_table(self.root / res["file"])
        ids = t.column("id").to_pylist()
        amounts = t.column("amount").to_pylist()
        rows = []
        if res["kind"] == "physical":
            for i, a in zip(ids, amounts):
                rows.append({"id": i, "amount": None if a is None else str(a)})
        else:
            converted, losses, overflows, _ = _convert_values(amounts, target)
            if overflows:
                raise MigrationFailed(shard, f"读时转换溢出: {overflows[0]}")
            li = iter(losses)
            for i, a, c in zip(ids, amounts, converted):
                row = {"id": i, "amount": None if c is None else str(c)}
                if a is not None:
                    row["loss"] = str(next(li))
                rows.append(row)
        return {"shard": shard, "kind": res["kind"], "rows": rows}

    # ---------------- 迁移 ----------------

    def migrate_shard(self, name, crash_at=None):
        """把单个分片从当前活动版本迁移到 plan 的目标 schema。

        流程：取迁移锁 → 逐行转换 → 溢出则整片拒绝 → 写 staging → 对账 →
        配额检查 → 原子 rename → 提交目录 → 回收无引用旧片。
        crash_at 可注入命名中断点（见 CRASH_POINTS）。
        """
        if crash_at is not None and crash_at not in CRASH_POINTS:
            raise WarehouseError(f"未知中断点 {crash_at!r}")
        with self._locked() as cat:
            if cat["plan"] is None:
                raise WarehouseError("尚未 set_plan")
            if cat["migration_lock"] is not None:
                raise MigrationInProgress(
                    f"迁移锁被 {cat['migration_lock']['shard']!r} 持有；"
                    "若为崩溃遗留请先 recover"
                )
            if name not in cat["shards"]:
                raise UnknownShard(name)
            sh = cat["shards"][name]
            target_schema = cat["plan"]["schema_version"]
            active_v = sh["versions"][str(sh["active"])]
            if active_v["schema_version"] == target_schema:
                cat["migration_lock"] = None
                return {"status": "already", "shard": name,
                        "schema_version": target_schema}
            src_file = active_v["file"]
            src_version = sh["active"]
            nextv = max(int(v) for v in sh["versions"]) + 1
            token = uuid.uuid4().hex[:12]
            cat["migration_lock"] = {"shard": name, "token": token,
                                     "target_schema": target_schema,
                                     "started": time.time()}
            plan = dict(cat["plan"])

        # ---- 锁外做重活：读源、逐行转换 ----
        table = pq.read_table(self.root / src_file)
        ids = table.column("id")
        amounts = table.column("amount").to_pylist()
        converted, losses, overflows, per_row = _convert_values(amounts, plan)
        for row, idv in zip(per_row, ids.to_pylist()):
            row["id"] = idv

        if overflows:
            recon_rel = f"meta/{name}__v{nextv}.failed.json"
            self._write_json(self.root / recon_rel, {
                "shard": name, "version": nextv, "schema_version": target_schema,
                "status": "rejected", "reason": "overflow",
                "source": {"version": src_version, "file": src_file},
                "target": plan, "overflows": overflows, "rows": per_row,
            })
            with self._locked() as cat:
                sh = cat["shards"][name]
                sh["failed"][str(target_schema)] = {
                    "reason": "overflow",
                    "overflow_count": len(overflows),
                    "overflows": overflows,
                    "recon": recon_rel,
                    "time": time.time(),
                }
                cat["migration_lock"] = None
            return {"status": "rejected", "shard": name, "reason": "overflow",
                    "overflow_count": len(overflows), "overflows": overflows,
                    "recon": recon_rel}

        # ---- 写 staging（目录不可见）----
        staging = self.root / "staging" / f"{name}__v{nextv}__{token}.parquet"
        new_table = pa.table({
            "id": ids,
            "amount": pa.array(converted, type=pa.decimal128(
                plan["precision"], plan["scale"])),
        })
        pq.write_table(new_table, staging)
        _fsync_file(staging)
        size = staging.stat().st_size

        if crash_at == "after_output_before_manifest":
            raise SimulatedCrash("crash: after_output_before_manifest")

        # ---- 对账：重读 staging，逐行核对 + 汇总恒等式 ----
        summary, recon = self._reconcile(
            staging, name, nextv, target_schema, src_version, src_file,
            table, per_row, plan)
        recon_rel = f"meta/{name}__v{nextv}.recon.json"
        self._write_json(self.root / recon_rel, recon)

        # ---- 配额 + 发布（同一临界区）----
        with self._locked() as cat:
            sh = cat["shards"][name]
            effective_max = max(cat["max_new_file_bytes"], size)
            quota = cat["startup_bytes"] + 2 * effective_max
            total = self.data_bytes()
            reclaimed = []
            if total > quota:
                # 先回收无读者引用的旧片；仍不足才背压（绝不删活读者依赖）
                reclaimed = self._reclaim_locked(cat)
                total = self.data_bytes()
            if total > quota:
                staging.unlink(missing_ok=True)  # 不发布，staging 回收
                cat["migration_lock"] = None
                raise Backpressure(
                    f"磁盘配额不足：需要 {total} 字节 > 配额 {quota} 字节；"
                    "旧片仍被读者引用，可稍后重试",
                    needed_bytes=total - quota, available_bytes=quota)
            final_rel = f"shards/{name}__v{nextv}.parquet"
            os.replace(staging, self.root / final_rel)
            _fsync_dir(self.root / "shards")
            sh["versions"][str(nextv)] = {
                "schema_version": target_schema,
                "file": final_rel,
                "bytes": size,
                "summary": summary,
                "recon": recon_rel,
            }
            sh["active"] = nextv
            sh["failed"].pop(str(target_schema), None)
            cat["max_new_file_bytes"] = effective_max
            cat["migration_lock"] = None
            if crash_at == "after_manifest_before_old_reader_release":
                raise SimulatedCrash("crash: after_manifest_before_old_reader_release")
        return {"status": "published", "shard": name, "version": nextv,
                "schema_version": target_schema, "bytes": size,
                "summary": summary, "recon": recon_rel, "reclaimed": reclaimed}

    def _reconcile(self, staging, name, nextv, target_schema, src_version,
                   src_file, src_table, per_row, plan):
        """对账：行数、非 NULL 数、逐行损失、原总和=新总和+损失总和、目标语义聚合。"""
        t = pq.read_table(staging)
        new_ids = t.column("id").to_pylist()
        new_vals = t.column("amount").to_pylist()
        old_ids = src_table.column("id").to_pylist()
        if new_ids != old_ids:
            raise WarehouseError(f"{name}: 对账失败：行 id 序列不一致（行数 {len(old_ids)}→{len(new_ids)}）")
        sum_old = Decimal(0)
        sum_new = Decimal(0)
        sum_loss = Decimal(0)
        non_null = 0
        with localcontext() as ctx:
            ctx.prec = DECIMAL_PREC
            for i, row in enumerate(per_row):
                if row["old"] is None:
                    if new_vals[i] is not None:
                        raise WarehouseError(f"{name}: 对账失败：第 {i} 行 NULL 未保留")
                    continue
                non_null += 1
                old = Decimal(row["old"])
                new = Decimal(row["new"])
                loss = Decimal(row["loss"])
                if new_vals[i] != new:
                    raise WarehouseError(f"{name}: 对账失败：第 {i} 行文件值 {new_vals[i]} != 转换值 {new}")
                if old - new != loss:  # 逐行核对，不依赖正负抵消
                    raise WarehouseError(f"{name}: 对账失败：第 {i} 行损失 {old}-{new} != {loss}")
                sum_old += old
                sum_new += new
                sum_loss += loss
            if sum_old != sum_new + sum_loss:
                raise WarehouseError(
                    f"{name}: 对账失败：原总和 {sum_old} != 新总和 {sum_new} + 损失 {sum_loss}")
        file_agg = _aggregate(new_vals)
        conv_agg = _aggregate([None if r["old"] is None else Decimal(r["new"]) for r in per_row])
        if (file_agg["rows"], file_agg["non_null"], file_agg["sum"],
                file_agg["min"], file_agg["max"]) != (
                conv_agg["rows"], conv_agg["non_null"], conv_agg["sum"],
                conv_agg["min"], conv_agg["max"]):
            raise WarehouseError(f"{name}: 对账失败：文件聚合与逐行转换聚合不一致")
        summary = _summary_to_json(conv_agg, loss_sum=sum_loss, source_sum=sum_old)
        recon = {
            "shard": name, "version": nextv, "schema_version": target_schema,
            "status": "published",
            "source": {"version": src_version, "file": src_file},
            "target": plan,
            "totals": {
                "rows": conv_agg["rows"], "non_null": non_null,
                "source_sum": str(sum_old), "converted_sum": str(sum_new),
                "loss_sum": str(sum_loss),
                "identity_holds": True,
                "min": summary["min"], "max": summary["max"],
            },
            "rows": per_row,
            "overflows": [],
        }
        return summary, recon

    @staticmethod
    def _write_json(path, obj):
        path.write_text(json.dumps(obj, indent=2, sort_keys=True))
        _fsync_file(path)

    # ---------------- 状态 ----------------

    def status(self):
        cat = self._load_catalog()
        return {
            "catalog_version": cat["catalog_version"],
            "schema_versions": cat["schema_versions"],
            "plan": cat["plan"],
            "shards": {
                name: {
                    "active": sh["active"],
                    "versions": {v: {"schema_version": vv["schema_version"],
                                     "bytes": vv["bytes"], "file": vv["file"]}
                                 for v, vv in sh["versions"].items()},
                    "failed": sorted(sh["failed"]),
                } for name, sh in cat["shards"].items()
            },
            "readers": {rid: {"schema_version": r["schema_version"],
                              "snapshot": r["snapshot"]}
                        for rid, r in cat["readers"].items()},
            "migration_lock": cat["migration_lock"],
            "startup_bytes": cat["startup_bytes"],
            "max_new_file_bytes": cat["max_new_file_bytes"],
            "quota_bytes": self.quota_bytes(cat),
            "data_bytes": self.data_bytes(),
            "meta_bytes": self.meta_bytes(),
        }
