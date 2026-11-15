"""版本化序列存储：piece-table + 每修订增量文件，不做全量序列复制。

磁盘布局（store 目录）：
  origin.fa            原始 FASTA 的完整字节保留（唯一一份原件）
  feature-defs.json    不可变 feature 规范定义（含每段粘附规则）
  rev-NNNNNN.adds.bin  该修订所有 insert 拼接的新增字节（编辑来源片段）
  rev-NNNNNN.seq.json  该修订的编辑记录与 piece-table 拼接指令（序列增量）
  rev-NNNNNN.feat.json 该修订对 feature 位置/溯源索引的增量
  manifest.json        提交状态清单，最后原子 rename 发布

恢复/查询只读取 manifest 中登记的文件；未登记的孤儿文件一律不可见。
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path

ALPHABET = set("ACGTN")
MAX_ORIGIN_BYTES = 1 << 20          # 1 MiB 源序列
MAX_ADDS_BYTES = 2 << 20            # 累计新增 2 MiB
MAX_VERSIONS = 20
COMPACT = dict(separators=(",", ":"), ensure_ascii=False)


class RevisionRejected(Exception):
    """整批拒绝：base 不匹配、非法重叠、越界、字母非法等，拒绝时不写任何文件。"""

    def __init__(self, errors):
        self.errors = list(errors) if isinstance(errors, (list, tuple)) else [str(errors)]
        super().__init__("; ".join(self.errors))


class InjectedCrash(RuntimeError):
    """测试用注入异常：模拟序列数据已写、feature 索引未完成时的崩溃。"""


class UnknownVersion(KeyError):
    pass


# ---------------------------------------------------------------- 基础工具
def load(path):
    """读取 FASTA，丢弃标题行/换行，小写转大写，校验字母表。"""
    seq = "".join(
        line.strip() for line in Path(path).read_text().splitlines()
        if not line.startswith(">")
    ).upper()
    bad = sorted(set(seq) - ALPHABET)
    if bad:
        raise RevisionRejected([f"非法序列字母: {''.join(bad)}（仅允许 ACGTN）"])
    if len(seq) > MAX_ORIGIN_BYTES:
        raise RevisionRejected([f"源序列 {len(seq)} 字节超过上限 {MAX_ORIGIN_BYTES}"])
    return seq


def apply(sequence, edits):
    """旧实现保留：无版本、无溯源的一次性拼接（baseline 演示用，新管线不调用）。"""
    out = []
    pos = 0
    for e in sorted(edits, key=lambda x: x["start"]):
        out.extend([sequence[pos:e["start"]], e["insert"]])
        pos = e["end"]
    return "".join(out) + sequence[pos:]


# ------------------------------------------------------- piece 元组辅助
# 原碱基 piece: ("O", origin_start, origin_end)
# 新增 piece:   ("A", revision, edit_id, add_offset, length)
def plen(p):
    return p[2] - p[1] if p[0] == "O" else p[4]


def psub(p, lo, hi):
    """取 piece 内部父坐标相对区间 [lo, hi)，溯源随切片保留。"""
    if p[0] == "O":
        return ("O", p[1] + lo, p[1] + hi)
    return ("A", p[1], p[2], p[3] + lo, hi - lo)


def _atomic_write(path: Path, payload: bytes):
    """同目录 tmp 文件 + os.replace，单次文件级原子落盘。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


# ================================================================ Store
class Store:
    def __init__(self, store_dir: Path):
        self.dir = Path(store_dir)
        self._manifest = None
        self._origin_seq = None
        self._adds_cache = {}
        self._pieces_cache = {}
        self._seq_cache = {}
        self._engine = None
        self._add_blob = None              # 发布期当前修订的 adds blob（未提交时旁路）
        self._pending_revision = None

    def feature_engine(self):
        """惰性挂接 feature 引擎并注册发布回调（引擎负责写 feat 增量）。"""
        if self._engine is None:
            from features import FeatureEngine
            self._engine = FeatureEngine(self)
            self._feature_delta_hook = self._engine.compute_delta
        return self._engine

    # -------------------------------- 建库 / 打开
    @classmethod
    def create(cls, store_dir, fasta_path, feature_defs=None) -> "Store":
        store_dir = Path(store_dir)
        if store_dir.exists() and any(store_dir.iterdir()):
            raise FileExistsError(f"store 目录已存在且非空: {store_dir}")
        store_dir.mkdir(parents=True)
        raw = Path(fasta_path).read_bytes()
        seq = load(fasta_path)
        _atomic_write(store_dir / "origin.fa", raw)
        from features import normalize_defs
        defs = normalize_defs(feature_defs or [], len(seq))
        _atomic_write(
            store_dir / "feature-defs.json",
            json.dumps({"features": defs}, **COMPACT).encode(),
        )
        manifest = {
            "format": 1,
            "origin_file": "origin.fa",
            "origin_length": len(seq),
            "origin_sha256": hashlib.sha256(seq.encode()).hexdigest(),
            "feature_defs_file": "feature-defs.json",
            "adds_total_bytes": 0,
            "head_version": 0,
            "revisions": [],
        }
        _atomic_write(store_dir / "manifest.json", json.dumps(manifest, indent=2).encode())
        return cls.open(store_dir)

    @classmethod
    def open(cls, store_dir) -> "Store":
        self = cls(Path(store_dir))
        mf = self.dir / "manifest.json"
        if not mf.exists():
            raise FileNotFoundError(f"缺少 manifest.json，不是合法 store: {store_dir}")
        self._manifest = json.loads(mf.read_text())
        # 清理未登记的临时/孤儿文件：它们绝不参与恢复与查询
        listed = {"manifest.json", "origin.fa", self._manifest["feature_defs_file"]}
        for r in self._manifest["revisions"]:
            listed.update({r["adds_file"], r["seq_file"], r["feat_file"]})
        for child in self.dir.iterdir():
            if child.is_file() and child.name not in listed:
                child.unlink()
        return self

    # -------------------------------- 基本访问
    @property
    def head_version(self):
        return self._manifest["head_version"]

    @property
    def versions(self):
        return [0] + [r["target"] for r in self._manifest["revisions"]]

    @property
    def origin_sequence(self) -> str:
        if self._origin_seq is None:
            self._origin_seq = load(self.dir / self._manifest["origin_file"])
        return self._origin_seq

    def length(self, version):
        self._require_version(version)
        if version == 0:
            return self._manifest["origin_length"]
        return self._rev_record(version)["length"]

    def _require_version(self, version):
        if version not in self.versions:
            raise UnknownVersion(f"版本 {version} 不存在或未完整提交（可见版本: {self.versions}）")

    def _rev_record(self, target):
        for r in self._manifest["revisions"]:
            if r["target"] == target:
                return r
        raise UnknownVersion(f"版本 {target} 未提交")

    def adds_bytes(self, revision, offset, length, blob=None) -> bytes:
        if blob is not None:                 # 发布期：当前修订尚未登记 manifest
            return blob[offset:offset + length]
        if revision == self._pending_revision and self._add_blob is not None:
            return self._add_blob[offset:offset + length]
        if revision not in self._adds_cache:
            rec = self._rev_record(revision)
            self._adds_cache[revision] = (self.dir / rec["adds_file"]).read_bytes()
        return self._adds_cache[revision][offset:offset + length]

    # -------------------------------- 校验
    def _validate(self, base_version, target_version, edits):
        errs = []
        if base_version != self.head_version:
            errs.append(f"base_version {base_version} 与当前 head {self.head_version} 不匹配")
        if target_version != base_version + 1:
            errs.append(f"target_version 必须为 base+1（收到 {base_version}->{target_version}）")
        if self.head_version >= MAX_VERSIONS:
            errs.append(f"版本数超过上限 {MAX_VERSIONS}")
        if not isinstance(edits, list) or not edits:
            errs.append("edits 必须为非空列表")
            raise RevisionRejected(errs)

        norm = []
        seen_ids = set()
        base_ok = base_version == self.head_version
        parent_len = self.length(base_version) if base_ok else None
        for i, e in enumerate(edits):
            try:
                s, t, ins, eid = e["start"], e["end"], e["insert"], e["id"]
            except (KeyError, TypeError):
                errs.append(f"edit[{i}] 缺少 start/end/insert/id")
                continue
            if not isinstance(s, int) or not isinstance(t, int):
                errs.append(f"edit {eid!r}: start/end 必须为整数")
            elif base_ok and not (0 <= s <= t <= parent_len):
                errs.append(f"edit {eid!r}: 坐标 [{s},{t}) 越界（父版本长度 {parent_len}）")
            if not isinstance(ins, str) or set(ins.upper()) - ALPHABET:
                errs.append(f"edit {eid!r}: insert 含非 ACGTN 字符: {ins!r}")
            if not eid or not isinstance(eid, str):
                errs.append(f"edit[{i}]: id 必须为非空字符串")
            elif eid in seen_ids:
                errs.append(f"edit id 重复: {eid!r}")
            else:
                seen_ids.add(eid)
            norm.append({
                "id": eid, "s": s, "t": t,
                "ins": ins.upper(), "order": e.get("order", 0),
            })

        # 同锚点分组与不重叠校验（全部基于父版本旧坐标）
        norm.sort(key=lambda e: (e["s"], e["order"], e["t"]))
        groups = []
        for e in norm:
            if groups and groups[-1][0]["s"] == e["s"]:
                groups[-1].append(e)
            else:
                groups.append([e])
        prev_end = 0
        for g in groups:
            if len(g) > 1:
                orders = [x.get("order") for x in g]
                if any(o is None for o in orders) or len(set(orders)) != len(orders):
                    errs.append(
                        f"锚点 {g[0]['s']} 存在同点多项编辑，必须各自给出唯一显式 order: "
                        + ",".join(x["id"] for x in g))
                # 同点多项仅在全部为零长插入（s==t）时合法；含删除区间即重叠
                if any(x["t"] > x["s"] for x in g):
                    errs.append(
                        f"锚点 {g[0]['s']} 的同点编辑中存在非零长区间，旧坐标重叠: "
                        + ",".join(x["id"] for x in g))
                g.sort(key=lambda x: x["order"])
            if g[0]["s"] < prev_end:
                errs.append(
                    f"编辑区间在旧坐标上重叠: 锚点 {g[0]['s']} < 前组结束 {prev_end}")
            prev_end = max(prev_end, max(x["t"] for x in g))

        adds = sum(len(e["ins"]) for e in norm)
        if self._manifest["adds_total_bytes"] + adds > MAX_ADDS_BYTES:
            errs.append(f"累计新增字节将超过 {MAX_ADDS_BYTES}（本次 {adds}）")
        if errs:
            raise RevisionRejected(errs)
        return norm

    # -------------------------------- piece-table 拼接
    def _splice_pieces(self, parent_pieces, edits, revision):
        """按父坐标把 edits 拼到 piece 表上；同点零长插入按 order 自然落位。"""
        out = []
        pi, coff = 0, 0
        cur = parent_pieces[0] if parent_pieces else None
        n = len(parent_pieces)

        def advance_to(anchor):
            nonlocal pi, coff, cur
            while cur is not None and coff < anchor:
                l = plen(cur)
                if coff + l <= anchor:
                    out.append(cur)
                    pi += 1
                    coff += l
                    cur = parent_pieces[pi] if pi < n else None
                else:
                    k = anchor - coff
                    out.append(psub(cur, 0, k))
                    cur = psub(cur, k, l)
                    coff = anchor

        def delete(span):
            nonlocal pi, coff, cur
            while span > 0:
                l = plen(cur)
                if l <= span:
                    span -= l
                    pi += 1
                    coff += l
                    cur = parent_pieces[pi] if pi < n else None
                else:
                    cur = psub(cur, span, l)
                    coff += span
                    span = 0

        for e in edits:
            advance_to(e["s"])
            delete(e["t"] - e["s"])
            if e["ins"]:
                # e["off"] 为该 edit 在本修订 adds blob 内的偏移（发布期注入）；
                # 回放时 _load_edits 同样提供，保证共享 blob 可逐段回查。
                out.append(("A", revision, e["id"], e.get("off", 0), len(e["ins"])))
        if cur is not None:
            out.append(cur)
            out.extend(parent_pieces[pi + 1:])
        return out

    def pieces(self, version):
        if version == getattr(self, "_pending_revision", None):
            return self._pieces_cache[version]   # 发布期：已拼接但尚未提交
        self._require_version(version)
        if version in self._pieces_cache:
            return self._pieces_cache[version]
        pieces = [("O", 0, self._manifest["origin_length"])]
        for rec in self._manifest["revisions"]:
            if rec["target"] > version:
                break
            edits = self._load_edits(rec)
            pieces = self._splice_pieces(pieces, edits, rec["target"])
            self._pieces_cache[rec["target"]] = pieces
        return pieces

    def _load_edits(self, rec):
        seq = json.loads((self.dir / rec["seq_file"]).read_text())
        return [
            {"id": e["id"], "s": e["s"], "t": e["t"], "order": e["order"],
             "off": e["off"],
             "ins": self.adds_bytes(rec["target"], e["off"], e["n"]).decode()}
            for e in seq["edits"]
        ]

    def sequence(self, version) -> str:
        """由 origin 片段与各修订 adds 片段重建完整序列（piece-table 回放）。"""
        self._require_version(version)
        if version in self._seq_cache:
            return self._seq_cache[version]
        parts = []
        for p in self.pieces(version):
            if p[0] == "O":
                parts.append(self.origin_sequence[p[1]:p[2]])
            else:
                parts.append(self.adds_bytes(p[1], p[3], p[4]).decode())
        seq = "".join(parts)
        self._seq_cache[version] = seq
        return seq

    def piece_ranges(self, version):
        """当前版本每个 piece 的绝对新区间，供 feature 索引定位坐标。"""
        origin_index, add_index = [], {}
        pos = 0
        for p in self.pieces(version):
            l = plen(p)
            if p[0] == "O":
                origin_index.append((p[1], pos, p))
            else:
                add_index[(p[1], p[2], p[3])] = (pos, pos + l)
            pos += l
        return origin_index, add_index, pos

    # -------------------------------- 发布修订
    def apply_revision(self, base_version, target_version, edits, crash=None):
        """原子发布：校验失败整批拒绝且不落盘；崩溃注入点用于验证发布原子性。"""
        norm = self._validate(base_version, target_version, edits)

        adds_blob = b"".join(e["ins"].encode() for e in norm)
        off, edit_recs = 0, []
        for e in norm:
            n = len(e["ins"])
            e["off"] = off
            edit_recs.append(
                {"id": e["id"], "s": e["s"], "t": e["t"],
                 "off": off, "n": n, "order": e["order"]})
            off += n

        new_pieces = self._splice_pieces(self.pieces(base_version), norm, target_version)
        new_len = sum(plen(p) for p in new_pieces)
        self._pending_revision, self._add_blob = target_version, adds_blob
        try:
            new_seq = "".join(
                self.origin_sequence[p[1]:p[2]] if p[0] == "O"
                else self.adds_bytes(p[1], p[3], p[4]).decode()
                for p in new_pieces)

            adds_name = f"rev-{target_version:06d}.adds.bin"
            seq_name = f"rev-{target_version:06d}.seq.json"
            feat_name = f"rev-{target_version:06d}.feat.json"
            # 1) 序列数据（新增片段 + 编辑/piece 拼接记录）
            _atomic_write(self.dir / adds_name, adds_blob)
            seq_payload = {
                "v": target_version, "base": base_version,
                "parent_len": self.length(base_version), "length": new_len,
                "adds_sha256": hashlib.sha256(adds_blob).hexdigest(),
                "seq_sha256": hashlib.sha256(new_seq.encode()).hexdigest(),
                "edits": edit_recs,
            }
            _atomic_write(self.dir / seq_name, json.dumps(seq_payload, **COMPACT).encode())
            if crash == "after_sequence":
                raise InjectedCrash("注入崩溃：序列数据已写，feature 索引尚未生成")

            # 2) feature 位置/溯源索引增量（由 features.FeatureEngine 回调注入）
            delta = self._feature_delta_hook(
                base_version, target_version, norm, new_pieces) \
                if self._feature_delta_hook else {}
            _atomic_write(
                self.dir / feat_name,
                json.dumps({"v": target_version, "delta": delta}, **COMPACT).encode(),
            )
            if crash == "before_commit":
                raise InjectedCrash("注入崩溃：feature 索引已写，manifest 尚未提交")

            # 3) 清单最后原子发布；此前任何崩溃都使新版本对 extract 不可见
            manifest = json.loads(self._manifest_path().read_text())
            manifest["revisions"].append({
                "target": target_version, "base": base_version,
                "adds_file": adds_name, "seq_file": seq_name, "feat_file": feat_name,
                "length": new_len, "seq_sha256": seq_payload["seq_sha256"],
            })
            manifest["head_version"] = target_version
            manifest["adds_total_bytes"] += len(adds_blob)
            _atomic_write(self._manifest_path(), json.dumps(manifest, indent=2).encode())
        finally:
            self._pending_revision = None
            self._add_blob = None
        self._manifest = manifest
        self._pieces_cache[target_version] = new_pieces
        self._seq_cache[target_version] = new_seq
        self._adds_cache.pop(target_version, None)
        return {"version": target_version, "length": new_len,
                "seq_sha256": seq_payload["seq_sha256"]}

    _feature_delta_hook = None

    def _manifest_path(self):
        return self.dir / "manifest.json"

    # -------------------------------- 存储审计
    def audit(self):
        """按实际落盘字节统计原件/增量/索引/元数据；恢复查询读取的文件全部计入。"""
        categories = {
            "origin": [], "sequence_delta": [], "feature_index": [], "metadata": []}
        for child in sorted(self.dir.iterdir()):
            if not child.is_file():
                continue
            size = child.stat().st_size
            n = child.name
            if n == self._manifest["origin_file"]:
                cat = "origin"
            elif n.endswith(".adds.bin") or n.endswith(".seq.json"):
                cat = "sequence_delta"
            elif n == self._manifest["feature_defs_file"] or n.endswith(".feat.json"):
                cat = "feature_index"
            else:
                cat = "metadata"
            categories[cat].append({"file": n, "bytes": size})
        totals = {k: sum(f["bytes"] for f in v) for k, v in categories.items()}
        totals["all"] = sum(totals.values())
        return {
            "store": str(self.dir),
            "versions": self.versions,
            "origin_sequence_length": self._manifest["origin_length"],
            "adds_total_bytes": self._manifest["adds_total_bytes"],
            "files": categories,
            "totals": totals,
            "new_persistent_bytes": totals["all"] - totals["origin"],
        }
