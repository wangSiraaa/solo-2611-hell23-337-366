"""Feature 位置索引与溯源保真引擎。

核心表示：每个 feature 的每一段在每个版本上是一条“溯源原子”链
  原碱基原子 ["O", origin_start, origin_end)
  新增原子   ["A", revision, edit_id, adds_blob_offset, length]

设计要点：
  * 原子链只随修订做增量演化，feat.json 仅持久化原子链真正变化的段；
  * 段的新坐标不落地，任何时候都由该版本 piece-table 的绝对区间反查，
    因此“编辑在段前方导致的整体平移”不会产生陈旧坐标，也不占增量；
  * 不移动包络、不虚报来源，空段永久保留 empty 状态。

粘附语义（删除前父坐标判定）：
  新增 insert 属于某段  ⇔  锚点严格位于段内部 (ps < anchor < px)
                          或 anchor == ps 且 left == include
                          或 anchor == px 且 right == include
锚在段外一律不纳入，即使删除范围跨入该段。
"""
from __future__ import annotations

import bisect
import json

MAX_FEATURES = 10000
MAX_SEGMENTS = 20
_INCLUDE, _EXCLUDE = "include", "exclude"
_COMP = str.maketrans("ACGTN", "TGCAN")


class FeatureError(ValueError):
    pass


def normalize_defs(raw, origin_length) -> list:
    """接受规范 feature 表；旧 inputs 的裸 [start,end] 段默认 left=right=exclude。"""
    if isinstance(raw, dict) and "features" in raw:
        raw = raw["features"]
    if not isinstance(raw, list):
        raise FeatureError("feature 表必须是列表")
    if len(raw) > MAX_FEATURES:
        raise FeatureError(f"feature 数 {len(raw)} 超过上限 {MAX_FEATURES}")
    defs, seen = [], set()
    for f in raw:
        fid = f.get("id")
        if not fid or fid in seen:
            raise FeatureError(f"feature id 缺失或重复: {fid!r}")
        seen.add(fid)
        strand = f.get("strand", "+")
        if strand not in ("+", "-"):
            raise FeatureError(f"{fid}: strand 必须为 + 或 -")
        segs_in = f.get("segments", [])
        if not isinstance(segs_in, list) or not segs_in or len(segs_in) > MAX_SEGMENTS:
            raise FeatureError(f"{fid}: 段数必须在 1..{MAX_SEGMENTS} 之间")
        segs = []
        for j, sg in enumerate(segs_in):
            if isinstance(sg, dict):
                s, x = sg["start"], sg["end"]
                left = sg.get("left", _EXCLUDE)
                right = sg.get("right", _EXCLUDE)
            else:
                s, x = sg[0], sg[1]
                left = right = _EXCLUDE
            if not (0 <= s < x <= origin_length):
                raise FeatureError(f"{fid} 段{j}: 非法 0 基半开区间 [{s},{x})（长度 {origin_length}）")
            if left not in (_INCLUDE, _EXCLUDE) or right not in (_INCLUDE, _EXCLUDE):
                raise FeatureError(f"{fid} 段{j}: 粘附规则必须是 include/exclude")
            segs.append({"start": s, "end": x, "left": left, "right": right})
        defs.append({"id": fid, "strand": strand, "segments": segs})
    return defs


def extract(sequence, feature):
    """旧无状态实现（baseline 保留）：直接按旧坐标切片，负向反向互补。"""
    s = "".join(sequence[a:b] for a, b in feature["segments"])
    return s.translate(_COMP)[::-1] if feature.get("strand") == "-" else s


def _atom_len(a):
    return a[2] - a[1] if a[0] == "O" else a[4]


def _plen_piece(p):
    return p[2] - p[1] if p[0] == "O" else p[4]


def _splice_segment(seg, edits):
    """对单个非空段应用一批编辑，返回新原子链（坐标不在此处计算）。"""
    ps = seg["ps"]
    L = seg["px"] - ps
    left, right = seg["left"], seg["right"]
    atoms, n = seg["atoms"], len(seg["atoms"])
    ai, ao, cursor = 0, 0, 0          # 当前原子、原子内已消耗偏移、段内父坐标游标
    out = []

    def walk(stop):
        """把存活原子从 cursor 输出到段内坐标 stop。"""
        nonlocal ai, ao, cursor
        while cursor < stop:
            a = atoms[ai]
            al = _atom_len(a)
            take = min(al - ao, stop - cursor)
            if a[0] == "O":
                out.append(["O", a[1] + ao, a[1] + ao + take])
            else:
                out.append(["A", a[1], a[2], a[3] + ao, take])
            ao += take
            cursor += take
            if ao == al:
                ai, ao = ai + 1, 0

    def erase(stop):
        """删除 (cursor, stop] 范围内的原映射。"""
        nonlocal ai, ao, cursor
        while cursor < stop:
            a = atoms[ai]
            al = _atom_len(a)
            skip = min(al - ao, stop - cursor)
            ao += skip
            cursor += skip
            if ao == al:
                ai, ao = ai + 1, 0

    for e in edits:
        rs, rt = e["s"] - ps, e["t"] - ps
        if rt < 0:                       # 严格在段左侧：只造成整体平移
            continue
        if rs > L:                       # 严格在段右侧（rs==L 是右端点，须交粘附判定）
            break
        rrs, rrt = max(rs, 0), min(rt, L)
        walk(rrs)                        # 删除前的存活碱基保留
        erase(rrt)                       # 原碱基被删除 → 删其映射
        included = (
            (0 < rs < L)
            or (rs == 0 and left == _INCLUDE)
            or (rs == L and right == _INCLUDE)
        )
        if included and e["ins"]:
            out.append(["A", e["rev"], e["id"], e["adds_off"], len(e["ins"])])
    walk(L)
    return out


class FeatureEngine:
    def __init__(self, store):
        self.store = store
        self.defs_path = store.dir / store._manifest["feature_defs_file"]
        self.defs = normalize_defs(json.loads(self.defs_path.read_text()),
                                   store._manifest["origin_length"])
        self._state_cache = {}
        self._range_cache = {}

    # ---------------- 状态回放（feat 增量链，只含原子链变化）
    def _initial_state(self):
        return {
            f["id"]: {
                "strand": f["strand"],
                "segments": [
                    {"empty": False, "left": sg["left"], "right": sg["right"],
                     "atoms": [["O", sg["start"], sg["end"]]]}
                    for sg in f["segments"]],
            }
            for f in self.defs
        }

    def state(self, version):
        """回放至指定版本的 feature 原子链状态（不复制序列文本）。"""
        self.store._require_version(version)
        if version in self._state_cache:
            return self._state_cache[version]
        if version == 0:
            st = self._initial_state()
            self._state_cache[0] = st
            return st
        st = {fid: {"strand": v["strand"],
                    "segments": [dict(seg, atoms=list(seg["atoms"]))
                                 for seg in v["segments"]]}
              for fid, v in self.state(version - 1).items()}
        rec = self.store._rev_record(version)
        delta = json.loads((self.store.dir / rec["feat_file"]).read_text())["delta"]
        for fdelta in delta["features"]:
            fid = fdelta["fid"]
            for idx, payload in fdelta["segments"].items():
                k = int(idx)
                st[fid]["segments"][k] = {
                    "empty": payload["empty"],
                    "left": st[fid]["segments"][k]["left"],
                    "right": st[fid]["segments"][k]["right"],
                    "atoms": [list(a) for a in payload["atoms"]],
                }
        self._state_cache[version] = st
        return st

    # ---------------- piece-table 绝对区间反查
    def _ranges(self, version, pieces=None):
        if pieces is not None:
            origin_index, add_index = [], {}
            pos = 0
            for p in pieces:
                l = _plen_piece(p)
                if p[0] == "O":
                    origin_index.append((p[1], pos, p))
                else:
                    add_index[(p[1], p[2], p[3])] = (pos, pos + l)
                pos += l
            return [p[0] for p in origin_index], origin_index, add_index, pos
        if version not in self._range_cache:
            origin_index, add_index, total = self.store.piece_ranges(version)
            self._range_cache[version] = (
                [p[0] for p in origin_index], origin_index, add_index, total)
        return self._range_cache[version]

    def _atom_base(self, version, a, pieces=None):
        """原子首碱基在指定版本序列中的绝对坐标。"""
        ost, oi, ai, _ = self._ranges(version, pieces)
        if a[0] == "O":
            k = bisect.bisect_right(ost, a[1]) - 1   # 覆盖 a[1] 的 origin piece
            if k < 0:
                raise RuntimeError(f"溯源原子在 v{version} piece-table 中失配: {a}")
            piece_start, new_start, piece = oi[k]
            piece_end = piece_start + (piece[2] - piece[1])
            if not (piece_start <= a[1] < piece_end):
                raise RuntimeError(f"溯源原子在 v{version} piece-table 中失配: {a}")
            return new_start + (a[1] - piece_start)
        return ai[(a[1], a[2], a[3])][0]

    def span(self, version, seg, pieces=None):
        """非空段在该版本的新区间 [start,end)，坐标完全派生、不落地。"""
        if seg["empty"]:
            return None
        first, last = seg["atoms"][0], seg["atoms"][-1]
        return [self._atom_base(version, first, pieces),
                self._atom_base(version, last, pieces) + _atom_len(last)]

    # ---------------- 发布期增量计算（由 Store.apply_revision 回调）
    def compute_delta(self, base_version, target_version, norm_edits, new_pieces):
        parent = self.state(base_version)
        edits = [{**e, "rev": target_version,
                  "adds_off": sum(len(x["ins"]) for x in norm_edits[:i])}
                 for i, e in enumerate(norm_edits)]

        feature_deltas, content_ids, moved_ids = [], [], []
        for fdef in self.defs:
            fid = fdef["id"]
            segs = parent[fid]["segments"]
            changed, any_content, any_moved = {}, False, False
            for j, seg in enumerate(segs):
                if seg["empty"]:
                    continue
                ps, px = self.span(base_version, seg)
                if not any(e["s"] <= px and e["t"] >= ps for e in edits):
                    # 编辑全在段外：原子不变；若编辑在段前方则仅坐标平移，
                    # 新坐标查询时派生，无需写增量。
                    if any(e["t"] <= ps and (e["ins"] or e["t"] > e["s"])
                           for e in edits):
                        any_moved = True
                    continue
                new_atoms = _splice_segment(
                    {**seg, "ps": ps, "px": px}, edits)
                if new_atoms != seg["atoms"]:
                    any_content = True
                    changed[str(j)] = {
                        "empty": not new_atoms,
                        "atoms": new_atoms,   # 空段原子链为 []，保留 empty
                    }
                else:
                    ns = self._atom_base(target_version, new_atoms[0], new_pieces)
                    if ns != ps:
                        any_moved = True
            if changed:
                feature_deltas.append({"fid": fid, "segments": changed})
            if any_content:
                content_ids.append(fid)
            elif any_moved:
                moved_ids.append(fid)
        return {"features": feature_deltas,
                "content_changed": content_ids,
                "coordinate_moved": moved_ids}

    # ---------------- 查询
    def _atom_bytes(self, a) -> str:
        if a[0] == "O":
            return self.store.origin_sequence[a[1]:a[2]]
        return self.store.adds_bytes(a[1], a[3], a[4]).decode()

    def feature_ids(self):
        return [f["id"] for f in self.defs]

    def locate(self, version, fid):
        """每段在该版本的新区间；empty 段为 null。"""
        st = self.state(version)
        self._require_fid(fid, st)
        return [self.span(version, sg) for sg in st[fid]["segments"]]

    def extract(self, version, fid):
        """由 trace 原子重建输出：origin 片段 + edit 插入片段逐字节拼接。"""
        return "".join(b["bases"] for seg in self.trace(version, fid)
                       for b in seg["blocks"])

    def trace(self, version, fid):
        """每个输出碱基对应一条可回查来源块；负向时顺序与来源方向一起反转。"""
        st = self.state(version)
        self._require_fid(fid, st)
        f = st[fid]
        result = []
        for j, seg in enumerate(f["segments"]):
            if seg["empty"]:
                result.append({"segment": j, "empty": True, "blocks": []})
                continue
            seg_blocks = []
            for a in seg["atoms"]:
                base = self._atom_base(version, a)
                if a[0] == "O":
                    ref = {"source": "origin", "start": a[1], "end": a[2]}
                else:
                    ref = {"source": "edit", "revision": a[1], "edit_id": a[2],
                           "adds_offset": a[3], "length": a[4]}
                seg_blocks.append({"ref": ref, "bases": self._atom_bytes(a),
                                   "new_start": base,
                                   "new_end": base + _atom_len(a),
                                   "direction": "+"})
            result.append({"segment": j, "empty": False,
                           "new_range": self.span(version, seg),
                           "direction": "+", "blocks": seg_blocks})
        if f["strand"] == "-":
            result = self._reverse_blocks(result)
        return result

    def _reverse_blocks(self, blocks):
        out = []
        for seg in reversed(blocks):
            if seg["empty"]:
                out.append(seg)
                continue
            rev_blocks = [{**b,
                           "bases": b["bases"].translate(_COMP)[::-1],
                           "direction": "-"}
                          for b in reversed(seg["blocks"])]
            out.append({"segment": seg["segment"], "empty": False,
                        "new_range": seg["new_range"],
                        "direction": "-", "blocks": rev_blocks})
        return out

    def affected(self, version):
        """区分内容改变与仅坐标移动（读该修订已提交的 feat 增量分类）。"""
        rec = self.store._rev_record(version)
        delta = json.loads((self.store.dir / rec["feat_file"]).read_text())["delta"]
        return {"version": version,
                "content_changed": list(delta["content_changed"]),
                "coordinate_moved": list(delta["coordinate_moved"])}

    def _require_fid(self, fid, st):
        if fid not in st:
            raise FeatureError(f"未知 feature id: {fid}（可用: {sorted(st)}）")
