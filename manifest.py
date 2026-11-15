"""规范化 DASH 采集清单：模型、校验与媒体时间覆盖分析。

升级自只读展开版本：提供带校验的清单/请求模型、PTO 裁切、
覆盖洞与相邻段重叠分析，供 plan / collect / verify 三个阶段共用。
所有时间一律用 fractions.Fraction（整数 tick 与有理数），不使用浮点。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path

MAX_REPRESENTATIONS = 8
MAX_SEGMENTS_PER_REPRESENTATION = 100
MAX_REVISIONS = 3
MAX_OBJECT_BYTES = 64 * 1024 * 1024  # 64 MiB


class ManifestError(ValueError):
    """清单或请求不满足规范化约束。"""


def parse_fraction(value) -> Fraction:
    """把 int / "3" / "3/2" / "1.5" 解析为 Fraction；拒绝布尔等非法值。"""
    if isinstance(value, Fraction):
        return value
    if isinstance(value, bool):
        raise ManifestError(f"非法时间值: {value!r}")
    if isinstance(value, int):
        return Fraction(value)
    text = str(value).strip()
    try:
        return Fraction(text)
    except (ValueError, ZeroDivisionError) as exc:
        raise ManifestError(f"非法有理数: {text!r}") from exc


def fmt(value: Fraction) -> str:
    """最简分数的字符串形式（分母为 1 时就是整数）。"""
    return str(value)


def _require(cond, msg):
    if not cond:
        raise ManifestError(msg)


def _require_int(value, name, lo=None, hi=None):
    _require(isinstance(value, int) and not isinstance(value, bool), f"{name} 必须是整数，得到 {value!r}")
    if lo is not None:
        _require(value >= lo, f"{name} 必须 >= {lo}，得到 {value}")
    if hi is not None:
        _require(value <= hi, f"{name} 必须 <= {hi}，得到 {value}")


@dataclass(frozen=True)
class ObjectDecl:
    """清单为每个对象声明的身份：URL + 修订 + 长度 + 摘要。"""
    url: str
    revision: str
    bytes: int
    sha256: str

    @property
    def key(self):
        return (self.url, self.revision)

    def as_dict(self):
        return {"url": self.url, "revision": self.revision,
                "bytes": self.bytes, "sha256": self.sha256}


@dataclass(frozen=True)
class Segment:
    """一个媒体段；保留原始寻址（t/d/scale/pto/number/url）。"""
    rep: str
    number: int
    t: int
    d: int
    scale: int
    pto: int
    url: str
    declared_bytes: int

    @property
    def raw_start(self) -> Fraction:
        return Fraction(self.t - self.pto, self.scale)

    @property
    def raw_end(self) -> Fraction:
        return Fraction(self.t + self.d - self.pto, self.scale)

    @property
    def start(self) -> Fraction:
        """PTO 裁切后的展示起点（负展示时间裁到 0）。"""
        return max(self.raw_start, Fraction(0))

    @property
    def end(self) -> Fraction:
        return max(self.raw_end, Fraction(0))

    @property
    def empty(self) -> bool:
        return self.end <= self.start

    @property
    def pto_clipped(self) -> bool:
        return self.raw_start < 0


@dataclass
class Representation:
    id: str
    init_url: str
    segments: list  # 按 (start, number) 排序的 Segment

    @property
    def quality(self) -> Fraction:
        """质量代理：平均段字节数（越大码率越高），仅用于同约束下的取舍。"""
        return Fraction(sum(s.declared_bytes for s in self.segments),
                        max(1, len(self.segments)))


class Manifest:
    """规范化采集清单：单修订、多表示、每对象带 revision/长度/摘要。"""

    def __init__(self, revision, representations, objects, path="", digest="", snapshot=""):
        self.revision = revision
        self.representations = representations  # dict: id -> Representation
        self.objects = objects                  # dict: url -> ObjectDecl
        self.path = path
        self.digest = digest
        self.snapshot = snapshot                # 原始文件文本（供发布快照与核验）

    @classmethod
    def load(cls, path):
        raw = Path(path).read_bytes()
        return cls.loads(raw.decode("utf-8"), path=str(path), raw=raw)

    @classmethod
    def loads(cls, text, path="", raw=None):
        data = json.loads(text)
        digest = hashlib.sha256(raw if raw is not None else text.encode("utf-8")).hexdigest()
        if not isinstance(data, dict):
            raise ManifestError("清单必须是 JSON 对象")
        revision = data.get("revision")
        _require(isinstance(revision, str) and revision, "清单缺少有效 revision")
        objects_raw = data.get("objects")
        _require(isinstance(objects_raw, dict), "清单缺少 objects 表")
        objects = {}
        for url, od in objects_raw.items():
            _require(isinstance(url, str) and url, "对象 URL 必须是非空字符串")
            _require(isinstance(od, dict), f"对象 {url} 的声明必须是 JSON 对象")
            rev = od.get("revision")
            size = od.get("bytes")
            sha = od.get("sha256")
            _require(isinstance(rev, str) and rev, f"对象 {url} 缺少 revision")
            _require_int(size, f"对象 {url} 的 bytes", 0, MAX_OBJECT_BYTES)
            _require(isinstance(sha, str) and len(sha) == 64 and
                     all(c in "0123456789abcdef" for c in sha),
                     f"对象 {url} 的 sha256 必须是 64 位小写十六进制")
            objects[url] = ObjectDecl(url, rev, size, sha)
        revisions = {o.revision for o in objects.values()}
        _require(len(revisions) <= MAX_REVISIONS,
                 f"修订数 {len(revisions)} 超过上限 {MAX_REVISIONS}")
        reps_raw = data.get("representations")
        _require(isinstance(reps_raw, list) and reps_raw, "清单缺少 representations")
        _require(len(reps_raw) <= MAX_REPRESENTATIONS,
                 f"表示数 {len(reps_raw)} 超过上限 {MAX_REPRESENTATIONS}")
        representations = {}
        for rep in reps_raw:
            _require(isinstance(rep, dict), "表示必须是 JSON 对象")
            rid = rep.get("id")
            _require(isinstance(rid, str) and rid, "表示缺少 id")
            _require(rid not in representations, f"表示 id 重复: {rid}")
            init = rep.get("init")
            _require(isinstance(init, str) and init in objects,
                     f"表示 {rid} 的 init {init!r} 未在 objects 中声明")
            scale = rep.get("scale")
            pto = rep.get("pto", 0)
            _require_int(scale, f"表示 {rid} 的 scale", 1)
            _require_int(pto, f"表示 {rid} 的 pto", 0)
            segs_raw = rep.get("segments", [])
            _require(isinstance(segs_raw, list), f"表示 {rid} 的 segments 必须是数组")
            _require(len(segs_raw) <= MAX_SEGMENTS_PER_REPRESENTATION,
                     f"表示 {rid} 的段数 {len(segs_raw)} 超过上限 {MAX_SEGMENTS_PER_REPRESENTATION}")
            segments = []
            for n, sr in enumerate(segs_raw):
                _require(isinstance(sr, dict), f"表示 {rid} 第 {n} 段必须是 JSON 对象")
                t, d, url = sr.get("t"), sr.get("d"), sr.get("url")
                _require_int(t, f"表示 {rid} 第 {n} 段的 t", 0)
                _require_int(d, f"表示 {rid} 第 {n} 段的 d", 1)
                _require(isinstance(url, str) and url in objects,
                         f"表示 {rid} 第 {n} 段的 url {url!r} 未在 objects 中声明")
                sb = sr.get("bytes")
                _require(sb == objects[url].bytes,
                         f"表示 {rid} 第 {n} 段声明字节数 {sb} 与对象 {url} 的 {objects[url].bytes} 不一致")
                segments.append(Segment(rid, n, t, d, scale, pto, url, sb))
            segments.sort(key=lambda s: (s.start, s.number))
            representations[rid] = Representation(rid, init, segments)
        return cls(revision, representations, objects,
                   path=path, digest=digest, snapshot=text)

    def decl(self, url) -> ObjectDecl:
        return self.objects[url]


class Request:
    """采集请求：展示半开区间、payload 预算、preferred 表示、切换点、并发上限。"""

    def __init__(self, start, end, budget, preferred, max_parallel, switch_points,
                 path="", digest="", snapshot=""):
        self.start = start
        self.end = end
        self.budget = budget
        self.preferred = preferred
        self.max_parallel = max_parallel
        self.switch_points = switch_points
        self.path = path
        self.digest = digest
        self.snapshot = snapshot

    @classmethod
    def load(cls, path):
        raw = Path(path).read_bytes()
        return cls.loads(raw.decode("utf-8"), path=str(path), raw=raw)

    @classmethod
    def loads(cls, text, path="", raw=None):
        data = json.loads(text)
        digest = hashlib.sha256(raw if raw is not None else text.encode("utf-8")).hexdigest()
        if not isinstance(data, dict):
            raise ManifestError("请求必须是 JSON 对象")
        rng = data.get("range")
        _require(isinstance(rng, list) and len(rng) == 2, "请求缺少 range [start, end)")
        start, end = parse_fraction(rng[0]), parse_fraction(rng[1])
        _require(start < end, f"请求区间为空: [{fmt(start)}, {fmt(end)})")
        budget = data.get("budget_payload_bytes")
        _require_int(budget, "budget_payload_bytes", 0)
        preferred = data.get("preferred")
        _require(isinstance(preferred, str) and preferred, "请求缺少 preferred 表示")
        max_parallel = data.get("max_parallel_reads", 1)
        _require_int(max_parallel, "max_parallel_reads", 1)
        sp = data.get("switch_points", [])
        _require(isinstance(sp, list), "switch_points 必须是数组")
        switch_points = [parse_fraction(p) for p in sp]
        return cls(start, end, budget, preferred, max_parallel, switch_points,
                   path=path, digest=digest, snapshot=text)

    def switch_intervals(self):
        """用落在区间内部的切换点把请求区间切成有序半开小区间。"""
        pts = sorted({p for p in self.switch_points if self.start < p < self.end})
        bounds = [self.start] + pts + [self.end]
        return list(zip(bounds, bounds[1:]))


# ---------------------------------------------------------------- 覆盖分析

def merge_intervals(intervals):
    """合并相交/相接的 [s, e) 区间。"""
    out = []
    for s, e in sorted(intervals):
        if e <= s:
            continue
        if out and s <= out[-1][1]:
            if e > out[-1][1]:
                out[-1] = (out[-1][0], e)
        else:
            out.append((s, e))
    return out


def holes_within(intervals, a, b):
    """[a, b) 内未被 intervals 覆盖的子区间（覆盖洞）。"""
    holes = []
    cur = a
    for s, e in merge_intervals(intervals):
        if e <= cur:
            continue
        if s > cur:
            holes.append((cur, min(s, b)))
        cur = max(cur, e)
        if cur >= b:
            break
    if cur < b:
        holes.append((cur, b))
    return [(s, e) for s, e in holes if e > s]


def overlap_regions(intervals):
    """相邻段重叠区域（同一表示内两段同时覆盖的正长度区域）。"""
    src = sorted((s, e) for s, e in intervals if e > s)
    regions = []
    for i in range(len(src)):
        for j in range(i + 1, len(src)):
            s = max(src[i][0], src[j][0])
            e = min(src[i][1], src[j][1])
            if s < e:
                regions.append((s, e))
    return merge_intervals(regions)


def clip_to(intervals, a, b):
    return [(max(s, a), min(e, b)) for s, e in intervals if min(e, b) > max(s, a)]


@dataclass
class Feasibility:
    ok: bool
    reasons: list
    segments: list  # 与考察区间相交的可见段（无论 ok 与否都给出，便于证据）


def run_feasibility(mf: Manifest, rep_id, s, e, bad, need_left, need_right):
    """表示 rep_id 在运行区间 [s, e) 上的可行性。

    bad: 已验真失败的对象 key 集合 (url, revision) —— 相关候选段不可见。
    need_left/need_right: 区间端点是内部切换点时，要求段不向另一侧溢出，
    这等价于切换两侧存在共同边界（覆盖到边界且没有段越过边界）。
    """
    rep = mf.representations[rep_id]
    reasons = []
    init_decl = mf.objects[rep.init_url]
    if init_decl.key in bad:
        reasons.append(f"初始化对象 {rep.init_url}@{init_decl.revision} 验真失败，整表示不可用")
    visible = [seg for seg in rep.segments
               if not seg.empty and mf.objects[seg.url].key not in bad]
    for g0, g1 in holes_within([(seg.start, seg.end) for seg in visible], s, e):
        reasons.append(f"覆盖洞 [{fmt(g0)}, {fmt(g1)})")
    segs = [seg for seg in visible if seg.start < e and seg.end > s]
    if need_left:
        for seg in segs:
            if seg.start < s:
                reasons.append(
                    f"段 {seg.url}(#{seg.number}) 越过切换点 {fmt(s)} 向左延伸到 {fmt(seg.start)}")
    if need_right:
        for seg in segs:
            if seg.end > e:
                reasons.append(
                    f"段 {seg.url}(#{seg.number}) 越过切换点 {fmt(e)} 向右延伸到 {fmt(seg.end)}")
    return Feasibility(ok=not reasons, reasons=reasons, segments=segs)


def boundary_segment(mf: Manifest, rep_id, point, side, bad=frozenset()):
    """切换点共同边界证据：side='left' 找 end==point 的段，'right' 找 start==point 的段。"""
    rep = mf.representations[rep_id]
    for seg in rep.segments:
        if seg.empty or mf.objects[seg.url].key in bad:
            continue
        if side == "left" and seg.end == point:
            return seg
        if side == "right" and seg.start == point:
            return seg
    return None


def analyze(mf: Manifest, req: Request, bad=frozenset()):
    """逐表示的覆盖证据：覆盖洞 / 相邻段重叠 / PTO 裁切，三者分列。"""
    out = {}
    for rid, rep in mf.representations.items():
        visible = [seg for seg in rep.segments
                   if not seg.empty and mf.objects[seg.url].key not in bad]
        ivs = [(seg.start, seg.end) for seg in visible]
        holes = holes_within(ivs, req.start, req.end)
        overlaps = clip_to(overlap_regions(ivs), req.start, req.end)
        clips = [{"number": s.number, "url": s.url, "raw_start": fmt(s.raw_start)}
                 for s in rep.segments if s.pto_clipped]
        out[rid] = {
            "holes": [[fmt(a), fmt(b)] for a, b in holes],
            "overlaps": [[fmt(a), fmt(b)] for a, b in overlaps],
            "pto_clips": clips,
            "covers_request": not holes and mf.objects[rep.init_url].key not in bad,
        }
    return out


# ---------------------------------------------------------------- 兼容旧接口

def load(path):
    """旧版只读展开接口（保留兼容；新代码请用 Manifest.load）。"""
    d = json.load(open(path))
    out = []
    for rep in d['representations']:
        for n, seg in enumerate(rep['segments']):
            out.append(dict(rep=rep['id'], number=n,
                            start=str(Fraction(seg['t'] - rep['pto'], rep['scale'])),
                            end=str(Fraction(seg['t'] + seg['d'] - rep['pto'], rep['scale'])),
                            url=seg['url'], init=rep['init'], bytes=seg['bytes']))
    return d['revision'], out
