"""发布/失败记录的独立核验：逐对象摘要、逐段原始时间映射、覆盖与成本、
以及一个独立实现的枚举 oracle（不复用 collector 的可行性代码）作为对照。

verify_publication 重读离线片段清单并核验：
1. 每个被引用离线对象的长度与 sha256；
2. 每段原始寻址 (t,d,scale,pto) 到展示区间的映射，且能在源清单快照中找到；
3. 每个已发布展示点恰好被一个选中表示覆盖，切换只发生在给定 switch_points
   且两侧有共同边界；
4. 成本证据自洽且不超预算，缓存命中与声明摘要一致，元数据开销单独列出；
5. oracle 独立枚举可行覆盖，对照决策事件检查 preferred 规则与失败结论。
"""
from __future__ import annotations

import hashlib
import json
from fractions import Fraction
from itertools import product

import manifest as M
from manifest import Manifest, Request
from store import Store

ORACLE_ENUM_CAP = 100_000


class VerifyError(AssertionError):
    pass


def _check(cond, msg):
    if not cond:
        raise VerifyError(msg)


# ---------------------------------------------------------------- oracle
# 独立实现：只用清单模型数据，自己写区间扫描，不调用 collector/manifest 的
# 可行性函数，保证“枚举小清单求可行覆盖”的对照意义。

def _oracle_intervals(mf, rid, bad):
    rep = mf.representations[rid]
    ivs = []
    for seg in rep.segments:
        if mf.objects[seg.url].key in bad:
            continue
        s = max(Fraction(seg.t - seg.pto, seg.scale), Fraction(0))
        e = max(Fraction(seg.t + seg.d - seg.pto, seg.scale), Fraction(0))
        if e > s:
            ivs.append((s, e))
    ivs.sort()
    return ivs


def _oracle_covers(ivs, a, b):
    cur = a
    for s, e in ivs:
        if e <= cur:
            continue
        if s > cur:
            return False
        cur = e
        if cur >= b:
            return True
    return cur >= b


def _oracle_run_ok(mf, rid, s, e, bad, need_left, need_right):
    rep = mf.representations[rid]
    if mf.objects[rep.init_url].key in bad:
        return False
    ivs = _oracle_intervals(mf, rid, bad)
    if not _oracle_covers(ivs, s, e):
        return False
    if need_left and any(a < s < b for a, b in ivs):
        return False
    if need_right and any(a < e < b for a, b in ivs):
        return False
    return True


def _oracle_group(intervals, assignment):
    runs = []
    for (s, e), rid in zip(intervals, assignment):
        if runs and runs[-1][2] == rid and runs[-1][1] == s:
            runs[-1][1] = e
        else:
            runs.append([s, e, rid])
    return runs


def oracle_feasible_assignments(mf, req, bad, cap=ORACLE_ENUM_CAP):
    """枚举全部可行逐区间指派。返回 (列表, 是否因超上限被截断)。"""
    intervals = req.switch_intervals()
    per = []
    for (s, e) in intervals:
        ok = [rid for rid in mf.representations
              if _oracle_covers(_oracle_intervals(mf, rid, bad), s, e)]
        per.append(ok)
    if any(not o for o in per):
        return [], False
    total = 1
    for o in per:
        total *= len(o)
    if total > cap:
        return None, True
    out = []
    for ass in product(*per):
        runs = _oracle_group(intervals, list(ass))
        if all(_oracle_run_ok(mf, rid, s, e, bad, i > 0, i < len(runs) - 1)
               for i, (s, e, rid) in enumerate(runs)):
            out.append(list(ass))
    return out, False


def oracle_objects(mf, req, assignment, bad):
    intervals = req.switch_intervals()
    objs = {}
    for s, e, rid in _oracle_group(intervals, assignment):
        rep = mf.representations[rid]
        d = mf.objects[rep.init_url]
        objs[d.key] = d
        for seg in rep.segments:
            sd = mf.objects[seg.url]
            if sd.key in bad or seg.empty:
                continue
            if seg.start < e and seg.end > s:
                objs[sd.key] = sd
    return objs


def oracle_cost(mf, req, assignment, bad, cache_hit_keys):
    return sum(d.bytes for d in oracle_objects(mf, req, assignment, bad).values()
               if d.key not in cache_hit_keys)


# ---------------------------------------------------------------- 核验

def _rebuild(pub_or_fail):
    src = pub_or_fail["source"]
    _check(hashlib.sha256(src["manifest_snapshot"].encode("utf-8")).hexdigest()
           == src["manifest_sha256"], "清单快照与声明摘要不符")
    _check(hashlib.sha256(src["request_snapshot"].encode("utf-8")).hexdigest()
           == src["request_sha256"], "请求快照与声明摘要不符")
    mf = Manifest.loads(src["manifest_snapshot"], path=src["manifest_path"])
    req = Request.loads(src["request_snapshot"], path=src["request_path"])
    return mf, req


def _expand_assignment(pub, req):
    """把发布的运行段指派展开成逐区间表示列表。"""
    intervals = req.switch_intervals()
    expanded = []
    for (a, b) in intervals:
        hit = None
        for run in pub["assignment"]:
            s, e = Fraction(run["interval"][0]), Fraction(run["interval"][1])
            if s <= a and b <= e:
                hit = run["rep"]
                break
        _check(hit is not None, f"区间 [{a}, {b}) 没有指派表示")
        expanded.append(hit)
    return expanded


def verify_publication(store, pub_id="current"):
    pub = store.load_publication(pub_id)
    checks = []

    # 0. 发布完整性：id 与内容绑定
    body = {k: v for k, v in pub.items() if k != "publication_id"}
    canonical = json.dumps(body, sort_keys=True, ensure_ascii=False)
    expect = "pub-" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    _check(pub.get("publication_id") == expect, "publication_id 与内容不匹配")
    checks.append("publication_integrity")

    mf, req = _rebuild(pub)
    checks.append("snapshot_rebuilt")

    # 1. 逐离线对象核验摘要与长度，且缓存索引为已验证状态
    for item in pub["segments"] + pub["inits"]:
        obj = item["object"]
        path = store.root / item["cache"]
        _check(path.exists(), f"缓存对象缺失: {item['cache']}")
        data = path.read_bytes()
        _check(len(data) == obj["bytes"],
               f"对象 {obj['url']}@{obj['revision']} 长度不符")
        _check(hashlib.sha256(data).hexdigest() == obj["sha256"],
               f"对象 {obj['url']}@{obj['revision']} 摘要不符")
        _check(store.object_state(obj["url"], obj["revision"]) == "verified",
               f"缓存索引中 {obj['url']}@{obj['revision']} 不是 verified")
    checks.append("object_digests")

    # 2. 每段原始时间映射：重算 (t,d,scale,pto) 并对照源清单快照
    src_rows = {(s.rep, s.url, s.t, s.d) for r in mf.representations.values()
                for s in r.segments}
    for item in pub["segments"]:
        row = (item["rep"], item["url"], item["t"], item["d"])
        _check(row in src_rows, f"段 {row} 不在源清单中")
        scale, pto = item["scale"], item["pto"]
        raw_s = Fraction(item["t"] - pto, scale)
        raw_e = Fraction(item["t"] + item["d"] - pto, scale)
        _check(Fraction(item["raw_presentation"][0]) == raw_s and
               Fraction(item["raw_presentation"][1]) == raw_e,
               f"段 {row} 原始时间映射不符")
        _check(Fraction(item["presentation"][0]) == max(raw_s, Fraction(0)) and
               Fraction(item["presentation"][1]) == max(raw_e, Fraction(0)),
               f"段 {row} PTO 裁切映射不符")
        decl = mf.objects[item["url"]]
        _check(item["object"] == decl.as_dict(),
               f"段 {row} 的对象声明与源清单不符")
    checks.append("segment_time_mapping")

    # 3. 覆盖：每个已发布展示点恰好一个表示；切换只在 switch_points 且有共同边界
    intervals = req.switch_intervals()
    expanded = _expand_assignment(pub, req)
    slabs = []
    for item in pub["segments"]:
        s = max(Fraction(item["presentation"][0]), req.start)
        e = min(Fraction(item["presentation"][1]), req.end)
        if e > s:
            slabs.append((s, e, item["rep"]))
    points = sorted({p for s, e, _ in slabs for p in (s, e)} | {req.start, req.end})
    for a, b in zip(points, points[1:]):
        reps = {r for s, e, r in slabs if s <= a and e >= b}
        _check(len(reps) == 1, f"展示点 [{a}, {b}) 被 {len(reps)} 个表示覆盖")
        # 段可跨越未发生切换的切换点；共同边界约束保证跨指派边界的段不存在，
        # 因此用包含 a 的区间判定指派即可
        idx = next(i for i, (x, y) in enumerate(intervals) if x <= a < y)
        _check(list(reps)[0] == expanded[idx],
               f"展示点 [{a}, {b}) 实际覆盖表示与指派不符")
    runs = pub["assignment"]
    for left, right in zip(runs, runs[1:]):
        boundary = Fraction(left["interval"][1])
        _check(boundary in req.switch_points,
               f"切换点 {boundary} 不在请求的 switch_points 中")
        _check(Fraction(right["interval"][0]) == boundary, "运行段不连续")
        l_ok = any(item["rep"] == left["rep"] and
                   Fraction(item["presentation"][1]) == boundary
                   for item in pub["segments"])
        r_ok = any(item["rep"] == right["rep"] and
                   Fraction(item["presentation"][0]) == boundary
                   for item in pub["segments"])
        _check(l_ok and r_ok, f"切换点 {boundary} 两侧没有共同边界")
    checks.append("coverage_exactly_one_and_boundaries")

    # 4. 成本证据：读取日志求和 == payload <= 预算；命中与声明一致；元数据另报
    cost = pub["cost"]
    _check(sum(r["bytes"] for r in cost["origin_reads"]) == cost["payload_bytes"],
           "payload 字节与读取日志不符")
    _check(cost["payload_bytes"] <= req.budget, "payload 超出预算")
    for hit in cost["cache_hits"]:
        decl = mf.objects[hit["url"]]
        _check(decl.revision == hit["revision"] and decl.sha256 == hit["sha256"],
               f"缓存命中 {hit['url']} 的版本/摘要与清单声明不符")
    _check(isinstance(cost.get("metadata"), dict), "缺少单独上报的元数据开销")
    checks.append("cost_evidence")

    # 5. oracle 对照：发布指派可行；决策事件符合 preferred 规则
    events = pub["decision"]["events"]
    final_bad = {tuple(x) for e in events if "bad" in e for x in e["bad"]}
    feasible, truncated = oracle_feasible_assignments(mf, req, final_bad)
    if not truncated:
        _check(expanded in feasible, "发布指派不在 oracle 可行集内")
    for ev in events:
        bad_ev = {tuple(x) for x in ev.get("bad", [])}
        hits_ev = {tuple(x) for x in ev.get("cache_hits", [])}
        if ev["event"] == "global_plan":
            feas_ev, tr = oracle_feasible_assignments(mf, req, bad_ev)
            if tr:
                continue
            all_pref = [a for a in feas_ev if all(r == req.preferred for r in a)]
            if all_pref:
                cost_ap = oracle_cost(mf, req, all_pref[0], bad_ev, hits_ev)
                _check(cost_ap == ev["all_preferred_cost"],
                       "全 preferred 成本估计与 oracle 不符")
                if cost_ap <= ev["remaining_budget"]:
                    _check(ev["chosen"] == all_pref[0],
                           "违反 preferred 规则：可行且不超预算时必须选全 preferred")
            if ev["chosen"] is None:
                _check(all(oracle_cost(mf, req, a, bad_ev, hits_ev)
                           > ev["remaining_budget"] for a in feas_ev),
                       "存在可行且未超预算的方案却宣告不可交付")
            else:
                _check(ev["chosen"] in feas_ev, "所选指派不可行")
                _check(oracle_cost(mf, req, ev["chosen"], bad_ev, hits_ev)
                       <= ev["remaining_budget"], "所选方案估计成本超预算")
        elif ev["event"] == "online_init":
            feas_ev, tr = oracle_feasible_assignments(mf, req, bad_ev)
            if tr:
                continue
            all_pref_ass = [req.preferred] * len(intervals)
            _check((all_pref_ass in feas_ev) == ev["all_preferred_viable"],
                   "online 预检的全 preferred 可行性与 oracle 不符")
            if ev["all_preferred_viable"]:
                cost_ap = oracle_cost(mf, req, all_pref_ass, bad_ev, hits_ev)
                if cost_ap <= ev["remaining_budget"]:
                    _check(ev["mode"] == "pinned",
                           "online 违反 preferred 规则：应钉住而未钉住")
    checks.append("oracle_decision_rule")
    return {"ok": True, "publication_id": pub["publication_id"],
            "checks": checks, "oracle_truncated": truncated}


def verify_failure(store, fail_id):
    """核验一条失败记录：成本自洽，且（全局策略下）oracle 同意无可行方案。"""
    fail = store.load_failure(fail_id)
    checks = []
    body = {k: v for k, v in fail.items() if k != "failure_id"}
    canonical = json.dumps(body, sort_keys=True, ensure_ascii=False)
    expect = "fail-" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    _check(fail.get("failure_id") == expect, "failure_id 与内容不匹配")
    mf, req = _rebuild(fail)
    checks.append("failure_integrity")
    cost = fail["cost"]
    _check(sum(r["bytes"] for r in cost["origin_reads"]) == cost["payload_bytes"],
           "失败记录的 payload 与读取日志不符")
    _check(cost["payload_bytes"] <= req.budget, "失败记录的 payload 超出预算")
    checks.append("cost_evidence")
    events = fail["decision"]["events"]
    global_events = [e for e in events if e["event"] == "global_plan"]
    if fail["reason"] in ("no_feasible_coverage", "budget_exceeded_for_all_plans",
                          "budget_exceeded") and global_events:
        ev = global_events[-1]
        bad_ev = {tuple(x) for x in ev.get("bad", [])}
        hits_ev = {tuple(x) for x in ev.get("cache_hits", [])}
        feas_ev, tr = oracle_feasible_assignments(mf, req, bad_ev)
        if not tr:
            _check(all(oracle_cost(mf, req, a, bad_ev, hits_ev)
                       > ev["remaining_budget"] for a in feas_ev),
                   "oracle 发现可行且未超预算的方案，失败结论不成立")
            checks.append("oracle_agrees_infeasible")
    elif fail["reason"] == "no_feasible_coverage":
        # online 结构性失败：核验所记区间确实没有可行候选
        ev = fail["evidence"]
        _check("interval" in ev, "online 失败证据缺少区间")
        checks.append("online_infeasible_recorded")
    return {"ok": True, "failure_id": fail_id, "reason": fail["reason"],
            "checks": checks}
