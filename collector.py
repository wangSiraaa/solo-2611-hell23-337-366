"""采集执行器：全局覆盖计划 / 分段在线选择两种策略，下载-缓存-覆盖验证联动。

预算口径：本次从 Origin 实际读出的全部 payload 字节（含初始化对象、
验真失败的读取与重复读取）；缓存命中须先证明版本及摘要（见 store.py）；
清单解析、缓存证明哈希、索引与发布清单等元数据开销单独上报，不计入预算。

两种策略共享同一套取数/缓存/验真/发布机制，只在“何时做选择”上不同：
- global：取数前枚举切换区间上的表示指派（带上限），应用 preferred 决策规则；
- online：先做一次全 preferred 预检（满足规则则钉住），否则逐区间在线贪心，
  用实际已花费与即时验真结果影响后续选择。
"""
from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from types import SimpleNamespace

import manifest as M
from manifest import Manifest, Request, ManifestError
from store import Store

MAX_ENUMERATED_ASSIGNMENTS = 100_000
MAX_EVIDENCE_CANDIDATES = 50
MAX_PARALLEL_LIMIT = 2


def _utcnow():
    return datetime.now(timezone.utc).isoformat()


class ObjectFailed(Exception):
    """对象长度或摘要验真失败：相关候选段从此不可见。"""

    def __init__(self, decl, observed_sha256, observed_bytes, reused_bad=False):
        self.decl = decl
        self.evidence = {
            "url": decl.url, "revision": decl.revision,
            "expected_sha256": decl.sha256, "observed_sha256": observed_sha256,
            "expected_bytes": decl.bytes, "observed_bytes": observed_bytes,
            "reused_bad_record": reused_bad,
        }
        super().__init__(f"对象验真失败: {decl.url}@{decl.revision}")


class BudgetAbort(Exception):
    """继续读取将超出 payload 预算（按声明长度预检）。"""

    def __init__(self, decl, spent, reserved, budget):
        self.decl = decl
        self.evidence = {
            "next_object": {"url": decl.url, "revision": decl.revision,
                            "declared_bytes": decl.bytes},
            "spent": spent, "reserved": reserved, "budget": budget,
        }
        super().__init__(f"预算不足: 已花费 {spent}，下一对象需 {decl.bytes}，预算 {budget}")


class Infeasible(Exception):
    """当前状态下不存在可行覆盖。"""

    def __init__(self, evidence):
        self.evidence = evidence
        super().__init__("无可行覆盖")


def group_runs(intervals, assignment):
    """把逐区间指派合并成极大同表示运行段 [(s, e, rep_id)]。"""
    runs = []
    for (s, e), rid in zip(intervals, assignment):
        if runs and runs[-1][2] == rid and runs[-1][1] == s:
            runs[-1] = (runs[-1][0], e, rid)
        else:
            runs.append((s, e, rid))
    return runs


@dataclass
class Plan:
    assignment: list   # 每区间一个表示 id
    runs: list         # [(s, e, rep_id, segments)]
    objects: list      # 有序去重的 ObjectDecl
    cost: int          # 估计本次需从 Origin 读取的 payload 字节
    rule: str


class GlobalPlanner:
    """全局覆盖计划：枚举切换区间上的表示指派（带上限），应用 preferred 决策规则。"""

    def __init__(self, mf, req, proof):
        self.mf = mf
        self.req = req
        self.proof = proof  # decl -> bool：缓存已证明版本及摘要
        self.intervals = req.switch_intervals()

    def _runs_ok(self, runs, bad):
        n = len(runs)
        segs_per_run = []
        for i, (s, e, rid) in enumerate(runs):
            f = M.run_feasibility(self.mf, rid, s, e, bad,
                                  need_left=i > 0, need_right=i < n - 1)
            if not f.ok:
                return None
            segs_per_run.append(f.segments)
        return segs_per_run

    def _objects_for(self, runs, segs_per_run):
        objs, seen = [], set()
        for (s, e, rid), segs in zip(runs, segs_per_run):
            init_decl = self.mf.objects[self.mf.representations[rid].init_url]
            if init_decl.key not in seen:
                seen.add(init_decl.key)
                objs.append(init_decl)
            for seg in segs:
                d = self.mf.objects[seg.url]
                if d.key not in seen:
                    seen.add(d.key)
                    objs.append(d)
        return objs

    def _cost(self, objs):
        return sum(d.bytes for d in objs if not self.proof(d))

    def _greedy_assignment(self, bad):
        """枚举超上限时的回退：逐区间贪心（preferred 优先、保持当前、质量）。"""
        assignment = []
        run_start = self.req.start
        for i, (s, e) in enumerate(self.intervals):
            cands = []
            for rid in self.mf.representations:
                f = M.run_feasibility(self.mf, rid, s, e, bad, False, False)
                if not f.ok:
                    continue
                if assignment and rid != assignment[-1]:
                    fl = M.run_feasibility(self.mf, assignment[-1], run_start, s,
                                           bad, False, True)
                    fr = M.run_feasibility(self.mf, rid, s, e, bad, True, False)
                    if not (fl.ok and fr.ok):
                        continue
                cands.append(rid)
            if not cands:
                return None
            pref = self.req.preferred

            def rank(rid):
                return (0 if rid == pref else 1,
                        0 if assignment and rid == assignment[-1] else 1,
                        -self.mf.representations[rid].quality)

            cands.sort(key=rank)
            assignment.append(cands[0])
            if len(assignment) == 1 or assignment[-1] != assignment[-2]:
                run_start = s
        return assignment

    def enumerate(self, bad):
        """返回 (可行列表, 证据)。可行项: (assignment, runs, segs_per_run, objects, cost)。"""
        mf = self.mf
        per_interval, interval_reasons = [], {}
        for (s, e) in self.intervals:
            ok_reps = []
            for rid in mf.representations:
                f = M.run_feasibility(mf, rid, s, e, bad, False, False)
                if f.ok:
                    ok_reps.append(rid)
                else:
                    interval_reasons[f"{rid}@[{M.fmt(s)},{M.fmt(e)})"] = f.reasons
            per_interval.append(ok_reps)
        feasible, candidates = [], []
        truncated = False
        if all(per_interval):
            total = 1
            for c in per_interval:
                total *= len(c)
            truncated = total > MAX_ENUMERATED_ASSIGNMENTS
            if truncated:
                # 超上限：全 preferred 直接判定（preferred 规则保持精确），
                # 降级情形用贪心构造一个可行候选（不要求最优）
                f_all = M.run_feasibility(mf, self.req.preferred,
                                          self.req.start, self.req.end,
                                          bad, False, False)
                if f_all.ok:
                    ap_runs = [(self.req.start, self.req.end, self.req.preferred)]
                    ap_objs = self._objects_for(ap_runs, [f_all.segments])
                    feasible.append(([self.req.preferred] * len(self.intervals),
                                     ap_runs, [f_all.segments], ap_objs,
                                     self._cost(ap_objs)))
                greedy = self._greedy_assignment(bad)
                if greedy is not None:
                    runs = group_runs(self.intervals, greedy)
                    segs_per_run = self._runs_ok(runs, bad)
                    if segs_per_run is not None:
                        objs = self._objects_for(runs, segs_per_run)
                        feasible.append((greedy, runs, segs_per_run, objs,
                                         self._cost(objs)))
            else:
                for assignment in product(*per_interval):
                    runs = group_runs(self.intervals, assignment)
                    segs_per_run = self._runs_ok(runs, bad)
                    if segs_per_run is None:
                        if len(candidates) < MAX_EVIDENCE_CANDIDATES:
                            candidates.append({"assignment": list(assignment),
                                               "feasible": False,
                                               "reason": "切换点无共同边界或段越界"})
                        continue
                    objs = self._objects_for(runs, segs_per_run)
                    cost = self._cost(objs)
                    feasible.append((list(assignment), runs, segs_per_run, objs, cost))
                    if len(candidates) < MAX_EVIDENCE_CANDIDATES:
                        candidates.append({"assignment": list(assignment),
                                           "feasible": True, "cost": cost})
        evidence = {"interval_reasons": interval_reasons, "candidates": candidates,
                    "truncated": truncated, "feasible_count": len(feasible)}
        return feasible, evidence

    def choose(self, bad, remaining_budget):
        """preferred 决策规则：

        1. 全 preferred 可行且估计本次读取不超预算 -> 必须选它；
        2. 全 preferred 可行但超预算 -> 本请求对 preferred 不可行，允许降级，
           但仍须全覆盖且不超预算；
        3. 全 preferred 不可行 -> 在可行且不超预算的方案中，优先多用 preferred，
           再比成本，再比质量（不允许一律最低质量绕过）。
        """
        mf, req = self.mf, self.req
        feasible, evidence = self.enumerate(bad)
        pref = req.preferred

        def pref_usage(ass):
            return sum(1 for r in ass if r == pref)

        def quality(ass):
            return sum((mf.representations[r].quality for r in ass), start=M.Fraction(0))

        all_pref = [f for f in feasible if all(r == pref for r in f[0])]
        chosen, rule = None, None
        if all_pref and all_pref[0][4] <= remaining_budget:
            chosen, rule = all_pref[0], "preferred_full_coverage_within_budget"
        else:
            affordable = [f for f in feasible if f[4] <= remaining_budget]
            if affordable:
                affordable.sort(key=lambda f: (-pref_usage(f[0]), f[4], -quality(f[0])))
                chosen = affordable[0]
                rule = "preferred_over_budget_downgrade" if all_pref else "no_full_preferred_plan"
            else:
                rule = "no_feasible_coverage" if not feasible else "budget_exceeded_for_all_plans"
        evidence["rule"] = rule
        evidence["remaining_budget"] = remaining_budget
        if all_pref:
            evidence["all_preferred_cost"] = all_pref[0][4]
        if chosen is None:
            evidence["cheapest_feasible_cost"] = min((f[4] for f in feasible), default=None)
            return None, evidence
        plan = Plan(assignment=chosen[0],
                    runs=[(s, e, rid, segs)
                          for (s, e, rid), segs in zip(chosen[1], chosen[2])],
                    objects=chosen[3], cost=chosen[4], rule=rule)
        return plan, evidence


class Engine:
    """一次采集运行：计划 -> 取数验真 -> 原子发布 / 明确失败。"""

    def __init__(self, mf, req, origin, store, strategy="global", control=None):
        if req.preferred not in mf.representations:
            raise ManifestError(f"preferred 表示 {req.preferred!r} 不在清单中")
        self.mf, self.req, self.origin, self.store = mf, req, origin, store
        self.strategy = strategy
        self.control = control
        self.spent = 0
        self._reserved = 0
        self._lock = threading.Lock()
        self.cache_ok = {}      # (url, rev) -> 缓存路径：本次已证明版本及摘要
        self.bad = {}           # (url, rev) -> 失败证据：相关候选段不可见
        self._proven_miss = set()
        self.reads = []         # 每次 Origin 读取（含失败与重复读取）
        self.hits = []          # 每次缓存命中（已证明）
        self.failed_reads = []
        self.decision_events = []
        self.meta_extra = {}    # 清单/请求等输入字节（元数据开销）
        self.eff_parallel = max(1, min(MAX_PARALLEL_LIMIT, req.max_parallel))

    # ---------------------------------------------------------- 缓存证明

    def proof(self, decl):
        """缓存命中前证明版本及摘要；结果在本次运行内记忆。"""
        key = decl.key
        if key in self.bad:
            return False
        if key in self.cache_ok:
            return True
        if key in self._proven_miss:
            return False
        path = self.store.verify_cached(decl)
        if path is None:
            self._proven_miss.add(key)
            return False
        self.cache_ok[key] = str(path)
        self.hits.append({"url": decl.url, "revision": decl.revision,
                          "bytes": decl.bytes, "sha256": decl.sha256})
        return True

    # ---------------------------------------------------------- 取数验真

    def ensure(self, decl):
        """确保一个对象可用：缓存证明 -> 预算预检 -> Origin 读取 -> 验真。"""
        key = decl.key
        if key in self.cache_ok:
            return "cache"
        if key in self.bad:
            ev = self.bad[key]
            raise ObjectFailed(decl, ev["observed_sha256"], ev["observed_bytes"],
                               reused_bad=True)
        with self._lock:
            if self.spent + self._reserved + decl.bytes > self.req.budget:
                raise BudgetAbort(decl, self.spent, self._reserved, self.req.budget)
            self._reserved += decl.bytes
        try:
            try:
                data, sha = self.origin.get(decl.url, decl.revision)
                read_error = None
            except OSError as exc:
                # 源对象不可读：按验真失败处理（0 字节计入），相关候选段不可见
                data, sha, read_error = b"", None, f"{type(exc).__name__}: {exc}"
        finally:
            with self._lock:
                self._reserved -= decl.bytes
        actual = len(data)
        with self._lock:
            self.spent += actual
        self.reads.append({"url": decl.url, "revision": decl.revision,
                           "bytes": actual, "sha256": sha,
                           **({"error": read_error} if read_error else {})})
        ok = read_error is None and actual == decl.bytes and sha == decl.sha256
        self._fire_after_object(decl, ok)
        if not ok:
            self.store.mark_bad(decl, sha, actual)
            self.failed_reads.append({
                "url": decl.url, "revision": decl.revision,
                "expected_sha256": decl.sha256, "observed_sha256": sha,
                "expected_bytes": decl.bytes, "observed_bytes": actual,
                **({"error": read_error} if read_error else {})})
            self.bad[key] = {"observed_sha256": sha, "observed_bytes": actual}
            raise ObjectFailed(decl, sha, actual)
        path = self.store.put_verified(decl, data)
        self.cache_ok[key] = str(path)
        return "origin"

    def execute(self, decls):
        """确保一组对象；并发至多 2 次源读取（max_parallel_reads=1 时串行）。"""
        todo = []
        for d in decls:
            if d.key in self.cache_ok:
                continue
            if d.key in self.bad:
                ev = self.bad[d.key]
                raise ObjectFailed(d, ev["observed_sha256"], ev["observed_bytes"],
                                   reused_bad=True)
            todo.append(d)
        if not todo:
            return
        if self.eff_parallel == 1 or len(todo) == 1:
            for d in todo:
                self.ensure(d)
            return
        errors = []
        with ThreadPoolExecutor(max_workers=self.eff_parallel) as pool:
            futures = [pool.submit(self.ensure, d) for d in todo]
            for fut in as_completed(futures):
                exc = fut.exception()
                if exc is not None:
                    errors.append(exc)
        if errors:
            for e in errors:
                if isinstance(e, ObjectFailed):
                    raise e
            raise errors[0]

    # ---------------------------------------------------------- 控制点

    def _ctx(self):
        return SimpleNamespace(manifest=self.mf, request=self.req, origin=self.origin,
                               store=self.store, spent=lambda: self.spent,
                               bad=self.bad, strategy=self.strategy)

    def _fire_after_plan(self):
        if self.control is not None and hasattr(self.control, "after_plan"):
            self.control.after_plan(self._ctx())

    def _fire_after_object(self, decl, ok):
        if self.control is not None and hasattr(self.control, "after_object"):
            self.control.after_object(self._ctx(), decl, ok)

    # ---------------------------------------------------------- 对象集合

    def _objects_for_runs(self, runs, segs_per_run):
        objs, seen = [], set()
        for (s, e, rid), segs in zip(runs, segs_per_run):
            init_decl = self.mf.objects[self.mf.representations[rid].init_url]
            if init_decl.key not in seen:
                seen.add(init_decl.key)
                objs.append(init_decl)
            for seg in segs:
                d = self.mf.objects[seg.url]
                if d.key not in seen:
                    seen.add(d.key)
                    objs.append(d)
        return objs

    def _interval_objects(self, rid, s, e):
        rep = self.mf.representations[rid]
        init_decl = self.mf.objects[rep.init_url]
        objs, seen = [init_decl], {init_decl.key}
        for seg in rep.segments:
            if seg.empty or seg.start >= e or seg.end <= s:
                continue
            d = self.mf.objects[seg.url]
            if d.key not in seen:
                seen.add(d.key)
                objs.append(d)
        return objs

    # ---------------------------------------------------------- 策略：全局

    def run_global(self):
        planner = GlobalPlanner(self.mf, self.req, self.proof)
        attempts = 0
        while True:
            attempts += 1
            if attempts > len(self.mf.objects) + 2:
                return self.fail("replan_exhausted", {"attempts": attempts})
            remaining = self.req.budget - self.spent
            plan, evidence = planner.choose(self.bad, remaining)
            self.decision_events.append({
                "event": "global_plan", "rule": evidence.get("rule"),
                "chosen": plan.assignment if plan else None,
                "estimated_cost": plan.cost if plan else None,
                "remaining_budget": remaining,
                "cache_hits": sorted([list(k) for k in self.cache_ok]),
                "bad": sorted([list(k) for k in self.bad]),
                "feasible_count": evidence.get("feasible_count"),
                "truncated": evidence.get("truncated"),
                "all_preferred_cost": evidence.get("all_preferred_cost"),
            })
            self._fire_after_plan()
            if plan is None:
                return self.fail(evidence.get("rule", "no_feasible_coverage"), evidence)
            try:
                self.execute(plan.objects)
            except ObjectFailed:
                # 某对象验真失败：相关候选段不可见，其他已验证缓存保留，重规划
                continue
            except BudgetAbort as exc:
                return self.fail("budget_exceeded", {**evidence, "abort": exc.evidence})
            return self.publish(plan)

    # ---------------------------------------------------------- 策略：在线

    def run_online(self):
        mf, req = self.mf, self.req
        intervals = req.switch_intervals()
        pref = req.preferred
        # 全 preferred 预检：可行且本次读取不超预算则必须钉住它
        f_all = M.run_feasibility(mf, pref, req.start, req.end, self.bad, False, False)
        pref_cost, pinned = None, False
        if f_all.ok:
            objs = self._objects_for_runs([(req.start, req.end, pref)], [f_all.segments])
            pref_cost = sum(d.bytes for d in objs if not self.proof(d))
            pinned = pref_cost <= req.budget - self.spent
        self.decision_events.append({
            "event": "online_init", "mode": "pinned" if pinned else "downgrade",
            "all_preferred_viable": f_all.ok, "all_preferred_cost": pref_cost,
            "remaining_budget": req.budget - self.spent,
            "cache_hits": sorted([list(k) for k in self.cache_ok]),
            "bad": sorted([list(k) for k in self.bad]),
        })
        self._fire_after_plan()
        committed = []
        run_start = req.start
        i = 0
        while i < len(intervals):
            s, e = intervals[i]
            try:
                rid = pref if pinned else self._online_choose(
                    i, intervals, committed, run_start)
            except Infeasible as exc:
                return self.fail("no_feasible_coverage", exc.evidence)
            try:
                self.execute(self._interval_objects(rid, s, e))
            except ObjectFailed:
                if pinned:
                    pinned = False  # preferred 对象失效，降级重选本区间
                continue            # bad 已更新，重选同一区间
            except BudgetAbort as exc:
                return self.fail("budget_exceeded",
                                 {"abort": exc.evidence, "interval_index": i})
            committed.append(rid)
            if len(committed) == 1 or committed[-1] != committed[-2]:
                run_start = s
            i += 1
        runs_plain = group_runs(intervals, committed)
        segs_per_run = []
        for idx, (s, e, rid) in enumerate(runs_plain):
            f = M.run_feasibility(mf, rid, s, e, self.bad, idx > 0,
                                  idx < len(runs_plain) - 1)
            segs_per_run.append(f.segments)
        plan = Plan(assignment=committed,
                    runs=[(s, e, rid, segs)
                          for (s, e, rid), segs in zip(runs_plain, segs_per_run)],
                    objects=self._objects_for_runs(runs_plain, segs_per_run),
                    cost=self.spent,
                    rule="online_pinned" if pinned else "online_downgrade")
        return self.publish(plan)

    def _online_choose(self, i, intervals, committed, run_start):
        """逐区间在线选择：preferred 优先，其次保持当前表示，再比边际成本与质量。"""
        s, e = intervals[i]
        cands = []
        for rid in self.mf.representations:
            f = M.run_feasibility(self.mf, rid, s, e, self.bad, False, False)
            if not f.ok:
                continue
            if committed and rid != committed[-1]:
                left = committed[-1]
                fl = M.run_feasibility(self.mf, left, run_start, s, self.bad,
                                       False, True)
                fr = M.run_feasibility(self.mf, rid, s, e, self.bad, True, False)
                if not (fl.ok and fr.ok):
                    continue
            cands.append(rid)
        if not cands:
            raise Infeasible({
                "interval": [M.fmt(s), M.fmt(e)], "interval_index": i,
                "reason": "无可行表示（覆盖洞/无共同边界/对象验真失败）",
                "bad": sorted([list(k) for k in self.bad])})

        def marginal(rid):
            objs = self._interval_objects(rid, s, e)
            return sum(d.bytes for d in objs if not self.proof(d))

        pref = self.req.preferred

        def rank(rid):
            return (0 if rid == pref else 1,
                    0 if committed and rid == committed[-1] else 1,
                    marginal(rid),
                    -self.mf.representations[rid].quality)

        cands.sort(key=rank)
        chosen = cands[0]
        self.decision_events.append({
            "event": "online_interval", "interval": [M.fmt(s), M.fmt(e)],
            "chosen": chosen, "candidates": cands,
            "marginal_cost": marginal(chosen),
            "remaining_budget": self.req.budget - self.spent})
        return chosen

    # ---------------------------------------------------------- 发布与失败

    def _cache_rel(self, decl):
        return str(Path("objects") / decl.sha256)

    def _source_block(self):
        mf, req = self.mf, self.req
        return {
            "manifest_path": mf.path, "manifest_sha256": mf.digest,
            "revision": mf.revision,
            "request_path": req.path, "request_sha256": req.digest,
            "manifest_snapshot": mf.snapshot, "request_snapshot": req.snapshot,
        }

    def _request_block(self):
        req = self.req
        return {"range": [M.fmt(req.start), M.fmt(req.end)],
                "budget_payload_bytes": req.budget,
                "preferred": req.preferred,
                "max_parallel_reads": req.max_parallel,
                "switch_points": [M.fmt(p) for p in req.switch_points]}

    def _metadata(self):
        m = dict(self.store.meta)
        m.update(self.meta_extra)
        return m

    def publish(self, plan):
        mf, req = self.mf, self.req
        segments_out, inits_out = [], []
        seen_init = set()
        for (s, e, rid, segs) in plan.runs:
            rep = mf.representations[rid]
            init_decl = mf.objects[rep.init_url]
            if (rid, rep.init_url) not in seen_init:
                seen_init.add((rid, rep.init_url))
                inits_out.append({"rep": rid, "url": rep.init_url,
                                  "object": init_decl.as_dict(),
                                  "cache": self._cache_rel(init_decl)})
            for seg in segs:
                decl = mf.objects[seg.url]
                segments_out.append({
                    "rep": rid, "number": seg.number, "t": seg.t, "d": seg.d,
                    "scale": seg.scale, "pto": seg.pto, "url": seg.url,
                    "presentation": [M.fmt(seg.start), M.fmt(seg.end)],
                    "raw_presentation": [M.fmt(seg.raw_start), M.fmt(seg.raw_end)],
                    "pto_clipped": seg.pto_clipped,
                    "object": decl.as_dict(), "cache": self._cache_rel(decl)})
        switch_checks = []
        for (s, e, rid, _), (_, _, rid2, _) in zip(plan.runs, plan.runs[1:]):
            left_seg = M.boundary_segment(mf, rid, e, "left", self.bad)
            right_seg = M.boundary_segment(mf, rid2, e, "right", self.bad)
            switch_checks.append({
                "at": M.fmt(e), "left_rep": rid, "right_rep": rid2,
                "common_boundary": left_seg is not None and right_seg is not None,
                "left_segment": ({"url": left_seg.url, "number": left_seg.number,
                                  "end": M.fmt(left_seg.end)} if left_seg else None),
                "right_segment": ({"url": right_seg.url, "number": right_seg.number,
                                   "start": M.fmt(right_seg.start)} if right_seg else None)})
        coverage = {
            "request_range": [M.fmt(req.start), M.fmt(req.end)],
            "switch_points": [M.fmt(p) for p in req.switch_points],
            "intervals": [[M.fmt(a), M.fmt(b)] for a, b in req.switch_intervals()],
            "runs": [{"interval": [M.fmt(s), M.fmt(e)], "rep": rid}
                     for (s, e, rid, _) in plan.runs],
            "switch_checks": switch_checks,
            "per_rep": M.analyze(mf, req, self.bad),
        }
        cost = {"payload_bytes": self.spent, "budget": req.budget,
                "origin_reads": self.reads, "cache_hits": self.hits,
                "failed_reads": self.failed_reads, "metadata": self._metadata()}
        pub = {
            "format": "offline-publication/1",
            "created_utc": _utcnow(),
            "strategy": self.strategy,
            "source": self._source_block(),
            "request": self._request_block(),
            "assignment": [{"interval": [M.fmt(s), M.fmt(e)], "rep": rid}
                           for (s, e, rid, _) in plan.runs],
            "segments": segments_out,
            "inits": inits_out,
            "coverage": coverage,
            "cost": cost,
            "decision": {"rule": plan.rule, "estimated_cost": plan.cost,
                         "events": self.decision_events},
        }
        pub_id = self.store.publish(pub)
        return {"status": "published", "publication_id": pub_id,
                "strategy": self.strategy, "rule": plan.rule,
                "assignment": [[list(r["interval"]), r["rep"]]
                               for r in coverage["runs"]],
                "payload_bytes": self.spent, "budget": req.budget,
                "origin_reads": self.reads, "cache_hits": self.hits,
                "failed_reads": self.failed_reads,
                "decision_events": self.decision_events}

    def fail(self, reason, evidence):
        failure = {
            "format": "offline-failure/1",
            "created_utc": _utcnow(),
            "strategy": self.strategy,
            "reason": reason,
            "source": self._source_block(),
            "request": self._request_block(),
            "evidence": evidence,
            "bad_objects": [{"url": u, "revision": r, **ev}
                            for (u, r), ev in self.bad.items()],
            "coverage": {"per_rep": M.analyze(self.mf, self.req, self.bad)},
            "cost": {"payload_bytes": self.spent, "budget": self.req.budget,
                     "origin_reads": self.reads, "cache_hits": self.hits,
                     "failed_reads": self.failed_reads, "metadata": self._metadata()},
            "decision": {"events": self.decision_events},
        }
        fail_id = self.store.record_failure(failure)
        return {"status": "failed", "failure_id": fail_id, "reason": reason,
                "strategy": self.strategy, "payload_bytes": self.spent,
                "budget": self.req.budget, "origin_reads": self.reads,
                "cache_hits": self.hits, "failed_reads": self.failed_reads,
                "decision_events": self.decision_events}


# ---------------------------------------------------------------- 入口函数

def _load_inputs(manifest_path, request_path):
    mf = Manifest.load(manifest_path)
    req = Request.load(request_path)
    meta = {"manifest_bytes": len(mf.snapshot.encode("utf-8")),
            "request_bytes": len(req.snapshot.encode("utf-8"))}
    return mf, req, meta


def collect(manifest_path, request_path, origin, store_dir,
            strategy="global", control=None):
    """执行一次采集：返回 published / failed 结果字典（失败不抛异常）。"""
    mf, req, meta = _load_inputs(manifest_path, request_path)
    store = Store(store_dir)
    engine = Engine(mf, req, origin, store, strategy=strategy, control=control)
    engine.meta_extra = meta
    if strategy == "global":
        return engine.run_global()
    if strategy == "online":
        return engine.run_online()
    raise ValueError(f"未知策略: {strategy!r}")


def plan(manifest_path, request_path, store_dir, strategy="global"):
    """只出计划与证据，不读 Origin 任何对象字节。"""
    mf, req, meta = _load_inputs(manifest_path, request_path)
    store = Store(store_dir)
    engine = Engine(mf, req, origin=None, store=store, strategy=strategy)
    engine.meta_extra = meta
    if strategy == "global":
        planner = GlobalPlanner(mf, req, engine.proof)
        p, evidence = planner.choose(engine.bad, req.budget)
        out = {"strategy": "global", "rule": evidence.get("rule"),
               "feasible_count": evidence.get("feasible_count"),
               "truncated": evidence.get("truncated"),
               "interval_reasons": evidence.get("interval_reasons"),
               "candidates": evidence.get("candidates")}
        if p is not None:
            out["assignment"] = p.assignment
            out["estimated_cost"] = p.cost
            out["objects"] = [d.as_dict() for d in p.objects]
        return out
    # online：只提交预检（钉住或降级），逐区间选择在执行时才发生
    f_all = M.run_feasibility(mf, req.preferred, req.start, req.end,
                              engine.bad, False, False)
    out = {"strategy": "online", "all_preferred_viable": f_all.ok,
           "all_preferred_reasons": f_all.reasons}
    if f_all.ok:
        objs = engine._objects_for_runs([(req.start, req.end, req.preferred)],
                                        [f_all.segments])
        cost = sum(d.bytes for d in objs if not engine.proof(d))
        out["all_preferred_cost"] = cost
        out["mode"] = "pinned" if cost <= req.budget else "downgrade"
    else:
        out["mode"] = "downgrade"
    return out
