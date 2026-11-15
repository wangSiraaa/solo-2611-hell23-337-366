"""离线片段数据管道的端到端测试：全部真实读字节，不只看场景名。

覆盖：共享 init 计费、同 URL 不同修订、表示内覆盖洞、跨表示边界不齐、
预算刚好与差 1 字节、对象损坏（源侧/缓存侧）、发布控制点（可见修订切换、
原子发布）、PTO 裁切、段超出请求区间、两种策略决策差异、并发上限、
以及随机小清单的 oracle 对照。

所有会改动 Origin 的用例都工作在临时目录的副本上，绝不触碰仓库夹具。
"""
import hashlib
import json
import random
import shutil
import subprocess
import sys
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from collector import collect, plan
from manifest import Manifest, Request, ManifestError
from origin import Origin
from store import Store
from verify import VerifyError, verify_failure, verify_publication

INPUTS = ROOT / "inputs"
REPO_ORIGIN = ROOT / "origin"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------- 工具

def make_origin(root: Path, objects: dict):
    """objects: {(revision, url): bytes} -> 写入本地 Origin 目录树。"""
    for (rev, url), data in objects.items():
        p = root / rev / url
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)


def write_manifest(path: Path, revision, reps, objects, declare=None):
    """按 origin 里的真实字节生成规范化清单（仅写文件，校验见 Manifest.load）。

    reps: [{"id","scale","pto","init","segments":[{"t","d","url"}]}]
    objects: {(rev, url): bytes}；declare: {url: rev} 指定清单声明的修订，
    缺省用清单自身 revision。
    """
    declare = declare or {}
    objs = {}
    for rep in reps:
        for url in [rep["init"]] + [s["url"] for s in rep["segments"]]:
            rev = declare.get(url, revision)
            data = objects[(rev, url)]
            objs[url] = {"revision": rev, "bytes": len(data), "sha256": sha(data)}
    mreps = []
    for rep in reps:
        segs = []
        for s in rep["segments"]:
            rev = declare.get(s["url"], revision)
            segs.append({"t": s["t"], "d": s["d"], "url": s["url"],
                         "bytes": len(objects[(rev, s["url"])])})
        mreps.append({"id": rep["id"], "scale": rep["scale"], "pto": rep["pto"],
                      "init": rep["init"], "segments": segs})
    doc = {"revision": revision, "representations": mreps, "objects": objs}
    path.write_text(json.dumps(doc, indent=2))
    return path


def write_request(path: Path, start="0", end="4", budget=40, preferred="high",
                  switch_points=("0", "2", "4"), max_parallel=1):
    doc = {"range": [start, end], "budget_payload_bytes": budget,
           "preferred": preferred, "max_parallel_reads": max_parallel,
           "switch_points": list(switch_points)}
    path.write_text(json.dumps(doc, indent=2))
    return path


class TmpEnv(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="pipe-test-"))
        self.store = self.tmp / "store"
        self._org_seq = 0

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def fresh_origin(self):
        """把仓库 origin/ 复制到临时目录，返回 Origin；用例可随意破坏它。"""
        self._org_seq += 1
        dst = self.tmp / f"org{self._org_seq}"
        shutil.copytree(REPO_ORIGIN, dst)
        return Origin(dst)

    def run_collect(self, manifest, request, origin, strategy="global", control=None):
        return collect(str(manifest), str(request), origin, str(self.store),
                       strategy=strategy, control=control)


# ---------------------------------------------------------------- 夹具自洽

class TestFixtureConsistency(TmpEnv):
    def test_declared_objects_match_origin_bytes(self):
        """inputs/ 每个清单声明的修订/长度/摘要都与 origin/ 实际字节一致。"""
        for mf_path in sorted(INPUTS.glob("*.json")):
            if mf_path.name == "request.json":
                continue
            mf = Manifest.load(mf_path)
            for decl in mf.objects.values():
                data = (REPO_ORIGIN / decl.revision / decl.url).read_bytes()
                self.assertEqual(len(data), decl.bytes, (mf_path.name, decl.url))
                self.assertEqual(sha(data), decl.sha256, (mf_path.name, decl.url))


# ---------------------------------------------------------------- 主输入场景

class TestMainInputs(TmpEnv):
    """使用仓库 inputs/ 的真实场景（Origin 用临时副本）。"""

    def test_shared_init_billed_once_and_preferred_chosen(self):
        """共享 init 只计一次费；preferred 全覆盖且不超预算时必须选它。"""
        origin = self.fresh_origin()
        res = self.run_collect(INPUTS / "r2.json", INPUTS / "request.json", origin)
        self.assertEqual(res["status"], "published")
        self.assertEqual(res["rule"], "preferred_full_coverage_within_budget")
        # 只读所选表示：init(8) + high-0(15) + high-1(15) = 38，init 只读一次
        self.assertEqual(res["payload_bytes"], 38)
        init_reads = [r for r in res["origin_reads"] if r["url"] == "init.bin"]
        self.assertEqual(len(init_reads), 1)
        self.assertEqual({r["url"] for r in res["origin_reads"]},
                         {"init.bin", "high-0.bin", "high-1.bin"})
        self.assertEqual([a[1] for a in res["assignment"]], ["high"])
        self.assertTrue(verify_publication(Store(self.store), "current")["ok"])

    def test_same_url_different_revisions_not_reused(self):
        """同 URL 不同修订是不同对象：r2 必须重新读取，旧发布仍可核验。"""
        r1 = self.run_collect(INPUTS / "r1.json", INPUTS / "request.json",
                              self.fresh_origin())
        self.assertEqual(r1["status"], "published")
        self.assertEqual(r1["payload_bytes"], 38)
        r2 = self.run_collect(INPUTS / "r2.json", INPUTS / "request.json",
                              self.fresh_origin())
        self.assertEqual(r2["status"], "published")
        # 缓存里已有 r1 同 URL 对象，但修订/摘要不同 => 全部重新读取
        self.assertEqual(r2["payload_bytes"], 38)
        self.assertEqual({(r["url"], r["revision"]) for r in r2["origin_reads"]},
                         {("init.bin", "r2"), ("high-0.bin", "r2"), ("high-1.bin", "r2")})
        store = Store(self.store)
        # 两个修订的对象都在缓存中
        m1 = Manifest.load(INPUTS / "r1.json")
        m2 = Manifest.load(INPUTS / "r2.json")
        self.assertTrue(store.object_path(m1.objects["init.bin"].sha256).exists())
        self.assertTrue(store.object_path(m2.objects["init.bin"].sha256).exists())
        # 当前发布的 init 是 r2 内容（不再引用旧初始化段）
        pub2 = store.load_publication("current")
        self.assertEqual(pub2["inits"][0]["object"]["revision"], "r2")
        self.assertEqual(pub2["inits"][0]["object"]["sha256"],
                         m2.objects["init.bin"].sha256)
        # 旧发布仍能读、仍能核验
        self.assertTrue(verify_publication(store, r1["publication_id"])["ok"])
        self.assertTrue(verify_publication(store, r2["publication_id"])["ok"])

    def test_hole_in_preferred_rep_no_cross_revision_fill(self):
        """preferred 有覆盖洞：同修订内组合覆盖；绝不拿另一修订的段补洞。"""
        origin = self.fresh_origin()
        res = self.run_collect(INPUTS / "high-gap.json", INPUTS / "request.json", origin)
        self.assertEqual(res["status"], "published")
        # high 在 [2,4) 有洞；high->low 在 2 有共同边界，混合方案被选中
        self.assertEqual([a[1] for a in res["assignment"]], ["high", "low"])
        self.assertEqual(res["payload_bytes"], 8 + 15 + 7)
        # 所有读取都钉在清单修订 r2 上（origin 里明明有 r1 的 high-1.bin）
        self.assertTrue(res["origin_reads"])
        self.assertEqual({r["revision"] for r in res["origin_reads"]}, {"r2"})
        store = Store(self.store)
        pub = store.load_publication("current")
        self.assertEqual(pub["coverage"]["per_rep"]["high"]["holes"], [["2", "4"]])
        self.assertEqual(pub["coverage"]["switch_checks"][0]["common_boundary"], True)
        self.assertTrue(verify_publication(store, "current")["ok"])

    def test_hole_everywhere_is_evidenced_failure(self):
        """唯一表示有洞且无替代：返回有证据的不可交付。"""
        objects = {("r1", "init.bin"): b"INITINIT",
                   ("r1", "a-0.bin"): b"aaaaaaaaaa"}
        make_origin(self.tmp / "org", objects)
        mf = write_manifest(self.tmp / "m.json", "r1", [
            {"id": "a", "scale": 10, "pto": 0, "init": "init.bin",
             "segments": [{"t": 0, "d": 20, "url": "a-0.bin"}]}], objects)
        req = write_request(self.tmp / "q.json", budget=1000, preferred="a")
        res = self.run_collect(mf, req, Origin(self.tmp / "org"))
        self.assertEqual(res["status"], "failed")
        self.assertEqual(res["reason"], "no_feasible_coverage")
        store = Store(self.store)
        fail = store.load_failure(res["failure_id"])
        self.assertEqual(fail["coverage"]["per_rep"]["a"]["holes"], [["2", "4"]])
        self.assertTrue(verify_failure(store, res["failure_id"])["ok"])
        self.assertIsNone(store.current())  # 失败不产生发布

    def test_unaligned_boundary_between_reps(self):
        """跨表示边界不齐：high->low 可行（共同边界在 2），low->high 不可行。"""
        origin = self.fresh_origin()
        res = self.run_collect(INPUTS / "unaligned.json", INPUTS / "request.json", origin)
        self.assertEqual(res["status"], "published")
        self.assertEqual([a[1] for a in res["assignment"]], ["high", "low"])
        self.assertEqual(res["payload_bytes"], 8 + 15 + 7)
        store = Store(self.store)
        pub = store.load_publication("current")
        # high 的洞 [2, 2.1) 与重叠、PTO 裁切分列
        self.assertEqual(pub["coverage"]["per_rep"]["high"]["holes"], [["2", "21/10"]])
        self.assertEqual(pub["coverage"]["per_rep"]["high"]["overlaps"], [])
        chk = pub["coverage"]["switch_checks"][0]
        self.assertEqual(chk["at"], "2")
        self.assertEqual((chk["left_segment"]["url"], chk["right_segment"]["url"]),
                         ("high-0.bin", "low-1.bin"))
        ev = pub["decision"]["events"][0]
        self.assertEqual(ev["rule"], "no_full_preferred_plan")
        self.assertTrue(verify_publication(store, "current")["ok"])

    def test_overlap_reported_distinct_from_hole(self):
        """相邻段重叠与覆盖洞区分：重叠不致命，但要如实报告。"""
        objects = {("r1", "init.bin"): b"INITINIT",
                   ("r1", "a-0.bin"): b"0123456789",
                   ("r1", "a-1.bin"): b"0123456789"}
        make_origin(self.tmp / "org", objects)
        mf = write_manifest(self.tmp / "m.json", "r1", [
            {"id": "a", "scale": 10, "pto": 0, "init": "init.bin",
             "segments": [{"t": 0, "d": 25, "url": "a-0.bin"},
                          {"t": 20, "d": 20, "url": "a-1.bin"}]}], objects)
        req = write_request(self.tmp / "q.json", budget=1000, preferred="a")
        res = self.run_collect(mf, req, Origin(self.tmp / "org"))
        self.assertEqual(res["status"], "published")
        store = Store(self.store)
        pub = store.load_publication("current")
        self.assertEqual(pub["coverage"]["per_rep"]["a"]["overlaps"], [["2", "5/2"]])
        self.assertEqual(pub["coverage"]["per_rep"]["a"]["holes"], [])
        self.assertTrue(verify_publication(store, "current")["ok"])


# ---------------------------------------------------------------- 预算

class TestBudget(TmpEnv):
    def _run(self, budget, manifest="r2.json", preferred="high"):
        req = write_request(self.tmp / f"q{budget}.json", budget=budget,
                            preferred=preferred)
        return self.run_collect(INPUTS / manifest, req, self.fresh_origin())

    def test_budget_exact_fit(self):
        """预算刚好 38 == init8+15+15：全 preferred 成功。"""
        res = self._run(38)
        self.assertEqual(res["status"], "published")
        self.assertEqual(res["payload_bytes"], 38)
        self.assertEqual([a[1] for a in res["assignment"]], ["high"])

    def test_budget_one_below_preferred_downgrades(self):
        """差 1 字节（37 < 38）：preferred 完整覆盖但超预算 -> 降级全覆盖。

        降级排序优先多用 preferred：low[0,2)+high[2,4) = 8+7+15 = 30 <= 37，
        比全 low(22) 更贴近 preferred，且仍全覆盖、不超预算。
        """
        res = self._run(37)
        self.assertEqual(res["status"], "published")
        self.assertEqual(res["rule"], "preferred_over_budget_downgrade")
        self.assertEqual([a[1] for a in res["assignment"]], ["low", "high"])
        self.assertEqual(res["payload_bytes"], 30)
        self.assertTrue(verify_publication(Store(self.store), "current")["ok"])
        # 预算再紧一档（29 < 30）：混合也超预算 -> 全 low(22) 兜底
        self.store = self.tmp / "store-tight"
        res2 = self._run(29)
        self.assertEqual(res2["status"], "published")
        self.assertEqual([a[1] for a in res2["assignment"]], ["low"])
        self.assertEqual(res2["payload_bytes"], 22)

    def test_budget_exact_low_and_one_below_fails(self):
        """low 刚好 22 成功；21 差 1 字节 -> 有证据的不可交付。"""
        ok = self._run(22)
        self.assertEqual(ok["status"], "published")
        self.assertEqual(ok["payload_bytes"], 22)
        # 差 1 字节：用全新存储，避免暖缓存把成本变成 0
        self.store = self.tmp / "store-cold"
        fail = self._run(21)
        self.assertEqual(fail["status"], "failed")
        self.assertEqual(fail["reason"], "budget_exceeded_for_all_plans")
        store2 = Store(self.store)
        rec = store2.load_failure(fail["failure_id"])
        self.assertEqual(rec["evidence"]["cheapest_feasible_cost"], 22)
        self.assertEqual(rec["cost"]["payload_bytes"], 0)  # 一次都没读就判定
        self.assertTrue(verify_failure(store2, fail["failure_id"])["ok"])
        self.assertIsNone(store2.current())  # 失败不产生发布

    def test_warm_cache_zero_payload(self):
        """缓存已证明版本及摘要：重复运行 payload 为 0，预算 0 也能发布。"""
        first = self._run(38)
        self.assertEqual(first["status"], "published")
        second = self._run(0)
        self.assertEqual(second["status"], "published")
        self.assertEqual(second["payload_bytes"], 0)
        self.assertEqual(len(second["origin_reads"]), 0)
        self.assertEqual(len(second["cache_hits"]), 3)
        self.assertTrue(verify_publication(Store(self.store), "current")["ok"])


# ---------------------------------------------------------------- 对象损坏

class _CorruptAfterPlan:
    """控制点：plan 之后破坏指定对象。"""

    def __init__(self, *targets):
        self.targets = list(targets)

    def after_plan(self, ctx):
        for url, rev in self.targets:
            ctx.origin.corrupt(url, rev)


class TestCorruption(TmpEnv):
    def test_origin_corruption_replans_and_publishes(self):
        """源对象损坏：验真失败 -> 相关段不可见 -> 重规划；失败读取计入预算。"""
        req = write_request(self.tmp / "q.json", budget=60, preferred="high")
        origin = self.fresh_origin()
        ctl = _CorruptAfterPlan(("high-1.bin", "r2"))
        res = self.run_collect(INPUTS / "r2.json", req, origin, control=ctl)
        self.assertEqual(res["status"], "published")
        # init8 + high-0 15 + high-1 15(失败读取也计费) + low-1 7 = 45
        self.assertEqual(res["payload_bytes"], 45)
        self.assertEqual([r["url"] for r in res["failed_reads"]], ["high-1.bin"])
        self.assertEqual([a[1] for a in res["assignment"]], ["high", "low"])
        store = Store(self.store)
        self.assertEqual(store.object_state("high-1.bin", "r2"), "bad")
        self.assertTrue(verify_publication(store, "current")["ok"])
        # 其他已验证缓存保留：源已修复的新运行里，init/high-0/low-1 全部命中，
        # 只有 high-1 因上次验真失败被重读（计费 15）并重新验真 -> 恢复全 preferred
        res2 = self.run_collect(INPUTS / "r2.json", req, self.fresh_origin())
        self.assertEqual(res2["status"], "published")
        self.assertEqual(res2["payload_bytes"], 15)
        self.assertEqual([r["url"] for r in res2["origin_reads"]], ["high-1.bin"])
        self.assertEqual({h["url"] for h in res2["cache_hits"]},
                         {"init.bin", "high-0.bin", "low-1.bin"})
        self.assertEqual([a[1] for a in res2["assignment"]], ["high"])
        self.assertEqual(store.object_state("high-1.bin", "r2"), "verified")

    def test_origin_corruption_budget_exhausted_fails(self):
        """损坏 + 预算不够重规划：失败读取计入后超预算 -> 明确失败。"""
        req = write_request(self.tmp / "q.json", budget=40, preferred="high")
        origin = self.fresh_origin()
        ctl = _CorruptAfterPlan(("high-1.bin", "r2"))
        res = self.run_collect(INPUTS / "r2.json", req, origin, control=ctl)
        self.assertEqual(res["status"], "failed")
        self.assertEqual(res["reason"], "budget_exceeded_for_all_plans")
        # 38 已花（含失败读取），重规划最省还需 7，超过剩余 2
        self.assertEqual(res["payload_bytes"], 38)
        store = Store(self.store)
        self.assertIsNone(store.current())
        self.assertTrue(verify_failure(store, res["failure_id"])["ok"])

    def test_corruption_of_unneeded_object_is_irrelevant(self):
        """损坏计划用不到的对象：不影响发布。"""
        req = write_request(self.tmp / "q.json", budget=40, preferred="high")
        origin = self.fresh_origin()
        ctl = _CorruptAfterPlan(("low-1.bin", "r2"))
        res = self.run_collect(INPUTS / "r2.json", req, origin, control=ctl)
        self.assertEqual(res["status"], "published")
        self.assertEqual(res["payload_bytes"], 38)

    def test_missing_origin_object_is_evidenced_failure(self):
        """源对象缺失：按验真失败处理（0 字节），相关段不可见后重规划。"""
        req = write_request(self.tmp / "q.json", budget=60, preferred="high")
        origin = self.fresh_origin()
        (origin.root / "r2" / "high-1.bin").unlink()
        res = self.run_collect(INPUTS / "r2.json", req, origin)
        self.assertEqual(res["status"], "published")
        self.assertEqual([a[1] for a in res["assignment"]], ["high", "low"])
        # init8 + high-0 15 + high-1 0(缺失) + low-1 7 = 30
        self.assertEqual(res["payload_bytes"], 30)
        self.assertEqual(res["failed_reads"][0]["url"], "high-1.bin")
        self.assertEqual(res["failed_reads"][0]["observed_bytes"], 0)
        self.assertIn("error", res["failed_reads"][0])
        self.assertTrue(verify_publication(Store(self.store), "current")["ok"])

    def test_cache_corruption_forces_refetch(self):
        """缓存文件被篡改：命中前证明失败 -> 重新从 Origin 读取（计费）。"""
        req = write_request(self.tmp / "q.json", budget=40, preferred="high")
        first = self.run_collect(INPUTS / "r2.json", req, self.fresh_origin())
        self.assertEqual(first["status"], "published")
        store = Store(self.store)
        m = Manifest.load(INPUTS / "r2.json")
        victim = store.object_path(m.objects["high-0.bin"].sha256)
        data = bytearray(victim.read_bytes())
        data[0] ^= 0xFF
        victim.write_bytes(bytes(data))
        second = self.run_collect(INPUTS / "r2.json", req, self.fresh_origin())
        self.assertEqual(second["status"], "published")
        # 只有被篡改的 high-0 重新读取；init/high-1 仍命中
        self.assertEqual(second["payload_bytes"], 15)
        self.assertEqual([r["url"] for r in second["origin_reads"]], ["high-0.bin"])
        self.assertTrue(verify_publication(Store(self.store), "current")["ok"])


# ---------------------------------------------------------------- 发布控制点

class _SwitchVisibleAfterPlan:
    def __init__(self, rev):
        self.rev = rev

    def after_plan(self, ctx):
        ctx.origin.switch_visible(self.rev)


class _CorruptAfterFirstObject:
    """控制点：任一对象返回后破坏另一个对象。"""

    def __init__(self, url, rev):
        self.target = (url, rev)
        self.done = False

    def after_object(self, ctx, decl, ok):
        if not self.done:
            self.done = True
            ctx.origin.corrupt(*self.target)


class TestControlPoints(TmpEnv):
    def test_visible_revision_switch_does_not_leak(self):
        """plan 后切换可见源修订：管线钉住清单修订读取，结果不受影响。"""
        req = write_request(self.tmp / "q.json", budget=40, preferred="high")
        origin = self.fresh_origin()
        origin.visible_revision = "r2"
        ctl = _SwitchVisibleAfterPlan("r1")
        res = self.run_collect(INPUTS / "r2.json", req, origin, control=ctl)
        self.assertEqual(res["status"], "published")
        self.assertEqual({r["revision"] for r in res["origin_reads"]}, {"r2"})
        # 演示风险：可见指针确实指向了不同内容，但我们从不按指针读
        visible_bytes, _ = origin.get_visible("init.bin")
        m2 = Manifest.load(INPUTS / "r2.json")
        self.assertNotEqual(sha(visible_bytes), m2.objects["init.bin"].sha256)
        self.assertTrue(verify_publication(Store(self.store), "current")["ok"])

    def test_corrupt_after_first_object_return(self):
        """任一对象返回后破坏另一对象：轮到它读取时被验真挡下。"""
        req = write_request(self.tmp / "q.json", budget=60, preferred="high")
        origin = self.fresh_origin()
        ctl = _CorruptAfterFirstObject("high-1.bin", "r2")
        res = self.run_collect(INPUTS / "r2.json", req, origin, control=ctl)
        self.assertEqual(res["status"], "published")
        self.assertEqual([r["url"] for r in res["failed_reads"]], ["high-1.bin"])
        self.assertEqual([a[1] for a in res["assignment"]], ["high", "low"])

    def test_atomic_failure_keeps_old_publication(self):
        """发布原子性：失败只追加失败记录，current 与旧发布原样可读。"""
        req_ok = write_request(self.tmp / "q1.json", budget=40, preferred="high")
        good = self.run_collect(INPUTS / "r2.json", req_ok, self.fresh_origin())
        self.assertEqual(good["status"], "published")
        store = Store(self.store)
        self.assertEqual(store.current(), good["publication_id"])
        # 第二次：high-1 在缓存里，先破坏其缓存文件与源对象，low-1 源也破坏，
        # 于是所有方案都缺对象 -> 不可交付
        origin2 = self.fresh_origin()
        origin2.corrupt("high-1.bin", "r2")
        origin2.corrupt("low-1.bin", "r2")
        m = Manifest.load(INPUTS / "r2.json")
        victim = store.object_path(m.objects["high-1.bin"].sha256)
        data = bytearray(victim.read_bytes())
        data[0] ^= 0xFF
        victim.write_bytes(bytes(data))
        req2 = write_request(self.tmp / "q2.json", budget=40, preferred="high")
        bad = self.run_collect(INPUTS / "r2.json", req2, origin2)
        self.assertEqual(bad["status"], "failed")
        self.assertEqual(bad["reason"], "no_feasible_coverage")
        self.assertEqual(store.current(), good["publication_id"])  # 指针未动
        self.assertTrue(verify_failure(store, bad["failure_id"])["ok"])
        # 旧发布清单仍可读；未被波及的已验证缓存保留
        pub = store.load_publication(good["publication_id"])
        self.assertEqual(pub["assignment"][0]["rep"], "high")
        self.assertEqual(store.object_state("init.bin", "r2"), "verified")
        self.assertEqual(store.object_state("high-0.bin", "r2"), "verified")

    def test_two_publications_old_still_verifies(self):
        """连续两次发布：current 指向新的，旧发布仍能完整核验。"""
        req = write_request(self.tmp / "q.json", budget=40, preferred="high")
        p1 = self.run_collect(INPUTS / "r1.json", req, self.fresh_origin())
        p2 = self.run_collect(INPUTS / "r2.json", req, self.fresh_origin())
        store = Store(self.store)
        self.assertEqual(store.current(), p2["publication_id"])
        self.assertTrue(verify_publication(store, p1["publication_id"])["ok"])
        self.assertTrue(verify_publication(store, p2["publication_id"])["ok"])


# ---------------------------------------------------------------- 时间与边界

class TestTimeMapping(TmpEnv):
    def test_pto_clip_distinguished(self):
        """PTO 裁切：负展示时间裁到 0，与洞/重叠分列；原始寻址保留。"""
        objects = {("r1", "init.bin"): b"INITINIT",
                   ("r1", "a-0.bin"): b"0123456789",
                   ("r1", "a-1.bin"): b"0123456789"}
        make_origin(self.tmp / "org", objects)
        mf = write_manifest(self.tmp / "m.json", "r1", [
            {"id": "a", "scale": 10, "pto": 5, "init": "init.bin",
             "segments": [{"t": 0, "d": 20, "url": "a-0.bin"},
                          {"t": 20, "d": 20, "url": "a-1.bin"}]}], objects)
        req = write_request(self.tmp / "q.json", start="0", end="3",
                            budget=100, preferred="a", switch_points=["0", "3"])
        res = self.run_collect(mf, req, Origin(self.tmp / "org"))
        self.assertEqual(res["status"], "published")
        store = Store(self.store)
        pub = store.load_publication("current")
        clips = pub["coverage"]["per_rep"]["a"]["pto_clips"]
        self.assertEqual([(c["number"], c["raw_start"]) for c in clips],
                         [(0, "-1/2")])
        seg0 = pub["segments"][0]
        self.assertEqual((seg0["t"], seg0["d"], seg0["scale"], seg0["pto"]),
                         (0, 20, 10, 5))  # 转存保留原寻址
        self.assertEqual(seg0["presentation"], ["0", "3/2"])
        self.assertTrue(verify_publication(store, "current")["ok"])

    def test_segments_may_exceed_request_range(self):
        """段可超出请求区间：首尾段越界允许，发布只覆盖请求区间。"""
        req = write_request(self.tmp / "q.json", start="1/2", end="7/2",
                            budget=40, preferred="high", switch_points=["2"])
        res = self.run_collect(INPUTS / "r2.json", req, self.fresh_origin())
        self.assertEqual(res["status"], "published")
        self.assertEqual(res["payload_bytes"], 38)
        store = Store(self.store)
        pub = store.load_publication("current")
        pres = [tuple(s["presentation"]) for s in pub["segments"]]
        self.assertEqual(pres, [("0", "2"), ("2", "4")])  # 段本身越界但原样转存
        self.assertTrue(verify_publication(store, "current")["ok"])

    def test_verify_detects_tampering(self):
        """核验能抓住：缓存对象被改、发布清单时间映射被改。"""
        req = write_request(self.tmp / "q.json", budget=40, preferred="high")
        res = self.run_collect(INPUTS / "r2.json", req, self.fresh_origin())
        store = Store(self.store)
        pub_id = res["publication_id"]
        # 篡改缓存对象字节
        m = Manifest.load(INPUTS / "r2.json")
        victim = store.object_path(m.objects["high-0.bin"].sha256)
        data = bytearray(victim.read_bytes())
        data[0] ^= 0xFF
        victim.write_bytes(bytes(data))
        with self.assertRaises(VerifyError):
            verify_publication(store, pub_id)
        # 恢复后再篡改发布清单里的 t
        victim.write_bytes((REPO_ORIGIN / "r2/high-0.bin").read_bytes())
        mpath = self.store / "publications" / pub_id / "manifest.json"
        pub = json.loads(mpath.read_text())
        pub["segments"][0]["t"] += 1
        mpath.write_text(json.dumps(pub, indent=2))
        with self.assertRaises(VerifyError):
            verify_publication(store, pub_id)


# ---------------------------------------------------------------- 策略

class TestStrategies(TmpEnv):
    """两种策略都实现；同一 preferred 规则下，决策实际影响预算/覆盖/质量。"""

    def _scenario(self):
        objects = {
            ("r1", "init.bin"): b"INITINIT",          # 8B，A/B/C 共享
            ("r1", "a-0.bin"): b"aaaaaaa",            # 7B
            ("r1", "a-1.bin"): b"AAAAAAA",            # 7B
            ("r1", "b-0.bin"): b"bbbbbb",             # 6B
            ("r1", "b-1.bin"): b"B" * 20,             # 20B
        }
        make_origin(self.tmp / "org", objects)
        mf = write_manifest(self.tmp / "m.json", "r1", [
            {"id": "A", "scale": 10, "pto": 0, "init": "init.bin",
             "segments": [{"t": 0, "d": 20, "url": "a-0.bin"},
                          {"t": 20, "d": 20, "url": "a-1.bin"}]},
            {"id": "B", "scale": 10, "pto": 0, "init": "init.bin",
             "segments": [{"t": 0, "d": 20, "url": "b-0.bin"},
                          {"t": 20, "d": 20, "url": "b-1.bin"}]},
            {"id": "C", "scale": 10, "pto": 0, "init": "init.bin",
             "segments": []},  # preferred 但没有任何段 -> 全覆盖不可行
        ], objects)
        req = write_request(self.tmp / "q.json", budget=40, preferred="C")
        return mf, req

    def test_global_vs_online_decisions_differ(self):
        mf, req = self._scenario()
        g = self.run_collect(mf, req, Origin(self.tmp / "org"), strategy="global")
        self.assertEqual(g["status"], "published")
        # 全局：B[0,2)+A[2,4) 共享 init -> 8+6+7=21 最省
        self.assertEqual([a[1] for a in g["assignment"]], ["B", "A"])
        self.assertEqual(g["payload_bytes"], 21)
        init_reads = [r for r in g["origin_reads"] if r["url"] == "init.bin"]
        self.assertEqual(len(init_reads), 1)  # 跨表示共享 init 只计一次
        shutil.rmtree(self.store)
        o = self.run_collect(mf, req, Origin(self.tmp / "org"), strategy="online")
        self.assertEqual(o["status"], "published")
        # 在线：区间 1 选边际最省的 B，区间 2 倾向保持 B -> 单个运行段，8+6+20=34
        self.assertEqual([a[1] for a in o["assignment"]], ["B"])
        self.assertEqual(o["assignment"][0][0], ["0", "4"])
        self.assertEqual(o["payload_bytes"], 34)
        # 两者都全覆盖且不超预算，但决策实际影响了成本与质量
        self.assertLess(g["payload_bytes"], o["payload_bytes"])
        self.assertTrue(verify_publication(Store(self.store), "current")["ok"])

    def test_both_strategies_honor_preferred_rule(self):
        for strategy in ("global", "online"):
            shutil.rmtree(self.store, ignore_errors=True)
            req = write_request(self.tmp / f"q-{strategy}.json", budget=40,
                                preferred="high")
            res = self.run_collect(INPUTS / "r2.json", req, self.fresh_origin(),
                                   strategy=strategy)
            self.assertEqual(res["status"], "published", strategy)
            self.assertEqual([a[1] for a in res["assignment"]], ["high"], strategy)
            self.assertEqual(res["payload_bytes"], 38, strategy)
            self.assertTrue(verify_publication(Store(self.store), "current")["ok"])

    def test_online_recovers_from_corruption(self):
        req = write_request(self.tmp / "q.json", budget=60, preferred="high")
        origin = self.fresh_origin()
        ctl = _CorruptAfterPlan(("high-1.bin", "r2"))
        res = self.run_collect(INPUTS / "r2.json", req, origin,
                               strategy="online", control=ctl)
        self.assertEqual(res["status"], "published")
        self.assertEqual([a[1] for a in res["assignment"]], ["high", "low"])
        self.assertEqual(res["payload_bytes"], 45)
        self.assertTrue(verify_publication(Store(self.store), "current")["ok"])

    def test_enumeration_cap_falls_back_to_greedy(self):
        """切换点×表示数超枚举上限：全 preferred 仍精确判定，降级走贪心。"""
        objects = {("r1", "init.bin"): b"INITINIT"}
        reps = []
        for r in range(8):  # 8 表示 × 8 区间 = 8^8 种指派，远超枚举上限
            segs = []
            for i in range(8):
                url = f"r{r}-s{i}.bin"
                objects[("r1", url)] = bytes([65 + r]) * (5 + r)
                segs.append({"t": i * 10, "d": 10, "url": url})
            reps.append({"id": f"rep{r}", "scale": 10, "pto": 0,
                         "init": "init.bin", "segments": segs})
        make_origin(self.tmp / "org", objects)
        mf = write_manifest(self.tmp / "m.json", "r1", reps, objects)
        req = write_request(self.tmp / "q.json", start="0", end="8",
                            budget=10_000, preferred="rep3",
                            switch_points=[str(i) for i in range(9)])
        res = self.run_collect(mf, req, Origin(self.tmp / "org"))
        self.assertEqual(res["status"], "published")
        # 全 preferred 直接判定：即使枚举被截断也必须选它
        self.assertEqual([a[1] for a in res["assignment"]], ["rep3"])
        ev = res["decision_events"][0]
        self.assertTrue(ev["truncated"])
        self.assertEqual(res["payload_bytes"], 8 + 8 * 8)
        self.assertTrue(verify_publication(Store(self.store), "current")["ok"])


# ---------------------------------------------------------------- 并发

class TestParallel(TmpEnv):
    def _manifest(self, n_segments=4):
        objects = {("r1", "init.bin"): b"INITINIT"}
        segs = []
        for i in range(n_segments):
            url = f"a-{i}.bin"
            objects[("r1", url)] = bytes([65 + i]) * 10
            segs.append({"t": i * 10, "d": 10, "url": url})
        make_origin(self.tmp / "org", objects)
        return write_manifest(self.tmp / "m.json", "r1", [
            {"id": "a", "scale": 10, "pto": 0, "init": "init.bin",
             "segments": segs}], objects)

    def test_parallel_reads_capped_at_two(self):
        mf = self._manifest()
        req = write_request(self.tmp / "q.json", budget=1000, preferred="a",
                            switch_points=[], max_parallel=2)
        origin = Origin(self.tmp / "org", delay=0.05)
        res = self.run_collect(mf, req, origin)
        self.assertEqual(res["status"], "published")
        self.assertEqual(origin.max_inflight, 2)  # 真的并发了，但没超 2

    def test_serial_when_max_parallel_one(self):
        mf = self._manifest()
        req = write_request(self.tmp / "q.json", budget=1000, preferred="a",
                            switch_points=[], max_parallel=1)
        origin = Origin(self.tmp / "org", delay=0.05)
        res = self.run_collect(mf, req, origin)
        self.assertEqual(res["status"], "published")
        self.assertEqual(origin.max_inflight, 1)


# ---------------------------------------------------------------- 校验

class TestValidation(TmpEnv):
    def test_limits_enforced(self):
        objects = {("r1", "init.bin"): b"INITINIT"}
        make_origin(self.tmp / "org", objects)
        reps = [{"id": f"r{i}", "scale": 10, "pto": 0, "init": "init.bin",
                 "segments": []} for i in range(9)]
        mf9 = write_manifest(self.tmp / "m9.json", "r1", reps, objects)
        with self.assertRaises(ManifestError):  # 9 个表示，超过上限 8
            Manifest.load(mf9)
        segs = [{"t": i, "d": 1, "url": "init.bin"} for i in range(101)]
        mf101 = write_manifest(self.tmp / "m101.json", "r1", [
            {"id": "a", "scale": 10, "pto": 0, "init": "init.bin",
             "segments": segs}], objects)
        with self.assertRaises(ManifestError):  # 101 段，超过上限 100
            Manifest.load(mf101)

    def test_missing_object_decl_rejected(self):
        path = self.tmp / "m.json"
        path.write_text(json.dumps({
            "revision": "r1",
            "representations": [{"id": "a", "scale": 10, "pto": 0,
                                 "init": "init.bin",
                                 "segments": [{"t": 0, "d": 10, "url": "ghost.bin",
                                               "bytes": 2}]}],
            "objects": {"init.bin": {"revision": "r1", "bytes": 8,
                                     "sha256": sha(b"INITINIT")}}}))
        with self.assertRaises(ManifestError):
            Manifest.load(path)

    def test_preferred_must_exist(self):
        req = write_request(self.tmp / "q.json", preferred="nosuch")
        with self.assertRaises(ManifestError):
            self.run_collect(INPUTS / "r2.json", req, self.fresh_origin())


# ---------------------------------------------------------------- oracle 模糊对照

class TestOracleFuzz(TmpEnv):
    """随机小清单：独立枚举 oracle 对照每次发布/失败决定。"""

    def _random_case(self, rng, tag):
        rev = "r1"
        objects = {}
        reps = []
        for r in range(rng.randint(1, 3)):
            rid = f"rep{r}"
            init = "init-shared.bin" if rng.random() < 0.5 else f"init-{rid}.bin"
            objects.setdefault((rev, init), rng.randbytes(rng.randint(4, 12)))
            segs = []
            tick = 0
            for n in range(rng.randint(1, 5)):
                tick += rng.choice([0, 0, 1, 3])          # 偶尔留空隙
                d = rng.choice([10, 20, 20, 30])
                url = f"{rid}-{n}.bin"
                objects[(rev, url)] = rng.randbytes(rng.randint(5, 30))
                segs.append({"t": 5 + tick, "d": d, "url": url})
                tick += d - rng.choice([0, 0, 5])          # 偶尔重叠
            reps.append({"id": rid, "scale": 10, "pto": 5,
                         "init": init, "segments": segs})
        make_origin(self.tmp / f"org{tag}", objects)
        mf = write_manifest(self.tmp / f"m{tag}.json", rev, reps, objects)
        end = rng.randint(2, 6)
        switches = sorted({str(Fraction(i)) for i in range(0, end + 1)
                           if rng.random() < 0.5} | {"0", str(end)})
        req = write_request(self.tmp / f"q{tag}.json", start="0", end=str(end),
                            budget=rng.randint(20, 120),
                            preferred=rng.choice([r["id"] for r in reps]),
                            switch_points=switches)
        return mf, req

    def test_fuzz_global_and_online(self):
        rng = random.Random(20261115)
        published = failed = 0
        for i in range(60):
            shutil.rmtree(self.store, ignore_errors=True)
            mf, req = self._random_case(rng, i)
            strategy = "global" if i % 2 == 0 else "online"
            res = self.run_collect(mf, req, Origin(self.tmp / f"org{i}"),
                                   strategy=strategy)
            store = Store(self.store)
            if res["status"] == "published":
                published += 1
                rep = verify_publication(store, res["publication_id"])
                self.assertTrue(rep["ok"])
                self.assertLessEqual(res["payload_bytes"],
                                     Request.load(req).budget)
            else:
                failed += 1
                self.assertTrue(verify_failure(store, res["failure_id"])["ok"])
        # 两种结果都真实出现过，说明预算/覆盖确实在起约束作用
        self.assertGreater(published, 0)
        self.assertGreater(failed, 0)


# ---------------------------------------------------------------- CLI

class TestCli(TmpEnv):
    def test_plan_collect_verify_exit_codes(self):
        env = {"PYTHONDONTWRITEBYTECODE": "1", "PATH": "/usr/bin:/bin"}
        py = sys.executable or "python3"
        pipe = ROOT / "pipeline.py"
        store = self.tmp / "store"
        r = subprocess.run([py, str(pipe), "plan", "--manifest", "inputs/r2.json",
                            "--request", "inputs/request.json", "--store", str(store)],
                           capture_output=True, text=True, cwd=ROOT, env=env)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout)["rule"],
                         "preferred_full_coverage_within_budget")
        r = subprocess.run([py, str(pipe), "collect", "--manifest", "inputs/r2.json",
                            "--request", "inputs/request.json", "--origin", "origin",
                            "--store", str(store)],
                           capture_output=True, text=True, cwd=ROOT, env=env)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout)["status"], "published")
        r = subprocess.run([py, str(pipe), "verify", "--store", str(store)],
                           capture_output=True, text=True, cwd=ROOT, env=env)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertTrue(json.loads(r.stdout)["ok"])
        # 预算 21 -> 不可交付退出码 3，失败记录可核验（全新存储避免暖缓存）
        store2 = self.tmp / "store2"
        req21 = write_request(self.tmp / "q21.json", budget=21, preferred="high")
        r = subprocess.run([py, str(pipe), "collect", "--manifest", "inputs/r2.json",
                            "--request", str(req21), "--origin", "origin",
                            "--store", str(store2)],
                           capture_output=True, text=True, cwd=ROOT, env=env)
        self.assertEqual(r.returncode, 3, r.stderr)
        fail_id = json.loads(r.stdout)["failure_id"]
        r = subprocess.run([py, str(pipe), "verify", "--store", str(store2),
                            "--failure", fail_id],
                           capture_output=True, text=True, cwd=ROOT, env=env)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertTrue(json.loads(r.stdout)["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
