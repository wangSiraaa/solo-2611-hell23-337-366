"""端到端验证：独立 oracle + 真实重读 + 粘附/同点/跨段/删除/负向/N/重复/原子性/预算。

oracle 完全独立实现：用字符+来源标签的列表做逐字编辑，不调用生产模块的
任何内部函数；生产侧只通过磁盘 store 的公共接口（且每版本重新打开）核对。
"""
from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

from sequence_store import Store, RevisionRejected, InjectedCrash
from pipeline import RevisionPipeline

ROOT = Path(__file__).resolve().parent
WORK = ROOT / ".work" / "verify"
COMP = str.maketrans("ACGTN", "TGCAN")


# ================================================================ 独立 oracle
class OracleFeature:
    """字符标签列表上的 feature 段演化（与生产代码完全独立的同语义实现）。

    每段持有 (il, ir, items)，items 为标签元组列表；另有 seg_idxs 与 items
    等长，记录每个标签在“当前完整序列”中的位置。位置靠父->新平移表维护，
    从不做内容搜索，因此重复段、同点插入、删除都无歧义。
    """

    def __init__(self, segments, strand):
        self.strand = strand
        self.segments = []      # [il, ir, items]
        self.seg_idxs = []      # [[完整序列位置...]]
        for s, x, il, ir in segments:
            self.segments.append([il, ir, [("O", i) for i in range(s, x)]])
            self.seg_idxs.append(list(range(s, x)))

    @staticmethod
    def _splice(tags, ps, px, il, ir, edits):
        """在段 [ps,px) 上应用 edits；tags 为段内相对顺序（长度 px-ps）。"""
        L = px - ps
        out, cursor = [], 0          # cursor 为段内相对坐标
        for e in edits:
            s = e["s"] - ps
            t = e["t"] - ps
            if t < 0 or s > L:       # 严格在段外；端点交给粘附规则
                continue
            keep_end = min(max(s, 0), L)
            if keep_end > cursor:    # 删除前的存活碱基保留
                out += tags[cursor:keep_end]
            cursor = max(cursor, min(t, L))   # 原碱基被删除 → 跳过其标签
            included = (0 < s < L) or (s == 0 and il) or (s == L and ir)
            if included:
                out += [("E", (e["rev"], e["id"], k)) for k in range(len(e["ins"]))]
        if cursor < L:               # 尾部存活原子
            out += tags[cursor:]
        return out

    def apply(self, edits, parent_full):
        """应用 edits，返回新完整标签序列；同步刷新每段标签与其完整序列位置。"""
        edits = oracle_edits(edits)
        # 1) 新完整序列 + 父索引->新索引 平移表 + 本次插入标签的新位置
        result, map_new, insert_pos, cursor, new_pos = [], {}, {}, 0, 0
        for e in edits:
            for q in range(cursor, e["s"]):
                result.append(parent_full[q]); map_new[q] = new_pos; new_pos += 1
            for k in range(len(e["ins"])):
                tag = ("E", (e["rev"], e["id"], k))
                result.append(tag); insert_pos.setdefault(tag[1], []).append(new_pos)
                new_pos += 1
            cursor = e["t"]
        for q in range(cursor, len(parent_full)):
            result.append(parent_full[q]); map_new[q] = new_pos; new_pos += 1

        # 2) 每段独立演化标签，并逐标签计算其在新完整序列中的位置。
        # 与 _splice 同序走一遍父段标签：被删除的标签跳过其 old_idxs，
        # 存活标签（O 或历史 E）按各自 old_idxs 经 map_new 平移，
        # 本次修订插入按 insert_pos 段内次序定位。
        current_tags = {("E", (e["rev"], e["id"], k))
                        for e in edits for k in range(len(e["ins"]))}
        new_segments, new_idxs = [], []
        for (il, ir, tags), old_idxs in zip(self.segments, self.seg_idxs):
            if not tags:                    # empty 永久保留
                new_segments.append([il, ir, []]); new_idxs.append([])
                continue
            ps, px = old_idxs[0], old_idxs[-1] + 1
            new_tags = self._splice(tags, ps, px, il, ir, edits)
            if not new_tags:               # 整段删除：保留 empty，无位置
                new_segments.append([il, ir, []]); new_idxs.append([])
                continue
            idxs, ti, cursor = [], 0, 0     # ti 遍历父段标签（含将被删除者）
            ins_consumed = {}
            for tg in new_tags:
                if tg in current_tags:     # 本次修订插入
                    key = tg[1]
                    n_used = ins_consumed.get(key, 0)
                    idxs.append(insert_pos[key][n_used])
                    ins_consumed[key] = n_used + 1
                else:                      # 存活标签：跳过被删除的父标签后对齐
                    while ti < len(tags) and tags[ti] != tg:
                        ti += 1
                    assert ti < len(tags), "oracle 存活标签对齐失败"
                    idxs.append(map_new[old_idxs[ti]])
                    ti += 1
            new_segments.append([il, ir, new_tags]); new_idxs.append(idxs)
        self.segments, self.seg_idxs = new_segments, new_idxs
        return result

    def range_of(self, j):
        idxs = self.seg_idxs[j]
        return None if not idxs else [idxs[0], idxs[-1] + 1]

    def bases(self, atom_lookup):
        """tags -> 碱基；负向逐段反向互补并反转段序。

        atom_lookup(tag) 独立解析每个标签；重复段各自求值，负向时每段独立
        反转，避免共享字符串带来的副作用。
        """
        chunks = []
        for seg in self.segments:
            chunks.append("".join(atom_lookup(tg) for tg in seg[2]))
        if self.strand == "-":
            chunks = [c.translate(COMP)[::-1] for c in reversed(chunks)]
        return "".join(chunks)


def oracle_edits(edits):
    """生产风格 edit -> oracle 内部风格（独立数据，不共享对象）。"""
    return [{"s": e["start"], "t": e["end"], "ins": e["insert"],
             "rev": e["rev"], "id": e["id"]} for e in edits]


def oracle_apply_sequence(parent_full, edits, o_atoms):
    """在父完整标签序列上拼接，返回 (新碱基串, 原子表, 新完整标签序列)。

    parent_full 是标签列表（不是碱基串），因此跨版本不会把演化后序列误当原参考。
    o_atoms 提供稳定的 ('O',i)->原参考碱基 映射，edit 标签在此追加。
    """
    edits = oracle_edits(edits)
    out, cursor = [], 0
    for e in edits:
        out += parent_full[cursor:e["s"]]
        out += [("E", (e["rev"], e["id"], k)) for k in range(len(e["ins"]))]
        cursor = e["t"]
    out += parent_full[cursor:]
    for e in edits:
        for k, c in enumerate(e["ins"]):
            o_atoms[("E", (e["rev"], e["id"], k))] = c
    return "".join(o_atoms[t] for t in out), o_atoms, out


# ================================================================ 测试夹具
def feat(id_, segs, strand="+"):
    return {"id": id_, "strand": strand,
            "segments": [{"start": s, "end": x, "left": il, "right": ir}
                         for s, x, il, ir in segs]}


def edit(id_, s, t, ins, order=None, rev=1):
    d = {"id": id_, "start": s, "end": t, "insert": ins}
    if order is not None:
        d["order"] = order
    d["rev"] = rev
    return d


class StoreHarness:
    """每个版本都从磁盘重新打开，模拟独立进程的真实重读。"""

    def __init__(self, name, origin, feature_defs):
        self.path = WORK / name
        if self.path.exists():
            shutil.rmtree(self.path)
        fa = WORK / f"{name}.fa"
        WORK.mkdir(parents=True, exist_ok=True)
        fa.write_text(">O\n" + origin + "\n")
        Store.create(self.path, fa, feature_defs)
        self.origin = origin
        self.defs = feature_defs

    def reopen(self):
        return RevisionPipeline(self.path)

    def publish(self, base, target, edits):
        p = self.reopen()
        p.apply_revision(base, target, [
            {k: v for k, v in e.items() if k != "rev"} for e in edits])
        return self.reopen()


def normalize_edits(edits):
    return sorted(edits, key=lambda e: (e["start"], e.get("order", 0), e["end"]))


# ================================================================ 用例
class _CleanCase(unittest.TestCase):
    def setUp(self):
        if WORK.exists():
            shutil.rmtree(WORK)


class FidelityTests(_CleanCase):


    def test_endpoint_adhesion_four_combinations(self):
        origin = "AACCGGTT"
        for il in ("include", "exclude"):
            for ir in ("include", "exclude"):
                h = StoreHarness(f"adh-{il}-{ir}", origin,
                                 [feat("g", [(2, 6, il, ir)])])
                edits = normalize_edits([edit("eL", 2, 2, "A"),
                                         edit("eR", 6, 6, "T")])
                p = h.publish(0, 1, edits)
                o = OracleFeature([(2, 6, il == "include", ir == "include")], "+")
                o.apply(edits, [("O", i) for i in range(len(origin))])
                atoms = {("O", i): c for i, c in enumerate(origin)}
                for e in edits:
                    for k, c in enumerate(e["insert"]):
                        atoms[("E", (e["rev"], e["id"], k))] = c
                self.assertEqual(p.extract(1, "g"), o.bases(atoms.__getitem__))
                # 直接语义断言
                want = ("A" if il == "include" else "") + "CCGG" + \
                       ("T" if ir == "include" else "")
                self.assertEqual(p.extract(1, "g"), want)

    def test_same_point_inserts_explicit_order(self):
        origin = "AAAA"
        h = StoreHarness("samepoint", origin, [feat("g", [(2, 4, "include", "exclude")])])
        edits = normalize_edits([
            edit("a", 2, 2, "C", order=2),
            edit("b", 2, 2, "G", order=1),
            edit("c", 2, 2, "T", order=0),
        ])
        p = h.publish(0, 1, edits)
        # order 0,1,2 => T G C 落于左端点（left=include）；序列 A A [TGC] A A
        self.assertEqual(p.sequence(1), "AATGCAA")
        self.assertEqual(p.extract(1, "g"), "TGCAA")
        # 同点但 left=exclude：插入不纳入，仅坐标移动
        h2 = StoreHarness("samepoint-ex", origin,
                          [feat("g", [(2, 4, "exclude", "exclude")])])
        p2 = h2.publish(0, 1, edits)
        self.assertEqual(p2.extract(1, "g"), "AA")
        self.assertEqual(p2.locate(1, "g"), [[5, 7]])
        # 缺 order / order 重复必须整批拒绝
        bad = [{"id": "a", "start": 2, "end": 2, "insert": "C"},
               {"id": "b", "start": 2, "end": 2, "insert": "G"}]
        p3 = h.reopen()
        with self.assertRaises(RevisionRejected):
            p3.apply_revision(1, 2, bad)
        self.assertEqual(p3.store.head_version, 1)   # 拒绝后 head 不动

    def test_cross_two_segments_replacement(self):
        # 段 A [1,4)="ACG" 段 B [6,9)="TTA"，替换 [3,7) 锚在 A 内部、B 外部
        origin = "AACCGGTTAC"
        h = StoreHarness("cross", origin,
                         [feat("A", [(1, 4, "exclude", "exclude")]),
                          feat("B", [(6, 9, "exclude", "exclude")])])
        edits = normalize_edits([edit("e", 3, 7, "AT")])
        p = h.publish(0, 1, edits)
        self.assertEqual(p.sequence(1), "AACATTAC")
        # A: 存活旧1,2="AC" + 插入（锚3严格在A内部）=> "ACAT"
        self.assertEqual(p.extract(1, "A"), "ACAT")
        # B: 删除跨入 B（删旧6）但锚3在 B 外 => 插入不纳入；存活旧7,8="TA"
        self.assertEqual(p.extract(1, "B"), "TA")
        self.assertEqual(p.locate(1, "B"), [[5, 7]])

    def test_full_segment_deletion_keeps_empty(self):
        origin = "ACGTACGT"
        h = StoreHarness("empty", origin, [feat("g", [(2, 5, "exclude", "exclude")])])
        edits = normalize_edits([edit("d", 2, 5, "")])
        p = h.publish(0, 1, edits)
        self.assertEqual(p.extract(1, "g"), "")
        self.assertEqual(p.locate(1, "g"), [None])
        tr = p.trace(1, "g")
        self.assertTrue(tr[0]["empty"])
        # 后续修订不能让 empty 段复活
        p2 = h.reopen()
        p2.apply_revision(1, 2, [{"id": "x", "start": 0, "end": 0, "insert": "N"}])
        p3 = h.reopen()
        self.assertEqual(p3.extract(2, "g"), "")
        self.assertEqual(p3.locate(2, "g"), [None])

    def test_negative_join_and_reversal(self):
        origin = "AACCGGTTACGT"
        defs = [feat("j", [(2, 4, "exclude", "exclude"),
                           (8, 10, "exclude", "exclude")], "-")]
        h = StoreHarness("neg", origin, defs)
        edits = normalize_edits([edit("ins", 3, 3, "N"),   # 段0内部
                                 edit("rep", 9, 9, "T")])   # 段1内部
        p = h.publish(0, 1, edits)
        o = OracleFeature([(2, 4, False, False), (8, 10, False, False)], "-")
        o.apply(edits, [("O", i) for i in range(len(origin))])
        atoms = {("O", i): c for i, c in enumerate(origin)}
        for e in edits:
            for k, c in enumerate(e["insert"]):
                atoms[("E", (e["rev"], e["id"], k))] = c
        self.assertEqual(p.extract(1, "j"), o.bases(atoms.__getitem__))
        # 负向 trace：段序反转、每块方向为 -、碱基反向互补
        tr = p.trace(1, "j")
        self.assertEqual([s["segment"] for s in tr], [1, 0])
        self.assertTrue(all(b["direction"] == "-" for s in tr for b in s["blocks"]))

    def test_n_bases_not_guessed(self):
        origin = "ACGT"
        h = StoreHarness("nbase", origin, [feat("g", [(0, 4, "include", "include")])])
        edits = normalize_edits([edit("n", 1, 3, "NN")])
        p = h.publish(0, 1, edits)
        self.assertEqual(p.sequence(1), "ANNT")
        self.assertEqual(p.extract(1, "g"), "ANNT")
        # N 互补仍是 N
        h2 = StoreHarness("nbase-neg", origin,
                          [feat("g", [(0, 4, "include", "include")], "-")])
        p2 = h2.publish(0, 1, edits)
        self.assertEqual(p2.extract(1, "g"), "ANNT".translate(COMP)[::-1])

    def test_duplicate_ranges_kept_duplicated(self):
        origin = "ACGTACGT"
        h = StoreHarness("dup", origin,
                         [feat("d", [(0, 3, "exclude", "exclude"),
                                     (0, 3, "exclude", "exclude")])])
        edits = normalize_edits([edit("e", 1, 1, "N")])
        p = h.publish(0, 1, edits)
        # 两个重复段都独立保留同一份内容（含插入）
        self.assertEqual(p.extract(1, "d"), "ANCGANCG")

    def test_multi_version_oracle_replay(self):
        origin = "AACCGGTTACGT"
        defs = [
            feat("f1", [(2, 4, "exclude", "exclude"),
                        (8, 10, "exclude", "exclude")], "-"),
            feat("f2", [(6, 8, "exclude", "exclude")], "+"),
        ]
        h = StoreHarness("multi", origin, defs)
        oracles = {
            "f1": OracleFeature([(2, 4, False, False), (8, 10, False, False)], "-"),
            "f2": OracleFeature([(6, 8, False, False)], "+"),
        }
        plan = {
            1: [edit("a", 0, 0, "TT", rev=1), edit("b", 6, 8, "N", rev=1)],
            2: [edit("c", 5, 5, "G", rev=2)],
            3: [edit("d", 0, 2, "", rev=3)],
            4: [edit("e", 3, 3, "AC", rev=4), edit("f", 9, 11, "T", rev=4)],
        }
        atoms = {("O", i): c for i, c in enumerate(origin)}   # O 标签稳定绑定原参考
        for ver in sorted(plan):
            edits = normalize_edits(plan[ver])
            h.publish(ver - 1, ver, edits)
        # 用统一入口做逐版本核对（始终基于带标签完整列表，O 标签永不被覆盖）
        seq_full = [("O", i) for i in range(len(origin))]   # 序列级完整标签
        oracle_full = {fid: list(seq_full) for fid in oracles}  # 每个特性独立一份
        for ver in sorted(plan):
            edits = normalize_edits(plan[ver])
            seq, atoms, seq_full = oracle_apply_sequence(seq_full, edits, atoms)
            for fid, o in oracles.items():
                oracle_full[fid] = o.apply(edits, oracle_full[fid])
            p = h.reopen()                    # 每版本真实重读
            self.assertEqual(p.sequence(ver), seq)
            for fid, o in oracles.items():
                self.assertEqual(p.extract(ver, fid), o.bases(atoms.__getitem__))
                # 坐标与 oracle 一致
                loc = p.locate(ver, fid)
                for j, seg in enumerate(o.segments):
                    if not seg[2]:
                        self.assertIsNone(loc[j])
                    else:
                        self.assertEqual(loc[j], o.range_of(j))

    def test_affected_classification(self):
        origin = "AACCGGTT"
        h = StoreHarness("aff", origin,
                         [feat("left", [(0, 2, "exclude", "exclude")]),
                          feat("hit", [(4, 6, "exclude", "exclude")])])
        # 锚 0 的插入严格在 left 段前方 -> left 仅坐标移动、内容不变；
        # 锚 5 严格在 hit 内部 -> hit 内容改变
        edits = [edit("e", 0, 0, "N"), edit("k", 5, 5, "T")]
        p = h.publish(0, 1, edits)
        aff = p.affected(1)
        self.assertEqual(aff["content_changed"], ["hit"])
        self.assertEqual(aff["coordinate_moved"], ["left"])
        # left 内容不变、坐标确实整体右移一位
        self.assertEqual(p.extract(1, "left"), "AA")
        self.assertEqual(p.locate(1, "left"), [[1, 3]])
        self.assertEqual(p.extract(1, "hit"), "GTG")
        self.assertEqual(p.locate(1, "hit"), [[5, 8]])


class AtomicityTests(_CleanCase):

    def test_rejected_revision_writes_nothing(self):
        h = StoreHarness("reject", "ACGT", [feat("g", [(0, 2, "include", "include")])])
        before = sorted(x.name for x in h.path.iterdir())
        p = h.reopen()
        with self.assertRaises(RevisionRejected):
            p.apply_revision(0, 1, [
                {"id": "a", "start": 1, "end": 2, "insert": "N"},
                {"id": "b", "start": 1, "end": 1, "insert": "G"}])  # 重叠
        after = sorted(x.name for x in h.path.iterdir())
        self.assertEqual(before, after)
        # 同锚点：一个非零长删除 + 一个零长插入，即使 order 不同也属重叠，必须拒绝
        with self.assertRaises(RevisionRejected):
            p.apply_revision(0, 1, [
                {"id": "del", "start": 3, "end": 4, "insert": "", "order": 0},
                {"id": "ins", "start": 3, "end": 3, "insert": "N", "order": 1}])
        self.assertEqual(p.store.head_version, 0)
        with self.assertRaises(RevisionRejected):
            p.apply_revision(9, 10, [{"id": "x", "start": 0, "end": 0, "insert": "N"}])
        self.assertEqual(p.store.head_version, 0)

    def test_crash_after_sequence_old_version_readable(self):
        h = StoreHarness("crash1", "ACGTACGT", [feat("g", [(1, 4, "include", "include")])])
        p = h.reopen()
        with self.assertRaises(InjectedCrash):
            p.apply_revision(0, 1, [{"id": "e", "start": 2, "end": 2, "insert": "N"}],
                             crash="after_sequence")
        # 重开：孤儿文件被清理，v1 不可见，v0 完整可读
        p2 = h.reopen()
        self.assertEqual(p2.store.versions, [0])
        self.assertEqual(p2.sequence(0), "ACGTACGT")
        self.assertEqual(p2.extract(0, "g"), "CGT")
        with self.assertRaises(KeyError):
            p2.sequence(1)
        # 同一版本号可重新成功发布
        p2.apply_revision(0, 1, [{"id": "e", "start": 2, "end": 2, "insert": "N"}])
        p3 = h.reopen()
        self.assertEqual(p3.sequence(1), "ACNGTACGT")

    def test_crash_before_commit_then_republish(self):
        h = StoreHarness("crash2", "ACGT", [feat("g", [(0, 2, "include", "include")])])
        p = h.reopen()
        with self.assertRaises(InjectedCrash):
            p.apply_revision(0, 1, [{"id": "e", "start": 1, "end": 1, "insert": "N"}],
                             crash="before_commit")
        p2 = h.reopen()
        self.assertEqual(p2.store.versions, [0])
        p2.apply_revision(0, 1, [{"id": "e", "start": 1, "end": 1, "insert": "N"}])
        self.assertEqual(h.reopen().extract(1, "g"), "ANC")


class BudgetAndBoundaryTests(_CleanCase):

    def test_large_20_single_byte_inserts_under_budget(self):
        path = WORK / "large"
        if path.exists():
            shutil.rmtree(path)
        Store.create(path, ROOT / "inputs" / "large-reference.fa", [])
        p = RevisionPipeline(path)
        p.apply_batch_file(ROOT / "inputs" / "large-revisions.json")
        # 每版本独立重读核对
        p2 = RevisionPipeline(path)
        origin = p2.store.origin_sequence
        expect = origin[:100] + "N" * 20 + origin[100:]
        self.assertEqual(p2.sequence(20), expect)
        audit = p2.audit()
        src_len = audit["origin_sequence_length"]
        self.assertLess(audit["new_persistent_bytes"], src_len,
                        f"新增持久字节 {audit['new_persistent_bytes']} 未低于源长 {src_len}")
        # 不存在任何未登记文件
        listed = set()
        for cat in audit["files"].values():
            listed.update(f["file"] for f in cat)
        on_disk = {x.name for x in path.iterdir() if x.is_file()}
        self.assertEqual(on_disk, listed)

    def test_replace_boundary_cases_file(self):
        spec = json.loads((ROOT / "inputs" / "replace-boundary-cases.json").read_text())
        origin = spec["source"]
        il = spec["left"] == "include"
        ir = spec["right"] == "include"
        s, x = spec["feature_segment"]
        for case in spec["cases"]:
            h = StoreHarness(f"bnd-{case['id']}", origin,
                             [feat("g", [(s, x, spec["left"], spec["right"])])])
            e = case["edit"]
            p = h.publish(0, 1, [edit(e.get("id", case["id"]), e["start"], e["end"],
                                      e["insert"])])
            self.assertEqual(p.extract(1, "g"), case["expected_feature"], case["id"])

    def test_randomized_against_oracle(self):
        """固定种子的随机属性测试：序列/碱基/坐标逐版本对照独立 oracle。"""
        import random
        from collections import defaultdict
        rng = random.Random(7)
        checked = 0
        for trial in range(40):
            n = rng.randint(8, 40)
            origin = "".join(rng.choice("ACGTN") for _ in range(n))
            segdefs, osd = [], []
            for _ in range(rng.randint(1, 4)):
                s = rng.randint(0, n - 2)
                x = rng.randint(s + 1, min(n, s + 8))
                il = rng.choice(("include", "exclude"))
                ir = rng.choice(("include", "exclude"))
                segdefs.append((s, x, il, ir))
                osd.append((s, x, il == "include", ir == "include"))
            strand = rng.choice(("+", "-"))
            h = StoreHarness(f"rand-{trial}", origin, [feat("g", segdefs, strand)])
            o = OracleFeature(osd, strand)
            atoms = {("O", i): c for i, c in enumerate(origin)}
            seq_full = [("O", i) for i in range(n)]
            of = list(seq_full)
            seq, ver = origin, 0
            for _ in range(6):
                cur_len = len(seq)
                raw, cnt, edits = [], defaultdict(int), []
                for _k in range(rng.randint(1, 3)):
                    s = rng.randint(0, cur_len)
                    t = rng.randint(s, min(cur_len, s + 4))
                    ins = "".join(rng.choice("ACGTN")
                                  for _ in range(rng.randint(0, 4)))
                    raw.append((s, t, ins))
                for s, t, ins in raw:
                    order = cnt[s]
                    cnt[s] += 1
                    edits.append(edit(f"e-{s}-{order}", s, t, ins,
                                      order=order, rev=ver + 1))
                edits = normalize_edits(edits)
                payload = [{k: v for k, v in e.items() if k != "rev"} for e in edits]
                probe = h.reopen()
                try:
                    probe.store._validate(ver, ver + 1, payload)
                except RevisionRejected:
                    continue                      # 整批拒绝，oracle 同步跳过
                h.publish(ver, ver + 1, payload)
                checked += 1
                ver += 1
                seq, atoms, seq_full = oracle_apply_sequence(seq_full, edits, atoms)
                of = o.apply(edits, of)
                p = h.reopen()
                self.assertEqual(p.sequence(ver), seq)
                self.assertEqual(p.extract(ver, "g"), o.bases(atoms.__getitem__))
                loc = p.locate(ver, "g")
                for j, seg in enumerate(o.segments):
                    want = None if not seg[2] else o.range_of(j)
                    self.assertEqual(loc[j], want)
        self.assertGreater(checked, 100)

    def test_alphabet_and_bounds_rejected(self):
        h = StoreHarness("illegal", "ACGT", [])
        p = h.reopen()
        with self.assertRaises(RevisionRejected):
            p.apply_revision(0, 1, [{"id": "x", "start": 0, "end": 0, "insert": "Z"}])
        with self.assertRaises(RevisionRejected):
            p.apply_revision(0, 1, [{"id": "x", "start": 3, "end": 5, "insert": "N"}])
        with self.assertRaises(RevisionRejected):
            p.apply_revision(0, 2, [{"id": "x", "start": 0, "end": 0, "insert": "N"}])

    def test_partial_replace_provenance_not_faked(self):
        # 替换 [2,4)，段 [1,5) exclude/exclude：锚2在段内部 -> 插入纳入，
        # 但来源必须标 edit；存活原碱基来源仍是 origin。
        origin = "ACGTACG"
        h = StoreHarness("prov", origin,
                         [feat("g", [(1, 5, "exclude", "exclude")])])
        p = h.publish(0, 1, [edit("e", 2, 4, "AT")])
        tr = p.trace(1, "g")
        refs = [(b["ref"]["source"], b["bases"])
                for sg in tr for b in sg["blocks"]]
        self.assertEqual(refs, [("origin", "C"), ("edit", "AT"), ("origin", "A")])
        self.assertEqual(p.extract(1, "g"), "CATA")


if __name__ == "__main__":
    if WORK.exists():
        shutil.rmtree(WORK)
    unittest.main(verbosity=2)
