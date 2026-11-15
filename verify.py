# -*- coding: utf-8 -*-
"""联合查询组件完整验证套件（真实 SQLite 快照 + 独立 oracle 逐身份比对）。

运行：
    PYTHONDONTWRITEBYTECODE=1 python3 verify.py
    FULL_SCALE=1 PYTHONDONTWRITEBYTECODE=1 python3 verify.py   # 8x50000 重负载

每个用例都：
  1. 用真实 SQLite backup 快照打开查询；
  2. 用变化的网络 page_size 逐 next-page 拉取并跨进程式重开；
  3. 与独立冻结排序 oracle 逐物理身份 (shard_id, local_id) 比较，
     检查全查询顺序、续接不重不漏；
  4. 核对 adapter 调用审计、内存页数上限、低 limit 取回行数约束。
"""
import json
import os
import shutil
import sys
import tempfile
import traceback
from pathlib import Path

M = Path(__file__).resolve().parent
sys.path.insert(0, str(M))

from capabilities import CapabilityError
from oracle import freeze_all, expected_order, expected_page, expected_identities
from query import JointQuery, QueryUnavailable
from shards import seed_fact
from snapshot import SnapshotExpired, ReopenBudgetExceeded
from testdata import build_shards, mutate_source, count_source_rows

FULL_SCALE = bool(os.environ.get("FULL_SCALE"))
RESULTS = []


def record(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(("  PASS " if ok else "  FAIL ") + name + (" -- " + detail if detail else ""))


def identities(rows):
    return [(r[0], r[1]) for r in rows]


def run_paged(query, page_sizes):
    """按给定页长序列逐页拉取（循环复用），直到 query_complete。

    返回 (全部行, 每页长度, done 序列)。固定短序列若页长总和小于 limit
    会提前停止，因此这里用索引取模循环，确保把整个查询收完。
    """
    all_rows, sizes, dones = [], [], []
    i = 0
    while True:
        ps = page_sizes[i % len(page_sizes)]
        i += 1
        page = query.next_page(ps)
        all_rows.extend(page["rows"])
        sizes.append(len(page["rows"]))
        dones.append(page["query_complete"])
        if page["query_complete"]:
            break
    return all_rows, sizes, dones


def assert_against_oracle(name, defs, bindings, offset, limit, with_ties,
                          got_rows, audit=None):
    frozen = freeze_all(bindings, list(defs))
    full = expected_order(frozen)
    want = expected_page(full, offset, limit, with_ties)
    ok = identities(got_rows) == expected_identities(want)
    if not ok:
        record(name, False, "顺序/身份与 oracle 不一致\n"
                "    got=%s\n    want=%s" % (identities(got_rows),
                                              expected_identities(want)))
        return full, want, False
    # 不重不漏：身份集合一致
    if len(set(identities(got_rows))) != len(got_rows):
        record(name, False, "出现重复物理身份")
        return full, want, False
    if audit is not None:
        # 内存页硬上限：每片至多一个 adapter 页
        if audit["peak_mem_pages"] > len(defs):
            record(name + "::mem-pages", False,
                   "peak_mem_pages=%d > shards=%d"
                   % (audit["peak_mem_pages"], len(defs)))
            return full, want, False
        # 取回行数绝不超过全量
        if audit["total_rows_fetched"] > len(full):
            record(name, False, "取回行数 %d 超过全量 %d"
                   % (audit["total_rows_fetched"], len(full)))
            return full, want, False
    record(name, True, "rows=%d" % len(got_rows))
    return full, want, True


# ----------------------------------------------------------------------
# 固定边界数据：并列组超过 max_page、offset 切在 ties 中
# ----------------------------------------------------------------------
def big_tie_inject():
    # score=90/day=1/biz=dup 的并列组共 7 行，配合 max_page=2 -> 跨 4 页
    return [(i, 90, 1, "dup", "tie%d" % i) for i in range(1, 8)]


def main():
    work = Path(tempfile.mkdtemp(prefix="jointquery-"))
    print("workdir =", work)
    try:
        _run_all(work)
    finally:
        if os.environ.get("KEEP_WORK"):
            print("保留工作目录:", work)
        else:
            shutil.rmtree(work, ignore_errors=True)

    failed = [r for r in RESULTS if not r[1]]
    print("\n==== 汇总: %d 用例, %d 失败 ====" % (len(RESULTS), len(failed)))
    if failed:
        for n, _, d in failed:
            print("FAIL:", n, d)
        sys.exit(1)


def _run_all(work):
    # ------------------------------------------------------------------
    # 用例 1：混合能力分片 + 并列组超过 max_page + offset 切在 ties 中
    # ------------------------------------------------------------------
    print("[1] 混合能力 / 跨页并列组 / offset 切在 ties 中")
    spec = {
        "A": {"n": 60, "max_page": 2, "seek": True, "with_ties": True,
              "inject": big_tie_inject()},
        "B": {"n": 40, "max_page": 3, "seek": True, "with_ties": False,
              "inject": [(1, 90, 1, "dup", "B-tie1"),
                         (2, 90, 1, "dup", "B-tie2"),
                         (3, 90, 1, "other", "B-other")]},
        "C": {"n": 30, "max_page": 1, "seek": False, "with_ties": False,
              "inject": [(1, 90, 1, "dup", "C-tie1")]},  # 仅 offset 片
        "D": {"n": 0,  "max_page": 4, "seek": True, "with_ties": True,
              "inject": []},                              # 空片
    }
    defs = build_shards(work / "t1", spec)
    for offset, limit, wt, psizes in [
        (0, 5, True, [1, 2, 3, 2, 1, 5]),       # ties 跨多次 next-page
        (3, 2, True, [2, 2, 1, 3]),             # offset 切在并列组内部
        (0, 1, True, [1, 1, 1, 1]),             # limit=1 但要补齐整个并列组
        (0, 10, False, [7, 3, 1]),
        (50, 5, True, [4, 4]),                  # 接近尾部
        (0, 0, True, [1]),                      # limit=0
        (9999, 5, False, [10]),                 # offset 超出全量
    ]:
        q = JointQuery.open(defs, offset, limit, wt, 5, work / "runs")
        got, sizes, dones = run_paged(q, psizes)
        assert_against_oracle(
            "t1 offset=%d limit=%d ties=%s" % (offset, limit, wt),
            defs, q.bindings, offset, limit, wt, got, q.audit())
        assert dones[-1], "查询应正常结束"
        q.close()

    # ------------------------------------------------------------------
    # 用例 2：NULL 排序语义 + 同业务 ID 保留物理身份
    # ------------------------------------------------------------------
    print("[2] NULL score/day 排序 + 同业务 ID 多物理身份")
    spec2 = {
        "N1": {"n": 50, "max_page": 4, "seek": True, "with_ties": True,
               "inject": [
                   (1, None, 1, "n-day", "null-score-1"),
                   (2, None, None, "n-both", "null-both"),
                   (3, 70, None, "n-nullday", "null-day-1"),
                   (4, 70, None, "dup", "dup1"),
                   (5, 70, None, "dup", "dup2"),
               ]},
        "N2": {"n": 50, "max_page": 2, "seek": True, "with_ties": False,
               "inject": [(1, 70, None, "dup", "dup3")]},
    }
    defs2 = build_shards(work / "t2", spec2, seed=42)
    # 页长序列循环使用，且必须持续翻页到 query_complete（run_paged 已在
    # complete 时 break），不能用固定短序列，否则页长总和 < limit 会提前停。
    page_cycle = [1, 3, 2, 5, 2, 4, 1, 3, 5, 2, 4, 3, 2, 1]
    for offset, limit, wt in [(0, 8, True), (2, 3, True), (0, 20, False)]:
        q = JointQuery.open(defs2, offset, limit, wt, 3, work / "runs")
        got, _, _ = run_paged(q, page_cycle)
        full, want, ok = assert_against_oracle(
            "t2 nulls offset=%d limit=%d" % (offset, limit),
            defs2, q.bindings, offset, limit, wt, got, q.audit())
        if ok:
            # 显式核对 NULL 排序：所有非 NULL score 行先于 NULL score 行；
            # 同 score 下 NULL day 行先于非 NULL day 行
            null_score_pos = [i for i, r in enumerate(got) if r[2] is None]
            non_null_pos = [i for i, r in enumerate(got) if r[2] is not None]
            if null_score_pos and non_null_pos and min(null_score_pos) < max(non_null_pos):
                record("t2 null-order", False, "NULL score 未排在最后")
            else:
                record("t2 null-order", True)
            # 同业务 ID 的三个物理身份都应保留
            dup_ids = [r[:2] for r in got if r[4] == "dup"]
            if len(dup_ids) != len(set(dup_ids)):
                record("t2 dup-identity", False, "同业务 ID 物理身份被合并")
            else:
                record("t2 dup-identity", True, "dup rows=%s" % dup_ids)
        q.close()

    # ------------------------------------------------------------------
    # 用例 3：页长变化（1..100 全合法）+ 中途重开（seek 片与 offset 片）
    # ------------------------------------------------------------------
    print("[3] 页长变化 + continuation 重开不重不漏")
    spec3 = {
        "S1": {"n": 80, "max_page": 5, "seek": True, "with_ties": True},
        "S2": {"n": 70, "max_page": 7, "seek": False, "with_ties": False},
        "S3": {"n": 60, "max_page": 3, "seek": True, "with_ties": False},
    }
    defs3 = build_shards(work / "t3", spec3, seed=7)
    q = JointQuery.open(defs3, 5, 30, True, 6, work / "runs")
    part1 = q.next_page(4)["rows"]
    part2 = q.next_page(2)["rows"]
    state_dir = q.state_dir
    bindings = dict(q.bindings)
    q2 = JointQuery.reopen(state_dir)
    part3 = q2.next_page(8)["rows"]
    part4 = q2.next_page(1)["rows"]
    q3 = JointQuery.reopen(state_dir)                 # 第二次重开
    rest = q3.exhaust()
    got = part1 + part2 + part3 + part4 + rest
    full, want, ok = assert_against_oracle(
        "t3 reopen-continuation", defs3, bindings, 5, 30, True, got, q3.audit())
    # 重开预算：S2 是 offset 片，每次重开都会重新连接；总连接 <=3
    snap_audit = q3.registry.audit()
    over = {sid: m for sid, m in snap_audit.items() if m["reopens"] > 2}
    record("t3 reopen-budget<=2", not over, json.dumps(
        {sid: m["reopens"] for sid, m in snap_audit.items()}, ensure_ascii=False))
    # 页长 1 与 100 均合法
    q4 = JointQuery.open(defs3, 0, 12, False, 100, work / "runs")
    pmin = q4.next_page(1)
    record("t3 page_size=1", len(pmin["rows"]) == 1)
    q4.close()
    q3.close()

    # ------------------------------------------------------------------
    # 用例 4：源更新但旧查询不变（版本向量绑定）
    # ------------------------------------------------------------------
    print("[4] 源库继续更新，绑定快照查询结果不变")
    src_path = defs3["S1"]["source"]
    before = count_source_rows(src_path)
    q = JointQuery.open(defs3, 0, 40, True, 10, work / "runs")
    frozen_before = freeze_all(q.bindings, list(defs3))
    new_id = mutate_source(src_path)
    after = count_source_rows(src_path)
    assert after == before + 1
    got = q.exhaust()
    full = expected_order(frozen_before)
    want = expected_page(full, 0, 40, True)
    untouched = (identities(got) == expected_identities(want)
                 and not any(r[1] == new_id and r[0] == "S1" for r in got))
    record("t4 snapshot-immutable", untouched,
           "源新增 local_id=%d 未泄漏进旧查询" % new_id)
    # 新查询必须能看到更新（重新建快照）
    q_new = JointQuery.open(defs3, 0, 1, False, 10, work / "runs")
    top = q_new.next_page()["rows"][0]
    record("t4 fresh-snapshot-sees-update",
           top[0] == "S1" and top[1] == new_id and top[2] == 1000000,
           "新快照首行=%s" % (top[:2],))
    q.close()
    q_new.close()

    # ------------------------------------------------------------------
    # 用例 5：快照丢失 -> 整次查询不可继续，不混新旧页
    # ------------------------------------------------------------------
    print("[5] 快照过期/缺失的失败语义")
    q = JointQuery.open(defs3, 2, 20, True, 5, work / "runs")
    page1 = q.next_page(3)
    victim = q.state_dir / "snap_S2.sqlite"
    victim.unlink()
    failed_next = False
    try:
        q.next_page(5)
    except QueryUnavailable as e:
        failed_next = True
        msg = str(e)
    record("t5 missing-snapshot-blocks-next", failed_next,
           "已交付 %d 行保留，query 标记未完成" % len(page1["rows"]))
    # 重开同样失败，且失败状态被持久化
    reopen_failed = False
    try:
        JointQuery.reopen(q.state_dir)
    except QueryUnavailable:
        reopen_failed = True
    record("t5 reopen-after-loss-fails", reopen_failed)
    persisted = json.loads((q.state_dir / "query_state.json").read_text("utf-8"))
    record("t5 failure-persisted", persisted["failure"] is not None,
           json.dumps(persisted["failure"], ensure_ascii=False))
    # 快照被篡改（字节变化）同样拒绝
    q2 = JointQuery.open(defs3, 0, 10, False, 5, work / "runs")
    with open(q2.state_dir / "snap_S1.sqlite", "ab") as f:
        f.write(b"\x00corrupt")
    corrupt_failed = False
    try:
        q2.next_page(5)
    except QueryUnavailable:
        corrupt_failed = True
    record("t5 tampered-snapshot-rejected", corrupt_failed)
    q.close(delete_snapshots=False)
    q2.close(delete_snapshots=False)

    # ------------------------------------------------------------------
    # 用例 6：持久页写入中断后重开（临时文件 + 已提交状态）
    # ------------------------------------------------------------------
    print("[6] 持久状态写入中断后的恢复/失败语义")
    # open 写 1 次；每次 next_page 成功末再写 1 次，故第 2 个 next_page
    # 末尾的状态写是第 3 次 —— 在 rename 前注入中断。
    q = JointQuery.open(defs3, 3, 25, True, 6, work / "runs",
                        crash_inject={"state_write": 3})
    delivered = q.next_page(4)["rows"]   # 第 1、2 次状态写成功
    crash_seen = False
    try:
        second = q.next_page(3)          # 数据已产出，第 3 次状态写在 rename 前崩溃
        delivered = delivered + second["rows"]
    except IOError:
        crash_seen = True               # 该页未提交，重开后会从上个检查点重放
    record("t6 crash-injected", crash_seen)
    tmp_exists = (q.state_dir / "query_state.tmp").exists()
    committed = (q.state_dir / "query_state.json").exists()
    record("t6 tmp-left-committed-kept", tmp_exists and committed)
    # 重开：基于上一个已提交状态恢复，tmp 被清理，续接不重不漏。
    # 只拼接崩溃前已“成功提交”的页（delivered 的第一页），崩溃页由重开重放。
    qr = JointQuery.reopen(q.state_dir)
    got = delivered[:4] + qr.exhaust()
    assert_against_oracle("t6 recover-after-crash", defs3, qr.bindings,
                          3, 25, True, got, qr.audit())
    record("t6 tmp-cleaned-on-reopen",
           not (q.state_dir / "query_state.tmp").exists())
    qr.close()

    # ------------------------------------------------------------------
    # 用例 7：不兼容排序必须拒绝（不本地重排）
    # ------------------------------------------------------------------
    print("[7] 能力不兼容拒绝")
    bad_defs = json.loads(json.dumps(defs3))
    bad_defs["S2"]["cap"]["sort"] = "some_other_sort"
    rejected = False
    try:
        JointQuery.open(bad_defs, 0, 5, False, 5, work / "runs")
    except CapabilityError:
        rejected = True
    record("t7 incompatible-sort-rejected", rejected)
    # page_size 越界
    rejected2 = False
    try:
        JointQuery.open(defs3, 0, 5, False, 101, work / "runs")
    except CapabilityError:
        rejected2 = True
    record("t7 page_size-101-rejected", rejected2)
    rejected3 = False
    try:
        JointQuery.open(defs3, -1, 5, False, 5, work / "runs")
    except CapabilityError:
        rejected3 = True
    record("t7 negative-offset-rejected", rejected3)

    # ------------------------------------------------------------------
    # 用例 8：低 limit 非并列基准 —— 实际取回行数显著少于全量
    # ------------------------------------------------------------------
    print("[8] 低 limit 非 tie 基准：取回行数（含 offset 跳过+lookahead）< 全量")
    big_spec = {
        "L1": {"n": 4000, "max_page": 100, "seek": True, "with_ties": True},
        "L2": {"n": 4000, "max_page": 50, "seek": True, "with_ties": False},
        "L3": {"n": 4000, "max_page": 25, "seek": False, "with_ties": False},
        "L4": {"n": 4000, "max_page": 80, "seek": True, "with_ties": True},
    }
    bdefs = build_shards(work / "t8", big_spec, seed=99)
    total_rows = sum(c["n"] for c in big_spec.values())
    q = JointQuery.open(bdefs, 10, 5, False, 5, work / "runs")
    got = q.exhaust()
    audit = q.audit()
    fetched = audit["total_rows_fetched"]
    record("t8 low-limit-fetch<total",
           len(got) == 5 and fetched < total_rows,
           "fetched=%d vs total=%d (%.1f%%)"
           % (fetched, total_rows, 100.0 * fetched / total_rows))
    assert_against_oracle("t8 correctness", bdefs, q.bindings,
                          10, 5, False, got, audit)
    q.close()

    # 同基准 WITH TIES 但边界无并列：lookahead 也必须有界
    q = JointQuery.open(bdefs, 0, 3, True, 3, work / "runs")
    got = q.exhaust()
    audit = q.audit()
    record("t8 ties-bounded-fetch",
           audit["total_rows_fetched"] < total_rows,
           "fetched=%d vs total=%d" % (audit["total_rows_fetched"], total_rows))
    assert_against_oracle("t8 ties correctness", bdefs, q.bindings,
                          0, 3, True, got, audit)
    q.close()

    # ------------------------------------------------------------------
    # 用例 9：重开预算 —— 读者最多重开两次
    # ------------------------------------------------------------------
    print("[9] 读者重开预算（最多两次）")
    q = JointQuery.open(defs3, 0, 30, False, 10, work / "runs")
    q.next_page(5)
    # 直接在绑定记录上验证重开预算这一强制点：初次打开 + 至多 2 次重开。
    # 同时用 drop_connection + 排空缓冲驱动真实 adapter 重连路径。
    rec = q.registry._bindings["S1"]
    rec.reopens = rec.MAX_REOPENS          # 模拟已用满两次重开
    q.adapters["S1"].drop_connection()
    q.streams["S1"]["buffer"] = []         # 强制下次取页重新建连
    budget_failed = False
    try:
        q.next_page(5)
    except QueryUnavailable as e:
        budget_failed = isinstance(e.__cause__, ReopenBudgetExceeded)
    record("t9 third-reopen-rejected", budget_failed)
    # 正常预算内（初次+2次重连）可成功读取
    rec.reopens = 0
    q.failure = None
    ok_reopen = False
    try:
        q.adapters["S1"].drop_connection()
        q.streams["S1"]["buffer"] = []
        q.next_page(5)
        ok_reopen = q.registry._bindings["S1"].reopens >= 1
    except QueryUnavailable:
        ok_reopen = False
    record("t9 reopen-within-budget-works", ok_reopen)
    q.close(delete_snapshots=False)

    # ------------------------------------------------------------------
    # 用例 10：offset-only 分片单独参与 + 每片最多一个内存页
    # ------------------------------------------------------------------
    print("[10] 全 offset-only 拓扑与内存页审计")
    off_spec = {
        "O1": {"n": 120, "max_page": 6, "seek": False, "with_ties": False},
        "O2": {"n": 110, "max_page": 9, "seek": False, "with_ties": True},
    }
    odefs = build_shards(work / "t10", off_spec, seed=123)
    q = JointQuery.open(odefs, 20, 40, True, 8, work / "runs")
    got, _, _ = run_paged(q, [8, 8, 8, 8, 8, 8, 8])
    audit = q.audit()
    assert_against_oracle("t10 offset-only", odefs, q.bindings,
                          20, 40, True, got, audit)
    record("t10 peak-mem-pages<=shards",
           audit["peak_mem_pages"] <= len(odefs),
           "peak=%d shards=%d" % (audit["peak_mem_pages"], len(odefs)))
    record("t10 only-offset-modes",
           all(c["mode"] == "offset" for c in audit["calls"]))
    q.close()

    # ------------------------------------------------------------------
    # 用例 11：tie 组跨多页 + 跨分片，且某片并列组 > max_page
    # ------------------------------------------------------------------
    print("[11] 超大并列组（>max_page）跨分片完整补齐")
    tie_spec = {
        "T1": {"n": 20, "max_page": 2, "seek": True, "with_ties": True,
               "inject": big_tie_inject()},                       # 7 行同键
        "T2": {"n": 20, "max_page": 2, "seek": True, "with_ties": False,
               "inject": [(i + 10, 90, 1, "dup", "T2-tie%d" % i)
                          for i in range(5)]},                     # 5 行同键
        "T3": {"n": 20, "max_page": 2, "seek": False, "with_ties": True,
               "inject": [(20, 90, 1, "dup", "T3-tie")]},          # offset+tie
    }
    tdefs = build_shards(work / "t11", tie_spec, seed=55)
    # 先定位超大并列组 (90,1,'dup') 在全局序中的起点，让边界行落在该组内
    probe_q = JointQuery.open(tdefs, 0, 1, False, 2, work / "runs")
    frozen = freeze_all(probe_q.bindings, list(tdefs))
    full = expected_order(frozen)
    tie_level = (90, 1, "dup")
    tie_offset = next(i for i, r in enumerate(full)
                      if (r[2], r[3], r[4]) == tie_level)
    tie_total = sum(1 for r in full if (r[2], r[3], r[4]) == tie_level)
    probe_q.close()
    q = JointQuery.open(tdefs, tie_offset, 1, True, 2, work / "runs")
    got = q.exhaust()
    audit = q.audit()
    full, want, ok = assert_against_oracle(
        "t11 mega-tie", tdefs, q.bindings, tie_offset, 1, True, got, audit)
    if ok:
        tie_rows = [r for r in got if (r[2], r[3], r[4]) == tie_level]
        record("t11 all-%d-ties-delivered" % tie_total,
               len(tie_rows) == tie_total,
               "tie rows=%d" % len(tie_rows))
        # tie 能力分片必须使用探测下推；offset-only T3 走 offset 探测
        probe_shards = {c["shard_id"] for c in audit["calls"]
                        if c["mode"] == "tie_probe"}
        record("t11 probe-pushdown-used",
               {"T1", "T3"} <= probe_shards and "T2" not in probe_shards,
               "probe shards=%s" % sorted(probe_shards))
    q.close()

    # ------------------------------------------------------------------
    # 用例 12：重负载（FULL_SCALE 时 8x50000；否则 8x2000 冒烟）
    # ------------------------------------------------------------------
    n_each = 50000 if FULL_SCALE else 2000
    print("[12] 重负载 8 x %d（FULL_SCALE=%s）" % (n_each, FULL_SCALE))
    heavy = {}
    for i, sid in enumerate(["H1", "H2", "H3", "H4", "H5", "H6", "H7", "H8"]):
        heavy[sid] = {
            "n": n_each,
            "max_page": [13, 27, 50, 100, 7, 33, 64, 15][i],
            "seek": i not in (2, 6),       # H3、H7 仅 offset
            "with_ties": i % 2 == 0,
        }
    hdefs = build_shards(work / "t12", heavy, seed=2026)
    total = n_each * 8
    for offset, limit, wt in [(0, 50, True), (1000, 200, False),
                              (total - 30, 100, True), (0, 0, False)]:
        q = JointQuery.open(hdefs, offset, limit, wt, 100, work / "runs")
        got, sizes, _ = run_paged(q, [37, 64, 100, 1, 50, 100])
        assert_against_oracle(
            "t12 offset=%d limit=%d ties=%s" % (offset, limit, wt),
            hdefs, q.bindings, offset, limit, wt, got, q.audit())
        if limit and not wt:
            # 非 tie 低 limit 取回远小于全量
            f = audit_total(q)
            record("t12 bounded-fetch offset=%d" % offset,
                   f < total, "fetched=%d total=%d" % (f, total))
        q.close()

    # 重负载上再做一次跨重开续接
    q = JointQuery.open(hdefs, 500, 300, True, 77, work / "runs")
    a = q.next_page(40)["rows"]
    qr = JointQuery.reopen(q.state_dir)
    b = qr.exhaust()
    got = a + b
    assert_against_oracle("t12 reopen", hdefs, qr.bindings,
                          500, 300, True, got, qr.audit())
    qr.close()


def audit_total(q):
    return q.audit()["total_rows_fetched"]


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(2)
