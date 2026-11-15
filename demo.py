# -*- coding: utf-8 -*-
"""联合查询组件端到端演示（可复跑日志）。

在仓库根目录执行：
    PYTHONDONTWRITEBYTECODE=1 python3 demo.py

内容：
  1. 建 4 个能力异构的真实 SQLite 分片（seek/offset、有/无 ties、空片）；
  2. open-query 绑定每片独立快照版本向量；
  3. 用变化的网络 page_size 逐 next-page 拉一个 WITH TIES 查询，中途重开一次；
  4. 与独立 oracle 逐物理身份比对；
  5. 打印 audit（adapter 调用、内存页峰值、持久化字节）。
日志同时写入 logs/demo.log。
"""
import json
import sys
import tempfile
from pathlib import Path

M = Path(__file__).resolve().parent
sys.path.insert(0, str(M))

from oracle import expected_identities, expected_order, expected_page, freeze_all
from query import JointQuery
from testdata import build_shards


def main():
    log_lines = []

    def log(obj):
        line = json.dumps(obj, ensure_ascii=False)
        print(line)
        log_lines.append(line)

    work = Path(tempfile.mkdtemp(prefix="jointquery-demo-"))
    spec = {
        "A": {"n": 60, "max_page": 2, "seek": True, "with_ties": True,
              # 7 行同业务键并列组，超过 max_page=2，跨多页
              "inject": [(i, 90, 1, "dup", "tie%d" % i) for i in range(1, 8)]},
        "B": {"n": 40, "max_page": 3, "seek": True, "with_ties": False,
              "inject": [(1, 90, 1, "dup", "B1"), (2, 90, 1, "dup", "B2")]},
        "C": {"n": 30, "max_page": 1, "seek": False, "with_ties": False,
              "inject": [(1, 90, 1, "dup", "C1")]},   # 仅 offset 片
        "D": {"n": 0, "max_page": 4, "seek": True, "with_ties": True,
              "inject": []},                          # 空片
    }
    defs = build_shards(work / "src", spec)
    log({"event": "shards_built", "workdir": str(work),
         "caps": {sid: d["cap"] for sid, d in defs.items()}})

    offset, limit, page_size = 3, 6, 2
    q = JointQuery.open(defs, offset, limit, True, page_size, work / "runs")
    log({"event": "query_opened", "query_id": q.query_id,
         "token_vector": q.token_vector,
         "params": q.params})

    delivered = []
    for i, ps in enumerate([1, 2, 2, 1, 3, 2]):
        page = q.next_page(ps)
        delivered.extend(page["rows"])
        log({"event": "next_page", "seq": i, "requested": ps,
             "returned": len(page["rows"]), "phase": page["phase"],
             "ties_in_page": page["ties_in_page"],
             "complete": page["query_complete"]})
        if page["query_complete"]:
            break

    frozen = freeze_all(q.bindings, list(defs))
    full = expected_order(frozen)
    want = expected_page(full, offset, limit, True)
    got_ids = [(r[0], r[1]) for r in delivered]
    want_ids = expected_identities(want)
    log({"event": "oracle_compare", "match": got_ids == want_ids,
         "got_identities": got_ids, "want_identities": want_ids,
         "no_dup_physical_identity": len(set(got_ids)) == len(got_ids)})

    audit = q.audit()
    log({"event": "audit",
         "adapter_calls": audit["adapter_calls"],
         "total_rows_fetched": audit["total_rows_fetched"],
         "frozen_total_rows": len(frozen),
         "peak_mem_pages": audit["peak_mem_pages"],
         "shard_count": audit["shard_count"],
         "persist_bytes": audit["persist_bytes"],
         "persist_writes": audit["persist_writes"]})

    # 重开续接演示：另开一个查询，取一页后 reopen 再收完
    q2 = JointQuery.open(defs, 0, 10, True, 4, work / "runs")
    head = q2.next_page(3)["rows"]
    q2r = JointQuery.reopen(q2.state_dir)
    rest = q2r.exhaust()
    ids2 = [(r[0], r[1]) for r in head + rest]
    want2 = expected_identities(expected_page(full, 0, 10, True))
    log({"event": "reopen_continuation", "match": ids2 == want2,
         "rows": len(ids2)})
    q.close()
    q2r.close()

    out_dir = M / "logs"
    out_dir.mkdir(exist_ok=True)
    (out_dir / "demo.log").write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    print("\n日志已写入", out_dir / "demo.log")
    if got_ids != want_ids or ids2 != want2:
        sys.exit(1)


if __name__ == "__main__":
    main()
