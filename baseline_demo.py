"""基线演示（已升级）：同一双修订场景重跑新管线，展示旧缺口已修复。

旧版三个已知缺口（复现见 legacy/original_impl/baseline_demo_original.py）：
  1. 按 URL 复用缓存 —— r2 的目录引用了 r1 的 init.bin；
  2. 下载全部表示 —— 52 字节全量读，无视 40 字节预算；
  3. 最终清单引用旧初始化段。
本脚本用真实字节重跑：r1 与 r2 各发布一次并核验，证明缓存身份按
(url, revision, sha256)、只取所选表示、发布引用正确的初始化对象。
运行：PYTHONDONTWRITEBYTECODE=1 python3 baseline_demo.py
"""
from pathlib import Path
import json
import sys
import tempfile

M = Path(__file__).resolve().parent
sys.path.insert(0, str(M))

from collector import collect
from manifest import Manifest
from origin import Origin
from store import Store
from verify import verify_publication

with tempfile.TemporaryDirectory() as t:
    store_dir = Path(t) / "store"
    origin = Origin(M / "origin")

    r1 = collect(M / "inputs/r1.json", M / "inputs/request.json", origin, store_dir)
    assert r1["status"] == "published", r1
    origin.reset_counters()
    r2 = collect(M / "inputs/r2.json", M / "inputs/request.json", origin, store_dir)
    assert r2["status"] == "published", r2

    store = Store(store_dir)
    v1 = verify_publication(store, r1["publication_id"])
    v2 = verify_publication(store, "current")

    # 缺口 1：同 URL 不同修订不再复用 —— r2 运行真实重读了 r2 修订的字节
    r2_reads = {(x["url"], x["revision"]) for x in r2["origin_reads"]}
    assert ("init.bin", "r2") in r2_reads and all(rv == "r2" for _, rv in r2_reads)
    # 缺口 2：只取所选表示（preferred=high 全覆盖且 38 <= 40 预算）
    assert r2["payload_bytes"] == 38
    assert {u for u, _ in r2_reads} == {"init.bin", "high-0.bin", "high-1.bin"}
    # 缺口 3：当前发布的 init 是 r2 内容，不是旧的 r1 缓存
    m2 = Manifest.load(M / "inputs/r2.json")
    pub2 = store.load_publication("current")
    assert pub2["inits"][0]["object"]["sha256"] == m2.objects["init.bin"].sha256

    print(json.dumps({
        "upgraded_pipeline": True,
        "publications": [r1["publication_id"], r2["publication_id"]],
        "r2_payload_bytes": r2["payload_bytes"],
        "budget": 40,
        "r2_origin_reads": r2["origin_reads"],
        "stale_cache_reproduced": False,
        "only_selected_rep_fetched": True,
        "verify": {"old_publication_ok": v1["ok"], "current_ok": v2["ok"]},
    }, indent=2, ensure_ascii=False))
