# -*- coding: utf-8 -*-
"""确定性测试数据构造：真实 SQLite 分片源库 + 能力声明。"""
import random
import sqlite3
from pathlib import Path

from shards import seed_fact

BUSINESS_POOL = ["biz-a", "biz-b", "biz-c", "dup", "tail", "x", "y"]
DAYS = [None, 1, 2, 3, 5, 8]
SCORES = [None, 0, 50, 80, 90, 100]


def build_shards(root, spec, seed=20261115):
    """spec: {shard_id: {"n": int, "max_page": int, "seek": bool,
                         "with_ties": bool, "inject": [...]}}

    inject 为可选的固定行列表 (local_id,score,day,business_id,payload)，
    用于构造跨页/跨分片并列组、NULL、同业务 ID 等边界。
    返回 shard_defs（供 JointQuery.open）。
    """
    rnd = random.Random(seed)
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    defs = {}
    for sid, cfg in spec.items():
        path = root / ("src_%s.sqlite" % sid)
        rows = []
        next_id = 1
        for fixed in cfg.get("inject", []):
            rows.append(fixed)
            next_id = max(next_id, fixed[0] + 1)
        for _ in range(cfg["n"]):
            rows.append((
                next_id,
                rnd.choice(SCORES),
                rnd.choice(DAYS),
                rnd.choice(BUSINESS_POOL),
                "p-%s-%d" % (sid, next_id),
            ))
            next_id += 1
        seed_fact(path, rows)
        defs[sid] = {
            "source": str(path),
            "cap": {
                "sort": "score_desc_nlast__day_asc_nfirst__local_asc",
                "max_page": cfg["max_page"],
                "seek": cfg["seek"],
                "with_ties": cfg["with_ties"],
            },
        }
    return defs


def mutate_source(path):
    """源库继续更新：插入更高分新行（旧查询的绑定快照不应看到它）。"""
    c = sqlite3.connect(path)
    try:
        nxt = c.execute("SELECT COALESCE(MAX(local_id),0)+1 FROM fact").fetchone()[0]
        c.execute(
            "INSERT INTO fact(local_id,score,day,business_id,payload) "
            "VALUES(?,?,?,?,?)",
            (nxt, 1000000, 0, "NEW-AFTER-SNAPSHOT", "mutated"))
        c.commit()
        return nxt
    finally:
        c.close()


def count_source_rows(path):
    c = sqlite3.connect(path)
    try:
        return c.execute("SELECT COUNT(*) FROM fact").fetchone()[0]
    finally:
        c.close()
