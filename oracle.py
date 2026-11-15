# -*- coding: utf-8 -*-
"""独立 oracle：冻结副本上的完整排序真值。

刻意不使用 SQL ORDER BY、不经过任何 adapter/下推路径：对每个分片快照执行
不带 ORDER BY 的裸 SELECT 取全量行，在 Python 侧按题面排序语义独立排序，
作为联合查询逐物理身份比对的真值来源。
"""
import sqlite3

from query import global_key, business_key, level_key


def freeze_all(bindings, shard_ids):
    """从只读快照抽取全量冻结副本：{(shard_id, local_id): row}。"""
    frozen = {}
    for sid in sorted(shard_ids):
        binding = bindings[sid]
        conn = sqlite3.connect(
            "file:%s?mode=ro" % binding["path"], uri=True)
        try:
            raw = conn.execute(
                "SELECT local_id, score, day, business_id, payload FROM fact"
            ).fetchall()
        finally:
            conn.close()
        for r in raw:
            ident = (sid, r[0])
            assert ident not in frozen, "物理身份冲突 %s" % (ident,)
            frozen[ident] = (sid,) + tuple(r)
    return frozen


def expected_order(frozen):
    """独立 Python 排序：业务排序 + 稳定尾键 (shard_id UTF8, local_id ASC)。"""
    rows = list(frozen.values())
    rows.sort(key=global_key)
    return rows


def expected_page(rows, offset, limit, with_ties):
    """模拟全局 OFFSET / LIMIT [WITH TIES] 的期望结果。"""
    if limit == 0:
        return []
    prefix = rows[offset:offset + limit]
    if not with_ties or not prefix:
        return prefix
    bkey = business_key(prefix[-1])
    blevel = level_key(prefix[-1])
    extra = []
    # 边界业务键可能与同层级其他键交错，必须扫到所有活动行越过边界层级为止
    for r in rows[offset + len(prefix):]:
        if level_key(r) > blevel:
            break
        if business_key(r) == bkey:
            extra.append(r)
    return prefix + extra


def expected_identities(rows):
    return [(r[0], r[1]) for r in rows]
