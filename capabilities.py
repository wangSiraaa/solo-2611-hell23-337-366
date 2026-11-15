# -*- coding: utf-8 -*-
"""分片能力声明与兼容性校验。

保留 baseline 使用的 ``validate(spec)``（旧 shape：score_desc_id_asc），
新增本次联合查询组件使用的能力模型：

- sort            : 固定排序语义 ID，全部分片必须与查询请求一致
- max_page        : 单次 adapter 调用允许的最大页长（1..100）
- seek            : 是否支持键集 seek（不支持则只能 OFFSET 重扫）
- with_ties       : 是否能在业务键边界切页（tie 感知下推 / tie 探测）
"""

# 唯一受支持的全局排序语义：
#   score DESC NULLS LAST, day ASC NULLS FIRST, local_id ASC
# 稳定尾键 (shard_id UTF8, local_id ASC) 在 merge 侧补全。
SORT_SCORE_DAY_LOCAL = "score_desc_nlast__day_asc_nfirst__local_asc"

# baseline 旧排序（无 day / 无业务键），仅供 baseline_demo 兼容校验
SORT_LEGACY_SCORE_ID = "score_desc_id_asc"

SUPPORTED_SORTS = frozenset((SORT_SCORE_DAY_LOCAL, SORT_LEGACY_SCORE_ID))

PAGE_SIZE_MIN = 1
PAGE_SIZE_MAX = 100


def validate(spec):
    """baseline 兼容入口。"""
    return spec.get("sort") == SORT_LEGACY_SCORE_ID and spec.get("max_page", 0) > 0


class CapabilityError(ValueError):
    """分片能力与查询请求不兼容（拒绝执行，不做本地重排）。"""


def validate_spec(spec):
    """校验单分片能力声明，返回规范化后的 capability dict。"""
    if not isinstance(spec, dict):
        raise CapabilityError("capability spec 必须是对象: %r" % (spec,))
    sort = spec.get("sort")
    if sort not in SUPPORTED_SORTS:
        raise CapabilityError("不支持的排序语义 %r（受支持: %s）" % (sort, sorted(SUPPORTED_SORTS)))
    max_page = spec.get("max_page")
    if not isinstance(max_page, int) or not (PAGE_SIZE_MIN <= max_page <= PAGE_SIZE_MAX):
        raise CapabilityError("max_page 必须是 %d..%d 的整数，实际 %r"
                              % (PAGE_SIZE_MIN, PAGE_SIZE_MAX, max_page))
    seek = bool(spec.get("seek", False))
    with_ties = bool(spec.get("with_ties", spec.get("tie_pushdown", False)))
    return {
        "sort": sort,
        "max_page": max_page,
        "seek": seek,
        "with_ties": with_ties,
    }


def check_query_compatible(shard_caps, requested_sort, page_size, with_ties):
    """查询打开前的能力兼容检查。

    所有分片必须声明与请求完全相同的固定排序语义；任何不一致直接拒绝，
    绝不允许在本地对一页数据悄悄重排。
    """
    if requested_sort not in SUPPORTED_SORTS:
        raise CapabilityError("查询请求了未知排序语义 %r" % (requested_sort,))
    if not shard_caps:
        raise CapabilityError("联合查询至少需要一个分片")
    if len(shard_caps) > 8:
        raise CapabilityError("分片数上限为 8，实际 %d" % (len(shard_caps),))
    if not (PAGE_SIZE_MIN <= page_size <= PAGE_SIZE_MAX):
        raise CapabilityError("网络 page_size 必须在 %d..%d，实际 %r"
                              % (PAGE_SIZE_MIN, PAGE_SIZE_MAX, page_size))
    names = set()
    for shard_id, cap in shard_caps.items():
        cap = validate_spec(cap)
        if cap["sort"] != requested_sort:
            raise CapabilityError(
                "分片 %s 排序语义 %r 与查询 %r 不兼容，拒绝本地重排"
                % (shard_id, cap["sort"], requested_sort))
        if not isinstance(shard_id, str) or not shard_id:
            raise CapabilityError("shard_id 必须是非空 UTF-8 字符串: %r" % (shard_id,))
        names.add(shard_id)
    if len(names) != len(shard_caps):
        raise CapabilityError("shard_id 必须唯一")
    # with_ties 是分片 *能力* 而非需求：无 ties 能力的分片仍可参与全局
    # WITH TIES 查询，只是 merge 侧必须使用有序前瞻（见 design.md）。
    return True
