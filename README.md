# 分片能力约束下的快照 WITH TIES 查询

在 baseline（可运行但有已知缺陷的工程起点）之上实现的联合查询组件：open-query /
next-page / close-query / audit，输出全局 OFFSET/LIMIT WITH TIES 与可重开分页状态，
全程流式 k 路归并，不先全量读分片再 sort。设计细节见 `design.md`。

## 环境

Python 3.11，无第三方依赖（`requirements.txt` 为空），全部使用标准库 `sqlite3`。

## 运行

```bash
# 1. baseline：复现旧 merge.py 跨分片 WITH TIES 丢同键尾部的已知缺陷
PYTHONDONTWRITEBYTECODE=1 python3 baseline_demo.py

# 2. 新组件端到端演示（写 logs/demo.log）
PYTHONDONTWRITEBYTECODE=1 python3 demo.py

# 3. 完整验证：12 组 50 个断言，真实 SQLite 快照 + 独立 oracle 逐物理身份比对
PYTHONDONTWRITEBYTECODE=1 python3 verify.py

# 4. 重负载：8 分片 x 50000 行（40 万行）
FULL_SCALE=1 PYTHONDONTWRITEBYTECODE=1 python3 verify.py
```

## 模块

| 文件 | 职责 |
| --- | --- |
| `capabilities.py` | 固定排序语义与能力（max_page/seek/with_ties）兼容校验，不兼容即拒绝 |
| `snapshot.py` | SQLite backup 真实只读快照、mode=ro 打开、token 注册/过期/close、重开预算（最多 2 次） |
| `shards.py` | baseline `Shard.top` 保留；新增快照-backed `ShardAdapter`（seek/offset/tie 探测，严格序校验） |
| `merge.py` | baseline 旧归并（保留以复现缺陷），新组件不使用它 |
| `query.py` | `JointQuery`：open/next_page/reopen/close/audit，三阶段流式状态机 + 崩溃安全持久化 |
| `oracle.py` | 独立冻结排序真值（裸 SELECT + Python 排序），逐物理身份比对 |
| `testdata.py` | 确定性数据构造与源库变更注入 |
| `verify.py` | 完整验证套件 |
| `design.md` | 能力计划、下推/缓冲策略、成本与失败语义说明 |

## 覆盖的关键场景

并列组超过 max_page、offset 切在 ties 中、空片/NULL score/day、同业务 ID 多物理身份、
网络页长 1..100 变化、仅 offset 分片、源库更新但旧查询快照不变、快照丢失/篡改整体失败、
持久页写入中断后重开不重不漏、读者重开预算、不兼容排序拒绝、低 limit 取回行数远少于全量。
