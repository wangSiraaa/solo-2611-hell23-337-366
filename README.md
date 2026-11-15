# 多父分段注释补丁与可查询隔离闭包

可持久重开的注释数据目录，支持 `import / apply-patch / query / inspect`。
主体是关系数据维护：缺父、环、身份不一致使实体及其依赖子进入**隔离**，多父实体保留
全部父边，父修复后在下一原子版本恢复；区间查询只命中当前可信实体的实际物理段。

## 运行

Python 3.11，仅用标准库（`requirements.txt` 为空）。

```bash
# 原始基线：复现旧模块的已知缺陷（旧 index.py/patcher.py 未改其行为）
PYTHONDONTWRITEBYTECODE=1 python3 baseline_demo.py

# 完整验证：真实 SQLite + 两连接读写 + 独立全图 oracle，32,487 项断言
PYTHONDONTWRITEBYTECODE=1 python3 verify.py        # 日志写入 verify.log

# 命令面
python3 cli.py import DIR --json inputs/graph.json [--source NAME]
python3 cli.py import DIR --gff3 legacy/features.gff3
python3 cli.py apply-patch DIR --patch inputs/patch.json [--base N] [--id ID]
python3 cli.py query DIR --lo A --hi B [--version N]
python3 cli.py inspect DIR --entity ID [--version N]
python3 cli.py inspect DIR --patch [ID]
python3 cli.py inspect DIR --versions
```

失败以非零退出并输出 `{"error": code, "message": ...}`，不改变任何已发布表。

## 模块

| 文件 | 职责 |
| --- | --- |
| `annotation.py` | GFF3 完整语义解析（含原 `read` 基线函数）与 graph.json 规范化 |
| `closure.py` | 纯函数图算法：SCC 找环、全量闭包（oracle）、增量闭包、受影响锥 |
| `store.py` | SQLite 持久化、SCD 版本行、当前态物理区间索引、单事务补丁、读写审计统计 |
| `cli.py` | import/apply-patch/query/inspect 命令面 |
| `verify.py` | 15 组场景 × 全量 oracle 对照 + 两连接 + 故障注入 |
| `DESIGN.md` | 状态组织、原子性、增量/全量适用条件等设计决策 |
| `verify.log` | 最近一次完整验证实际执行日志 |
| `demo_session.log` | CLI 实际操作日志（含过期 base 被拒、隔离解释） |

## 关键语义

- 坐标 0 基半开；实体可多段、多父；父可前向声明。
- `replace` 完整替换同 ID 实体（不是附加段），也可重建早先被删的实体；补丁绑定
  base 版本，过期 base 拒绝。
- 隔离沿依赖传播到全部实际受影响子项；未受影响实体的 `content_version` /
  `status_version` 不虚增。
- 只保留最近两个已发布版本；读者在补丁中途进入只能看到旧或新的完整版本。
- 写审计显式统计读取实体数、改写段数、索引行数（触及 ≠ 改写）。

旧范围（`legacy/`）仅作输入格式示例；本实现以 `contract.md` 与 `DESIGN.md` 为准。
