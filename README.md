# 大导出流的事务边界安全续传恢复

受限 SQL 导出流（最多 32 MiB、100 事务、单事务 2 MiB、4 表；`INTEGER/TEXT`、主键、立即外键；
`CREATE TABLE/INDEX`、`BEGIN/COMMIT`、`COPY ... FROM STDIN` 文本）的**事务安全续传恢复器**。

核心保证：

* **COPY 数据永不当作 SQL**：数据里的 `COMMIT;`、`--`、`/*`、引号只是字段字节。
* **一个源单元（BEGIN…COMMIT，或独立 DDL）= 一个目标事务**；断点行**写在同一目标事务里**，
  所以“数据已提交”与“源已消费到字节 N”是同一个原子事实。
* 读者在控制点**看不到半个源事务**；未知语句 / 坏 COPY 行 / 截断事务会**回滚该事务**并保留前一完整事务。
* `resume` 只重读**有界边界块**（身份 ≤4 KiB + 边界 ≤60 KiB），总固定开销 **≤64 KiB**，再加至多一个未提交源事务；
  不重读历史前缀，也不宣称其已被重新验真。
* `audit-source` 显式**全量**核验已发布前缀（单独计费）；发现篡改即标记目标失信并拒绝续传。

设计取舍、"已提交但确认丢失"的恢复证据、放弃的方案与理由、真实运行数字、未覆盖边界：见 **[DESIGN.md](DESIGN.md)**。

## 快速开始

无第三方依赖（Python 3.11 标准库 + SQLite）。

```bash
python -m unittest discover -s tests -t .    # 59 个不变量测试
python demo.py                               # 9 个端到端场景，含 338 个字节切点扫描
python demo.py --quick                       # 跳过逐字节扫描
./bin/demo-cli.sh                            # 纯命令行演示
```

命令行：

```bash
python -m streamlex restore SOURCE --target DB --manifest M [--crash-at POINT] [--observe-target DB]
python -m streamlex audit-source SOURCE --target DB --manifest M
python -m streamlex status --target DB
```

退出码：`0` 成功 / `2` 输入被拒 / `3` 审计失信或拒绝续传 / `4` 源非纯追加改变 / `130` 模拟崩溃。

## 目录

| 路径 | 说明 |
| --- | --- |
| `streamlex/lexer.py` | 字节级增量词法 / COPY 解析 |
| `streamlex/store.py` | 源读取、内容身份、不可变分块清单、边界校验 |
| `streamlex/target.py` | SQLite 目标与同事务断点 |
| `streamlex/engine.py` | 恢复引擎（单元规划、原子应用、续传、审计） |
| `streamlex/cli.py` | 命令行入口 |
| `demo.py`, `bin/demo-cli.sh` | 可复现场景 |
| `tests/` | 单元 / 集成 / CLI 测试 |
| `fixtures/` | 原始输入与切点 |

基线材料 `lexer.py` / `restore.py` 保留为历史参考；`demo.py` 已是升级版入口。
