"""真实 SQLite 业务库的连接与建库材料。

约束（contract.md）：
- 每表单列 INTEGER PRIMARY KEY，外键单列且指向父表 PK；
- 外键动作仅 CASCADE / SET NULL / 延期 NO ACTION（DEFERRABLE INITIALLY DEFERRED）；
- 外键可能成环；最多 12 表 / 30000 行；
- 永远以 ``PRAGMA foreign_keys=ON`` 连接，任何执行路径都不得禁用外键。

本模块只负责连接与种子数据，规划/执行/审计分别在 retention.py、auditor.py、
verifier.py。legacy/ 目录保留旧 TEXT-PK 材料，不被本模块依赖。
"""

import sqlite3

# 12 表业务库：root=account，retained=legal_hold，其余 ordinary。
# 覆盖 CASCADE / SET NULL / 延期 NO ACTION、NOT NULL 置空冲突、自引用环、
# 两表环、多路径（同一子行同时被 CASCADE 与 SET NULL 路径触及）。
SCHEMA_SQL = """
PRAGMA foreign_keys=ON;
CREATE TABLE account(
  id INTEGER PRIMARY KEY,
  label TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE TABLE event(
  id INTEGER PRIMARY KEY,
  account_id INTEGER NOT NULL REFERENCES account(id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL
);
CREATE TABLE comment(
  id INTEGER PRIMARY KEY,
  event_id INTEGER NOT NULL REFERENCES event(id) ON DELETE CASCADE,
  body TEXT
);
CREATE TABLE legal_hold(
  id INTEGER PRIMARY KEY,
  event_id INTEGER REFERENCES event(id)
    ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
  expires_at INTEGER
);
CREATE TABLE pinned_event(
  id INTEGER PRIMARY KEY,
  event_id INTEGER NOT NULL REFERENCES event(id) ON DELETE SET NULL,
  label TEXT
);
CREATE TABLE subscription(
  id INTEGER PRIMARY KEY,
  account_id INTEGER REFERENCES account(id) ON DELETE SET NULL,
  label TEXT NOT NULL
);
CREATE TABLE tag(
  id INTEGER PRIMARY KEY,
  event_id INTEGER REFERENCES event(id)
    ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
  label TEXT NOT NULL
);
CREATE TABLE binder(
  id INTEGER PRIMARY KEY,
  account_id INTEGER NOT NULL REFERENCES account(id) ON DELETE CASCADE,
  event_id INTEGER REFERENCES event(id) ON DELETE SET NULL
);
CREATE TABLE node(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER REFERENCES node(id) ON DELETE CASCADE,
  label TEXT
);
CREATE TABLE alpha(
  id INTEGER PRIMARY KEY,
  beta_id INTEGER REFERENCES beta(id) ON DELETE CASCADE
);
CREATE TABLE beta(
  id INTEGER PRIMARY KEY,
  alpha_id INTEGER REFERENCES alpha(id) ON DELETE CASCADE
);
CREATE TABLE audit_log(
  id INTEGER PRIMARY KEY,
  account_id INTEGER REFERENCES account(id) ON DELETE SET NULL,
  note TEXT
);
"""

# 富种子：保留账户 a1（被有效 hold 保护）、可删账户 a2（hold 已过期）、
# 无关账户 a3；以及环、多路径、NOT NULL 置空、幸存 NO ACTION 等材料。
SEED_SQL = """
INSERT INTO account VALUES
  (1, 'retained-account', 100),
  (2, 'expiring-account', 100),
  (3, 'unrelated-account', 200);
INSERT INTO event VALUES
  (10, 1, 110), (11, 1, 120),
  (20, 2, 110), (21, 2, 130),
  (30, 3, 210);
INSERT INTO comment VALUES
  (100, 10, 'keep'), (101, 20, 'will-cascade'), (102, 21, 'will-cascade');
-- hold 1：永久保护 event 10；hold 2：在逻辑时钟 500 时已过期（== 即过期）；
-- hold 3：event_id 为 NULL，只保护 hold 行自身。
INSERT INTO legal_hold VALUES
  (1, 10, NULL), (2, 20, 500), (3, NULL, NULL);
-- pinned_event.event_id NOT NULL + SET NULL：event 21 被删时计划必须阻断解释。
INSERT INTO pinned_event VALUES (1, 21, 'not-null-conflict');
-- subscription 可置空。
INSERT INTO subscription VALUES (1, 2, 'nullable-set-null');
-- tag 对 event 11 有幸存 NO ACTION（event 11 随 a1 进入闭包时阻断）。
INSERT INTO tag VALUES (1, 11, 'surviving-no-action');
-- binder：account 路径 CASCADE 与 event 路径 SET NULL 多路径，删除优先。
INSERT INTO binder VALUES (1, 2, 20);
-- 自引用 CASCADE 环 n1<->n2：先建行（parent NULL）再互指，n3 挂 n2。
INSERT INTO node VALUES (1, NULL, 'n1'), (2, NULL, 'n2'),
                        (3, NULL, 'n3'), (4, NULL, 'loner');
UPDATE node SET parent_id=2 WHERE id=1;
UPDATE node SET parent_id=1 WHERE id=2;
UPDATE node SET parent_id=2 WHERE id=3;
-- 两表 CASCADE 环：先建行再互指。
INSERT INTO alpha VALUES (1, NULL);
INSERT INTO beta VALUES (1, NULL);
UPDATE alpha SET beta_id=1 WHERE id=1;
UPDATE beta SET alpha_id=1 WHERE id=1;
-- 无关日志：account_id 为 NULL，改它不应使计划失效。
INSERT INTO audit_log VALUES (1, NULL, 'irrelevant');
"""

# baseline_demo.py 使用的最小三表库（与旧 baseline 形状一致，hold 增加 expires_at）。
BASELINE_SCHEMA_SQL = """
PRAGMA foreign_keys=ON;
CREATE TABLE account(id INTEGER PRIMARY KEY);
CREATE TABLE event(
  id INTEGER PRIMARY KEY,
  account_id INTEGER NOT NULL REFERENCES account(id) ON DELETE CASCADE,
  created INTEGER
);
CREATE TABLE legal_hold(
  id INTEGER PRIMARY KEY,
  event_id INTEGER REFERENCES event(id)
    ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,
  expires_at INTEGER
);
"""

BASELINE_SEED_SQL = """
PRAGMA foreign_keys=ON;
INSERT INTO account VALUES (1);
INSERT INTO event VALUES (11, 1, 5);
"""


def connect(path):
    """打开一个外键始终开启的连接。"""
    c = sqlite3.connect(str(path))
    c.execute("PRAGMA foreign_keys=ON")
    return c


def _split_statements(sql):
    """按分号切句（本仓库 DDL/DML 无字符串内分号、无触发器）。"""
    return [s.strip() for s in sql.split(";") if s.strip()]


def _build(path, schema_sql, seed_sql=None):
    c = connect(path)
    try:
        # DDL 自动提交；含前向 FK 的环表种子必须在同一事务内插入，
        # 否则延期约束会在中间语句的隐式提交点报错。
        c.executescript(schema_sql)
        if seed_sql:
            c.execute("BEGIN")
            for stmt in _split_statements(seed_sql):
                c.execute(stmt)
            c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    return path


def seed(path):
    """创建 12 表业务库并写入富种子。"""
    return _build(path, SCHEMA_SQL, SEED_SQL)


def seed_baseline(path):
    """创建 baseline_demo 使用的最小三表库。"""
    return _build(path, BASELINE_SCHEMA_SQL, BASELINE_SEED_SQL)
