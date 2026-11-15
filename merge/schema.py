"""Database schema for the constrained MERGE executor.

Extends fixtures/target-schema.sql with:
  * typeof CHECK constraints so the int64/text invariants are enforced by
    the database as a backstop (SQLite column affinity alone would not
    reject a text value written into an INTEGER column).
  * rev-guard triggers. rev is maintained by the write paths (the merge
    executor and the external writer entry), and the triggers make any
    write that forgets to maintain rev fail instead of silently
    corrupting the revision tracking. Direct writes that bypass the
    provided writer entry are outside the legal input envelope; these
    triggers turn the common bypass into an error.
  * merge_receipt, the durable receipt store that makes committed plans
    idempotent under client retries (same plan_id -> same receipt).
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS parent(
  id INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS target(
  id INTEGER PRIMARY KEY,
  business_key TEXT UNIQUE,
  amount INTEGER NOT NULL CHECK (typeof(amount) = 'integer'),
  parent_id INTEGER REFERENCES parent(id),
  rev INTEGER NOT NULL CHECK (typeof(rev) = 'integer')
);

CREATE TRIGGER IF NOT EXISTS trg_target_rev_ins
AFTER INSERT ON target
WHEN NEW.rev <> 1
BEGIN
  SELECT RAISE(ABORT, 'insert into target must set rev=1');
END;

CREATE TRIGGER IF NOT EXISTS trg_target_rev_upd
AFTER UPDATE ON target
WHEN NEW.rev <> OLD.rev + 1
BEGIN
  SELECT RAISE(ABORT, 'update of target must bump rev by exactly 1');
END;

CREATE TABLE IF NOT EXISTS merge_receipt(
  plan_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  receipt_json TEXT NOT NULL,
  created_utc TEXT NOT NULL
);
"""

SEED_SQL = """
INSERT OR IGNORE INTO parent(id) VALUES (1);
INSERT OR IGNORE INTO target(id, business_key, amount, parent_id, rev)
VALUES (1, 'A', 10, 1, 1), (2, 'B', 20, NULL, 1);
"""


def init_schema(conn, seed=False):
    conn.executescript(SCHEMA_SQL)
    if seed:
        conn.executescript(SEED_SQL)
