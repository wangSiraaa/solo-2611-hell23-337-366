"""The revision-maintaining write entry for external writers.

All writes to the target table that are not part of a merge plan must go
through this entry (the "另一个写者" in the concurrency protocol). Each
helper performs one short committed transaction and maintains rev; the
rev-guard triggers in schema.py abort any write that forgets, so direct
ad-hoc writes that bypass this entry fail loudly instead of silently
corrupting the revision tracking the merge executor relies on.
"""


def update_amount(conn, key, amount):
    """Set the amount of an existing key (short committed transaction)."""
    with _txn(conn):
        cur = conn.execute(
            "UPDATE target SET amount = ?, rev = rev + 1 WHERE business_key = ?",
            (amount, key),
        )
    return cur.rowcount


def update_parent(conn, key, parent_id):
    """Reassign the parent of an existing key (unrelated-column write)."""
    with _txn(conn):
        cur = conn.execute(
            "UPDATE target SET parent_id = ?, rev = rev + 1 WHERE business_key = ?",
            (parent_id, key),
        )
    return cur.rowcount


def rename_key(conn, old_key, new_key):
    """Rename a business key."""
    with _txn(conn):
        cur = conn.execute(
            "UPDATE target SET business_key = ?, rev = rev + 1 WHERE business_key = ?",
            (new_key, old_key),
        )
    return cur.rowcount


def insert_row(conn, key, amount, parent_id=None):
    """Insert a new target row with rev=1."""
    with _txn(conn):
        cur = conn.execute(
            "INSERT INTO target(business_key, amount, parent_id, rev) VALUES (?, ?, ?, 1)",
            (key, amount, parent_id),
        )
    return cur.lastrowid


def delete_row(conn, key):
    """Delete a target row by key."""
    with _txn(conn):
        cur = conn.execute("DELETE FROM target WHERE business_key = ?", (key,))
    return cur.rowcount


class _txn:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.execute("BEGIN IMMEDIATE")

    def __exit__(self, exc_type, exc, tb):
        self.conn.execute("ROLLBACK" if exc_type else "COMMIT")
        return False
