"""Shared fixture inputs for tests (generated variants of the same family)."""


def _read(path):
    with open(path, "rb") as fh:
        return fh.read()


EXPORT = _read("fixtures/export.sql")

# A richer member of the same legal input family: comments, quotes, UTF-8,
# COPY escapes, a pseudo-boundary inside COPY data, and a transaction that
# creates a parent table, copies parent rows, creates a child table and copies
# child rows.
RICH = (
    b"-- export header ; COMMIT; (comment, not a boundary)\n"
    b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"
    b"CREATE TABLE child(id INTEGER PRIMARY KEY,"
    b" parent_id INTEGER REFERENCES parent(id), note TEXT);\n"
    b"BEGIN;\n"
    b"/* a block comment containing ; and COMMIT; */\n"
    b"COPY parent FROM STDIN;\n"
    b"1\talpha; COMMIT;\n"
    b"2\tcaf\xc3\xa9 \xe4\xb8\xad\xe6\x96\x87\n"
    b"3\ta\\tb\\nc\\\\d\n"
    b"4\t-- not a comment\n"
    b"5\t\\N\n"
    b"6\tit's \"quoted\"\n"
    b"\\.\n"
    b"INSERT INTO parent VALUES (7, 'it''s; ok');\n"
    b"COPY child FROM STDIN;\n"
    b"10\t1\tCOMMIT; data\n"
    b"11\t2\tline\\\\two\n"
    b"12\t6\t\xf0\x9f\x98\x80 emoji\n"
    b"\\.\n"
    b"COMMIT;\n"
)

# A transaction in the same family whose child rows violate immediate FK, used
# to prove the whole transaction is refused and the previous one is preserved.
FK_BAD = (
    b"CREATE TABLE parent(id INTEGER PRIMARY KEY);\n"
    b"CREATE TABLE child(id INTEGER PRIMARY KEY,"
    b" parent_id INTEGER REFERENCES parent(id));\n"
    b"BEGIN;\nCOPY parent FROM STDIN;\n1\n\\.\nCOMMIT;\n"
    b"BEGIN;\nCOPY parent FROM STDIN;\n2\n\\.\n"
    b"COPY child FROM STDIN;\n20\t999\n\\.\nCOMMIT;\n"
)

# DDL created *inside* the transaction, with dependent COPY/CREATE ordering.
DDL_IN_TXN = (
    b"BEGIN;\n"
    b"CREATE TABLE parent(id INTEGER PRIMARY KEY, label TEXT);\n"
    b"COPY parent FROM STDIN;\n1\tp\n\\.\n"
    b"CREATE TABLE child(id INTEGER PRIMARY KEY,"
    b" parent_id INTEGER REFERENCES parent(id), note TEXT);\n"
    b"CREATE INDEX child_parent_idx ON child(parent_id);\n"
    b"COPY child FROM STDIN;\n10\t1\tdep\n\\.\n"
    b"COMMIT;\n"
)


def append_family(prefix, k):
    """One more committed transaction of the same shape, with fresh ids."""
    return (
        b"BEGIN;\nCOPY parent FROM STDIN;\n%d\tp%d\n\\.\n"
        b"COPY child FROM STDIN;\n%d\t%d\tc%d\n\\.\nCOMMIT;\n"
        % (100 + k, k, 200 + k, 100 + k, k)
    )
