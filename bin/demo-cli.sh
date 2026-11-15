#!/usr/bin/env bash
# Whole demo suite, driven through the CLI, with reproducible output.
#
#   ./bin/demo-cli.sh            # uses fixtures/export.sql
#
# It writes into a fresh temp dir, prints the commands and the measured
# resume/audit read counts, and exits non-zero if any assertion fails.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/streamlex-cli.XXXXXX")"
export PYTHONPATH="$ROOT"
PY="${PYTHON:-python3}"
FAIL=0

say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
check() { if [ "$1" -ne 0 ]; then echo "FAILED: $2"; FAIL=1; else echo "ok: $2"; fi; }

say "normal path"
cp "$ROOT/fixtures/export.sql" "$WORK/s.sql"
(cd "$WORK" && "$PY" -m streamlex restore s.sql --target t.db --manifest m.json) \
  > "$WORK/normal.json"; check $? "restore full export"
"$PY" - "$WORK/normal.json" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
assert d['stats']['txns_committed']==2, d['stats']
assert d['checkpoint']['byte_offset']>0
print("  committed txns:", d['stats']['txns_committed'], "ckpt offset:", d['checkpoint']['byte_offset'])
PY

say "half-transaction observation + crash after source COMMIT"
rm -f "$WORK/c.db" "$WORK/c.json"
(cd "$WORK" && cp s.sql c.sql && "$PY" -m streamlex restore c.sql --target c.db --manifest c.json \
   --crash-at after_copy_end --observe-target c.db) > "$WORK/crash.out"
RC=$?
check $([ "$RC" -eq 130 ] && echo 0 || echo 1) "crash exit status 130 (got $RC)"
grep -q 'reader@after_copy_end' "$WORK/crash.out" && echo "  reader observation captured"
"$PY" - "$WORK/c.db" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1])
tabs=[r[0] for r in c.execute("select name from sqlite_master where type='table' and name not like '__streamlex%'")]
counts={t:c.execute('select count(*) from "%s"'%t).fetchone()[0] for t in tabs}
# No half transaction: every table has the same (zero) count.
assert len(set(counts.values()))<=1, counts
print("  visible at crash (no half txn):", counts)
PY

say "resume after crash"
(cd "$WORK" && "$PY" -m streamlex restore c.sql --target c.db --manifest c.json) > "$WORK/r.json"
check $? "resume"
"$PY" - "$WORK/r.json" "$WORK/c.db" <<'PY'
import json,sqlite3,sys
d=json.load(open(sys.argv[1])); c=sqlite3.connect(sys.argv[2])
assert c.execute('select count(*) from parent').fetchone()[0]==2
assert c.execute('select count(*) from child').fetchone()[0]==2
assert c.execute('pragma foreign_key_check').fetchall()==[]
assert d['resume_bytes_read']<=65536, d['resume_bytes_read']
print("  resume_from:",d['resume_from'],"boundary reads:",d['resume_bytes_read'])
PY

say "audit-source after tampering an old prefix byte"
cp "$WORK/s.sql" "$WORK/a.sql"
(cd "$WORK" && "$PY" -m streamlex restore a.sql --target a.db --manifest a.json) >/dev/null
"$PY" - "$WORK/a.sql" <<'PY'
import sys
p=sys.argv[1]; b=bytearray(open(p,'rb').read()); b[5]=ord('X'); open(p,'wb').write(bytes(b))
PY
(cd "$WORK" && "$PY" -m streamlex audit-source a.sql --target a.db --manifest a.json) \
   > "$WORK/audit.out" 2> "$WORK/audit.err"
RC=$?
check $([ "$RC" -eq 3 ] && echo 0 || echo 1) "audit exits 3 on tamper (got $RC)"
sed 's/^/  /' "$WORK/audit.err"
(cd "$WORK" && "$PY" -m streamlex restore a.sql --target a.db --manifest a.json) \
   >/dev/null 2> "$WORK/refuse.err"
RC=$?
check $([ "$RC" -eq 3 ] && echo 0 || echo 1) "resume refuses untrusted target (got $RC)"
sed 's/^/  /' "$WORK/refuse.err"

say "exhaustive byte-cut sweep (python demo.py)"
"$PY" "$ROOT/demo.py" > "$WORK/demo.out" 2>&1
check $? "demo.py"
tail -12 "$WORK/demo.out" | sed 's/^/  /'

printf '\nwork dir: %s\n' "$WORK"
exit "$FAIL"
