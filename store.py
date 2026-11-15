"""Persistent annotation data directory.

State organization chosen: in-place update under one `BEGIN IMMEDIATE`
transaction on SCD (slowly-changing-dimension) rows. Each current row carries
NULL version_to; publishing closes old rows by writing version_to = new
version and inserts only the changed rows. A separate current-only
seg_index is rewritten only for entities whose status/segments actually
changed. Readers in another connection with a long read transaction keep
seeing the old snapshot through WAL; after commit they see the new complete
version, never a mixture.

Only the newest two published versions are retained; pruning happens in the
same transaction as publication, so no reader can ever observe an
intermediate pruned state.
"""
import json
import os
import sqlite3
from datetime import datetime, timezone

from closure import (full_closure, incremental_closure, affected_set)
from annotation import normalize_entity

DB_NAME = "annotation.db"
MAGIC = "annotation-dir/1"
MAX_ENTITIES = 10000
MAX_SEGMENTS = 30000
MAX_PATCHES = 100
MAX_OPS_PER_PATCH = 100
KEEP_VERSIONS = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS entity_version(
  id TEXT NOT NULL, version_from INTEGER NOT NULL, version_to INTEGER,
  status TEXT NOT NULL, reason TEXT NOT NULL, body TEXT NOT NULL,
  content_version INTEGER NOT NULL, status_version INTEGER NOT NULL,
  PRIMARY KEY(id, version_from)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ev_current ON entity_version(version_to) WHERE version_to IS NULL;
CREATE INDEX IF NOT EXISTS ev_ver ON entity_version(version_from, version_to);
CREATE TABLE IF NOT EXISTS segment_version(
  id TEXT NOT NULL, ord INTEGER NOT NULL, lo INTEGER NOT NULL, hi INTEGER NOT NULL,
  version_from INTEGER NOT NULL, version_to INTEGER,
  PRIMARY KEY(id, ord, version_from)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS sv_ver ON segment_version(version_from, version_to);
CREATE TABLE IF NOT EXISTS edge_version(
  id TEXT NOT NULL, parent TEXT NOT NULL, version_from INTEGER NOT NULL,
  version_to INTEGER, PRIMARY KEY(id, parent, version_from)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS edg_ver ON edge_version(version_from, version_to);
CREATE TABLE IF NOT EXISTS seg_index(
  id TEXT NOT NULL, ord INTEGER NOT NULL, lo INTEGER NOT NULL, hi INTEGER NOT NULL,
  PRIMARY KEY(id, ord)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS seg_idx_range ON seg_index(lo, hi);
CREATE TABLE IF NOT EXISTS provenance(
  id TEXT NOT NULL, ord INTEGER NOT NULL, lo INTEGER NOT NULL, hi INTEGER NOT NULL,
  version INTEGER NOT NULL, source TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS prov_id ON provenance(id, version);
CREATE TABLE IF NOT EXISTS patch_record(
  patch_id TEXT PRIMARY KEY, base_version INTEGER NOT NULL,
  new_version INTEGER NOT NULL, applied_at TEXT NOT NULL,
  payload TEXT NOT NULL, summary TEXT NOT NULL);
"""


class PatchError(Exception):
    def __init__(self, code, message):
        super().__init__("[%s] %s" % (code, message))
        self.code = code
        self.message = message


def _now():
    return datetime.now(timezone.utc).isoformat()


def open_store(path, create=False):
    path = os.path.abspath(path)
    db = os.path.join(path, DB_NAME)
    if not create and not os.path.exists(db):
        raise PatchError("no_store", "not an annotation directory: %s" % path)
    os.makedirs(path, exist_ok=True)
    conn = sqlite3.connect(db, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=FULL")
    conn.executescript(SCHEMA)
    row = conn.execute("SELECT value FROM meta WHERE key='magic'").fetchone()
    if row is None:
        if not create:
            raise PatchError("no_store", "store metadata missing")
        conn.execute("INSERT INTO meta VALUES('magic',?)", (MAGIC,))
        conn.execute("INSERT INTO meta VALUES('head_version','0')")
        conn.execute("INSERT INTO meta VALUES('patch_seq','0')")
    elif row[0] != MAGIC:
        raise PatchError("bad_store", "unexpected store magic %r" % row[0])
    return AnnotationStore(conn, path)


class AnnotationStore:
    def __init__(self, conn, path):
        self.conn = conn
        self.path = path
        self.fault = None  # test control point: raises inside the txn
        self.hook = None   # test control point: callable(stage, store), no raise

    # ------------------------------------------------------------------ utils
    @property
    def head_version(self):
        return int(self.conn.execute(
            "SELECT value FROM meta WHERE key='head_version'").fetchone()[0])

    def _bump(self, key):
        self.conn.execute("UPDATE meta SET value=value+1 WHERE key=?", (key,))
        return int(self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)).fetchone()[0])

    # ---------------------------------------------------------------- import
    def import_graph(self, entities, source, extra_reasons=None):
        """Create version 1 from a full entity mapping (JSON or parsed GFF3)."""
        if len(entities) > MAX_ENTITIES:
            raise PatchError("limit", "too many entities: %d" % len(entities))
        nseg = sum(len(e["segments"]) for e in entities.values())
        if nseg > MAX_SEGMENTS:
            raise PatchError("limit", "too many segments: %d" % nseg)
        status, reasons = full_closure(entities, extra_reasons)
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            if self.head_version != 0:
                raise PatchError("already_imported",
                                 "store already contains an import")
            version = 1
            for eid, e in entities.items():
                body = json.dumps(e, sort_keys=True, separators=(",", ":"))
                conn.execute(
                    "INSERT INTO entity_version VALUES(?,?,?,?,?,?,?,?)",
                    (eid, version, None, status[eid],
                     json.dumps(reasons.get(eid, [])), body,
                     version, version))
                for ord_, (lo, hi) in enumerate(e["segments"]):
                    conn.execute(
                        "INSERT INTO segment_version VALUES(?,?,?,?,?,?)",
                        (eid, ord_, lo, hi, version, None))
                    conn.execute(
                        "INSERT INTO provenance VALUES(?,?,?,?,?,?)",
                        (eid, ord_, lo, hi, version, source))
                for p in e["parents"]:
                    conn.execute(
                        "INSERT INTO edge_version VALUES(?,?,?,?)",
                        (eid, p, version, None))
                if status[eid] == "trusted":
                    for ord_, (lo, hi) in enumerate(e["segments"]):
                        conn.execute(
                            "INSERT INTO seg_index VALUES(?,?,?,?)",
                            (eid, ord_, lo, hi))
            conn.execute("UPDATE meta SET value='1' WHERE key='head_version'")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return {"version": version, "entities": len(entities),
                "segments": nseg,
                "quarantined": sorted(i for i, s in status.items()
                                      if s == "quarantined")}

    # ------------------------------------------------------------- patch ops
    def _validate_patch(self, patch, base):
        head = self.head_version
        if base != head:
            raise PatchError("stale_base",
                             "patch base %d != current head %d" % (base, head))
        if not isinstance(patch, list) or not patch:
            raise PatchError("bad_patch", "patch must be a non-empty op list")
        if len(patch) > MAX_OPS_PER_PATCH:
            raise PatchError("limit",
                             "patch has %d ops > %d"
                             % (len(patch), MAX_OPS_PER_PATCH))
        norm = []
        for op in patch:
            if not isinstance(op, dict) or op.get("op") not in ("replace",
                                                                "delete"):
                raise PatchError("bad_patch", "unknown op %r" % op)
            oid = op.get("id")
            if not isinstance(oid, str) or not oid:
                raise PatchError("bad_patch", "op missing string id")
            if op["op"] == "delete":
                norm.append(("delete", oid, None))
            else:
                ent = normalize_entity(op.get("entity"))
                if ent["id"] != oid:
                    raise PatchError("identity_mismatch",
                                    "op id %r != entity id %r"
                                    % (oid, ent["id"]))
                norm.append(("replace", oid, ent))
        # the authoritative patch-count check runs inside the write txn
        return norm

    def apply_patch(self, patch, base=None, patch_id=None,
                    extra_reasons=None):
        """Validate against base, then publish one new atomic version."""
        conn = self.conn
        if base is None:
            base = self.head_version
        norm = self._validate_patch(patch, base)
        extra_reasons = dict(extra_reasons or {})
        targets = [oid for _, oid, _ in norm]

        conn.execute("BEGIN IMMEDIATE")
        stats = {"entities_read": 0, "segments_read": 0,
                 "edges_read": 0, "segments_rewritten": 0,
                 "index_rows_written": 0, "entity_rows_written": 0,
                 "edge_rows_written": 0, "index_rows_deleted": 0}
        try:
            # --- touch only the entities the patch can possibly affect
            target_rows = conn.execute(
                "SELECT id FROM entity_version WHERE version_to IS NULL AND id IN (%s)"
                % ",".join("?" * len(targets)), targets).fetchall()
            stats["entities_read"] += len(target_rows)
            existing = {r["id"] for r in target_rows}
            for kind, oid, _ in norm:
                if kind == "delete" and oid not in existing:
                    raise PatchError("unknown_id",
                                     "cannot delete missing entity %r" % oid)
                # replace may re-create an entity deleted by an earlier patch
                # (that is the forward-declaration/repair path); only imports
                # may introduce ids that never existed anywhere.
            # recursive CTE: the target plus every descendant reachable via
            # old child edges. Seeds come from VALUES so deleted targets still
            # expand the cone. Parent hops are deliberately excluded: a patch
            # on a child never changes its parents' status.
            q = ("WITH RECURSIVE cone(id) AS ("
                 " SELECT * FROM (VALUES %s) "
                 " UNION SELECT ev.id FROM edge_version ev"
                 " JOIN cone c ON ev.parent=c.id AND ev.version_to IS NULL)"
                 " SELECT DISTINCT id FROM cone") % \
                ",".join(["(?)" for _ in targets])
            old_cone = {r[0] for r in conn.execute(q, targets)}

            # candidate new graph restricted to the old descendant cone
            old_bodies, statuses, old_reasons, segments, old_parents = \
                {}, {}, {}, {}, {}
            ids = sorted(old_cone)
            ph = ",".join("?" * len(ids))
            for r in conn.execute(
                    "SELECT id,status,reason,body FROM entity_version "
                    "WHERE version_to IS NULL AND id IN (%s)" % ph, ids):
                stats["entities_read"] += 1
                old_bodies[r["id"]] = json.loads(r["body"])
                statuses[r["id"]] = r["status"]
                old_reasons[r["id"]] = json.loads(r["reason"])
            for r in conn.execute(
                    "SELECT id,lo,hi FROM segment_version "
                    "WHERE version_to IS NULL AND id IN (%s) ORDER BY id,ord" % ph,
                    ids):
                stats["segments_read"] += 1
                segments.setdefault(r["id"], []).append([r["lo"], r["hi"]])
            for r in conn.execute(
                    "SELECT id,parent FROM edge_version "
                    "WHERE version_to IS NULL AND id IN (%s) ORDER BY id,parent"
                    % ph, ids):
                stats["edges_read"] += 1
                old_parents.setdefault(r["id"], []).append(r["parent"])

            # apply ops to the candidate graph
            bodies = {i: dict(e) for i, e in old_bodies.items()}
            deleted_ids = set()
            for kind, oid, ent in norm:
                if kind == "delete":
                    bodies.pop(oid, None)
                    deleted_ids.add(oid)
                else:
                    bodies[oid] = ent
            for oid in deleted_ids:
                statuses.setdefault(oid, "__missing__")
            new_parents = {i: list(e["parents"]) for i, e in bodies.items()}
            affected = affected_set(targets, old_parents, new_parents)
            # parents of affected nodes lying outside the old descendant cone
            # still decide existence / trusted status; load just those rows.
            outside = {p for i in affected for p in new_parents.get(i, [])
                       if p not in old_cone}
            if outside:
                oph = ",".join("?" * len(outside))
                found = set()
                for r in conn.execute(
                        "SELECT id,status,body FROM entity_version "
                        "WHERE version_to IS NULL AND id IN (%s)" % oph,
                        tuple(outside)):
                    stats["entities_read"] += 1
                    found.add(r["id"])
                    bodies.setdefault(r["id"], json.loads(r["body"]))
                    statuses.setdefault(r["id"], r["status"])
                for p in outside - found:
                    statuses.setdefault(p, "__missing__")

            # global limits are totals, not cone-local; compute with aggregates
            # so an unrelated 9999-entity graph is never loaded into Python.
            cur_entities = conn.execute(
                "SELECT COUNT(*) FROM entity_version WHERE version_to IS NULL"
            ).fetchone()[0]
            cur_segments = conn.execute(
                "SELECT COUNT(*) FROM segment_version WHERE version_to IS NULL"
            ).fetchone()[0]
            n_deleted = len(deleted_ids)
            n_created = sum(
                1 for kind, oid, _ in norm
                if kind == "replace" and oid not in existing)
            total_entities = cur_entities - n_deleted + n_created
            if total_entities > MAX_ENTITIES:
                raise PatchError("limit", "entity limit %d" % MAX_ENTITIES)
            seg_delta = 0
            for kind, oid, ent in norm:
                if kind == "replace" and oid in bodies:
                    seg_delta += len(ent["segments"]) - len(segments.get(oid, []))
            total_segments = cur_segments - sum(
                len(segments.get(d, [])) for d in deleted_ids) + seg_delta
            if total_segments > MAX_SEGMENTS:
                raise PatchError("limit", "segment limit %d" % MAX_SEGMENTS)
            n_patches = conn.execute(
                "SELECT COUNT(*) FROM patch_record").fetchone()[0]
            if n_patches >= MAX_PATCHES:
                raise PatchError("limit", "patch count limit %d reached"
                                 % MAX_PATCHES)
            new_extra = {i: r for i, r in extra_reasons.items()
                         if i in affected}
            recomputed = incremental_closure(
                affected, bodies, new_extra, statuses)

            version = self.head_version + 1
            changed_entities, status_changed, content_changed = [], [], []
            for i in sorted(affected):
                if i not in bodies:
                    changed_entities.append(i)  # deletion
                    continue
                new_status, new_reason = recomputed[i]
                content_diff = old_bodies.get(i) != bodies[i]
                state_diff = (statuses.get(i) != new_status
                              or old_reasons.get(i, []) != new_reason)
                if content_diff:
                    content_changed.append(i)
                if state_diff:
                    status_changed.append((i, statuses.get(i), new_status))
                if content_diff or state_diff:
                    changed_entities.append(i)

            if self.hook:
                self.hook("after_closure", self)
            # control point: after closure, before any index switch
            if self.fault == "before_index_switch":
                raise RuntimeError("injected fault before_index_switch")

            # --- close + insert only changed SCD rows
            for i in changed_entities:
                if i not in bodies:
                    conn.execute(
                        "UPDATE entity_version SET version_to=? WHERE id=? AND version_to IS NULL",
                        (version, i))
                    conn.execute(
                        "UPDATE segment_version SET version_to=? WHERE id=? AND version_to IS NULL",
                        (version, i))
                    conn.execute(
                        "UPDATE edge_version SET version_to=? WHERE id=? AND version_to IS NULL",
                        (version, i))
                    cur = conn.execute(
                        "DELETE FROM seg_index WHERE id=?", (i,))
                    stats["index_rows_deleted"] += cur.rowcount
                    stats["entity_rows_written"] += 1
                    continue
                ent = bodies[i]
                new_status, new_reason = recomputed[i]
                old = conn.execute(
                    "SELECT content_version,status_version FROM entity_version WHERE id=? AND version_to IS NULL",
                    (i,)).fetchone()
                is_new = old is None  # re-created after an earlier delete
                content_diff = is_new or old_bodies.get(i) != ent
                state_diff = is_new or (
                    statuses.get(i) != new_status
                    or old_reasons.get(i, []) != new_reason)
                if is_new:
                    cv = sv = version
                else:
                    cv = version if content_diff else old["content_version"]
                    sv = version if state_diff else old["status_version"]
                if not is_new:
                    conn.execute(
                        "UPDATE entity_version SET version_to=? WHERE id=? AND version_to IS NULL",
                        (version, i))
                conn.execute(
                    "INSERT INTO entity_version VALUES(?,?,?,?,?,?,?,?)",
                    (i, version, None, new_status,
                     json.dumps(new_reason),
                     json.dumps(ent, sort_keys=True, separators=(",", ":")),
                     cv, sv))
                stats["entity_rows_written"] += 1 if is_new else 2

                if content_diff:
                    old_seg = segments.get(i, [])
                    new_seg = ent["segments"]
                    if not is_new:
                        # close all old segment rows, open only new physical rows
                        conn.execute(
                            "UPDATE segment_version SET version_to=? WHERE id=? AND version_to IS NULL",
                            (version, i))
                    for ord_, (lo, hi) in enumerate(new_seg):
                        conn.execute(
                            "INSERT INTO segment_version VALUES(?,?,?,?,?,?)",
                            (i, ord_, lo, hi, version, None))
                    stats["segments_rewritten"] += len(old_seg) + len(new_seg)
                    for ord_, (lo, hi) in enumerate(new_seg):
                        conn.execute(
                            "INSERT INTO provenance VALUES(?,?,?,?,?,?)",
                            (i, ord_, lo, hi, version,
                             "patch:%s" % (patch_id or version)))

                old_edges = old_parents.get(i, [])
                new_edges = ent["parents"]
                if is_new or old_edges != new_edges:
                    if not is_new:
                        conn.execute(
                            "UPDATE edge_version SET version_to=? WHERE id=? AND version_to IS NULL",
                            (version, i))
                    for p in new_edges:
                        conn.execute(
                            "INSERT INTO edge_version VALUES(?,?,?,?)",
                            (i, p, version, None))
                    stats["edge_rows_written"] += len(old_edges) + len(new_edges)

                # --- current-only physical interval index, status-aware.
                # idempotent cleanup first so re-created entities cannot
                # collide with rows left from a previous life.
                old_st = statuses.get(i)
                cur = conn.execute(
                    "DELETE FROM seg_index WHERE id=?", (i,))
                stats["index_rows_deleted"] += cur.rowcount
                if new_status == "trusted":
                    for ord_, (lo, hi) in enumerate(ent["segments"]):
                        conn.execute(
                            "INSERT INTO seg_index VALUES(?,?,?,?)",
                            (i, ord_, lo, hi))
                        stats["index_rows_written"] += 1
            if self.hook:
                self.hook("after_index_switch", self)
            if self.fault == "after_index_switch":
                raise RuntimeError("injected fault after_index_switch")

            # --- retain only the newest KEEP_VERSIONS published versions
            keep_from = version - KEEP_VERSIONS + 1
            pruned = 0
            for tbl in ("entity_version", "segment_version", "edge_version"):
                cur = conn.execute(
                    "DELETE FROM %s WHERE version_to IS NOT NULL AND version_to < ?"
                    % tbl, (keep_from,))
                pruned += cur.rowcount

            seq = self._bump("patch_seq")
            if patch_id is None:
                patch_id = "patch-%04d" % seq
            if conn.execute(
                    "SELECT 1 FROM patch_record WHERE patch_id=?",
                    (patch_id,)).fetchone():
                raise PatchError("bad_patch",
                                 "duplicate patch_id %r" % patch_id)
            conn.execute(
                "INSERT INTO patch_record VALUES(?,?,?,?,?,?)",
                (patch_id, base, version, _now(),
                 json.dumps(patch, sort_keys=True),
                 json.dumps({"changed_entities": changed_entities,
                             "status_changed": status_changed,
                             "content_changed": content_changed,
                             "pruned_rows": pruned},
                            sort_keys=True)))
            conn.execute(
                "UPDATE meta SET value=? WHERE key='head_version'", (version,))
            if self.hook:
                self.hook("before_commit", self)
            if self.fault == "before_commit":
                raise RuntimeError("injected fault before_commit")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        stats["affected_entities"] = sorted(affected)
        stats["changed_entities"] = changed_entities
        stats["status_changed"] = status_changed
        stats["content_changed"] = content_changed
        stats["new_version"] = version
        stats["patch_id"] = patch_id
        return stats

    # ----------------------------------------------------------------- reads
    def query(self, lo, hi, version=None):
        """Overlap hit: current trusted entities' actual segments only."""
        if version is None:
            rows = self.conn.execute(
                "SELECT DISTINCT id FROM seg_index WHERE lo<? AND hi>? ORDER BY id",
                (hi, lo)).fetchall()
            return [r[0] for r in rows]
        self._check_readable(version)
        rows = self.conn.execute(
            "SELECT DISTINCT sv.id FROM segment_version sv "
            "JOIN entity_version ev ON ev.id=sv.id "
            "AND ev.version_from<=? AND (ev.version_to IS NULL OR ev.version_to>?) "
            "WHERE sv.version_from<=? AND (sv.version_to IS NULL OR sv.version_to>?) "
            "AND ev.status='trusted' AND sv.lo<? AND sv.hi>? ORDER BY sv.id",
            (version, version, version, version, hi, lo)).fetchall()
        return [r[0] for r in rows]

    def _check_readable(self, version):
        head = self.head_version
        if version <= 0 or version < head - KEEP_VERSIONS + 1 or version > head:
            raise PatchError("version_unavailable",
                             "version %s not retained (head=%d)"
                             % (version, head))

    def inspect_entity(self, eid, version=None):
        """Explain any entity, including quarantined ones and deleted history."""
        if version is None:
            r = self.conn.execute(
                "SELECT * FROM entity_version WHERE id=? AND version_to IS NULL",
                (eid,)).fetchone()
            if r is None:
                # maybe it existed in a retained historical version
                r = self.conn.execute(
                    "SELECT * FROM entity_version WHERE id=? ORDER BY version_from DESC LIMIT 1",
                    (eid,)).fetchone()
                if r is None:
                    raise PatchError("unknown_id", "no entity %r" % eid)
        else:
            self._check_readable(version)
            r = self.conn.execute(
                "SELECT * FROM entity_version WHERE id=? AND version_from<=? "
                "AND (version_to IS NULL OR version_to>?)",
                (eid, version, version)).fetchone()
            if r is None:
                raise PatchError("unknown_id",
                                 "entity %r absent at version %d"
                                 % (eid, version))
        ver = r["version_from"]
        segs = [list(x) for x in self.conn.execute(
            "SELECT lo,hi FROM segment_version WHERE id=? AND version_from<=? "
            "AND (version_to IS NULL OR version_to>?) ORDER BY ord",
            (eid, ver, ver))]
        edges = [x[0] for x in self.conn.execute(
            "SELECT parent FROM edge_version WHERE id=? AND version_from<=? "
            "AND (version_to IS NULL OR version_to>?) ORDER BY parent",
            (eid, ver, ver))]
        prov = [dict(lo=p["lo"], hi=p["hi"], ord=p["ord"],
                     version=p["version"], source=p["source"])
                for p in self.conn.execute(
                    "SELECT * FROM provenance WHERE id=? ORDER BY version,ord",
                    (eid,))]
        return {"id": r["id"], "version_from": r["version_from"],
                "version_to": r["version_to"], "status": r["status"],
                "reasons": json.loads(r["reason"]),
                "body": json.loads(r["body"]), "segments": segs,
                "parents": edges,
                "content_version": r["content_version"],
                "status_version": r["status_version"],
                "provenance": prov}

    def inspect_patch(self, patch_id=None):
        if patch_id is None:
            return [dict(r) for r in self.conn.execute(
                "SELECT patch_id,base_version,new_version,applied_at,summary "
                "FROM patch_record ORDER BY new_version")]
        r = self.conn.execute(
            "SELECT * FROM patch_record WHERE patch_id=?",
            (patch_id,)).fetchone()
        if r is None:
            raise PatchError("unknown_patch", "no patch %r" % patch_id)
        d = dict(r)
        d["payload"] = json.loads(d["payload"])
        d["summary"] = json.loads(d["summary"])
        return d

    def versions(self):
        head = self.head_version
        retained = [v for v in (head - 1, head) if v >= 1]
        return {"head": head, "readable_versions": retained,
                "patches": self.inspect_patch()}

    def close(self):
        self.conn.close()
