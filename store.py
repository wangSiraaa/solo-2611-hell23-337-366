"""对象缓存与发布存储：sqlite 索引 + 内容寻址对象 + 原子发布。

缓存身份是 (url, revision) —— 不能只按 URL 复用：同一 URL 在不同修订
（语法代际）是不同对象。对象字节按 sha256 内容寻址落盘（同内容物理去重），
但命中前必须证明版本及摘要：索引行状态、声明摘要、文件长度与实时重算摘要
四者一致才算命中；证明过程只读本地缓存，计入元数据开销，不计 payload 预算。

发布是原子的：发布目录先整体落盘再改名，current 指针用临时文件 + os.replace
切换；失败只追加 failures/ 记录，绝不动旧发布。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def _utcnow():
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, root):
        self.root = Path(root)
        (self.root / "objects").mkdir(parents=True, exist_ok=True)
        (self.root / "publications").mkdir(exist_ok=True)
        (self.root / "failures").mkdir(exist_ok=True)
        # 元数据开销计数（另报，不纳入 payload 预算）
        self.meta = {"cache_proof_bytes": 0, "index_ops": 0, "publication_bytes": 0}
        with self._connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS objects(
                       url TEXT NOT NULL, revision TEXT NOT NULL,
                       sha256 TEXT NOT NULL, bytes INTEGER NOT NULL,
                       path TEXT NOT NULL, state TEXT NOT NULL,
                       observed_sha256 TEXT, observed_bytes INTEGER,
                       updated_utc TEXT NOT NULL,
                       PRIMARY KEY (url, revision))""")

    # ------------------------------------------------------------ 缓存索引

    def _connect(self):
        self.meta["index_ops"] += 1
        return sqlite3.connect(self.root / "index.sqlite3")

    def _row(self, url, revision):
        with self._connect() as db:
            return db.execute(
                "SELECT sha256, bytes, path, state FROM objects WHERE url=? AND revision=?",
                (url, revision)).fetchone()

    def object_state(self, url, revision):
        row = self._row(url, revision)
        return row[3] if row else None

    def object_path(self, sha256):
        return self.root / "objects" / sha256

    def verify_cached(self, decl):
        """缓存命中证明：版本(修订)与摘要都匹配才返回路径，否则返回 None。

        发现缓存文件被篡改（长度/摘要与声明不符）时，删除该错误文件并丢弃
        索引行 —— 这只是缓存失效，对象仍可从 Origin 重新读取，不算对象损坏。
        """
        row = self._row(decl.url, decl.revision)
        if not row or row[3] != "verified" or row[0] != decl.sha256 or row[1] != decl.bytes:
            return None
        path = self.root / row[2]
        if not path.exists():
            self._drop(decl.url, decl.revision)
            return None
        data = path.read_bytes()
        self.meta["cache_proof_bytes"] += len(data)
        observed = hashlib.sha256(data).hexdigest()
        if len(data) != decl.bytes or observed != decl.sha256:
            self._drop(decl.url, decl.revision)
            try:
                path.unlink()
            except OSError:
                pass
            return None
        return path

    def _drop(self, url, revision):
        with self._connect() as db:
            db.execute("DELETE FROM objects WHERE url=? AND revision=?", (url, revision))

    def put_verified(self, decl, data):
        """写入一个已验真对象（调用方已核对长度与摘要）。"""
        rel = Path("objects") / decl.sha256
        path = self.root / rel
        if not path.exists():
            tmp = self.root / "objects" / (".tmp-" + decl.sha256)
            tmp.write_bytes(data)
            os.replace(tmp, path)
        with self._connect() as db:
            db.execute(
                """INSERT INTO objects(url, revision, sha256, bytes, path, state, updated_utc)
                   VALUES(?,?,?,?,?,'verified',?)
                   ON CONFLICT(url, revision) DO UPDATE SET
                       sha256=excluded.sha256, bytes=excluded.bytes, path=excluded.path,
                       state='verified', observed_sha256=NULL, observed_bytes=NULL,
                       updated_utc=excluded.updated_utc""",
                (decl.url, decl.revision, decl.sha256, decl.bytes, str(rel), _utcnow()))
        return path

    def mark_bad(self, decl, observed_sha256, observed_bytes):
        """记录 Origin 侧验真失败（长度/摘要不符）：相关候选段从此不可见。"""
        with self._connect() as db:
            db.execute(
                """INSERT INTO objects(url, revision, sha256, bytes, path, state,
                                       observed_sha256, observed_bytes, updated_utc)
                   VALUES(?,?,?,?,?,'bad',?,?,?)
                   ON CONFLICT(url, revision) DO UPDATE SET
                       state='bad',
                       observed_sha256=excluded.observed_sha256,
                       observed_bytes=excluded.observed_bytes,
                       updated_utc=excluded.updated_utc""",
                (decl.url, decl.revision, decl.sha256, decl.bytes,
                 str(Path("objects") / decl.sha256),
                 observed_sha256, observed_bytes, _utcnow()))

    def bad_objects(self):
        with self._connect() as db:
            return db.execute(
                "SELECT url, revision, sha256, observed_sha256, observed_bytes "
                "FROM objects WHERE state='bad'").fetchall()

    # ------------------------------------------------------------ 原子发布

    def publish(self, pub):
        """原子发布：目录整体落盘后改名，再原子切换 current 指针。旧发布保留。"""
        canonical = json.dumps(pub, sort_keys=True, ensure_ascii=False)
        pub_id = "pub-" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        pub = dict(pub, publication_id=pub_id)
        final = self.root / "publications" / pub_id
        if not final.exists():
            tmp = self.root / "publications" / (".tmp-" + pub_id)
            if tmp.exists():
                for p in sorted(tmp.rglob("*"), reverse=True):
                    p.unlink() if p.is_file() else p.rmdir()
            tmp.mkdir()
            payload = json.dumps(pub, indent=2, ensure_ascii=False, sort_keys=True)
            (tmp / "manifest.json").write_text(payload, encoding="utf-8")
            os.rename(tmp, final)
        self.meta["publication_bytes"] += len(
            json.dumps(pub, ensure_ascii=False).encode("utf-8"))
        ptr_tmp = self.root / ".current.tmp"
        ptr_tmp.write_text(pub_id, encoding="utf-8")
        os.replace(ptr_tmp, self.root / "current")
        return pub_id

    def record_failure(self, failure):
        """明确的失败结果：追加失败记录，不触碰 current 与任何旧发布。"""
        canonical = json.dumps(failure, sort_keys=True, ensure_ascii=False)
        fail_id = "fail-" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        failure = dict(failure, failure_id=fail_id)
        payload = json.dumps(failure, indent=2, ensure_ascii=False, sort_keys=True)
        tmp = self.root / "failures" / (".tmp-" + fail_id)
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, self.root / "failures" / (fail_id + ".json"))
        return fail_id

    # ------------------------------------------------------------ 读取

    def current(self):
        ptr = self.root / "current"
        return ptr.read_text(encoding="utf-8").strip() if ptr.exists() else None

    def load_publication(self, pub_id="current"):
        if pub_id == "current":
            pub_id = self.current()
            if pub_id is None:
                raise FileNotFoundError("存储中还没有任何发布")
        path = self.root / "publications" / pub_id / "manifest.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def load_failure(self, fail_id):
        path = self.root / "failures" / (fail_id + ".json")
        return json.loads(path.read_text(encoding="utf-8"))

    def list_publications(self):
        return sorted(p.name for p in (self.root / "publications").iterdir()
                      if p.is_dir() and not p.name.startswith(".tmp"))

    def list_failures(self):
        return sorted(p.name[:-5] for p in (self.root / "failures").glob("fail-*.json"))
