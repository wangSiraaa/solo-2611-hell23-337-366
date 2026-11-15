# -*- coding: utf-8 -*-
"""SQLite 真实快照：backup 生成只读副本 + token 注册/过期/关闭。

baseline 保留的两个入口：
  create(source, dest, token)  -> 备份元信息 dict
  open_snapshot(binding)      -> mode=ro 的 sqlite3 连接

新增：
  SnapshotRegistry：进程内 token 注册表，记录绑定版本向量、读者重开预算
  （每个绑定最多额外重开两次）、过期与关闭语义。快照文件本身是本地真实
  SQLite 文件，源库可以继续写入；任何连接都通过 mode=ro 打开，且每次打开
  前重新校验 sha256，文件被删/被换即判定快照不可用。
"""
from pathlib import Path
import sqlite3
import hashlib
import json
import threading
import time


def create(source, dest, token):
    """用 SQLite online backup 生成源库的真实只读副本。"""
    dest = Path(dest)
    if dest.exists():
        raise ValueError("snapshot already exists")
    dest.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect("file:" + str(Path(source).resolve()) + "?mode=ro", uri=True)
    out = sqlite3.connect(dest)
    try:
        src.backup(out)
    finally:
        out.close()
        src.close()
    return {
        "token": token,
        "path": str(dest.resolve()),
        "sha256": hashlib.sha256(dest.read_bytes()).hexdigest(),
    }


def verify_binding(binding):
    """校验快照文件仍存在且字节未变，返回 bool，不抛异常。"""
    p = Path(binding["path"])
    try:
        if not p.exists() or not p.is_file():
            return False
        h = hashlib.sha256(p.read_bytes()).hexdigest()
        return h == binding["sha256"]
    except OSError:
        return False


def open_snapshot(binding):
    """以 mode=ro 打开快照；文件缺失/被篡改时抛 ValueError。"""
    if not verify_binding(binding):
        raise ValueError("snapshot unavailable or changed")
    return sqlite3.connect(
        "file:" + str(Path(binding["path"]).resolve()) + "?mode=ro", uri=True
    )


class SnapshotExpired(LookupError):
    """token 已过期/关闭，或快照文件缺失/被篡改。"""


class ReopenBudgetExceeded(RuntimeError):
    """同一快照绑定的读者重开次数超过预算（最多两次）。"""


class SnapshotBinding:
    """单个分片快照的注册记录。"""

    # 初次打开 1 次 + 最多重开 2 次 = 总共 3 次连接
    MAX_REOPENS = 2

    def __init__(self, shard_id, binding, registry):
        self.shard_id = shard_id
        self.binding = dict(binding)
        self._registry = registry
        self.opened = 0
        self.reopens = 0
        self.expired = False
        self.closed = False
        self.created_at = time.time()

    def _check_live(self):
        if self.closed:
            raise SnapshotExpired("snapshot token for shard %s 已 close" % self.shard_id)
        if self.expired:
            raise SnapshotExpired("snapshot token for shard %s 已过期" % self.shard_id)
        if not verify_binding(self.binding):
            self.expired = True
            raise SnapshotExpired(
                "shard %s 的快照文件缺失或被篡改: %s"
                % (self.shard_id, self.binding["path"]))

    def open(self):
        """打开一次只读连接；第 2、3 次打开计入重开预算。"""
        with self._registry._lock:
            self._check_live()
            if self.opened > 0:
                if self.reopens >= self.MAX_REOPENS:
                    raise ReopenBudgetExceeded(
                        "shard %s 读者重开超过预算 %d 次"
                        % (self.shard_id, self.MAX_REOPENS))
                self.reopens += 1
            self.opened += 1
            attempt = self.opened
        try:
            conn = open_snapshot(self.binding)
        except ValueError:
            with self._registry._lock:
                self.expired = True
            raise SnapshotExpired(
                "shard %s 快照不可用（第 %d 次打开）" % (self.shard_id, attempt))
        return conn

    def expire(self):
        self.expired = True

    def to_meta(self):
        return {
            "shard_id": self.shard_id,
            "token": self.binding["token"],
            "path": self.binding["path"],
            "sha256": self.binding["sha256"],
            "opened": self.opened,
            "reopens": self.reopens,
            "expired": self.expired,
            "closed": self.closed,
        }


class SnapshotRegistry:
    """token 注册表：注册、查询、过期、关闭。"""

    def __init__(self):
        self._bindings = {}
        self._lock = threading.RLock()

    def register(self, shard_id, binding):
        with self._lock:
            if shard_id in self._bindings:
                raise ValueError("shard %s 已注册 snapshot token" % shard_id)
            rec = SnapshotBinding(shard_id, binding, self)
            self._bindings[shard_id] = rec
            return rec

    def get(self, shard_id):
        with self._lock:
            rec = self._bindings.get(shard_id)
            if rec is None:
                raise SnapshotExpired("shard %s 无已注册 snapshot token" % shard_id)
            rec._check_live()
            return rec

    def expire(self, shard_id):
        with self._lock:
            self.get(shard_id).expire()

    def close_shard(self, shard_id):
        with self._lock:
            rec = self._bindings.pop(shard_id, None)
            if rec is None:
                return False
            rec.closed = True
            return True

    def close_all(self):
        with self._lock:
            recs = list(self._bindings.values())
            self._bindings.clear()
            for rec in recs:
                rec.closed = True
            return [r.shard_id for r in recs]

    def all_live(self, shard_ids, strong=False):
        """供 next-page 前置检查：任一快照过期/缺失即整体不可继续。

        strong=False（默认，廉价）：只查注册状态、文件存在性与大小，
        用于每次 next-page 前的快速门禁（Linux 下已打开连接仍能读
        已 unlink 的 inode，必须显式检查路径）。
        strong=True：额外逐字节 sha256 校验，连接打开时使用。
        """
        with self._lock:
            missing, bad = [], []
            for sid in shard_ids:
                rec = self._bindings.get(sid)
                if rec is None or rec.closed or rec.expired:
                    missing.append(sid)
                    continue
                p = Path(rec.binding["path"])
                if not p.exists() or not p.is_file():
                    rec.expired = True
                    missing.append(sid)
                    continue
                if strong:
                    if not verify_binding(rec.binding):
                        rec.expired = True
                        bad.append(sid)
            return not (missing or bad), sorted(set(missing + bad))

    def audit(self):
        with self._lock:
            return {sid: rec.to_meta() for sid, rec in sorted(self._bindings.items())}

    def export_json(self, path):
        Path(path).write_text(
            json.dumps(self.audit(), ensure_ascii=False, indent=2), encoding="utf-8")
