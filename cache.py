"""Content-addressed verified-object cache shared by concurrent fetch plans.

Invariants:

* Only *verified* bytes ever leave the cache.  A fetch writes into a private
  temp file under ``tmp/``; another session can neither open nor read it.
* Promotion is a single atomic ``os.replace`` under a per-key lock, so a
  second writer that loses the race discards its own temp file and adopts
  the already-published object instead of publishing unverified bytes.
* Cache keys are sha256 digests, so same-content replicas (and same-content
  assets across catalog snapshots) share one object; every contributing
  build/identity/replica is kept as provenance.
* References are counted per plan.  Releasing a plan lets the reaper remove
  objects that no remaining plan references; shared objects survive.
"""

import hashlib
import json
import os
import threading
from datetime import datetime, timezone


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')


class BlockCache:
    def __init__(self, cache_dir):
        self.dir = os.path.abspath(os.fspath(cache_dir))
        self.blocks_dir = os.path.join(self.dir, 'blocks')
        self.tmp_dir = os.path.join(self.dir, 'tmp')
        os.makedirs(self.blocks_dir, exist_ok=True)
        os.makedirs(self.tmp_dir, exist_ok=True)
        self.index_path = os.path.join(self.dir, 'index.json')
        self._lock = threading.RLock()
        self._key_locks = {}
        self.index = self._load_index()
        # Observable counters for logs / verification.
        self.hits = 0
        self.publishes = 0
        self.discarded_temps = 0
        self.duplicate_verifications = 0

    # -- persistence -------------------------------------------------------
    def _load_index(self):
        if os.path.exists(self.index_path):
            with open(self.index_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {'objects': {}}

    def _save_index_locked(self):
        tmp = self.index_path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(self.index, f, indent=2, sort_keys=True)
        os.replace(tmp, self.index_path)

    # -- key locking -------------------------------------------------------
    def _key_lock(self, key):
        with self._lock:
            lk = self._key_locks.get(key)
            if lk is None:
                # RLock so a lease can wrap internal get/publish calls.
                lk = threading.RLock()
                self._key_locks[key] = lk
            return lk

    class KeyLease:
        """Held while a session fetches+verifies one cache object."""

        def __init__(self, cache, key, plan_id):
            self.cache = cache
            self.key = key
            self.plan_id = plan_id
            self._lock = cache._key_lock(key)

        def __enter__(self):
            self._lock.acquire()
            # May have been published while we waited: caller re-checks.
            return self

        def __exit__(self, *exc):
            self._lock.release()

    def lease(self, key, plan_id):
        """Acquire the per-object lock. Re-check contains() afterwards."""
        return self.KeyLease(self, key, plan_id)

    # -- reads -------------------------------------------------------------
    def contains(self, key):
        with self._lock:
            return key in self.index['objects']

    def path_for(self, key):
        """Path to a *verified* object. Caller must hold a hit (contains)."""
        if not self.contains(key):
            raise KeyError(key)
        return os.path.join(self.blocks_dir, key[:2], key)

    def get(self, key, plan_id):
        """Return verified bytes for key, or None on a miss.

        A miss never returns a temp path: unverified data is not shared.
        """
        with self._lock:
            if key not in self.index['objects']:
                return None
        path = self.path_for(key)
        with open(path, 'rb') as f:
            data = f.read()
        with self._lock:
            self.hits += 1
            self._add_ref_locked(key, plan_id)
            self._save_index_locked()
        return data

    # -- writes ------------------------------------------------------------
    def new_temp(self, label):
        """Private temp file; unique per call so sessions cannot collide."""
        # UUID makes the name unique even across threads sharing a pid.
        import uuid
        name = f'{label}-{threading.get_ident()}-{uuid.uuid4().hex}'
        path = os.path.join(self.tmp_dir, name)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)
        return path

    def publish(self, tmp_path, key, data, provenance, plan_id,
                before_publish=None):
        """Atomically promote verified bytes.

        ``provenance`` is (build_id, identity_tuple, replica_url).  If the
        object already exists (another plan won the race) the temp file is
        discarded and the existing object is reused.  ``before_publish`` (if
        given) runs while holding the per-key lock, which lets a caller force
        a deterministic interleaving of concurrent publishers.
        """
        # Content integrity: the key must really name these bytes.
        actual = hashlib.sha256(data).hexdigest()
        if actual != key:
            os.remove(tmp_path)
            raise ValueError(
                f'publish refused: digest {actual[:12]} != key {key[:12]}')
        with self._key_lock(key):
            if before_publish is not None:
                before_publish(key, tmp_path)
            with self._lock:
                if key in self.index['objects']:
                    # Lost the race: another verified copy is authoritative.
                    os.remove(tmp_path)
                    self.discarded_temps += 1
                    self.duplicate_verifications += 1
                    self._merge_provenance_locked(key, provenance, plan_id)
                    self._save_index_locked()
                    return False
                dest_dir = os.path.join(self.blocks_dir, key[:2])
                os.makedirs(dest_dir, exist_ok=True)
                dest = os.path.join(dest_dir, key)
                os.replace(tmp_path, dest)  # atomic on POSIX
                self.index['objects'][key] = {
                    'size': len(data),
                    'created_at': _now(),
                    'refs': [plan_id],
                    'provenance': [{
                        'build_id': provenance[0],
                        'identity': list(provenance[1]),
                        'replica_url': provenance[2],
                        'at': _now(),
                    }],
                }
                self.publishes += 1
                self._save_index_locked()
                return True

    def discard_temp(self, tmp_path):
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            with self._lock:
                self.discarded_temps += 1

    def _add_ref_locked(self, key, plan_id):
        refs = self.index['objects'][key]['refs']
        if plan_id not in refs:
            refs.append(plan_id)

    def _merge_provenance_locked(self, key, provenance, plan_id):
        obj = self.index['objects'][key]
        self._add_ref_locked(key, plan_id)
        entry = {'build_id': provenance[0],
                 'identity': list(provenance[1]),
                 'replica_url': provenance[2], 'at': _now()}
        if entry not in obj['provenance']:
            obj['provenance'].append(entry)

    def note_provenance(self, key, provenance, plan_id):
        """Record that an existing object served another snapshot/plan."""
        with self._lock:
            if key not in self.index['objects']:
                raise KeyError(key)
            self._merge_provenance_locked(key, provenance, plan_id)
            self._save_index_locked()

    # -- lifecycle ---------------------------------------------------------
    def release_plan(self, plan_id):
        """Drop every reference held by a plan; unreferenced objects go."""
        removed = []
        with self._lock:
            for key, obj in list(self.index['objects'].items()):
                if plan_id in obj['refs']:
                    obj['refs'].remove(plan_id)
                if not obj['refs']:
                    path = self.path_for(key)
                    if os.path.exists(path):
                        os.remove(path)
                    removed.append(key)
                    del self.index['objects'][key]
            self._save_index_locked()
        return removed

    def footprint(self):
        total = 0
        with self._lock:
            for obj in self.index['objects'].values():
                total += obj['size']
        return total

    def object_count(self):
        with self._lock:
            return len(self.index['objects'])

    def stats(self):
        with self._lock:
            return {
                'objects': len(self.index['objects']),
                'footprint_bytes': self.footprint(),
                'hits': self.hits,
                'publishes': self.publishes,
                'discarded_temps': self.discarded_temps,
                'duplicate_verifications': self.duplicate_verifications,
            }
