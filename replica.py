"""Local, real-file replica adapters.

The baseline ``Origin`` is preserved untouched so ``baseline_demo.py`` keeps
running.  The rest of the module implements the real adapters used by the
new pipeline:

* :class:`LocalAdapter`        -- whole-file-only adapter (no range).
* :class:`RangeLocalAdapter`   -- adapter that answers real byte-range GETs.
* :class:`ReplicaResolver`     -- resolves catalog URLs to adapters and can
                                  mutate the backing files between plan and
                                  fetch so that GET-time corruption / content
                                  swaps / unavailability are *really* checked.

Everything reads real local files; no network and no cloud credentials.
"""

import hashlib
import os
import shutil
from dataclasses import dataclass

BLOCK_SIZE = 4096


class ReplicaError(Exception):
    """Raised when a replica cannot be read (missing file, permission, ...)."""


class RangeNotSupported(ReplicaError):
    """Raised when a whole-file-only adapter is asked for a ranged GET."""


def sha256_hex(b):
    return hashlib.sha256(b).hexdigest()


def block_digests(data, block_size=BLOCK_SIZE):
    """Return ``[(start, end, sha256), ...]`` for fixed-size blocks."""
    out = []
    for start in range(0, len(data), block_size):
        chunk = data[start:start + block_size]
        out.append((start, start + len(chunk), sha256_hex(chunk)))
    return out


class Origin:
    """Baseline adapter kept verbatim for the legacy demo."""

    def __init__(self, root):
        self.root = root
        self.log = []

    def get(self, path, start, end):
        with (self.root / path).open('rb') as f:
            f.seek(start)
            b = f.read(end - start)
        self.log.append({'path': path, 'start': start, 'end': end, 'bytes': len(b)})
        return b


@dataclass
class _AccessRecord:
    method: str          # 'HEAD' or 'GET'
    url: str
    start: int | None
    end: int | None
    requested_bytes: int
    returned_bytes: int
    status: str          # 'ok' | 'error'
    note: str = ''


class _BaseLocalAdapter:
    """Common real-file behaviour plus deterministic fault injection."""

    range_capable = False

    def __init__(self, root):
        self.root = os.path.abspath(os.fspath(root))
        # One ordered, shared log for every adapter handed out by a resolver.
        self.log: list[_AccessRecord] = []
        # url -> fault spec. Faults are installed *after* build so the catalog
        # stays clean and fetch has to discover them via real reads.
        self.faults: dict[str, dict] = {}

    # -- path resolution ---------------------------------------------------
    def _path(self, url):
        # Catalog URLs are relative POSIX paths under the replica root.
        parts = [p for p in url.split('/') if p not in ('', '.')]
        if any(p == '..' for p in parts):
            raise ReplicaError(f'refuse path traversal in url {url!r}')
        return os.path.join(self.root, *parts)

    def set_fault(self, url, kind, **kw):
        """Install a fault: unavailable | corrupt | swap | short | head_lie."""
        self.faults[url] = {'kind': kind, **kw}

    def clear_faults(self):
        self.faults.clear()

    def _record(self, method, url, start, end, requested, returned, status, note=''):
        self.log.append(_AccessRecord(method, url, start, end,
                                      requested, returned, status, note))

    # -- HEAD --------------------------------------------------------------
    def head(self, url):
        """Real HEAD: stat the backing file and return (size, sha256|None).

        The sha256 is only returned when the file can actually be hashed; the
        point of HEAD is cheap metadata, so callers must still verify GET
        bytes themselves.
        """
        fault = self.faults.get(url)
        if fault and fault['kind'] == 'unavailable':
            self._record('HEAD', url, None, None, 0, 0, 'error', 'unavailable')
            raise ReplicaError(f'{url}: unavailable')
        path = self._path(url)
        if not os.path.exists(path):
            self._record('HEAD', url, None, None, 0, 0, 'error', 'missing')
            raise ReplicaError(f'{url}: no such file')
        size = os.path.getsize(path)
        if fault and fault['kind'] == 'head_lie':
            size = int(fault.get('size', size + 7))
        self._record('HEAD', url, None, None, 0, 0, 'ok')
        return size

    def _read_bytes(self, url, start=None, end=None):
        """Actual disk read, applying installed faults to the returned bytes.

        The backing files on disk are *not* modified by ``corrupt`` faults;
        the adapter flips a byte on the way out, which faithfully models a
        transport-level corruption while keeping the fixture reusable.
        ``swap`` is applied by mutating the real file in the resolver so that
        every adapter (and a second catalog build) sees the new content.
        """
        fault = self.faults.get(url)
        if fault and fault['kind'] == 'unavailable':
            self._record('GET', url, start, end,
                         (end - start) if start is not None else 0,
                         0, 'error', 'unavailable')
            raise ReplicaError(f'{url}: unavailable')
        path = self._path(url)
        if not os.path.exists(path):
            self._record('GET', url, start, end,
                         (end - start) if start is not None else 0,
                         0, 'error', 'missing')
            raise ReplicaError(f'{url}: no such file')
        with open(path, 'rb') as f:
            if start is None:
                f.seek(0)
                data = f.read()
            else:
                f.seek(start)
                data = f.read(end - start)
        requested = len(data)
        if fault and fault['kind'] == 'corrupt' and data:
            at = int(fault.get('offset', 0))
            if at < len(data):
                data = data[:at] + bytes([data[at] ^ 0xFF]) + data[at + 1:]
        if fault and fault['kind'] == 'short':
            keep = max(0, int(fault.get('keep', len(data) // 2)))
            data = data[:keep]
        return data, requested


class LocalAdapter(_BaseLocalAdapter):
    """Whole-file-only replica. A ranged request is an error, never faked."""

    range_capable = False

    def get(self, url, start=None, end=None):
        if start is not None or end is not None:
            # Whole-file adapters must not pretend they only fetched the
            # requested bytes: reading the whole file is the only option.
            data, requested = self._read_bytes(url, None, None)
            self._record('GET', url, 0, len(data), requested, len(data), 'ok',
                         'whole-file fallback for ranged request')
            return data, True  # True == whole_file semantics
        data, requested = self._read_bytes(url)
        self._record('GET', url, None, None, requested, len(data), 'ok')
        return data, False


class RangeLocalAdapter(_BaseLocalAdapter):
    """Range-capable replica: only the requested bytes leave the file."""

    range_capable = True

    def get(self, url, start=None, end=None):
        if start is None and end is None:
            data, requested = self._read_bytes(url)
            self._record('GET', url, None, None, requested, len(data), 'ok')
            return data, False
        if start is None:
            start = 0
        data, requested = self._read_bytes(url, start, end)
        self._record('GET', url, start, end, requested, len(data), 'ok')
        return data, False


class ReplicaResolver:
    """Hands out adapters for a replica root and performs real file swaps."""

    def __init__(self, root):
        self.root = os.path.abspath(os.fspath(root))
        self._shared_log: list[_AccessRecord] = []
        # Known faults survive adapter() calls (plan may resolve before fetch).
        self.faults: dict[str, dict] = {}

    def adapter(self, descriptor):
        range_capable = (descriptor['range_capable']
                         if isinstance(descriptor, dict)
                         else descriptor.range_capable)
        if range_capable:
            a = RangeLocalAdapter(self.root)
        else:
            a = LocalAdapter(self.root)
        a.log = self._shared_log
        a.faults = self.faults
        return a

    @property
    def log(self):
        return self._shared_log

    def access_log(self):
        """Serializable copy of every real HEAD/GET performed."""
        return [r.__dict__.copy() for r in self._shared_log]

    def totals(self):
        req = sum(r.requested_bytes for r in self._shared_log)
        ret = sum(r.returned_bytes for r in self._shared_log)
        calls = len(self._shared_log)
        return {'calls': calls, 'requested_bytes': req,
                'returned_bytes': ret}

    # -- fault injection ---------------------------------------------------
    def make_unavailable(self, url):
        """Really remove the backing file (recorded so tests can restore)."""
        path = self._path(url)
        if os.path.exists(path):
            os.rename(path, path + '.unavailable')
        self.faults[url] = {'kind': 'unavailable'}

    def restore(self, url):
        path = self._path(url)
        if os.path.exists(path + '.unavailable') and not os.path.exists(path):
            os.rename(path + '.unavailable', path)
        self.faults.pop(url, None)

    def corrupt_transport(self, url, offset=0):
        """Byte flips only inside the adapter; the disk file stays intact."""
        self.faults[url] = {'kind': 'corrupt', 'offset': offset}

    def short_read(self, url, keep):
        self.faults[url] = {'kind': 'short', 'keep': keep}

    def head_lie(self, url, size):
        self.faults[url] = {'kind': 'head_lie', 'size': size}

    def swap_content(self, url, new_bytes):
        """Really overwrite the backing file: same URL, different content."""
        path = self._path(url)
        backup = path + '.orig'
        if not os.path.exists(backup):
            shutil.copy2(path, backup)
        with open(path, 'wb') as f:
            f.write(new_bytes)
        # No adapter fault needed: the real file changed, real GETs see it.

    def restore_swap(self, url):
        path = self._path(url)
        backup = path + '.orig'
        if os.path.exists(backup):
            shutil.copy2(backup, path)
            os.remove(backup)

    def _path(self, url):
        parts = [p for p in url.split('/') if p not in ('', '.')]
        if any(p == '..' for p in parts):
            raise ReplicaError(f'refuse path traversal in url {url!r}')
        return os.path.join(self.root, *parts)
