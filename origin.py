"""本地 Origin：按 (url, revision) 提供真实字节，并记录读取日志。

升级点：
- 每次 get 记录 url/revision/字节数/实际摘要，bytes 累计本次进程内读出的 payload；
- visible_revision 模拟“当前可见源修订”指针，供控制点切换；
  管线永远按清单钉住的修订读取，不依赖该指针（get_visible 仅用于演示风险）；
- corrupt 是测试/控制点用的本地破坏手段（翻转首字节）；
- 线程安全，并统计并发水位（验证 max_parallel_reads 上限）。
"""
from pathlib import Path
import hashlib
import threading
import time


class Origin:
    def __init__(self, root, visible_revision=None, delay=0.0):
        self.root = Path(root)
        self.bytes = 0
        self.log = []
        self.visible_revision = visible_revision
        self.delay = delay
        self._lock = threading.Lock()
        self._inflight = 0
        self.max_inflight = 0

    def get(self, url, revision):
        """按钉住的修订读取一个对象，返回 (字节, 实际 sha256)。"""
        with self._lock:
            self._inflight += 1
            self.max_inflight = max(self.max_inflight, self._inflight)
        try:
            data = (self.root / revision / url).read_bytes()
            if self.delay:
                time.sleep(self.delay)
        finally:
            with self._lock:
                self._inflight -= 1
        digest = hashlib.sha256(data).hexdigest()
        with self._lock:
            self.bytes += len(data)
            self.log.append({"url": url, "revision": revision,
                             "bytes": len(data), "sha256": digest})
        return data, digest

    def get_visible(self, url):
        """按当前可见修订读取（仅演示用；管线不使用，避免被指针切换误导）。"""
        if self.visible_revision is None:
            raise RuntimeError("visible_revision 未设置")
        return self.get(url, self.visible_revision)

    def switch_visible(self, revision):
        """控制点：切换可见源修订。"""
        self.visible_revision = revision

    def corrupt(self, url, revision):
        """控制点/测试：破坏一个对象（翻转首字节），返回是否成功。"""
        p = self.root / revision / url
        data = bytearray(p.read_bytes())
        if not data:
            return False
        data[0] ^= 0xFF
        p.write_bytes(bytes(data))
        return True

    def reset_counters(self):
        with self._lock:
            self.bytes = 0
            self.log = []
            self.max_inflight = 0
