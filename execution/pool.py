from __future__ import annotations

import queue
from threading import Lock


class ConnectionPool:
    """
    这里做的是任务型连接池，不追求 Web 服务那种复杂治理。
    目标只有三个：
    1. 每个 worker/job 维护一套 src_pool/tgt_pool。
    2. 连接按需懒创建，最多 workers 个。
    3. 表级线程 acquire/release，异常连接按 broken 丢弃。
    """

    def __init__(self, factory, size: int, acquire_timeout: float | None = None):
        self.factory = factory
        self.size = size
        self.acquire_timeout = acquire_timeout
        self._idle = queue.Queue(maxsize=size)
        self._created = 0
        self._closed = False
        self._lock = Lock()

    def acquire(self):
        if self._closed:
            raise RuntimeError("connection pool already closed")

        try:
            return self._idle.get_nowait()
        except queue.Empty:
            pass

        with self._lock:
            if self._created < self.size:
                self._created += 1
                try:
                    return self.factory()
                except Exception:
                    self._created -= 1
                    raise

        try:
            return self._idle.get(timeout=self.acquire_timeout)
        except queue.Empty as exc:
            raise TimeoutError("acquire connection timeout") from exc

    def release(self, conn, broken: bool = False) -> None:
        if conn is None:
            return

        if self._closed or broken:
            try:
                conn.close()
            finally:
                with self._lock:
                    self._created = max(0, self._created - 1)
            return

        self._idle.put(conn)

    def close_all(self) -> None:
        self._closed = True
        while True:
            try:
                conn = self._idle.get_nowait()
            except queue.Empty:
                break
            try:
                conn.close()
            except Exception:
                pass
