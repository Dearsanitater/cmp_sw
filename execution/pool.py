from queue import Queue
#维护全局连接池，每个连接池对应一个数据库配置，连接池大小可配置，默认10。

class ConnectionPool:
    def __init__(self, factory, size: int):
        self.factory = factory
        self.size = size
        self.queue = Queue(maxsize=size)

        for _ in range(size):
            self.queue.put(factory())

    def acquire(self):
        return self.queue.get()

    def release(self, conn):
        self.queue.put(conn)

    def close_all(self):
        while not self.queue.empty():
            conn = self.queue.get()
            try:
                conn.close()
            except Exception:
                pass