from __future__ import annotations

import sys
from pathlib import Path

from connector.registry import create_connector
from .pool import ConnectionPool


class CompareJobRunner:
    """
    runner 只做入口编排：
    1. 基于 DB.ini 对应的 connector 创建连接工厂。
    2. 按 workers 预备 src/tgt 连接池，后续 compare 接管时可直接复用。
    3. 把任务参数原样交给 core.compare，不在这里复制任何比对逻辑。
    """

    def __init__(self, job):
        self.job = job

    def run(self):
        src_factory = lambda: create_connector(self.job.src_key)
        tgt_factory = lambda: create_connector(self.job.tgt_key)

        # compare 现在仍是老模块导入方式，这里只补运行时路径兼容，不改 compare 内核。
        core_dir = Path(__file__).resolve().parents[1] / "core"
        if str(core_dir) not in sys.path:
            sys.path.insert(0, str(core_dir))

        src_pool = ConnectionPool(src_factory, size=self.job.workers)
        tgt_pool = ConnectionPool(tgt_factory, size=self.job.workers)

        try:
            from core.compare_refactor import ergodic_database

            worker = ergodic_database(schema_collect_workers=getattr(self.job, "workers", 4))
            worker.src_factory = src_factory
            worker.tgt_factory = tgt_factory
            worker.src_pool = src_pool
            worker.tgt_pool = tgt_pool
            worker.src_schema = getattr(self.job, "src_schema", None)
            worker.tgt_schema = getattr(self.job, "tgt_schema", None)
            worker.workers = getattr(self.job, "workers", 4)
            worker.batch_size = getattr(self.job, "batch_size", 1000)

            err_handling = getattr(self.job, "err_handling", 3)
            if_cpdata = 1 if getattr(self.job, "compare_rows", True) else 0
            control = getattr(self.job, "control", None)

            worker.compare(
                self.job.src_key,
                self.job.tgt_key,
                err_handling,
                if_cpdata,
                control=control,
            )
            return worker
        finally:
            src_pool.close_all()
            tgt_pool.close_all()
