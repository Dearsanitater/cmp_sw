from concurrent.futures import ThreadPoolExecutor, as_completed

from connector.registry import create_connector
from .pool import ConnectionPool


class CompareJobRunner:
    def __init__(self, job):
        self.job = job
        self.src_pool = ConnectionPool(lambda: create_connector(job.src_key), size=job.workers)
        self.tgt_pool = ConnectionPool(lambda: create_connector(job.tgt_key), size=job.workers)

    def run(self):
        try:
            src = self.src_pool.acquire()
            tgt = self.tgt_pool.acquire()

            try:
                src_tables = set(src.list_tables())
                tgt_tables = set(tgt.list_tables())
            finally:
                self.src_pool.release(src)
                self.tgt_pool.release(tgt)

            common_tables = sorted(src_tables & tgt_tables)
            only_src = sorted(src_tables - tgt_tables)
            only_tgt = sorted(tgt_tables - src_tables)

            print(f"源端独有表: {only_src}")
            print(f"目标端独有表: {only_tgt}")
            print(f"共同表数量: {len(common_tables)}")

            with ThreadPoolExecutor(max_workers=self.job.workers) as executor:
                futures = {
                    executor.submit(self.compare_one_table, table): table
                    for table in common_tables
                }

                for future in as_completed(futures):
                    table = futures[future]
                    try:
                        result = future.result()
                        print(f"[{table}] {result}")
                    except Exception as e:
                        print(f"[{table}] FAILED: {e}")

        finally:
            self.src_pool.close_all()
            self.tgt_pool.close_all()

    def compare_one_table(self, table):
        src = self.src_pool.acquire()
        tgt = self.tgt_pool.acquire()

        try:
            src_cols = src.get_columns(table)
            tgt_cols = tgt.get_columns(table)

            src_col_names = [c.name for c in src_cols]
            tgt_col_names = [c.name for c in tgt_cols]

            common_cols = [c for c in src_col_names if c in tgt_col_names]

            if not common_cols:
                return "NO COMMON COLUMNS"

            src_count = self.count_rows(src, table, common_cols)
            tgt_count = self.count_rows(tgt, table, common_cols)

            if src_count != tgt_count:
                return f"ROW COUNT DIFF src={src_count}, tgt={tgt_count}"

            src_hashes = self.hash_rows(src, table, common_cols)
            tgt_hashes = self.hash_rows(tgt, table, common_cols)

            if src_hashes == tgt_hashes:
                return "OK"

            return "DATA DIFF"

        finally:
            self.src_pool.release(src)
            self.tgt_pool.release(tgt)

    def count_rows(self, connector, table, columns):
        count = 0
        for batch in connector.scan_rows(table, columns, self.job.batch_size):
            count += len(batch)
        return count

    def hash_rows(self, connector, table, columns):
        result = []

        for batch in connector.scan_rows(table, columns, self.job.batch_size):
            for row in batch:
                normalized = tuple("" if v is None else str(v).strip() for v in row)
                result.append(hash(normalized))

        return sorted(result)