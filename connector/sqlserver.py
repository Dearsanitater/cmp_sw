import pyodbc
from .base import BaseConnector, Column


class SqlServerConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        driver = config.driver or "ODBC Driver 18 for SQL Server"
        self.conn = pyodbc.connect(
            f"DRIVER={{{driver}}};"
            f"SERVER={config.host};"
            f"DATABASE={config.database};"
            f"UID={config.username};"
            f"PWD={config.password};"
            "TrustServerCertificate=yes;"
        )

    def list_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE='BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        return [r[0] for r in cur.fetchall()]

    def get_columns(self, table):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, table)

        return [
            Column(name=r.COLUMN_NAME, type_name=r.DATA_TYPE, ordinal=r.ORDINAL_POSITION)
            for r in cur.fetchall()
        ]

    def scan_rows(self, table, columns, batch_size=1000):
        col_sql = ", ".join(f"[{c}]" for c in columns)
        order_col = columns[0]
        sql = f"SELECT {col_sql} FROM [{table}] ORDER BY [{order_col}]"

        cur = self.conn.cursor()
        cur.execute(sql)

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield rows

    def get_obj(self, table: str, columns: list[str], batch_size: int = 1000):
        pass

    def close(self):
        self.conn.close()