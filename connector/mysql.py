import pymysql
from .base import BaseConnector, Column


class MySqlConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        self.conn = pymysql.connect(
            host=config.host,
            port=config.port or 3306,
            user=config.username,
            password=config.password,
            database=config.database,
            charset="utf8mb4",
        )

    def list_tables(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_TYPE='BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        return [r[0] for r in cur.fetchall()]

    def get_columns(self, table):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (table,))

        return [
            Column(name=r[1], type_name=r[2], ordinal=r[0])
            for r in cur.fetchall()
        ]

    def scan_rows(self, table, columns, batch_size=1000):
        col_sql = ", ".join(f"`{c}`" for c in columns)
        order_col = columns[0]
        sql = f"SELECT {col_sql} FROM `{table}` ORDER BY `{order_col}`"

        cur = self.conn.cursor()
        cur.execute(sql)

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield rows

    def close(self):
        self.conn.close()