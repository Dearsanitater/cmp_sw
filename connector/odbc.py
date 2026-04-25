import pyodbc
from .base import BaseConnector, Column


class OdbcConnector(BaseConnector):
    """
    兜底连接器。
    适合 AS400 / DB2 / 某些国产库。
    前提是项目运行的win/linux系统或 Docker 镜像里已经装好 ODBC driver。
    """

    def __init__(self, config):
        super().__init__(config)

        if not config.driver:
            raise ValueError("ODBC connector requires config.driver")

        self.conn = pyodbc.connect(
            f"DRIVER={{{config.driver}}};"
            f"SYSTEM={config.host};"
            f"UID={config.username};"
            f"PWD={config.password};"
            f"DATABASE={config.database};"
        )

    def list_tables(self, schema: str | None = None):
        cur = self.conn.cursor()
        return [
            row.table_name
            for row in cur.tables(tableType="TABLE")
        ]

    def get_columns(self, table, schema: str | None = None):
        cur = self.conn.cursor()
        return [
            Column(
                name=row.column_name,
                type_name=row.type_name,
                ordinal=row.ordinal_position,
            )
            for row in cur.columns(table=table)
        ]

    def get_obj(self, table, schema: str | None = None, col_num: int | None = None) -> list[dict]:
        raise NotImplementedError("ODBC connector does not implement get_obj yet")

    def get_row(self, table, columns, batch_size=1000, schema: str | None = None, order_by=None):
        yield from self.scan_rows(table, columns, batch_size=batch_size, schema=schema, order_by=order_by)

    def scan_rows(self, table, columns, batch_size=1000, schema: str | None = None, order_by=None):
        col_sql = ", ".join(columns)
        sql = f"SELECT {col_sql} FROM {table}"
        if order_by:
            if isinstance(order_by, (list, tuple)):
                sql += " ORDER BY " + ", ".join(order_by)
            else:
                sql += f" ORDER BY {order_by}"

        cur = self.conn.cursor()
        cur.execute(sql)

        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield rows

    def close(self):
        self.conn.close()
