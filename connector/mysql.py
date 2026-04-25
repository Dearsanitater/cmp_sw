from __future__ import annotations

import pymysql

from .base import BaseConnector, Column


def _quote_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


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

    def _schema_name(self, schema: str | None = None) -> str:
        return schema or self.config.database

    def _qualified_table(self, table: str, schema: str | None = None) -> str:
        return f"{_quote_identifier(self._schema_name(schema))}.{_quote_identifier(table)}"

    def list_tables(self, schema: str | None = None) -> list[str]:
        table_schema = self._schema_name(schema)
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """,
            (table_schema,),
        )
        return [row[0] for row in cur.fetchall()]

    def get_columns(self, table: str, schema: str | None = None) -> list[Column]:
        table_schema = self._schema_name(schema)
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT
                ORDINAL_POSITION,
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (table_schema, table),
        )

        columns = []
        for row in cur.fetchall():
            columns.append(
                Column(
                    name=row[1],
                    type_name=row[2],
                    ordinal=int(row[0]),
                    nullable=(row[3] == "YES") if row[3] is not None else None,
                    length=row[4],
                    precision=row[5],
                    scale=row[6],
                )
            )
        return columns

    def scan_rows(
        self,
        table: str,
        columns: list[str],
        batch_size: int = 1000,
        schema: str | None = None,
        order_by: str | None = None,
    ):
        quoted_columns = ", ".join(_quote_identifier(name) for name in columns)
        order_column = _quote_identifier(order_by or columns[0])
        sql = (
            f"SELECT {quoted_columns} "
            f"FROM {self._qualified_table(table, schema)} "
            f"ORDER BY {order_column}"
        )

        cur = self.conn.cursor()
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield rows

    def close(self) -> None:
        self.conn.close()
