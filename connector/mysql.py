import pymysql
import re

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

    def get_row(
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

    def _pre_getrow(self, columns_meta, table: str, schema: str | None = None) -> str:
        """
        MySQL 目前不需要像 SQL Server 那样做复杂类型投影，
        这里保留同名入口，方便 compare 后续统一走 connector 取数。
        """
        if not columns_meta:
            return f"SELECT * FROM {self._qualified_table(table, schema)}"
        return f"SELECT * FROM {self._qualified_table(table, schema)}"

    def get_row(
        self,
        table: str,
        columns_meta,
        batch_size: int = 1000,
        schema: str | None = None,
        order_by=None,
    ):
        sql = self._pre_getrow(columns_meta, table, schema)
        if order_by:
            if isinstance(order_by, (list, tuple)):
                order_sql = ", ".join(_quote_identifier(name) for name in order_by)
            else:
                order_sql = _quote_identifier(order_by)
            sql = f"{sql} ORDER BY {order_sql}"

        cur = self.conn.cursor()
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield rows

    def get_obj(self, table: str, schema: str | None = None) -> list[dict]:
        """
        沿用 compare.cons_analysis/mysql 分支的字段结构，
        让 cons_compare 可以直接消费 connector 返回值。
        """
        qualified_table = self._qualified_table(table, schema)
        cur = self.conn.cursor()
        cur.execute(f"SHOW CREATE TABLE {qualified_table}")
        create_table_statement = cur.fetchone()[1]

        lines = create_table_statement.splitlines()
        col_num = len(self.get_columns(table, schema))
        columns_info = []

        for line in lines[1 : col_num + 1]:
            line = line.strip().rstrip(",")
            if not line.startswith("`"):
                continue

            column_name = line.split(" ", 1)[0].strip("`")
            default_match = re.search(r"\bDEFAULT\b\s+(.+?)(?:\s|,|$)", line)
            col_default = default_match.group(1).strip() if default_match else None

            columns_info.append(
                {
                    "col_name": column_name,
                    "pk": None,
                    "unique_name": None,
                    "unique_complex": None,
                    "check_name": None,
                    "check_define": None,
                    "col_default": col_default,
                    "index_name": None,
                    "index_type": None,
                    "index_complex": None,
                }
            )

        for line in lines[col_num:]:
            line = line.strip().rstrip(",")
            if "PRIMARY KEY" in line:
                pk_cols = re.findall(r"\((.*?)\)", line)[0].replace("`", "")
                first_col = pk_cols.split(",")[0]
                pk_value = "YES" if len(pk_cols.split(",")) == 1 else pk_cols
                for row in columns_info:
                    if row["col_name"] == first_col:
                        row["pk"] = pk_value
                        row["index_name"] = "PRIMARY"
                        row["index_type"] = "UNIQUE"
                        break
                continue

            if line.startswith("UNIQUE KEY") or line.startswith("KEY") or line.startswith("INDEX"):
                cols_match = re.findall(r"\(`(.*?)`\)", line)
                if not cols_match:
                    continue
                index_cols = cols_match[0]
                first_col = re.sub("`", "", index_cols.split(",")[0])
                index_complex = None if len(index_cols.split(",")) == 1 else re.sub("`", "", index_cols)
                index_name_match = re.search(r"`(.*?)`", line)
                index_name = index_name_match.group(0) if index_name_match else None
                for row in columns_info:
                    if row["col_name"] == first_col:
                        row["index_name"] = index_name
                        if line.startswith("UNIQUE KEY"):
                            row["unique_name"] = index_name
                            row["unique_complex"] = index_complex
                            row["index_type"] = "UNIQUE"
                        break
                continue

            if line.startswith("CONSTRAINT") and "CHECK" in line:
                check_name = re.findall(r"(?<=CONSTRAINT\s`).*?(?=`\sCHECK)", line)[0].replace(" ", "")
                check_define_text = re.findall(r"\(\(\((.*?)\)\)\)", line)[0]
                check_define = re.sub(r"`|\(|\)|\s", "", check_define_text)
                first_col = re.sub("`", "", re.search(r"`(.*?)`", check_define_text).group(0))
                for row in columns_info:
                    if row["col_name"] == first_col:
                        row["check_name"] = check_name
                        row["check_define"] = check_define
                        break
        return columns_info

    def scan_rows(
        self,
        table: str,
        columns_meta,
        batch_size: int = 1000,
        schema: str | None = None,
        order_by=None,
    ):
        yield from self.get_row(
            table,
            columns_meta,
            batch_size=batch_size,
            schema=schema,
            order_by=order_by,
        )

    def close(self) -> None:
        self.conn.close()
