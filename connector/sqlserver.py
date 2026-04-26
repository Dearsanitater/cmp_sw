import configparser
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock, local

import pyodbc

from .base import BaseConnector, SchemaSnapshot


COMPARE_INI = Path(__file__).resolve().parents[1] / "resource" / "compare.ini"
COMPARE_SQL = configparser.ConfigParser()
COMPARE_SQL.read(COMPARE_INI, encoding="utf-8")

COL_TUP = ("col_num", "col_name", "type", "len", "num_pre", "num_scale", "time_sh", "null", "tab_name")


def _empty_cons_row(column_name: str) -> dict:
    return {
        "col_name": column_name,
        "pk": None,
        "unique_name": None,
        "unique_complex": None,
        "check_name": "",
        "check_define": "",
        "col_default": None,
        "index_name": None,
        "index_type": None,
        "index_complex": None,
    }


class SqlServerConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        self.type_cache = {}
        self._column_count_cache = {}
        driver = config.driver or "ODBC Driver 18 for SQL Server"
        self._connect_args = (
            f"DRIVER={{{driver}}};"
            f"SERVER={config.host};"
            f"DATABASE={config.database};"
            f"UID={config.username};"
            f"PWD={config.password};"
            "TrustServerCertificate=yes;"
        )
        start = time.monotonic()
        self.conn = self._open_connection()
        print(f"mssql conn({config.host}): {time.monotonic() - start:.2f}秒")

    def _open_connection(self):
        return pyodbc.connect(self._connect_args)

    def _schema_name(self, schema=None):
        return schema or self.config.schema or "dbo"

    def _qualified_table(self, table, schema=None):
        schema_name = self._schema_name(schema)
        return f"[{schema_name}].[{table}]"

    def _list_tables_with_conn(self, conn, schema=None):
        cur = conn.cursor()
        base_sql = COMPARE_SQL.get("sqlserver", "table_name")
        schema_name = self._schema_name(schema)
        sql = base_sql.format(schema="") + f" and schema_id=SCHEMA_ID('{schema_name}')"
        cur.execute(sql)
        return [re.sub("'", '"', row[0]) for row in cur.fetchall()]

    def _get_columns_with_conn(self, conn, table, schema=None):
        start = time.monotonic()
        cur = conn.cursor()
        schema_name = self._schema_name(schema)
        sql = COMPARE_SQL.get("sqlserver", "col").format(tbname=table, schema=schema_name)
        col_list = []
        col = {"tab_num": 0, "tab_name": table, "tab_col": []}
        for item in cur.execute(sql):
            col_list.append({COL_TUP[index]: item[index] for index in range(len(item))})
        col["tab_col"] = col_list
        print(f"{self.config.host} {table}列信息获取耗时: {time.monotonic() - start:.2f}秒")
        return col

    def list_tables(self, schema=None):
        return self._list_tables_with_conn(self.conn, schema)

    def get_columns(self, table, schema=None):
        return self._get_columns_with_conn(self.conn, table, schema)

    def list_primary_tables(self, schema=None) -> list[str]:
        cur = self.conn.cursor()
        sql = COMPARE_SQL.get("pri", "mssql").format(dbname=self.config.database)
        cur.execute(sql)
        return [item[0] for item in cur.fetchall()]

    def collect_schema_snapshot(self, schema=None, worker_count: int = 1) -> SchemaSnapshot:
        schema_name = self._schema_name(schema)
        table_names = [
            name
            for name in self._list_tables_with_conn(self.conn, schema_name)
            if not str(name).lower().startswith("i2_logkeeper") and "systransch" not in str(name).lower()
        ]

        if worker_count <= 1 or len(table_names) <= 1:
            tables = []
            for index, table_name in enumerate(table_names, start=1):
                columns = self._get_columns_with_conn(self.conn, table_name, schema_name)
                tables.append(self._normalize_table_snapshot(index, table_name, columns))
            return SchemaSnapshot(
                tab_num=len(table_names),
                tables=tables,
                table_names=table_names,
                primary_tables=self.list_primary_tables(schema_name),
                primary_map={},
            )

        worker_state = local()
        created_connections = []
        created_connections_lock = Lock()

        def load_table(task):
            index, table_name = task
            worker_conn = getattr(worker_state, "conn", None)
            if worker_conn is None:
                worker_conn = self._open_connection()
                worker_state.conn = worker_conn
                with created_connections_lock:
                    created_connections.append(worker_conn)
            columns = self._get_columns_with_conn(worker_conn, table_name, schema_name)
            return self._normalize_table_snapshot(index, table_name, columns)

        try:
            workers = min(max(int(worker_count), 1), len(table_names))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                tables = list(executor.map(load_table, enumerate(table_names, start=1)))
        finally:
            for worker_conn in created_connections:
                try:
                    worker_conn.close()
                except Exception:
                    pass

        return SchemaSnapshot(
            tab_num=len(table_names),
            tables=tables,
            table_names=table_names,
            primary_tables=self.list_primary_tables(schema_name),
            primary_map={},
        )

    def get_obj(self, table, schema=None, col_num=None):
        schema_name = self._schema_name(schema)
        table_name = f"{schema_name}.{table}".replace("'", "''")
        if col_num is None:
            cache_key = (schema_name, table)
            col_num = self._column_count_cache.get(cache_key)
            if col_num is None:
                col_num = len(self.get_columns(table, schema_name)["tab_col"])
                self._column_count_cache[cache_key] = col_num

        cursor = self.conn.cursor()
        cursor.execute(f"EXEC sp_help '{table_name}'")

        raw_rows = []
        while True:
            if cursor.description:
                rows = cursor.fetchall()
                if rows and rows[0] is not None and len(rows[0]) not in (1, 4):
                    raw_rows.extend(rows)
            if not cursor.nextset():
                break

        columns_info = [_empty_cons_row(item[0]) for item in raw_rows[:col_num]]
        for item in raw_rows[col_num - 1 :]:
            if len(item) == 3:
                first_col = item[2]
                index_name = item[0]
                index_type = re.findall(r"(.*?)\slocated\son", item[1])[0]
                if len(first_col.split(",")) == 1:
                    index_complex = None
                else:
                    index_complex = first_col
                    first_col = first_col.split(",")[0]
                for row in columns_info:
                    if row["col_name"] == first_col:
                        row["index_name"] = index_name
                        row["index_type"] = index_type
                        row["index_complex"] = index_complex
                        break

            elif len(item) > 4:
                if "CHECK" in item[0]:
                    if "Table Level" not in item[0]:
                        col_name = re.search(r"(?<=on\scolumn\s)(.*?)$", item[0]).group(0)
                        check_name = item[1]
                    else:
                        col_name = columns_info[0]["col_name"]
                        check_name = "LvTB" + item[1]
                    for row in columns_info:
                        if row["col_name"] == col_name:
                            row["check_name"] += check_name
                            row["check_define"] += item[-1]
                            break

                if "DEFAULT" in item[0]:
                    col_name = re.search(r"(?<=on\scolumn\s)(.*?)$", item[0]).group(0)
                    default_value = re.findall(r"\((.*?)\)", item[-1])[0].replace("(", "").replace(")", "")
                    for row in columns_info:
                        if row["col_name"] == col_name:
                            row["col_default"] = default_value
                            break

                if "UNIQUE" in item[0]:
                    first_col = item[-1]
                    if len(first_col.split(",")) == 1:
                        unique_complex = None
                    else:
                        unique_complex = first_col
                        first_col = first_col.split(",")[0]
                    for row in columns_info:
                        if row["col_name"] == first_col:
                            row["unique_name"] = item[1]
                            row["unique_complex"] = unique_complex
                            break

                if "PRIMARY" in item[0]:
                    first_col = item[-1]
                    if len(first_col.split(",")) == 1:
                        pk = "YES"
                    else:
                        pk = first_col
                        first_col = first_col.split(",")[0]
                    for row in columns_info:
                        if row["col_name"] == first_col:
                            row["pk"] = pk
                            break

        return columns_info

    def _pre_getrow(self, columns_meta, table, schema=None):
        fields = []
        converted_count = 0

        for col in columns_meta:
            col_name = col["col_name"]
            col_type = (col.get("type") or "").lower()

            if col_type in self.type_cache:
                if "binary" in col_name:
                    expr = "convert(varchar(max),{col_name},1)"
                else:
                    expr = self.type_cache[col_type]
                fields.append(expr.format(col_name=col_name))
                converted_count += 1
            elif col_type.startswith("varchar"):
                self.type_cache[col_type] = 'CONVERT(nvarchar(max),"{col_name}")'
                fields.append(self.type_cache[col_type].format(col_name=col_name))
                converted_count += 1
            elif col_type.startswith("char") or col_type.startswith("float") or col_type.startswith("real"):
                self.type_cache[col_type] = 'CONVERT(nvarchar(max),"{col_name}")'
                fields.append(self.type_cache[col_type].format(col_name=col_name))
                converted_count += 1
            elif col_type.startswith("text"):
                self.type_cache[col_type] = 'CONVERT(ntext,"{col_name}")'
                fields.append(self.type_cache[col_type].format(col_name=col_name))
                converted_count += 1
            elif col_type.startswith("geo"):
                self.type_cache[col_type] = '"{col_name}".STAsText()'
                fields.append(self.type_cache[col_type].format(col_name=col_name))
                converted_count += 1
            elif col_type.startswith("bigint"):
                self.type_cache[col_type] = "convert(decimal(38,0),{col_name})"
                fields.append(self.type_cache[col_type].format(col_name=col_name))
                converted_count += 1
            elif col_type.startswith("sql_variant") or col_type.startswith("hierarchyid"):
                if "binary" in col_name:
                    expr = "convert(varchar(max),{col_name},1)"
                    fields.append(expr.format(col_name=col_name))
                else:
                    self.type_cache[col_type] = 'cast("{col_name}" as nvarchar(max))'
                    fields.append(self.type_cache[col_type].format(col_name=col_name))
                converted_count += 1
            elif col_type in ("datetime2", "datetimeoffset", "time"):
                self.type_cache[col_type] = 'cast("{col_name}" as datetime)'
                fields.append(self.type_cache[col_type].format(col_name=col_name))
                converted_count += 1
            elif "offset" in col_type:
                self.type_cache[col_type] = 'CONVERT(varchar(800),"{col_name}")'
                fields.append(self.type_cache[col_type].format(col_name=col_name))
                converted_count += 1
            elif col_name.startswith("**"):
                continue
            elif col_type.startswith("timestamp"):
                self.type_cache[col_type] = "'timestamp不参与比对'"
                fields.append(self.type_cache[col_type])
                converted_count += 1
            else:
                fields.append(f'"{col_name}"')

        table_sql = self._qualified_table(table, schema)
        if len(fields) == 1 or not fields or converted_count == 0:
            return f"select * from {table_sql}"
        return f"select {','.join(fields)} from {table_sql}"

    def get_row(self, table, columns_meta, batch_size=1000, schema=None, order_by=None):
        sql = self._pre_getrow(columns_meta, table, schema)
        if order_by:
            if isinstance(order_by, (list, tuple)):
                order_sql = ", ".join(f'"{col}"' for col in order_by)
            else:
                order_sql = f'"{order_by}"'
            sql = f"{sql} order by {order_sql}"

        cur = self.conn.cursor()
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            yield rows
#yield流式获取get_row结果
    def scan_rows(self, table, columns_meta, batch_size=1000, schema=None, order_by=None):
        yield from self.get_row(
            table,
            columns_meta,
            batch_size=batch_size,
            schema=schema,
            order_by=order_by,
        )

    def close(self):
        self.conn.close()
