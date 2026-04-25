import configparser
import re,time
from pathlib import Path

import pyodbc

from .base import BaseConnector


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
        start=time.monotonic()
        self.conn = pyodbc.connect(
            f"DRIVER={{{driver}}};"
            f"SERVER={config.host};"
            f"DATABASE={config.database};"
            f"UID={config.username};"
            f"PWD={config.password};"
            "TrustServerCertificate=yes;"
        )
        print(f'mssql conn({config.host}): {time.monotonic() - start:.2f}秒')
        

    def _qualified_table(self, table, schema=None):#标准化表名
        if schema:
            return f"[{schema}].[{table}]"
        return f"[{table}]"

    def _qualified_table_fixed(self, table, schema=None):
        # 统一补 schema，避免 compare 再自己拼 SQL Server 表名。
        if schema:
            return f"[{schema}].[{table}]"
        return f"[{table}]"

    def list_tables(self, schema=None):
        cur = self.conn.cursor()
        base_sql = COMPARE_SQL.get("sqlserver", "table_name")
        schema_name = schema or "dbo"
        sql = base_sql.format(schema="") + f" and schema_id=SCHEMA_ID('{schema_name}')"
        cur.execute(sql)
        return [re.sub("'", '"', row[0]) for row in cur.fetchall()]

    def get_columns(self, table, schema=None):
        start=time.monotonic()
        cur = self.conn.cursor()
        schema_name = schema or "dbo"
        sql = COMPARE_SQL.get("sqlserver", "col").format(tbname=table, schema=schema_name)
        col_list = []
        col = {"tab_num": 0, "tab_name": table, "tab_col": []}
        for item in cur.execute(sql):
            col_list.append({COL_TUP[index]: item[index] for index in range(len(item))})
        col["tab_col"] = col_list
        print(f'{self.config.host} {table}列信息获取耗时: {time.monotonic() - start:.2f}秒')
        return col

    def get_obj(self, table, schema=None, col_num=None):
        """
        返回和 compare.cons_analysis/sqlserver 分支一致的列级约束结构，
        这样 cons_compare 可以直接消费，不需要 runner 再做字段改写。
        """

        schema_name = schema or "dbo"
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
        """
        按 compare.mss_spc 的老规则生成 SQL Server 行扫描 SQL。
        这里保留类型转换逻辑，后续 compare 可以直接下沉到 connector。
        """
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

        table_sql = self._qualified_table_fixed(table, schema)
        if len(fields) == 1 or not fields or converted_count == 0:
            return f"select * from {table_sql}"
        return f"select {','.join(fields)} from {table_sql}"

    def get_row(self, table, columns_meta, batch_size=1000, schema=None, order_by=None):
        """
        兼容老 compare 的“拿整表数据”语义，但把取数入口收口到 connector。
        返回批次迭代器，方便后面切并发或异步时不改 compare 外层边界。
        """
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

    def close(self):
        self.conn.close()
