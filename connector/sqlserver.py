import pyodbc,re
from .base import BaseConnector, Column
import configparser
col_tup=('col_num','col_name','type','len','num_pre','num_scale','time_sh','null','tab_name')

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

    def list_tables(self,schema=None):#允许自定义schema,默认dbo
        cur = self.conn.cursor()
        base_sql=configparser.ConfigParser().get("sqlserver","table_name")
        if schema:
            str=base_sql+f'''and schema_id=SCHEMA_ID('f{schema}')'''
        else:
            str=base_sql+f'''and schema_id=SCHEMA_ID('dbo')'''
        cur.execute(str)
        return [re.sub('\'','"',r[0]) for r in cur.fetchall()]
#此处可以开进程并发查（表结构-scanrow）和get_obj（约束、索引等元数据），后续考虑解耦，单独一个模块专门查元数据，connector只负责连接和查询数据
    def get_columns(self, table,schema=None):
        cur = self.conn.cursor()
        col_list=[]
        col = {'tab_num': 0, 'tab_name': '', 'tab_col': []}
        base_sql=configparser.ConfigParser().get("sqlserver","col")
        if schema:
            str=base_sql.format(tbname=table,schema=schema)
        else:
            str=base_sql.format(tbname=table,schema="dbo")

        p=cur.execute(str)
        for item in p:
            col_list.append({col_tup[x]:item[x] for x in range(len(item))})
        col['tab_name']=table
        col['tab_col']=col_list
        return col

    def scan_rows(self, table, columns, batch_size=100):
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
    def _pre_getrow(self,table):
        pass
    def close(self):
        self.conn.close()