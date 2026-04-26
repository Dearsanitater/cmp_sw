from __future__ import annotations

import configparser
from pathlib import Path

from connector.hbase_conn import HBaseConnector
from connector.registry import load_config


BASE_DIR = Path(__file__).resolve().parent
DB_INI = BASE_DIR / "resource" / "DB.ini"


def use_db_ini():
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(DB_INI, encoding="utf-8")
    return parser


def list_hbase_sources():
    parser = use_db_ini()
    result = []
    for section in parser.sections():
        if parser.get(section, "type", fallback="").lower() == "hbase":
            result.append(
                {
                    "name": section,
                    "namespace": parser.get(section, "namespace", fallback="dbo"),
                    "ker": parser.get(section, "ker", fallback="0"),
                    "host": parser.get(section, "host", fallback=""),
                }
            )
    return result


class connFactory:
    def __init__(self, dbname):
        self.dbname = dbname
        self.connector = HBaseConnector(load_config(dbname))

    def hb_namespace(self):
        admin = self.connector.conn.getAdmin()
        try:
            return [str(item.getName()) for item in admin.listNamespaceDescriptors()]
        finally:
            admin.close()

    def hb_tab(self, namespace=None):
        return self.connector.list_tables(namespace)

    def browse_one_tb(self, tbname, limit=200, namespace=None):
        table = None
        scanner = None
        rows = []
        columns = ["__rowkey__"]
        try:
            table = self.connector.conn.getTable(
                self.connector.TableName.valueOf(self.connector._qualified_table(tbname, namespace))
            )
            scanner = table.getScanner(self.connector.Scan())
            for result in scanner:
                row_data = {"__rowkey__": str(self.connector.Bytes.toString(result.getRow()))}
                cells = result.listCells()
                if cells is not None:
                    for cell in cells:
                        qualifier = str(
                            self.connector.Bytes.toString(
                                cell.getQualifierArray(),
                                cell.getQualifierOffset(),
                                cell.getQualifierLength(),
                            )
                        )
                        value = str(
                            self.connector.Bytes.toString(
                                cell.getValueArray(),
                                cell.getValueOffset(),
                                cell.getValueLength(),
                            )
                        )
                        row_data[qualifier] = value
                        if qualifier not in columns:
                            columns.append(qualifier)
                rows.append(row_data)
                if limit and len(rows) >= limit:
                    break
            return {
                "tab_num": 1,
                "tables": [
                    {
                        "tab_num": 1,
                        "tab_name": tbname,
                        "tab_col": [
                            {
                                "col_name": column,
                                "col_num": index,
                                "type": "text",
                                "len": None,
                                "null": "NO",
                                "num_pre": 1,
                                "num_scale": 1,
                                "time_sh": None,
                            }
                            for index, column in enumerate(columns, start=1)
                        ],
                    }
                ],
                "columns": columns,
                "rows": [[row.get(column, "") for column in columns] for row in rows],
            }
        finally:
            if scanner is not None:
                scanner.close()
            if table is not None:
                table.close()
