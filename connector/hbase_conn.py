from __future__ import annotations

import configparser
import os
import threading
import time
from pathlib import Path
from typing import Iterable

from .base import BaseConnector


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_INI = ROOT_DIR / "resource" / "DB.ini"
LEGACY_ROOT_DIR = Path(r"D:\PythonObj\pythonProject")
DEFAULT_JVM_PATH = r"C:\Program Files\Java\jre1.8.0_201\bin\server\jvm.dll"

_JVM_LOCK = threading.Lock()
_CONN_LOCK = threading.Lock()
_CONNECTIONS = {}


def _load_section(db_key: str):
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(DB_INI, encoding="utf-8")
    if not parser.has_section(db_key):
        raise KeyError(f"missing hbase db config section: {db_key}")
    return parser[db_key]


def _hbase_jars(section) -> list[str]:#新项目暂时没lib，回去找老项目的lib
    hbase_dir = ROOT_DIR / "lib" / "hbase"
    if not hbase_dir.exists():
        hbase_dir = LEGACY_ROOT_DIR / "lib" / "hbase"
    jars = [str(path) for path in hbase_dir.glob("*.jar")]
    if not jars:
        raise FileNotFoundError(f"no hbase jar found under {hbase_dir}")
    if section.get("ker", fallback="0") == "1":
        hyperlib_dir = ROOT_DIR / "lib" / "hyperlib"
        if not hyperlib_dir.exists():
            hyperlib_dir = LEGACY_ROOT_DIR / "lib" / "hyperlib"
        jars.append(str(hyperlib_dir))
    return jars

def _ensure_jvm(section) -> None:
    import jpype

    with _JVM_LOCK:#这个是进程级别的jvm，后期考虑怎么弄成全局
        if jpype.isJVMStarted():
            print("jvm exists, reuse")
            return

        jars = _hbase_jars(section)
        jvm_path = section.get("jvm_path", fallback=DEFAULT_JVM_PATH)
        if section.get("ker", fallback="0") == "1":
            print("no kerberos jvm exists, starting...")
            jpype.startJVM(
                f"-Djava.security.krb5.conf={ROOT_DIR / 'lib' / 'krb5.ini'}",
                f"-Djava.security.auth.login.config={ROOT_DIR / 'lib' / 'hyperlib' / 'jaas.conf'}",
                "-Dsun.security.krb5.debug=true",
                "-Dfile.encoding=UTF-8",
                "-Djavax.security.auth.useSubjectCredsOnly=false",
                classpath=jars,
                jvmpath=jvm_path,
            )
            return

        print("no jvm exists, starting...")
        jpype.startJVM(
            "-Dfile.encoding=UTF-8",
            classpath=jars,
            jvmpath=jvm_path,
        )


def _create_hbase_connection(db_key: str, section):
    import jpype
    from jpype import JClass, JProxy

    _ensure_jvm(section)

    zk_host = section.get("zk_host", fallback=section.get("host", fallback=""))
    zk_port = section.get("zk_port", fallback="2181")#拿参数配置jvm套件的zk地址和端口，兼容之前版本的host配置
    parent = section.get("znode_parent", fallback="/hbase")

    HBaseConfiguration = JClass("org.apache.hadoop.hbase.HBaseConfiguration")
    ConnectionFactory = JClass("org.apache.hadoop.hbase.client.ConnectionFactory")
    hbase_conf = HBaseConfiguration.create()
    hbase_conf.set("hbase.zookeeper.quorum", zk_host)
    hbase_conf.set("hbase.zookeeper.property.clientPort", zk_port)
    hbase_conf.set("zookeeper.znode.parent", parent)

    if section.get("ker", fallback="0") == "1":
        UserGroupInformation = JClass("org.apache.hadoop.security.UserGroupInformation")
        Configuration = JClass("org.apache.hadoop.conf.Configuration")
        PrivilegedExceptionAction = JClass("java.security.PrivilegedExceptionAction")

        conf = Configuration()
        conf.set("hadoop.security.authentication", "kerberos")
        UserGroupInformation.setConfiguration(conf)
        UserGroupInformation.loginUserFromKeytab(
            section.get("principle"),
            section.get("kpath"),
        )
        ugi = UserGroupInformation.getLoginUser()

        def run():
            return ConnectionFactory.createConnection(hbase_conf)

        action = JProxy(PrivilegedExceptionAction, dict(run=run))
        return ugi.doAs(action)

    return ConnectionFactory.createConnection(hbase_conf)


def _shared_hbase_connection(db_key: str, section):
    with _CONN_LOCK:
        conn = _CONNECTIONS.get(db_key)
        if conn is not None:
            return conn
        start = time.perf_counter()
        conn = _create_hbase_connection(db_key, section)
        _CONNECTIONS[db_key] = conn
        print(f"{db_key} hbase connection cost: {(time.perf_counter() - start) * 1000:.2f}ms")
        return conn


class HBaseConnector(BaseConnector):
    """
    HBase only provides table names and row content for compare.
    Column metadata and object/constraint metadata are copied from the source side.
    """

    def __init__(self, config):
        super().__init__(config)
        self._section = _load_section(config.key)
        self.namespace = (
            self._section.get("namespace", fallback=None)
            or config.schema
            or config.database
            or "default"
        )
        self.conn = _shared_hbase_connection(config.key, self._section)

        from jpype import JClass

        self.TableName = JClass("org.apache.hadoop.hbase.TableName")
        self.Scan = JClass("org.apache.hadoop.hbase.client.Scan")
        self.Bytes = JClass("org.apache.hadoop.hbase.util.Bytes")

    def _namespace(self, schema=None) -> str:
        return schema or self.namespace

    def _qualified_table(self, table: str, schema=None) -> str:
        return f"{self._namespace(schema)}:{table}"

    def list_tables(self, schema: str | None = None) -> list[str]:
        admin = self.conn.getAdmin()
        try:
            tables = admin.listTableNamesByNamespace(self._namespace(schema))
            return [str(table.getQualifierAsString()) for table in tables]
        finally:
            admin.close()

    def get_columns(self, table: str, schema: str | None = None):
        return []

    def get_obj(self, table: str, schema: str | None = None, col_num: int | None = None):
        return []

    def get_row(
        self,
        table: str,
        columns,
        batch_size: int = 1000,
        schema: str | None = None,
        order_by=None,
    ) -> Iterable[list[tuple]]:
        column_names = [column["col_name"] for column in columns]
        defaults = {name: "" for name in column_names}
        hbase_table = None
        scanner = None
        batch = []
        try:
            hbase_table = self.conn.getTable(self.TableName.valueOf(self._qualified_table(table, schema)))
            scanner = hbase_table.getScanner(self.Scan())
            for result in scanner:
                row_data = defaults.copy()
                if "__rowkey__" in row_data:
                    row_data["__rowkey__"] = str(self.Bytes.toString(result.getRow()))
                cells = result.listCells()
                if cells is not None:
                    for cell in cells:
                        qualifier = str(
                            self.Bytes.toString(
                                cell.getQualifierArray(),
                                cell.getQualifierOffset(),
                                cell.getQualifierLength(),
                            )
                        )
                        if qualifier in row_data:
                            row_data[qualifier] = str(
                                self.Bytes.toString(
                                    cell.getValueArray(),
                                    cell.getValueOffset(),
                                    cell.getValueLength(),
                                )
                            )
                batch.append(tuple(row_data[name] for name in column_names))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
        finally:
            if scanner is not None:
                scanner.close()
            if hbase_table is not None:
                hbase_table.close()

    def close(self) -> None:
        if self.conn is None:
            return
        with _CONN_LOCK:
            cached = _CONNECTIONS.get(self.config.key)
            if cached is self.conn:
                _CONNECTIONS.pop(self.config.key, None)
        try:
            self.conn.close()
        finally:
            self.conn = None
