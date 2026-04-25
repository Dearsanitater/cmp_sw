import time

from .config import get_db_config, get_db_type, get_section_for_key
from .sessions import Db2Session, DbSession, ThreadLocalSessionProxy


class ConnectionFactory:
    def __init__(self, config=None):
        self.config = config or get_db_config()

    def create(self, db_key, explicit_type=None):
        section = get_section_for_key(db_key)
        db_type = explicit_type or get_db_type(section)
        params = self._section_dict(section)
        builders = {
            "mysql": self._create_mysql,
            "postgre": self._create_postgre,
            "oracle": self._create_oracle,
            "sqlserver": self._create_sqlserver,
            "clickhouse": self._create_clickhouse,
            "dm": self._create_dm,
            "db2": self._create_db2,
            "db2_as400": self._create_as400,
            "ob_oracle": self._create_ob_oracle,
            "ob_mysql": self._create_mysql,
            "hive": self._create_hive,
        }
        if db_type not in builders:
            raise KeyError(f"Unsupported database type '{db_type}' for '{db_key}'")
        return builders[db_type](section, db_type, params)

    def create_thread_local(self, db_key, explicit_type=None):
        return ThreadLocalSessionProxy(
            session_factory=lambda: self.create(db_key, explicit_type=explicit_type)
        )

    def _section_dict(self, section):
        return {key: self.config.get(section, key) for key in self.config.options(section)}

    def _create_mysql(self, section, db_type, params):
        import pymysql

        start = time.perf_counter()
        connection = pymysql.connect(
            host=params["host"],
            user=params["username"],
            password=params["password"],
            db=params["databasename"],
            port=int(params["port"]),
        )
        self._print_cost(db_type, params["host"], start)
        return DbSession(connection, section, db_type)

    def _create_sqlserver(self, section, db_type, params):
        import pyodbc

        start = time.perf_counter()
        connection = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={params['host']};"
            f"DATABASE={params['databasename']};"
            f"UID={params['username']};"
            f"PWD={params['password']};"
            "TrustServerCertificate=yes;"
        )
        self._print_cost(db_type, params["host"], start)
        return DbSession(connection, section, db_type)

    def _create_oracle(self, section, db_type, params):
        import cx_Oracle

        start = time.perf_counter()
        dsn = f"{params['host']}:{int(params['port'])}/{params['databasename']}"
        connection = cx_Oracle.connect(params["username"], params["password"], dsn)
        with connection.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_LANGUAGE='AMERICAN'")
            cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6'")
            cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF7 TZH:TZM'")

        def pretreatment_oracle(cursor, name, default_type, size, precision, scale):
            if default_type in (cx_Oracle.DATETIME, cx_Oracle.TIMESTAMP):
                return cursor.var(cx_Oracle.STRING, arraysize=cursor.arraysize)
            return None

        connection.outputtypehandler = pretreatment_oracle
        self._print_cost(db_type, params["host"], start)
        return DbSession(connection, section, db_type)

    def _create_postgre(self, section, db_type, params):
        import psycopg2

        start = time.perf_counter()
        schema = params.get("schema") or self.config.get("postgre", "schema", fallback="public")
        connection = psycopg2.connect(
            host=params["host"],
            user=params["username"],
            password=params["password"],
            database=params["databasename"],
            port=int(params["port"]),
            client_encoding="GBK" if section.endswith("gbk") else "utf8",
            options=f"-c search_path={schema}",
        )
        self._print_cost(db_type, params["host"], start)
        return DbSession(connection, section, db_type)

    def _create_clickhouse(self, section, db_type, params):
        import clickhouse_driver

        start = time.perf_counter()
        connection = clickhouse_driver.connect(
            host=params["host"],
            user=params["username"],
            password=params["password"],
            database=params["databasename"],
            port=int(params["port"]),
        )
        self._print_cost(db_type, params["host"], start)
        return DbSession(connection, section, db_type)

    def _create_dm(self, section, db_type, params):
        import dmPython

        start = time.perf_counter()
        connection = dmPython.connect(
            host=params["host"],
            user=params["username"],
            password=params["password"],
            port=int(params["port"]),
            local_code=1,
        )
        self._print_cost(db_type, params["host"], start)
        return DbSession(connection, section, db_type)

    def _create_db2(self, section, db_type, params):
        import ibm_db

        start = time.perf_counter()
        dbinfo = (
            f"DATABASE={params['databasename']};"
            f"HOSTNAME={params['host']};"
            f"PORT={params['port']};"
            "PROTOCOL=TCPIP;"
            f"UID={params['username'].upper()};"
            f"PWD={params['password']}"
        )
        connection = ibm_db.connect(dbinfo, "", "")
        self._print_cost(db_type, params["host"], start)
        return Db2Session(connection, section, db_type)

    def _create_as400(self, section, db_type, params):
        import pyodbc

        start = time.perf_counter()
        connection = pyodbc.connect(
            DRIVER="{IBM I Access ODBC Driver}",
            system=params["host"],
            UID=params["username"],
            PWD=params["password"],
            DBQ=params["databasename"],
            charset="GBK" if section.endswith("gbk") else "utf8",
        )
        self._print_cost(db_type, params["host"], start)
        return DbSession(connection, section, db_type)

    def _create_ob_oracle(self, section, db_type, params):
        import jaydebeapi

        start = time.perf_counter()
        ob_jar = "lib/oceanbase-client-2.4.1.jar"
        jdbc_url = f"jdbc:oceanbase://{params['host']}:{params['port']}/{params['databasename']}"
        connection = jaydebeapi.connect(
            "com.alipay.oceanbase.jdbc.Driver",
            jdbc_url,
            [params["username"], params["password"]],
            ob_jar,
        )
        self._print_cost(db_type, params["host"], start)
        return DbSession(connection, section, db_type)

    def _create_hive(self, section, db_type, params):
        from pyhive import hive

        start = time.perf_counter()
        connection = hive.Connection(
            host=params["host"],
            port=int(params["port"]),
            auth="KERBEROS",
            kerberos_service_name="hive",
            database=params.get("databasename", "default"),
        )
        self._print_cost(db_type, params["host"], start)
        return DbSession(connection, section, db_type)

    @staticmethod
    def _print_cost(db_type, host, start):
        duration = (time.perf_counter() - start) * 1000
        print(f"{db_type} {host} 链接耗时:{duration:.3f}毫秒")
