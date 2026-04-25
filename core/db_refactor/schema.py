from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, local

from .config import get_compare_config, get_db_config, get_section_for_key, get_db_type


@dataclass
class SchemaSnapshot:
    tab_num: int
    tables: list
    table_names: list
    primary_tables: list = field(default_factory=list)
    primary_map: dict = field(default_factory=dict)


class BaseSchemaCollector:
    col_tup = (
        "col_num",
        "col_name",
        "type",
        "len",
        "num_pre",
        "num_scale",
        "time_sh",
        "null",
        "tab_name",
    )

    def __init__(self, compare_config=None, db_config=None):
        self.compare_config = compare_config or get_compare_config()
        self.db_config = db_config or get_db_config()

    def collect(self, session, db_key, worker_count=1, session_factory=None):
        section = get_section_for_key(db_key)
        table_names = [name for name in self.list_tables(session, section) if not self.should_skip_table(name)]
        final = self._collect_tables(
            session=session,
            section=section,
            table_names=table_names,
            worker_count=worker_count,
            session_factory=session_factory,
        )
        return SchemaSnapshot(
            tab_num=len(table_names),
            tables=final,
            table_names=table_names,
            primary_tables=self.list_primary_tables(session, section),
            primary_map=self.list_primary_map(session, section),
        )

    def _collect_tables(self, session, section, table_names, worker_count=1, session_factory=None):
        if worker_count and worker_count > 1 and session_factory and len(table_names) > 1:
            return self._collect_tables_concurrently(section, table_names, worker_count, session_factory)
        return self._collect_tables_serial(session, section, table_names)

    def _collect_tables_serial(self, session, section, table_names):
        final = []
        for index, table_name in enumerate(table_names, start=1):
            rows = self.list_columns(session, section, table_name)
            final.append(self._build_table_snapshot(index, table_name, rows))
        return final

    def _collect_tables_concurrently(self, section, table_names, worker_count, session_factory):
        workers = min(max(int(worker_count), 1), len(table_names))
        worker_state = local()
        created_sessions = []
        created_sessions_lock = Lock()

        def load_table(task):
            index, table_name = task
            worker_session = getattr(worker_state, "session", None)
            if worker_session is None:
                worker_session = session_factory()
                worker_state.session = worker_session
                with created_sessions_lock:
                    created_sessions.append(worker_session)
            rows = self.list_columns(worker_session, section, table_name)
            return self._build_table_snapshot(index, table_name, rows)

        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                return list(executor.map(load_table, enumerate(table_names, start=1)))
        finally:
            for worker_session in created_sessions:
                try:
                    worker_session.close()
                except Exception:
                    pass

    def _build_table_snapshot(self, index, table_name, rows):
        return {
            "tab_num": index,
            "tab_name": table_name,
            "tab_col": [self.row_to_column(item) for item in rows],
        }

    def should_skip_table(self, table_name):
        return str(table_name).startswith("i2_logkeeper")

    def row_to_column(self, row):
        normalized = self.normalize_row(list(row))
        return {self.col_tup[index]: normalized[index] for index in range(min(len(normalized), len(self.col_tup)))}

    def normalize_row(self, row):
        return row

    def list_primary_tables(self, session, section):
        return []

    def list_primary_map(self, session, section):
        return {}

    def list_tables(self, session, section):
        raise NotImplementedError

    def list_columns(self, session, section, table_name):
        raise NotImplementedError

    @staticmethod
    def _first_column(rows):
        return [row[0] for row in rows]


class SqlServerSchemaCollector(BaseSchemaCollector):
    def should_skip_table(self, table_name):
        lower_name = str(table_name).lower()
        return lower_name.startswith("i2_logkeeper") or "systransch" in lower_name

    def list_tables(self, session, section):
        schema = self.db_config.get(section, "schema", fallback="dbo")
        sql = self.compare_config.get("sqlserver", "table_name").format(
            schema=f"and schema_id=SCHEMA_ID('{schema}')"
        )
        session.execute(sql)
        return self._first_column(session.fetchall())

    def list_columns(self, session, section, table_name):
        schema = self.db_config.get(section, "schema", fallback="dbo")
        sql = self.compare_config.get("sqlserver", "col").format(
            tbname=str(table_name).replace("'", "''"),
            schema=schema,
        )
        session.execute(sql)
        return session.fetchall()

    def list_primary_tables(self, session, section):
        sql = self.compare_config.get("pri", "mssql").format(
            dbname=self.db_config.get(section, "databasename")
        )
        session.execute(sql)
        return [item[0] for item in session.fetchall()]


class OracleSchemaCollector(BaseSchemaCollector):
    def list_tables(self, session, section):
        schema = self.db_config.get(section, "schema").upper()
        session.execute(self.compare_config.get("oracle", "table_name").format(schema=schema))
        return self._first_column(session.fetchall())

    def list_columns(self, session, section, table_name):
        schema = self.db_config.get(section, "schema").upper()
        escaped = str(table_name).replace("'", "''")
        sql = self.compare_config.get("oracle", "col").format(schema=schema) + f"'{escaped}' order by COLUMN_ID"
        session.execute(sql)
        return session.fetchall()


class MySqlSchemaCollector(BaseSchemaCollector):
    def list_tables(self, session, section):
        schema = self.db_config.get(section, "databasename")
        session.execute(self.compare_config.get("mysql", "table_name").format(schema=schema))
        return self._first_column(session.fetchall())

    def list_columns(self, session, section, table_name):
        schema = self.db_config.get(section, "databasename")
        sql = self.compare_config.get("mysql", "col").format(
            schema=schema,
            tbname=str(table_name).replace('"', '""'),
        ) + f'"{table_name}" order by ORDINAL_POSITION'
        session.execute(sql)
        return session.fetchall()


class PostgresSchemaCollector(BaseSchemaCollector):
    def list_tables(self, session, section):
        schema = self.db_config.get(section, "schema")
        session.execute(
            f"select TABLE_NAME from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' and table_type ='BASE TABLE'"
        )
        return self._first_column(session.fetchall())

    def list_columns(self, session, section, table_name):
        schema = self.db_config.get(section, "schema")
        escaped = str(table_name).replace("'", "''")
        sql = self.compare_config.get("postgre", "col").format(schema=schema) + f"'{escaped}' order by ORDINAL_POSITION"
        session.execute(sql)
        return session.fetchall()


class ClickHouseSchemaCollector(BaseSchemaCollector):
    def list_tables(self, session, section):
        session.execute(self.compare_config.get("clickhouse", "table_name"))
        return self._first_column(session.fetchall())

    def list_columns(self, session, section, table_name):
        dbname = self.db_config.get(section, "databasename")
        sql = self.compare_config.get("clickhouse", "col").format(
            dbname=dbname,
            tbname=str(table_name).replace("'", "''"),
        )
        session.execute(sql)
        return session.fetchall()

    def normalize_row(self, row):
        if len(row) >= 3 and "Nullable" in str(row[2]):
            row[2] = str(row[2]).removeprefix("Nullable(").removesuffix(")")
            row[-1] = "yes"
        elif row:
            row[-1] = "no"
        return row


class Db2SchemaCollector(BaseSchemaCollector):
    def list_tables(self, session, section):
        schema = self.db_config.get(section, "username").upper()
        session.execute(f"select tabname from syscat.tables where tabschema='{schema}' and type='T'")
        return self._first_column(session.fetchall())

    def list_columns(self, session, section, table_name):
        schema = self.db_config.get(section, "username").upper()
        session.execute(self.compare_config.get("db2", "col").format(schema=schema, tbname=table_name))
        return session.fetchall()


class DmSchemaCollector(BaseSchemaCollector):
    def list_tables(self, session, section):
        schema = self.db_config.get(section, "databasename")
        session.execute(self.compare_config.get("dm", "table_name").format(schema=schema))
        return self._first_column(session.fetchall())

    def list_columns(self, session, section, table_name):
        schema = self.db_config.get(section, "databasename")
        session.execute(self.compare_config.get("dm", "col").format(schema=schema, tbname=table_name))
        return session.fetchall()

    def normalize_row(self, row):
        if row and row[-1] == "N":
            row[-1] = "NO"
        elif row and row[-1] == "Y":
            row[-1] = "YES"
        return row


class HiveSchemaCollector(BaseSchemaCollector):
    def list_tables(self, session, section):
        dbname = self.db_config.get(section, "databasename")
        session.execute(self.compare_config.get("hive", "table_name").format(dbname=dbname))
        return self._first_column(session.fetchall())

    def list_columns(self, session, section, table_name):
        dbname = self.db_config.get(section, "databasename")
        session.execute(self.compare_config.get("hive", "col").format(dbname=dbname, tbname=table_name))
        return session.fetchall()

    def list_primary_map(self, session, section):
        dbname = self.db_config.get(section, "databasename")
        session.execute(self.compare_config.get("hive", "pri").format(dbname=dbname))
        primary_map = {}
        for table_name, column_name in session.fetchall():
            primary_map.setdefault(table_name, []).append(column_name)
        return primary_map

    def normalize_row(self, row):
        if len(row) >= 2:
            row[-2] = "YES" if row[-2] == "TRUE" else "No"
        return row


class DialectRegistry:
    def __init__(self, compare_config=None, db_config=None):
        self.compare_config = compare_config or get_compare_config()
        self.db_config = db_config or get_db_config()
        self._registry = {
            "sqlserver": SqlServerSchemaCollector(self.compare_config, self.db_config),
            "oracle": OracleSchemaCollector(self.compare_config, self.db_config),
            "ob_oracle": OracleSchemaCollector(self.compare_config, self.db_config),
            "mysql": MySqlSchemaCollector(self.compare_config, self.db_config),
            "ob_mysql": MySqlSchemaCollector(self.compare_config, self.db_config),
            "postgre": PostgresSchemaCollector(self.compare_config, self.db_config),
            "clickhouse": ClickHouseSchemaCollector(self.compare_config, self.db_config),
            "db2": Db2SchemaCollector(self.compare_config, self.db_config),
            "dm": DmSchemaCollector(self.compare_config, self.db_config),
            "hive": HiveSchemaCollector(self.compare_config, self.db_config),
        }

    def resolve(self, db_type_or_key):
        db_type = get_db_type(db_type_or_key)
        if db_type not in self._registry:
            raise KeyError(f"No schema collector registered for '{db_type_or_key}'")
        return self._registry[db_type]
