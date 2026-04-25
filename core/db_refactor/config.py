import configparser
from functools import lru_cache
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "resource" / "DB.ini"
COMPARE_PATH = ROOT_DIR / "resource" / "compare.ini"

TYPE_ALIASES = {
    "mysql": "mysql",
    "tdsql": "mysql",
    "tidb": "mysql",
    "postgre": "postgre",
    "postgres": "postgre",
    "postgresql": "postgre",
    "lightdb": "postgre",
    "dws": "postgre",
    "tdsql_pg": "postgre",
    "opengauss": "postgre",
    "opengauss_gbk": "postgre",
    "gaussdb": "postgre",
    "oracle": "oracle",
    "sqlserver": "sqlserver",
    "ssms": "sqlserver",
    "mssql": "sqlserver",
    "clickhouse": "clickhouse",
    "dm": "dm",
    "db2": "db2",
    "db2_as400": "db2_as400",
    "as400": "db2_as400",
    "oboracle": "ob_oracle",
    "ob_oracle": "ob_oracle",
    "obmysql": "ob_mysql",
    "ob_mysql": "ob_mysql",
    "elastic": "elastic",
    "elasticsearch": "elastic",
    "es": "elastic",
    "hive": "hive",
    "argo": "hive",
    "argodb": "hive",
    "hdfs": "hdfs",
    "hadoop": "hdfs",
    "hbase": "hbase",
}

PREFERRED_SECTIONS = {
    "mysql": "mysql",
    "postgre": "postgre",
    "oracle": "oracle",
    "sqlserver": "sqlserver",
    "clickhouse": "clickhouse",
    "dm": "dm",
    "db2": "db2",
    "db2_as400": "db2_as400",
    "ob_oracle": "ob_oracle",
    "ob_mysql": "ob_mysql",
    "hive": "hive",
    "elastic": "elastic",
    "hdfs": "hdfs",
}


def normalize_db_type(db_type):
    if db_type is None:
        return ""
    return TYPE_ALIASES.get(str(db_type).strip().lower(), str(db_type).strip().lower())


@lru_cache(maxsize=1)
def get_db_config():
    parser = configparser.RawConfigParser()
    parser.read(CONFIG_PATH, encoding="utf-8")
    return parser


@lru_cache(maxsize=1)
def get_compare_config():
    parser = configparser.RawConfigParser()
    parser.read(COMPARE_PATH, encoding="utf-8")
    return parser


def get_db_type(db_key):
    config = get_db_config()
    if config.has_section(db_key) and config.has_option(db_key, "type"):
        return normalize_db_type(config.get(db_key, "type"))
    return normalize_db_type(db_key)


def get_section_for_key(db_key):
    config = get_db_config()
    if config.has_section(db_key):
        return db_key
    normalized = normalize_db_type(db_key)
    preferred = PREFERRED_SECTIONS.get(normalized, normalized)
    if config.has_section(preferred):
        return preferred
    if config.has_section(normalized):
        return normalized
    raise KeyError(f"Cannot resolve config section for database key '{db_key}'")
