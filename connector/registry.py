from __future__ import annotations

import configparser
from pathlib import Path

from .base import DbConfig
from .hbase_conn import HBaseConnector
from .mysql import MySqlConnector
from .odbc import OdbcConnector
from .sqlserver import SqlServerConnector


CONNECTORS = {
    "sqlserver": SqlServerConnector,
    "mysql": MySqlConnector,
    "odbc": OdbcConnector,
    "hbase": HBaseConnector,
}

DEFAULT_DB_INI = Path(__file__).resolve().parents[1] / "resource" / "DB.ini"


def load_config(db_key: str, path: str | Path = DEFAULT_DB_INI) -> DbConfig:
    cfg = configparser.ConfigParser()
    cfg.read(Path(path), encoding="utf-8")

    if not cfg.has_section(db_key):
        raise KeyError(f"missing db config section: {db_key}")

    section = cfg[db_key]
    return DbConfig(
        key=db_key,
        type=(section.get("type") or "").strip().lower(),
        host=(section.get("host") or "").strip(),
        port=section.getint("port", fallback=None),
        database=(section.get("databasename", fallback=section.get("database", "")) or "").strip(),
        username=section.get("username", fallback=None),
        password=section.get("password", fallback=None),
        schema=section.get("schema", fallback=None),
        driver=section.get("driver", fallback=None),
    )


def create_connector(db_key: str):
    config = load_config(db_key)
    connector_cls = CONNECTORS.get(config.type)
    if connector_cls is None:
        raise KeyError(f"unsupported db type: {config.type}")
    return connector_cls(config)
