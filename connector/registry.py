import configparser
from pathlib import Path
#获取DB.ini的路径，确保在任何工作目录下都能正确读取配置文件。
from .base import DbConfig
from .sqlserver import SqlServerConnector
from .mysql import MySqlConnector
from .odbc import OdbcConnector


CONNECTORS = {
    "sqlserver": SqlServerConnector,
    "mysql": MySqlConnector,
    "odbc": OdbcConnector,
}


def load_config(db_key: str, path="resource/DB.ini") -> DbConfig:
    cfg = configparser.ConfigParser()
    cfg.read(path, encoding="utf-8")

    if not cfg.has_section(db_key):
        raise KeyError(f"missing db config section: {db_key}")

    section = cfg[db_key]

    return DbConfig(
        key=db_key,
        type=section.get("type"),
        host=section.get("host"),
        port=section.getint("port", fallback=None),
        database=section.get("databasename", fallback=section.get("database", "")),
        username=section.get("username"),
        password=section.get("password"),
        driver=section.get("driver", fallback=None),
    )

#读DB.ini,根据type值从上面的CONNECTORS字典里找到对应的连接器类，实例化并返回。
def create_connector(db_key: str):
    config = load_config(db_key)
    connector_cls = CONNECTORS.get(config.type)

    if not connector_cls:
        raise KeyError(f"unsupported db type: {config.type}")

    return connector_cls(config)