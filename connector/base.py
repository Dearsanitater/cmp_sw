from abc import ABC, abstractmethod
from dataclasses import dataclass

#先定义config模板，字段类型模板，约束类型模板
@dataclass
class DbConfig:
    key: str
    type: str
    host: str
    port: str
    database: str
    username: str
    password: str
    driver: str | None = None


@dataclass
class Column:
    name: str
    type_name: str
    ordinal: int

#数据库连接分类定义，base为抽象基类
class BaseConnector(ABC):
    def list_tables(self) -> list[str]:
        raise NotImplementedError

    def get_columns(self, table: str) -> list[str]:#查询col元数据，表结构、列类型、列长度等
        raise NotImplementedError

    def scan_rows(self, table: str, columns: list[str], batch_size: int = 1000):#py轮子性能一般，batch看情况使用，全表查询
        raise NotImplementedError
    
    def get_obj(self, table: str, columns: list[str], batch_size: int = 1000):#查询对象、约束等元数据/DDL建表语句，看情况后续处理
        raise NotImplementedError
    
    def close(self):
        raise NotImplementedError