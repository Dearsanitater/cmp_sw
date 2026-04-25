from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterable


@dataclass(slots=True)
class DbConfig:
    key: str
    type: str
    host: str
    port: int | None
    database: str
    username: str | None
    password: str | None
    schema: str | None = None
    driver: str | None = None


@dataclass(slots=True)
class Column:
    name: str
    type_name: str
    ordinal: int
    nullable: bool | None = None
    length: int | None = None
    precision: int | None = None
    scale: int | None = None


@dataclass(slots=True)
class SchemaSnapshot:
    tab_num: int
    tables: list
    table_names: list
    primary_tables: list | None = None
    primary_map: dict | None = None


class BaseConnector(ABC):
    def __init__(self, config: DbConfig):
        self.config = config

    @abstractmethod
    def list_tables(self, schema: str | None = None) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_columns(self, table: str, schema: str | None = None) -> list[Column]:
        raise NotImplementedError

    @abstractmethod
    def get_obj(
        self,
        table: str,
        schema: str | None = None,
        col_num: int | None = None,
    ) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def get_row(
        self,
        table: str,
        columns,
        batch_size: int = 1000,
        schema: str | None = None,
        order_by = None,
    ) -> Iterable[list[tuple]]:
        raise NotImplementedError

    def scan_rows(
        self,
        table: str,
        columns,
        batch_size: int = 1000,
        schema: str | None = None,
        order_by = None,
        ) -> Iterable[list[tuple]]:
        # 统一保留 scan_rows 别名，便于把老 compare/db_refactor 的入口慢慢收回 connector。
        return self.get_row(
            table,
            columns,
            batch_size=batch_size,
            schema=schema,
            order_by=order_by,
        )

    def list_primary_tables(self, schema: str | None = None) -> list[str]:
        return []

    def list_primary_map(self, schema: str | None = None) -> dict:
        return {}

    def _normalize_table_snapshot(self, index: int, table_name: str, columns) -> dict:
        if isinstance(columns, dict):
            columns["tab_num"] = index
            columns["tab_name"] = table_name
            return columns

        tab_col = []
        for column in columns:
            tab_col.append(
                {
                    "col_num": column.ordinal,
                    "col_name": column.name,
                    "type": column.type_name,
                    "len": column.length,
                    "num_pre": column.precision,
                    "num_scale": column.scale,
                    "time_sh": column.scale,
                    "null": "YES" if column.nullable else "NO" if column.nullable is not None else None,
                    "tab_name": table_name,
                }
            )
        return {"tab_num": index, "tab_name": table_name, "tab_col": tab_col}

    def collect_schema_snapshot(self, schema: str | None = None, worker_count: int = 1) -> SchemaSnapshot:
        # 默认实现保持串行，避免在同一连接上做不安全并发；高性能版本由具体 connector 覆盖。
        table_names = self.list_tables(schema)
        tables = []
        for index, table_name in enumerate(table_names, start=1):
            columns = self.get_columns(table_name, schema)
            tables.append(self._normalize_table_snapshot(index, table_name, columns))
        return SchemaSnapshot(
            tab_num=len(table_names),
            tables=tables,
            table_names=table_names,
            primary_tables=self.list_primary_tables(schema),
            primary_map=self.list_primary_map(schema),
        )

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
