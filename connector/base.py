from __future__ import annotations

from abc import ABC, abstractmethod
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
    def get_row(
        self,
        table: str,
        columns: list[str],
        batch_size: int = 1000,
        schema: str | None = None,
        order_by: str | None = None,
    ) -> Iterable[list[tuple]]:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
