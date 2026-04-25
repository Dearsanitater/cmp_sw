from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobStatus:
    CREATED = "created"
    PLANNING = "planning"
    READY = "ready"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_FAILED = "partial_failed"


class TableTaskStatus:
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(slots=True)
class TableTaskState:
    table_name: str
    src_table: dict[str, Any] | None
    tgt_table: dict[str, Any] | None
    status: str = TableTaskStatus.PENDING
    result: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    started_at: str | None = None
    finished_at: str | None = None


@dataclass(slots=True)
class JobState:
    job_id: str
    src_key: str
    tgt_key: str
    status: str = JobStatus.CREATED
    table_tasks: list[TableTaskState] = field(default_factory=list)
    src_snapshot: tuple[int, list[dict[str, Any]]] | None = None
    tgt_snapshot: tuple[int, list[dict[str, Any]]] | None = None
    src_only_tables: list[str] = field(default_factory=list)
    tgt_only_tables: list[str] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    finished_at: str | None = None
