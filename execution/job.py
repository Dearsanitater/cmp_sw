from __future__ import annotations

from dataclasses import dataclass, field
import uuid


@dataclass(slots=True)
class CompareJob:
    src_key: str
    tgt_key: str
    src_schema: str | None = None
    tgt_schema: str | None = None
    workers: int = 4
    batch_size: int = 1000
    compare_rows: bool = True
    report_dir: str = "resource/test_report"
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
