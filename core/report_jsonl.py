from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any


class JsonlReportWriter:
    def __init__(self, report_dir: str | Path, job_id: str):
        self.report_dir = Path(report_dir)
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.report_dir / f"{job_id}.jsonl"
        self._lock = Lock()

    def write_event(self, event_type: str, payload: dict[str, Any]) -> None:
        line = {"event": event_type, **payload}
        with self._lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(line, ensure_ascii=False) + "\n")

    def close(self) -> None:
        return None
