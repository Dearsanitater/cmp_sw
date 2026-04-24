from dataclasses import dataclass, field
import uuid


@dataclass
class CompareJob:
    src_key: str
    tgt_key: str
    workers: int = 4
    batch_size: int = 1000
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))