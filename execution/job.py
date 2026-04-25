from dataclasses import dataclass, field
import uuid


@dataclass
class CompareJob:
    src_key: str#数据库名
    src_dbn: str
    src_schema:str
    tgt_key: str
    tgt_dbn: str
    tgt_schema: str
    workers: int = 4
    batch_size: int = 100
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))