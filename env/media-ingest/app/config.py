from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    data_dir: str = "./data"
    database_url: str = "sqlite:///./data/ingest.db"
    redis_url: str = "redis://localhost:6379/0"
    upload_ttl_seconds: int = 24 * 3600
    sweep_interval_seconds: float = 30.0
    merge_lock_seconds: int = 300
    stale_merge_seconds: int = 60
    max_total_chunks: int = 100_000

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            data_dir=os.environ.get("DATA_DIR", cls.data_dir),
            database_url=os.environ.get("DATABASE_URL", cls.database_url),
            redis_url=os.environ.get("REDIS_URL", cls.redis_url),
            upload_ttl_seconds=int(os.environ.get("UPLOAD_TTL_SECONDS", str(cls.upload_ttl_seconds))),
            sweep_interval_seconds=float(os.environ.get("SWEEP_INTERVAL_SECONDS", str(cls.sweep_interval_seconds))),
            merge_lock_seconds=int(os.environ.get("MERGE_LOCK_SECONDS", str(cls.merge_lock_seconds))),
            stale_merge_seconds=int(os.environ.get("STALE_MERGE_SECONDS", str(cls.stale_merge_seconds))),
            max_total_chunks=int(os.environ.get("MAX_TOTAL_CHUNKS", str(cls.max_total_chunks))),
        )
