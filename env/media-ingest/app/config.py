from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Settings:
    data_dir: str = "./data"
    database_url: str = "sqlite:///./data/ingest.db"
    upload_ttl_seconds: int = 24 * 3600
    sweep_interval_seconds: float = 30.0
    merge_lease_seconds: int = 120
    retry_backoff_seconds: int = 5
    max_total_chunks: int = 100_000
    # Local merge process parallelism (per-tenant max_parallel_merges is enforced
    # separately by the scheduler; multiple worker processes share the DB queue).
    worker_threads: int = 4
    # Defaults applied when an unknown tenant is auto-bootstrapped (tests/demo).
    default_capacity_bytes: int = 100 * 1024 * 1024 * 1024  # 100 GiB
    default_max_parallel_merges: int = 2
    default_weight: int = 1

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            data_dir=os.environ.get("DATA_DIR", cls.data_dir),
            database_url=os.environ.get("DATABASE_URL", cls.database_url),
            upload_ttl_seconds=int(os.environ.get("UPLOAD_TTL_SECONDS", str(cls.upload_ttl_seconds))),
            sweep_interval_seconds=float(os.environ.get("SWEEP_INTERVAL_SECONDS", str(cls.sweep_interval_seconds))),
            merge_lease_seconds=int(os.environ.get("MERGE_LEASE_SECONDS", str(cls.merge_lease_seconds))),
            retry_backoff_seconds=int(os.environ.get("RETRY_BACKOFF_SECONDS", str(cls.retry_backoff_seconds))),
            max_total_chunks=int(os.environ.get("MAX_TOTAL_CHUNKS", str(cls.max_total_chunks))),
            worker_threads=int(os.environ.get("WORKER_THREADS", str(cls.worker_threads))),
            default_capacity_bytes=int(os.environ.get("DEFAULT_CAPACITY_BYTES", str(cls.default_capacity_bytes))),
            default_max_parallel_merges=int(
                os.environ.get("DEFAULT_MAX_PARALLEL_MERGES", str(cls.default_max_parallel_merges))
            ),
            default_weight=int(os.environ.get("DEFAULT_WEIGHT", str(cls.default_weight))),
        )
