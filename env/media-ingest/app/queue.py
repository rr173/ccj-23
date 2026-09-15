from __future__ import annotations

import redis


class MergeQueue:
    """Redis list queue with a processing list for crash recovery.

    Duplicates in the queue are harmless: run_merge() is idempotent and the
    versions.upload_id UNIQUE constraint collapses concurrent merges.
    """

    QUEUE = "merge:queue"
    PROCESSING = "merge:processing"

    _fake_server = None  # shared so same-process fake:// instances see each other

    def __init__(self, url: str):
        if url.startswith("fake://"):
            import fakeredis

            if MergeQueue._fake_server is None:
                MergeQueue._fake_server = fakeredis.FakeServer()
            self.r = fakeredis.FakeRedis(server=MergeQueue._fake_server, decode_responses=True)
        else:
            self.r = redis.Redis.from_url(url, decode_responses=True)

    def enqueue(self, upload_id: str) -> None:
        self.r.lpush(self.QUEUE, upload_id)

    def dequeue(self, timeout: int = 5) -> str | None:
        return self.r.brpoplpush(self.QUEUE, self.PROCESSING, timeout=timeout)

    def ack(self, upload_id: str) -> None:
        self.r.lrem(self.PROCESSING, 1, upload_id)

    def requeue(self, upload_id: str) -> None:
        self.r.lrem(self.PROCESSING, 1, upload_id)
        self.r.lpush(self.QUEUE, upload_id)

    def recover(self) -> int:
        """Move everything left in processing back to the queue (worker startup)."""
        n = 0
        while self.r.rpoplpush(self.PROCESSING, self.QUEUE):
            n += 1
        return n

    def acquire_merge_lock(self, upload_id: str, ttl_seconds: int) -> bool:
        return bool(self.r.set(f"lock:merge:{upload_id}", "1", nx=True, ex=ttl_seconds))

    def release_merge_lock(self, upload_id: str) -> None:
        self.r.delete(f"lock:merge:{upload_id}")
