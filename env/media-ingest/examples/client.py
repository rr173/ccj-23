#!/usr/bin/env python3
"""Reference client: chunked upload with per-chunk digests, resume, and progress polling.

Usage:
    python examples/client.py /path/to/bigfile.bin [--api http://localhost:8080] [--chunk-size 8388608]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.request


def sha256_file(path, chunk_size):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while buf := f.read(chunk_size):
            h.update(buf)
    return h.hexdigest()


class Client:
    def __init__(self, base, tenant_id, request_key=None):
        self.base = base.rstrip("/")
        self.tenant_id = tenant_id
        self.request_key = request_key

    def _req(self, method, path, body=None, headers=None):
        h = {"X-Tenant-ID": self.tenant_id}
        h.update(headers or {})
        r = urllib.request.Request(self.base + path, data=body, method=method, headers=h)
        try:
            with urllib.request.urlopen(r) as resp:
                return resp.status, json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read() or b"{}")

    def create_upload(self, size, chunk_size, total_chunks, expected_sha256):
        headers = {"Content-Type": "application/json"}
        if self.request_key:
            headers["X-Idempotency-Key"] = self.request_key
        st, body = self._req("POST", "/uploads", json.dumps({
            "total_size": size, "chunk_size": chunk_size,
            "total_chunks": total_chunks, "expected_sha256": expected_sha256,
        }).encode(), headers)
        assert st == 201, body
        return body["upload_id"]

    def put_chunk(self, uid, index, data):
        digest = hashlib.sha256(data).hexdigest()
        st, body = self._req("PUT", f"/uploads/{uid}/chunks/{index}", data,
                             {"X-Chunk-SHA256": digest})
        if st == 422:  # corrupted in transit -> caller retries
            raise RuntimeError(f"chunk {index} rejected: {body}")
        assert st == 200, body
        return body["duplicate"]

    def status(self, uid):
        st, body = self._req("GET", f"/uploads/{uid}")
        assert st == 200, body
        return body

    def complete(self, uid):
        st, body = self._req("POST", f"/uploads/{uid}/complete")
        assert st in (202, 409), body
        return st, body


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--api", default="http://localhost:8080")
    ap.add_argument("--tenant", required=True, help="X-Tenant-ID")
    ap.add_argument("--request-key", default=None,
                    help="idempotency key (X-Idempotency-Key); defaults to file path")
    ap.add_argument("--chunk-size", type=int, default=8 * 1024 * 1024)
    args = ap.parse_args()

    c = Client(args.api, args.tenant, args.request_key or f"upload:{args.file}")
    size = __import__("os").path.getsize(args.file)
    cs = args.chunk_size
    total = (size + cs - 1) // cs
    print(f"hashing {args.file} ({size} bytes)...")
    digest = sha256_file(args.file, cs)
    uid = c.create_upload(size, cs, total, digest)
    print(f"upload_id={uid} chunks={total}")

    # Resume support: ask the server what's still missing instead of re-sending everything.
    missing = set(c.status(uid)["missing"])
    with open(args.file, "rb") as f:
        for i in sorted(missing):
            f.seek(i * cs)
            data = f.read(cs if i < total - 1 else size - cs * (total - 1))
            for attempt in range(5):
                try:
                    dup = c.put_chunk(uid, i, data)
                    break
                except RuntimeError:
                    if attempt == 4:
                        raise
                    time.sleep(0.5 * (attempt + 1))
            print(f"  chunk {i}/{total} {'(duplicate)' if dup else 'stored'}")

    st, body = c.complete(uid)
    assert st == 202, body
    while True:
        s = c.status(uid)
        print(f"  status={s['status']} received={s['received']}/{s['total_chunks']}")
        if s["status"] == "sealed":
            v = s["version"]
            print(f"SEALED version={v['version_id']} sha256={v['sha256']}")
            assert v["sha256"] == digest, "final digest mismatch!"
            print(f"download: {args.api}{v['download_url']}")
            return 0
        if s["status"] in ("failed", "expired"):
            print(f"ERROR: {s['error']}", file=sys.stderr)
            return 1
        time.sleep(1)


if __name__ == "__main__":
    sys.exit(main())
