# media-ingest

分片上传 → 校验 → 后台合并 → 不可变封存的媒体摄取服务。接入层（API）与合并层（Worker）是两个独立部署的进程，通过 Redis 队列解耦，共享一个数据卷和元数据库。

## 架构

```
client                api (ingest 层)                 worker (合并层)
  │  POST /uploads ──────► 创建上传会话 (DB)
  │  PUT  /chunks/{i} ───► 校验 SHA-256 ─► staging/<uid>/<i>.part   (tmp + 原子 rename)
  │  POST /complete ─────► status=merging ─► LPUSH merge:queue ────► BRPOP 取任务
  │  GET  /uploads/{id} ─► received/checksum_failed/merging/sealed   按序流式合并
  │                                                                  整体 SHA-256
  │  GET  /versions/{vid}/content ◄── 只在封存后可见                 原子 rename 到 objects/
  │                                                                  chmod 444 + 版本行提交
```

- **元数据**：`uploads` / `chunks` / `versions` 三张表（Postgres；本地开发默认 SQLite）。
- **队列**：Redis list + processing list，worker 启动时把处理中的任务放回队列。
- **存储**：单卷内 `tmp/ → staging/ → objects/`，全部 `os.replace` 原子发布。

## 正确性保证

| 需求 | 机制 |
|---|---|
| 分片内容校验 | 客户端必须带 `X-Chunk-SHA256`；服务端边流式落盘边算摘要，不一致返回 422 并记入 `checksum_failed` |
| 缺字节 | 创建时校验 `chunk_size*(n-1) < total_size ≤ chunk_size*n`；每个分片的**精确字节数**按序号推导并强制；合并时逐分片重新校验摘要和长度 |
| 乱序/重复/断线续传 | 分片按序号独立落盘，顺序无关；相同分片重传返回 `duplicate:true`；`GET /uploads/{id}` 返回 `missing` 列表，客户端只补传缺口 |
| 可验证整体摘要 | 整体摘要 = 按序号拼接所有分片后的 SHA-256；封存时与 `expected_sha256` 比对；下载时带 `X-Content-SHA256` / `ETag` 头；`objects/<vid>.json` 保存逐分片摘要清单 |
| 不可变版本 | 对象原子 rename 进 `objects/` 后 `chmod 444`，永不覆盖；`versions.upload_id` 唯一约束 |
| 崩溃/存储不可写不暴露半成品 | 合并全程写 `tmp/`，只有完整 fsync 后才原子发布；版本行未提交前 API 查不到任何版本；崩溃只留孤儿 tmp 文件（清理器回收） |
| 重试合并只得一个版本 | 条件认领（`UPDATE ... WHERE version_id IS NULL`）+ `versions.upload_id` UNIQUE + Redis 合并锁；`/complete` 可任意重试 |
| 进度查询 | `status`: `uploading` / `merging` / `sealed` / `failed` / `expired` / `aborted`，另含 `received`、`checksum_failed[]`、`missing[]` |
| 超时清理 | sweeper 把超过 TTL 仍未完成的上传标记 `expired` 并删除暂存分片；已封存上传永不过期 |

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/uploads` | 创建会话：`{total_size, chunk_size, total_chunks, expected_sha256?, ttl_seconds?}` |
| PUT | `/uploads/{uid}/chunks/{i}` | 上传分片，头 `X-Chunk-SHA256: <hex>`；200 成功（含 `duplicate`），422 校验失败，409 状态不允许 |
| GET | `/uploads/{uid}` | 进度：received / checksum_failed / missing / status / version |
| POST | `/uploads/{uid}/complete` | 触发合并（幂等，202）；分片不齐返回 409 + missing |
| GET | `/versions/{vid}` | 版本元数据 |
| GET | `/versions/{vid}/content` | 下载封存对象（带 `X-Content-SHA256`、`ETag`、`Cache-Control: immutable`） |
| DELETE | `/uploads/{uid}` | 中止未完成的上传并清理（已封存的返回 409） |
| GET | `/healthz` | 健康检查 |

## 运行

### Docker（推荐）

```bash
docker compose up --build
# 合并层独立扩缩容：
docker compose up -d --scale worker=3
```

API 在 `http://localhost:8080`（文档页 `/docs`）。

### 本地开发

```bash
pip install -r requirements.txt -r requirements-dev.txt
# 终端 1（API，单进程演示用内存队列）：
REDIS_URL=fake:// uvicorn app.main:build_app --factory --port 8080
# 终端 2（Worker，需要真实 Redis；或装一个 redis-server）：
REDIS_URL=redis://localhost:6379/0 python -m app.worker
```

### 客户端示例

```bash
python examples/client.py /path/to/bigfile.bin --api http://localhost:8080
```

### 测试

```bash
pytest tests/ -q
```

覆盖：乱序+重复上传、校验失败恢复、断线续传、合并中途崩溃/存储不可写（无半成品、重试仅一个版本）、整体摘要不匹配、TTL 过期清理、队列崩溃恢复。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `./data` | 数据卷根目录（api 与 worker 必须共享） |
| `DATABASE_URL` | `sqlite:///./data/ingest.db` | 元数据库 |
| `REDIS_URL` | `redis://localhost:6379/0` | 任务队列（`fake://` 为单进程演示用内存队列） |
| `UPLOAD_TTL_SECONDS` | `86400` | 上传会话过期时间 |
| `SWEEP_INTERVAL_SECONDS` | `30` | 清理器周期 |
| `STALE_MERGE_SECONDS` | `60` | merging 状态超过该时长视为卡住并重新入队 |
| `MERGE_LOCK_SECONDS` | `300` | 单上传合并锁 TTL |
