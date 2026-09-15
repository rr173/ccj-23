# media-ingest

分片上传 → 校验 → 多租户公平调度合并 → 不可变封存的媒体摄取服务。接入层（API）与合并层（Worker）是独立部署的进程，共享一个元数据库和一个数据卷。

## 多租户容量与调度

每个租户有三个可在线调整的旋钮（`PUT /tenants/{id}/policy` 会追加一个**不可变策略版本**）：

| 旋钮 | 含义 |
|---|---|
| `capacity_bytes` | 租户容量上限（已预留 + 已使用字节数）。仅在**创建上传**时按当时版本检查原子预留；调低不删除任何对象，只拒绝新的预留 |
| `max_parallel_merges` | 该租户同一时刻最多并行的合并数（按创建时策略版本分桶计数） |
| `weight` | 调度权重。租户间按权重比例获得合并机会，租户内严格 FIFO |

### 容量账目（追加式台账）

`capacity_ledger` 只追加、永不改删：

```
reserve      +size   创建上传时与 upload 行同一事务原子写入
release      -size   中止 / 过期，只写一次
commit_used  +size   合并成功：与 release(-size) 同一事务，把预留精确转成已用
```

- `UNIQUE(upload_id, event_type)` 是"只变化一次"的硬约束，重试、崩溃都不会重复计费。
- 创建在每个租户的写锁事务内完成（SQLite 为 `BEGIN IMMEDIATE`，Postgres 为 `SELECT … FOR UPDATE`），并发创建串行化，**永远不会突破上限**。
- 容量恒等式：`占用 = SUM(bytes_delta) = reserved_bytes + used_bytes`。

### 幂等创建

`POST /uploads` 带 `X-Idempotency-Key: <key>`（或 body 内 `request_key`）：

- 同租户 + 同键 + **同参数**（大小/分块/摘要/TTL 的指纹）→ 返回原上传（`replayed: true`），不重复预留；
- 同租户 + 同键 + **不同参数** → `409 request_key_reused_with_different_parameters`；
- 键按租户隔离，不同租户可以用相同键。

### 加权公平调度

合并队列是数据库表 `merge_jobs`（不再依赖外部 broker）。每个任务入队时获得：

- `seq`：租户内单调序号 → 同租户严格 FIFO；
- `vtag = seq / weight`：加权虚拟标签。调度器每趟领取全局 `vtag` 最小且满足条件的任务。

持续积压时服务比例趋近权重（如权重 3:1 时约 3:1，实测序列 `HHHL HHHL HHHL`），而每个租户的 `vtag` 都单调增长，所以**低权重租户不会被长期饿死**。

可执行条件（也是排队原因的解释依据）：

1. 退避：`not_before` 未到 → `scheduling_order`；
2. 并行上限：`(租户, 策略版本)` 桶内 running 数 ≥ `max_parallel_merges` → `parallel_limit`；
3. 容量门：当前占用 ≥ 当前容量上限时，除 FIFO 队首外一律不派新槽 → `capacity`（封存本身不改变占用，预留转已用，所以已接受的队首即使在上限被调低后仍能推进）；
4. 否则按 vtag 等待 → `scheduling_order`。

### 崩溃 / 重启一致性

- 所有容量与队列状态都在数据库里（WAL SQLite 或 Postgres），文件发布全部走同卷 `os.replace` 原子重命名。
- 任务被领取时带租约（lease）；worker 心跳续租。进程崩溃后心跳停止，租约过期，任务由 `running` 回收为 `queued`，**不重复入队、不丢失**。
- 合并封存是单事务：`Version` 行 + 上传状态 + 台账预留转已用 + 任务完成一起提交；崩溃在提交前则全部不留痕、任务重试；对象路径按 upload 确定性派生，提交前崩溃留下的孤对象会被重试直接复用。
- worker 多进程 / 多线程安全：条件 `UPDATE … WHERE status='queued'` 配合行级写锁，同一上传不会被并发处理。
- 策略版本快照在 upload 行上：策略变更只影响之后创建的上传。

## 架构

```
client                api (ingest 层)                      worker (合并层)
  │  POST /tenants/{id}/policy ─► 追加 policy_versions
  │  POST /uploads (X-Tenant-ID, X-Idempotency-Key)
  │        └─ 租户写锁事务: 幂等查重 → 当前策略容量检查 → upload + reserve 原子提交
  │  PUT  /chunks/{i} ─► 校验 SHA-256 ─► staging/<uid>/<i>.part
  │  POST /complete ───► status=queued + merge_jobs 行(UNIQUE upload_id)
  │                          ▲ 轮询 claim_next: 加权 vtag + 并行/容量门 + 租约
  │                          └─ 线程池按序流式合并、整体 SHA-256、原子封存
  │  GET /tenants/{id}/status ─► reserved/used/running/queued + 每个排队任务原因
  │  GET /versions/{vid}/content (封存后可见，chmod 444)
```

- **元数据**：`tenants` / `policy_versions` / `uploads` / `chunks` / `versions` / `idempotent_requests` / `capacity_ledger` / `merge_jobs`（Postgres；本地默认 SQLite WAL）。
- **存储**：单卷内 `tmp/ → staging/ → objects/`，全部 `os.replace` 原子发布。

## API

所有上传/下载接口都需要 `X-Tenant-ID` 头。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tenants/{id}` | 创建租户：`{capacity_bytes, max_parallel_merges, weight}` |
| PUT | `/tenants/{id}/policy` | 调整旋钮（追加新版本，不影响存量上传） |
| GET | `/tenants/{id}` | 当前策略版本 |
| GET | `/tenants/{id}/status` | 已预留/已使用/运行中/排队中数量，及每个排队任务 `reason` |
| POST | `/uploads` | 创建会话（头 `X-Tenant-ID`、可选 `X-Idempotency-Key`）；容量不足返回 507 |
| PUT | `/uploads/{uid}/chunks/{i}` | 上传分片，头 `X-Chunk-SHA256` |
| GET | `/uploads/{uid}` | 进度，排队时含 `queue.reason` |
| POST | `/uploads/{uid}/complete` | 入合并队列（幂等，202） |
| GET | `/versions/{vid}` / `/versions/{vid}/content` | 版本元数据 / 下载 |
| DELETE | `/uploads/{uid}` | 中止并释放一次预留（封存对象不可删） |
| GET | `/healthz` | 健康检查 |

## 运行

### Docker（推荐）

```bash
docker compose up --build
# 合并层独立扩缩容（共享 DB 队列与租约，安全）：
docker compose up -d --scale worker=3
```

API 在 `http://localhost:8080`（文档页 `/docs`）。

### 本地开发

```bash
pip install -r requirements.txt -r requirements-dev.txt
# 终端 1（API）：
uvicorn app.main:build_app --factory --port 8080
# 终端 2（Worker）：
python -m app.worker
```

### 测试

```bash
pytest tests/ -q
```

覆盖：同一租户并发逼近容量上限、跨进程容量竞争、幂等重放与参数冲突、中止/过期/封存各自且仅一次的容量变化、加权 3:1 调度且低权重不饥饿、单租户并行上限让其它租户继续运行、调低容量后对象仍可下载但新预留被拒、创建/入队/封存各阶段模拟崩溃后的恢复一致性、进程重启后的租约回收。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `./data` | 数据卷根目录（api 与 worker 必须共享） |
| `DATABASE_URL` | `sqlite:///./data/ingest.db` | 元数据库（容量账目与持久队列都在其中） |
| `UPLOAD_TTL_SECONDS` | `86400` | 上传会话过期时间 |
| `SWEEP_INTERVAL_SECONDS` | `30` | 清理器周期 |
| `MERGE_LEASE_SECONDS` | `120` | 合并任务租约时长（worker 心跳续租，过期被回收） |
| `RETRY_BACKOFF_SECONDS` | `5` | 瞬时合并失败后的退避 |
| `WORKER_THREADS` | `4` | 单个 worker 进程内的并行合并线程数 |
| `MAX_TOTAL_CHUNKS` | `100000` | 单上传分片数上限 |
| `DEFAULT_CAPACITY_BYTES` / `DEFAULT_MAX_PARALLEL_MERGES` / `DEFAULT_WEIGHT` | `100GiB` / `2` / `1` | 未显式建租户时首次使用的引导默认值 |
