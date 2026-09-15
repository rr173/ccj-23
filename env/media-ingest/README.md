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

## 归档：保留期 / 法律保全 / 内容去重 / 可证明删除

已封存对象（`archived_versions`）的生命周期由 `app/archive/` 子系统独立管理，接口前缀 `/archive`。

### 版本化保留策略

- `PUT /archive/tenants/{id}/policy` **追加**一条不可变策略版本 `{retention_seconds}`；`GET .../policies` 可查全部版本。
- 封存（`PUT /archive/objects/{oid}/versions/{v}`，body 即对象字节）时把 `(policy_version, retention_seconds)` **快照到版本行**；旧对象永远按封存时的版本计算到期时间，策略更新只影响之后封存的对象。

### 删除资格（明确返回阻止原因）

`GET /archive/objects/{oid}/versions/{v}/eligibility` 返回 `eligible` 与全部 `blockers`，每条带 `reason`：

| reason | 含义 |
|---|---|
| `retention` | 保留期未届满（附 `expires_at`、`remaining_seconds`、策略版本） |
| `legal_hold` | 存在一项或多项生效法律保全（附全部 hold） |
| `reference` | 仍有其他持久引用（pin）指向该版本 |

已删除版本返回 `already_deleted`。`shared_content` 段单独说明物理内容被多少活动引用共享——它**不阻止逻辑删除**，只决定物理内容是否随本次删除清理。

### 法律保全

- `PUT /archive/objects/{oid}/holds/{key}`（body 可带 `reason`）：对象级、可叠加；保全跨过保留期到期时间仍然阻止删除。
- `DELETE .../holds/{key}`：解除一项；**解除最后一项后对象才重新具备删除资格**。保全行只追加（`released_at`），历史不可销毁。

### 内容去重引用

- 物理内容按 SHA-256 内容寻址（`content_blobs` + `<data>/archive/content/ab/<sha256>`），字节相同的任意数量版本——**包括跨租户版本**——共享一份物理内容。
- 每个 `(租户, 对象, 版本)` 的访问权限与生命周期互相隔离：下载严格按租户+版本鉴权；删除一个版本只释放它自己的那一个引用。
- 引用计数只通过一条带条件的 `UPDATE ... WHERE state='active' AND refcount>0` 递减，并由版本行 `refs_released_at` 精确一次标记保护：并发/重复删除**不会把计数减成负数**；最后一个活动引用删除后 blob 进入 `pending_purge`，物理文件才允许清理。

### 多阶段、崩溃安全、幂等的删除

删除状态机（`archive_delete_ops`）每阶段独立事务落盘：

```
1. logical delete   版本 -> tombstoned（下载立即永久失效，不可复活）
2. release refs     精确一次的条件递减；最后引用使 blob -> pending_purge
3. purge+finalize   认领为 purging -> os.unlink（幂等）-> purged -> 出具删除证明
```

- 启动时 `ArchiveService.resume()` 自动续做：`purging` 的 blob 补做 unlink，未 finalized 的 op 从断点推进；无崩溃时是空操作。
- 崩溃在逻辑删除之后 => 版本永远是 tombstone，**不会恢复下载**；崩溃在物理清理前后 => 共享内容只要还有活动引用就保持 `active`，**不会被误删**。
- 删除支持 `X-Idempotency-Key`：并发或重复请求收敛到同一个 op，**只有一次引用变化、一份删除证明**；重复调用返回原证明（`replayed: true`）。

### 删除证明（不可篡改、可查询）

删除完成后生成 `deletion_certificates`：记录对象与内容哈希、**策略版本与保留期快照**、删除时保全/pin 状态、逻辑删除时间、物理清理结果（`purged` 或 `retained_shared` + 剩余引用数）。

- 行只插入、不修改不删除；全局序号形成**哈希链**（每条记录的 `record_hash` 链接上一条），并用 HMAC-SHA256 签名（密钥 `ARCHIVE_PROOF_KEY`，生产必须覆盖）。
- 查询：`GET /archive/certificates/{id}`、`GET /archive/objects/{oid}/versions/{v}/certificate`、`GET /archive/certificates`；响应自带 `chain_valid`，篡改任一行（内容/签名/顺序）即验证失败。

### 归档相关 API（均需 `X-Tenant-ID`）

| 方法 | 路径 | 说明 |
|---|---|---|
| PUT | `/archive/tenants/{id}/policy` | 发布新保留策略版本 |
| GET | `/archive/tenants/{id}/policy` / `/policies` | 当前 / 全部策略版本 |
| PUT | `/archive/objects/{oid}/versions/{v}` | 封存指定版本（body=字节），快照当前策略 |
| POST | `/archive/objects/{oid}/versions` | 封存下一个自增版本 |
| GET | `/archive/objects/{oid}/versions/{v}` / `/content` | 元数据 / 下载（tombstone 与跨租户均 404） |
| GET | `/archive/objects/{oid}/versions/{v}/eligibility` | 删除资格与逐条阻止原因 |
| DELETE | `/archive/objects/{oid}/versions/{v}` | 幂等删除（可带 `X-Idempotency-Key`） |
| PUT/DELETE/GET | `/archive/objects/{oid}/holds/{key}` [`/holds`] | 法律保全 放置/解除/列表 |
| PUT/DELETE | `/archive/objects/{oid}/versions/{v}/pins/{key}` | 其他持久引用（第三类删除阻止项） |
| GET | `/archive/certificates/...` | 删除证明查询与链校验 |

## 派生配方：从封存版本拼接新的不可变对象

已封存对象可以按一份**派生配方**（recipe）生成新的不可变封存版本，接口前缀 `/derive`，后台由独立进程 `python -m app.derive.worker` 推进（可横向扩缩容）。

### 配方与归一化摘要

配方是一个有序段列表，每段固定引用**同一租户**内一个对象的**固定版本**，选一个半开字节区间 `[start, end)`，并声明该区间字节的 SHA-256（摘要针对区间字节本身）：

```json
{
  "segments": [
    {"object_id": "a", "version": 1, "range": [0, 10], "sha256": "..."},
    {"object_id": "b", "version": 3, "range": "5-25", "expected_sha256": "..."}
  ]
}
```

区间写法可以是 `[s,e]` / `{"start":s,"end":e}` / 字符串 `"s-e"`，段内字段顺序任意，摘要大小写不敏感，未知字段忽略。归一化后按排序键的紧凑 JSON 计算 SHA-256，得到**配方摘要** `recipe_digest`——语义相同但写法不同的配方得到同一个摘要；段顺序（决定拼接顺序）或区间不同则摘要不同。

### 受理：整体原子、不泄露存在性

`POST /derive/jobs`（必须带 `X-Tenant-ID` 与 `X-Idempotency-Key` 或 body 内 `request_key`，并指定 `output_object_id`）：

- 幂等：同租户 + 同请求键 + **同配方摘要** → 返回同一个任务（`replayed: true`）；同键不同配方 → `409 request_key_reused_with_different_recipe`（附原任务 id 与原摘要）。
- 受理在**单个写事务**内固定全部源版本：每段必须存在、属于本租户、处于 `active`（未进入删除流程）、区间不越界，且对区间字节现场重算的摘要必须与声明一致。任一不满足 → 整个请求拒绝，不留下任务、保护或容量记录。
- 不存在的版本与**其他租户**的版本返回完全相同的 `source_not_found`，无法借此探测其他租户的对象是否存在；摘要不符为 `digest_mismatch`，区间越界为 `range_out_of_bounds`，已在删除流程为 `source_deleting`。
- 同一事务写入：任务、每段状态（初始 `waiting`）、对每个**去重后**源版本的保护行、以及一条派生容量预留（`derivation_ledger` 的 `reserve`）。

### 源版本保护与容量

- 任务存续期间，每个源版本都带一个活动的 `derivation_protections` 行，它在归档删除资格里表现为 `reason: reference` 阻塞项，并列出引用它的 `job_id`；删除任一源版本会被阻止。终态（计费完成 / 取消 / 失败）一次性释放全部保护。
- 派生字节在**成功发布前不计入已用容量**，只以预留存在；容量账目与上传链路同构：`reserve +size` →（取消/失败）`release -size` 一次，或（成功）`release -size` + `commit_used +size` 成对且只发生一次。容量上限复用租户策略的 `capacity_bytes`。

### 状态与取消

`GET /derive/jobs/{id}` 返回任务状态、结果坐标、每段状态（`waiting` / `processing` / `verified` / `failed`）与未完成段的 `blocked_reason`（等待 worker、等待前序段、取消请求、失败原因等），以及 `open_protections`。完成前结果对象在归档接口下载一律 404。

`POST /derive/jobs/{id}/cancel`：排队中任务同步终止；处理中/拼接中任务在最近的安全边界终止。终态清理删除不可见暂存、只释放一次预留、释放全部保护。**已发布（含已计费）任务不可取消**。

### 并发、发布一次、崩溃恢复

- 领取即租约 + **fence 令牌**：条件 UPDATE 让同一任务只能被一个 worker 推进；worker 崩溃后租约过期可被重新领取，旧 worker 即使存活，其后续带旧 fence 的写入全部失效。
- 处理状态机：`queued → processing → assembling → published → billed`。段文件与拼接文件都走 tmp+fsync+`os.replace`，已校验段重启后直接复用，不重复提取。
- **发布与计费是两个事务**：先在一个事务内原子写入新的 `archived_versions` 行、内容 blob 引用计数并把任务置 `published`（此刻才可下载）；下一个事务把预留转成**一次**已用容量并释放保护。
- 三个异常退出点（写完任意段后、全部拼接完未发布、发布后未确认计费）重启后均恢复一致：不重复发布（输出版本唯一）、不重复计费（`UNIQUE(job_id, event_type)`）、不留下永久保护或孤儿暂存。`resume(force=True)` 在 API 启动与 `python -m app.derive.worker --recover` 时执行。

### 派生相关 API（均需 `X-Tenant-ID`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/derive/jobs` | 提交配方（头 `X-Idempotency-Key` 或 body `request_key`），整体受理 |
| GET | `/derive/jobs/{id}` | 任务与每段状态/阻塞原因、保护数、结果坐标 |
| GET | `/derive/jobs` | 列出本租户任务 |
| POST | `/derive/jobs/{id}/cancel` | 取消排队/处理中任务；已发布 409 |
| GET | `/derive/capacity` | 派生容量的预留/已用/上限 |

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

派生（`tests/test_derive.py`，另含真实 `os._exit` 子进程场景 `tests/derive_crash_scenario.py`）：多源多区间按序拼接正确、等价配方同摘要（异序/异写法则不同）、请求键幂等重放与异方冲突、跨租户/摘要错/区间越界/删除中源整体拒绝且不留残余、任务期间删除任一源版本被 reference 原因阻止、多进程并发领取只发布一次且只计费一次、排队与处理中取消后暂存/预留/保护全部释放且已发布不可取消、三个异常退出点重启后输出/计费/保护一致、完成前不可下载且不计容量、HTTP 全流程语义。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `./data` | 数据卷根目录（api 与 worker 必须共享） |
| `DATABASE_URL` | `sqlite:///./data/ingest.db` | 元数据库（容量账目与持久队列都在其中） |
| `UPLOAD_TTL_SECONDS` | `86400` | 上传会话过期时间 |
| `SWEEP_INTERVAL_SECONDS` | `30` | 清理器周期 |
| `MERGE_LEASE_SECONDS` | `120` | 合并任务租约时长（worker 心跳续租，过期被回收） |
| `DERIVE_LEASE_SECONDS` | `120` | 派生任务租约时长（fence 令牌 + 过期回收，同合并任务模型） |
| `RETRY_BACKOFF_SECONDS` | `5` | 瞬时合并失败后的退避 |
| `WORKER_THREADS` | `4` | 单个 worker 进程内的并行合并线程数 |
| `MAX_TOTAL_CHUNKS` | `100000` | 单上传分片数上限 |
| `DEFAULT_CAPACITY_BYTES` / `DEFAULT_MAX_PARALLEL_MERGES` / `DEFAULT_WEIGHT` | `100GiB` / `2` / `1` | 未显式建租户时首次使用的引导默认值 |
