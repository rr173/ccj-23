# durastor — 封存媒体版本的耐久副本管理

按租户耐久策略，把每个封存媒体版本的副本分布到多个存储节点上，并提供
复制、巡检、修复、节点排空与崩溃恢复的完整闭环。所有控制面状态持久化在
单个 SQLite 库（WAL 模式）中；副本内容以"每版本一文件"的形式存放在各
节点目录下，封存母本存放在 origin 目录。

## 运行测试

```bash
cd durastor
python3 -m unittest discover -s tests -v
```

## 核心概念

| 概念 | 说明 |
| --- | --- |
| 节点 | `register_node(id, capacity, fault_domain, status)`；状态 `active / draining / offline` |
| 策略 | `publish_policy(tenant, target_replicas, min_readable, allowed_nodes)`，每次发布产生新的不可变版本 |
| 媒体版本 | `seal_version()` 封存内容并**固定当时的策略版本**；计算全量 sha256 与分块校验清单（巡检依据） |
| 计划位置 | 每个 `(version, node)` 一条记录，状态机：`pending → copying → verified → quarantined → repairing → verified`，以及 `removed` |
| 任务 | `replicate / repair / migrate`，租约认领（`claimed` + 租约过期），支持多进程并发 |
| 健康度 | `verified >= target` → healthy；`>= min_readable` → degraded；否则 unreadable |

## 关键不变量

- **放置原子性**：选节点（优先跨故障域的贪心扩散）与容量预留在同一事务
  内完成；容量不足 / 可用故障域不足 / 节点排空中都会给出具体阻塞原因，
  且不留半份计划或部分预留（事务回滚）。
- **容量只结算一次**：预留发生在计划时；确认副本不再动容量；释放在位置
  转入 `removed` 时由受保护的状态迁移保证恰好一次。
- **一副本一节点**：`(version_id, node_id)` 唯一约束 + 任务租约互斥，
  并发处理同一版本同一节点只会形成一份有效副本。
- **读取只用已校验副本**：未完成或已隔离的副本不参与读取；读取时即时
  校验，失败即隔离并换下一副本。
- **不传播损坏**：修复/迁移的源必须是 `verified` 副本；源数据先对封存
  摘要验证，不符则隔离该源并换源；无可信源时任务保持 `blocked` 并说明
  原因，版本保持 unreadable。
- **排空安全**：仅迁移仍计入耐久度的副本；替代副本确认前原副本不计提
  移除（不会跌破最低可读副本数）；节点重新上线不会让隔离副本自动恢复。

## 崩溃恢复（`recover()`，每次 worker 启动时执行，幂等）

| 崩溃窗口 | 恢复动作 |
| --- | --- |
| 副本文件写完但未确认 | 文件摘要对得上 → 原地确认；对不上（撕裂写）→ 删除并回到 `pending` 重复制 |
| 隔离记录已写但修复任务未建 | 扫描无修复任务的隔离记录，按确定性任务 ID `INSERT OR IGNORE` 补建 |
| 替代副本已确认但原节点容量未释放 | 完成迁移收尾：原位置转 `removed`、释放容量、任务置 `done`（幂等） |

另有过期租约回收与中断修复（`repairing`）回退到 `quarantined`。恢复只
处理租约已过期的任务，不会干扰存活 worker。

## 验收标准 ↔ 测试对照（`tests/test_acceptance.py`，15 个用例）

- 三节点跨两故障域符合策略的放置、策略版本固定 → `test_placement_three_nodes_two_fault_domains`
- 容量不足 / 故障域不足 / 节点排空时不留部分预留 → `test_insufficient_capacity_*`、`test_insufficient_fault_domains_blocked`、`test_draining_nodes_excluded_from_placement`
- 并发处理只生成一份副本、容量只结算一次 → `test_concurrent_workers_single_replica_single_settlement`
- 巡检发现损坏 → 读取避开隔离副本 → 修复完成 → `test_scrub_quarantines_and_repair_heals`
- 修复不把损坏扩散到其他节点 → `test_repair_never_propagates_corruption`
- 全部可信副本丢失 → 保持不可读、任务阻塞且原因明确 → `test_all_trusted_replicas_lost_stays_unreadable`
- 排空节点安全迁移后释放容量；无合法目标时阻塞并保留原副本 → `test_drain_migrates_then_releases_capacity`、`test_drain_blocked_without_legal_target_keeps_replica`
- 节点重新上线不恢复隔离副本 → `test_reonline_does_not_restore_quarantined_replica`
- 三个异常退出点恢复后副本/任务/账目一致（含撕裂写变体）→ `test_crash_after_replica_write_before_confirm`、`test_recovery_discards_torn_replica`、`test_crash_after_quarantine_before_repair_task`、`test_crash_after_replacement_confirmed_before_release`
