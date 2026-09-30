# 运维观测与容量控制

[当前状态](current-status.md) · [待办](TODO.md) · [性能评估](performance.md)

P2-03 已完成选定单实例范围：聚合观测、真实故障告警与窗口恢复、JSON/向量批次限制，以及所有 AMS Docket 生产者的原子队列准入和 bounded scheduler。整体进度为 18/20，P1-06/07 的真实模型与端到端验收仍待完成，最新全量结果见[当前状态](current-status.md)。

## 指标与告警

`GET /v1/operations` 返回聚合指标，无记忆正文、任务载荷或租户 ID。认证启用时必须具有显式 `operations` role，普通用户和 `admin` role 不隐含此权限；开发禁用认证时沿用开发模式。

- 索引就绪：实际 bounded list 查询；维护锁、合同或查询错误触发 `index_unavailable`。
- 队列：Stream XLEN、延迟 ZSET ZCARD、consumer group pending 合计。pending 是 Stream 中已交付部分，不能再加到队列容量计数中。
- outbox：`extraction_outbox`、`structure_outbox`、`summary_outbox`、`task_cancel_outbox` 四类 SCAN 计数；限制匹配数及 cursor 页数，达到上限返回 `scan_truncated=true`，计数是保守下界。
- 内存：INFO memory used_memory，与部署显式容量或数据库 maxmemory 比较。
- 持久 counters：`recency_fallback`、`index_unavailable`、`lua_failure`、`outbox_invalid`、`outbox_redispatch`、`batch_rejected`、`queue_rejected`。观测失败不替代业务异常或把成功变成失败。

| 设置 | 默认 | 用途 |
| --- | --- | --- |
| WORKING_MEMORY_MAX_BYTES | 1048576 | 编码后工作记忆 JSON 硬上限 |
| MEMORY_VECTOR_BATCH_LIMIT | 100 | 单次 add_memories 硬限制；模型及发布前拒绝 |
| DOCKET_MAX_PENDING_TASKS | 64 | ZCARD(queue)+XLEN(stream) 原子准入上限；可设 1–64 |
| DOCKET_SCHEDULER_BATCH_SIZE | 64 | 每次到期搬运上限；可设 1–64，默认每 250ms 调度 |
| OPERATIONS_SCAN_LIMIT | 1000 | 每类 outbox 每次扫描上限 |
| OPERATIONS_QUEUE_ALERT_THRESHOLD | 64 | queued+delayed 积压告警阈值，与硬准入分别配置 |
| OPERATIONS_MEMORY_LIMIT_BYTES | 0 | 0 使用数据库 maxmemory；非 0 为部署容量 |
| OPERATIONS_MEMORY_ALERT_RATIO | 0.9 | 内存使用比例告警 |
| OPERATIONS_ERROR_WINDOW_SECONDS | 300 | 最近错误告警窗口；累计计数仍保留 |

alerts 包含 `index_unavailable`、`queue_backlog`、`memory_capacity`、`outbox_scan_limit`，以及最近窗口内的 `recency_fallback`、`lua_failure`、`outbox_invalid`、`queue_rejected`。last-event 使用数据库 TIME；默认 300 秒无新错误后解除对应告警，累计 counter 不清空。外部监控可轮询该端点。

## 队列硬限制与恢复

AMS 的应用级 `BoundedDocket` 在原 schedule 六键事务内、任何破坏性写入前检查 `ZCARD(queue)+XLEN(stream)`。立即、延迟、重派、自动周期任务共享此限制；Stream 包含正在执行/PEL 的任务。稳定键重复不增加容量；替换或重排只按事务内实际移除的旧项目抵扣。历史积压已超限时允许不增长的重排，拒绝新增增长，不能通过删除观测 counter 绕过限制。

容量拒绝抛出 `QueueCapacityError` 并增加 `queue_rejected`，不会留下新 runs/parked 数据。四类业务 outbox 保留原身份供启动、周期或手动 reconciliation；稳定任务键防止已入队工作重复增长。worker 启动时自动任务遇满队列会延后，开始消费后继续尝试。默认恢复周期为 30 秒，可通过 `--outbox-recovery-interval` 调整。

生产到期 scheduler 只允许 SHA `46dec5a67c168033d38f45742aba3572a9d73be9` 的动态键，保持 GLOBAL 原子模式。Lua 使用 `LIMIT`，最多搬运 64 个选定 ID，保留剩余和未来任务；取消通过同事务 XACK/XDEL 清理 Stream/PEL，claim 保留 stream_id 以支持活跃取消。部署与审计命令见[兼容性](compatibility.md)。

## 保留与容量边界

revision/incarnation、长期记忆四级删除 generation、extraction/discrete/structure 收据及迁移 receipt 当前永久保留。待消费、执行中的 AMS task 不设置元数据 TTL；终态 task 才使用有界保留。永久保护记录不按会话 TTL 删除，不能通过 GC 重新允许旧任务发布。离线 GC 必须先建立可证明的旧载荷排空与代数保留协议；当前没有自动安全 GC，也不承诺数据库总容量恒定。

队列硬上限只约束 Docket queue+Stream；durable outbox 和永久保护记录仍需内存预算与告警。基准容量预算、原始上游 scheduler 测量和最终适配之间的区别见[性能评估](performance.md)。测试位于 `test_operations.py`、`test_bounded_docket.py`、`test_policy_cas.py`，最新合并验收结果统一见[当前状态](current-status.md)。
