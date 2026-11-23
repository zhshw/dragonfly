# raft.memory / 原 cluster profile 与优化项

**当前保留 v9；最新 v10 接收确认快路径实验无收益，代码已回退。**

| 版本 | 确认吞吐 MiB/s | 对比 |
| --- | ---: | ---: |
| 原始 `raft.memory`（profile 优化前） | 169.56 | — |
| 保留的 `raft.memory`（v9 原实验，含以下优化） | **389.95** | **+130.0%** |
| 原 cluster（v9 同轮对照） | 580.94 | 为该轮 Raft 的 1.49 倍 |

最新版本包含的优化项：

- **P0-1：减少编码与缓冲复制**。共享不可变 WAL、单条 SET 直接编码、drain 一次预分配并直接聚合，effect 与 WAL 共用缓冲。
- **P0-2：有界批量应用与借用参数**。多笔已提交 effect 一次投递 owner，SET 同步借用 key/value，减少往返、复制和清零。
- **P1-4：follower 复用已验证的接收批次**。普通 SET/no-op 从共享缓冲应用，省去本轮负载的 WAL 读回、重复 CRC/解码和 value 副本。
- **P1-7：Fetch 直接封帧**。只计算字段长度后一次预留，去掉 body → frame 副本和尾部扩容复制。
- **P1-5：SET 输入所有权转交**。接管已解析参数，drain 后释放输入，票据仍按原有序回复释放配额。

均为 1 连接、pipeline 64、未插桩三轮中位数，计时包含 SET + 原写连接 WAIT 1。
**+130.0% 是原始基线到最新版本的跨轮累计对比**，包含上述全部优化及 WAIT 等待变化；各阶段增量不能相加。

**最新一轮 v10：follower 接收确认快路径（无收益，已回退）**。以下使用 v9 同轮复测作增量基线。

| 指标 | v9 本轮复测 | v10 候选（已回退） | 变化 |
| --- | ---: | ---: | ---: |
| 确认吞吐（三轮中位数） | **400.62 MiB/s** | 385.40 MiB/s | **−3.8%** |
| 批次 P95（三轮中位数） | **58.66 ms** | 70.09 ms | **+19.5%** |
| 接收完成至 Fetch 发送累计等待（单次诊断） | 88.62 ms | 12.01 ms | −86.4% |
| 两节点 CPU 合计（单次诊断） | 822 ms | 742 ms | −9.7% |
| leader 峰值 RSS（单次诊断） | 430.79 MiB | 438.50 MiB | +1.8% |

三组确认吞吐配对 **−4.0% / −7.7% / +9.5%**，没有稳定收益；本轮原 cluster 为 **618.30 MiB/s**。
局部确认等待确实缩短，但 Fetch 次数 **200 → 256**，leader worker 排队区间并集 **217.73 → 295.80 ms**。
因此回退本轮代码，保留全部实验记录，不扩展连接数。400.62 是 v9 复测，不是新优化收益；上表 v9 原实验 389.95 保留原含义。
三版本完整指标、火焰图和 trace 见 [v10 实验](profile/v10-follower-receipt.md)；历史优化见[实验索引](#experiments)，口径见 [benchmark](replication-benchmark.md)。

## 1. 路径差异：哪些工作可以减少

| 阶段 | 原 cluster | raft.memory | 优化方向 |
| --- | --- | --- | --- |
| 接收 | RESP 参数 → SET 执行 | RESP 参数 → 异步票据 → 排队 | 一份输入所有权，缩短大 value 存活期 |
| 复制准备 | shard journal 编码并流式发送 | effect → Kafka batch → drain 聚合 → WAL 缓冲 | 直接编码到最终缓冲，传递所有权 |
| 确认 | 本地 SET 回复；WAIT 追踪副本应用 | WAL 接收 → follower Fetch → HW → 本地应用 → SET 回复 | 保留确认条件，批量推进完成通知 |
| 应用 | replica flow 消费 journal 并执行 | 组 worker → owner 批量 → 借用 SET 参数 → 命令分发 → shard | 继续减少通用分发和逐笔 shard 等待 |
| 重放数据 | 接收的 journal 数据 | leader/follower 普通 effect 复用不可变 WAL 缓冲，缺口从 WAL 重放 | 保留有界缓存及生命周期校验，继续减少 Fetch 副本 |

Raft 的任期、日志顺序、HW 和恢复边界仍由 Raft 负责；借鉴原 cluster 的数据处理方式，保持两种集群设计独立。

## 2. 优化项与当前状态

<a id="wal-buffer-remaining"></a>

### P0-1：一次编码，传递同一个不可变 WAL 缓冲（已实现）

单条 SET 直接编码，drain 按长度、段边界和预算选定连续前缀，一次预留最终 WAL 缓冲。
冻结后由 I/O 与 effect 缓存共享所有权，effect 借用 key/value 视图；保留日志字节、CRC、同 key 顺序与确认条件。
实现过程和实验结果分别见[共享缓冲与直接编码](profile/v4-wal-buffer.md)、[直接聚合与共享](profile/v6-drain-sharing.md)。

<a id="drain-preallocation"></a>

### P0-1a：drain 合批缓冲预分配（已由直接聚合覆盖）

聚合前一次 reserve，所有记录直接写入最终缓冲；没有“只加 reserve”的独立实验。
实验边界见[直接聚合与共享](profile/v6-drain-sharing.md)。

<a id="follower-receipt-progress"></a>

### P0-1b：follower 接收确认快路径（提前 Fetch 试验无收益，已回退）

**问题仍存在：** `ReceiveFetchResponse` 追加后，报告接收位置的下一次 Fetch 仍经过
`RunPeer → RunOnWorker → Next → Tick → Synchronize → apply_`；统一 pending 检查也会等待 memory 后台 fsync。

**v10 已验证并回退：** 普通响应后准备下一笔 Fetch，先发送已发布位置，再由原 worker 串行应用；
每 peer 仍只有一笔 RPC，结构记录和角色变化保留屏障，WAIT 使用 applied tag。
接收至发送等待 **88.62 → 12.01 ms**，但正式确认吞吐 **400.62 → 385.40 MiB/s（−3.8%）**，
P95 **+19.5%**；未证明整体收益，不保留该实现。数据和补丁见 [v10 实验](profile/v10-follower-receipt.md)。

**尚未实施：** 有界、按日志顺序的独立异步应用队列，以及完成后回到 worker 推进连续 applied 前缀。
不能从本次局部等待下降推断完整异步化收益；重新评估需覆盖组 worker 排队、RPC 次数和端到端确认。
换主、控制记录、截断、快照、迁移、存储失败仍需屏障；不得普遍放开 `allow_pending` 或提前报告未完成追加。
与 P1-6 的追加/后台同步分离、已完成的 P1-4 接收缓存分别评估，不重复计算收益。

### P0-2：批量应用，继续评估 shard 内批量执行

已实现最多 32 笔 / 1 MiB 的 owner 批量投递，按 1 ms 预算返回已完成前缀；SET 同步借用 key/value，仍走原有事务。
实现范围和验证结果见[批量应用实验](profile/v5-batched-apply.md)。

- 后续评估按 shard 合批执行，减少逐笔命令分发、跨线程等待和结果捕获；复用既有 shard 操作。
- 保留 TTL 清除、OOM、类型覆盖、DB 选择、统计、索引与变更通知，不能绕过 DbSlice/事务约束。
- 同 key、条件写、控制/no-op、迁移和快照边界保留屏障；只公布全部完成的连续 applied 前缀。
  部分失败不能推进失败位置或提前回复。

**验收：** owner/shard 投递次数、应用 CPU、排队与临时参数分配下降，重放一致性和故障回归通过。

### P1-3：按 applied offset 批量完成票据，减少 worker 回查

**代码依据：** `TrySet` 为每笔创建确认 fiber；`WaitApplied → VisitUntil → RunOnWorker` 查询应用进度。
当前使用事件等待；查询已应用前缀仍可能先等追加或后台同步结束，批量 owner 投递没有移除此处等待。
最新批次末尾的 offset 关联结果，见[v9 确认路径复核](profile/v9-input-ownership.md#v9-confirmation-review)；
本轮没有显示应用完成后的票据回查是主要尾部瓶颈，不能仅凭 worker 访问次数将其列为下一项。

- 按 `(epoch, end_offset)` 登记待完成票据，由应用进度一次完成连续前缀，减少每笔唤醒后重新排队查询。
- 角色/任期变化、存储失败、超时和关闭必须完成或取消相应票据；连接回复仍按原顺序。
- 数据复制和应用缩短之后再决定是否进一步拆组 worker；日志位置、任期和 HW 的修改继续串行。
  Fetch 已允许部分在途访问，不重复把“允许 Fetch”当成新优化。

**验收：** 按操作类型统计 worker 访问和唤醒次数、排队 P95、fiber 数及栈内存；确认吞吐改善，超时和换主无挂起票据。

<a id="follower-received-cache"></a>

### P1-4：复用近期日志批次，减少 follower 读回与重复解码（已实现）

follower 完成完整批次校验和本模式追加后，将不可变接收缓冲及已验证元信息交给有界 effect 缓存。
普通 SET/no-op 借用视图，缺口、控制记录、恢复和超预算继续走 WAL；截断、换任期和快照使缓存失效。
网络输入与磁盘重读仍完整校验，缓存命中不代替实际应用成功；缓存预算不等于全部在途内存上限。
选择依据、正确性和性能结果见[follower 缓存实验](profile/v7-follower-cache.md)。

### P1-5：统一内存预算，缩短副本生命周期（输入所有权转交已实现）

**已完成第一步：** 普通异步 SET 将已解析参数的所有权转交给请求，消除 `AsyncPendingSet` 再复制 key/value，
在 drain 不再引用输入后释放大缓冲；票据反压仍保持到有序回复释放。拒绝入队不能消耗原参数，断连后已接收写仍可独立完成。
实现、测试与性能结论见[v9 实验](profile/v9-input-ownership.md#input-ownership)。
本轮对照和正确性验证已通过；这只是 P1-5 的第一步，全流程内存预算仍未统一，内存测量见实验文件。

**现状：** 提交/排队已有 **8 MiB / 256 笔**限制；每个数据副本的 effect 缓存另有 **8 MiB / 4096 条**上限，
现在同时覆盖 leader 追加和 follower 接收缓冲。
这些是不同阶段的逻辑限制，有的允许空队列接收首个大请求；它们既不是总 RSS 上限，也不能直接相加推算实际占用。
WAL 副本、Fetch 响应、应用参数、确认 fiber 和 allocator 容量仍需另行统计。
共享 effect 缓存按唯一 WAL capacity 记账，诊断采集追加后的预算快照；这不是全流程内存预算。
缓存容量和峰值 RSS 的逐轮测量保存在实验文件中，当前没有全流程内存下降的结论。

- 分别记录逻辑在途字节和唯一缓冲的实际 capacity；共享引用只统计一次，票据/排队指标不能重复累计。
- 输入已转交 WAL 或应用后及时释放大 value，保留未完成票据的反压；缓存及可复用缓冲池均设字节上限和回收策略。
- 优先消除整个临时对象，再消除“清零后立刻覆盖”的初始化；只在能保证完整写入的专用缓冲使用未初始化存储。
  RESP parser 的清零两边都存在，先处理 Raft 额外路径，不贸然改公共参数容器。

**验收：** 分配次数/MiB、分配字节/MiB、在途唯一缓冲峰值、fiber 栈占用和峰值 RSS；慢副本及超时下内存能回落。
进程 RSS、allocator 保留内存、KV 数据和内核 page cache 分开报告，避免把缓存移动误称为内存减少。

### P1-6：将 memory 后台同步与普通追加的占用状态拆开

**代码依据：** 后台同步达到 **64 MiB 或 100 ms**触发，`TryFlushBufferedAsync` 使用同一个 `pending_`，
因此 `RunOnWorker` 的大多数操作仍会等排空。阶段诊断见[初始 trace](replication-observability.md)，排空等待不等于纯设备时间。

- 评估独立的追加状态与 flush 状态：后台同步固定捕获 `(segment, file_end, offset)`，只发布该 cut 的持久标记，
  新追加留到下一轮；不能在旧 FSYNC 完成后把不断增长的当前尾部误报为已持久化。
- 换段、截断、控制记录、快照及存储错误仍需屏障；`flush` 模式的多数持久确认不变。
  此项改动风险较高，排在减少复制、应用和回查之后。

**验收：** 同步期间的追加排队下降；同时验证标记切点、短写、同步失败和故障恢复。不能只凭正常退出后的读回宣布安全。

### P1-7：Fetch 直接构造完整响应帧（已实现）

按同一 schema 只测量字段长度，一次 reserve 后直接写完整 Fetch response，避免 body → frame 副本和尾部扩容复制。
必要的 records 复制保留；其他 RPC 走原路径，版本校验、null/empty、tag 冲突与失败时输出不变语义保持一致。
方案依据和实测结果见[Fetch 封帧实验](profile/v8-fetch-frame.md)。

后续若共享 parked Fetch 缓冲或采用分散缓冲发送，需另行处理所有权、部分发送、背压和内存预算。


## 3. 验证顺序与收益记录

1. 先增加按阶段的复制/分配计数及 apply 子阶段：WAL 读取、CRC/解码、owner 排队、实际 KV 应用。
   已增加进程 RSS 和应用读取/解码、owner 排队/批次 trace；复制/分配字节尚未完整采集。
   内存诊断单独运行，不把 profiler 开销混入正式吞吐。
2. 使用最新实现作为增量基线，按剩余路径选择下一项，不重复计算已消除的读回与封帧复制成本。
   逐轮数据和结论只在对应实验文件中维护；当前状态在本页更新。
3. 正式性能仍用未插桩 Release，先测 **1 连接、pipeline 64**，相同数据量、value 大小及 SET + WAIT 1 口径，
   交错运行至少三轮，记录中位数和范围。没有收益就停止扩展连接数；确认收益后只扩到 **2、8**，不恢复 64。
4. 每轮保留原 cluster 对照，完成离线副本重启、WAIT 2 和全部值读回；另按改动覆盖同 key/条件写、段边界、
   慢副本、换主、截断和故障注入。共享路径改动也需确认 `flush` 模式正确性。
5. 记录：确认吞吐、批次 P95、CPU ms/MiB、复制/分配字节比、临时内存峰值、worker 排队及每千笔同步次数。
   只有实测后填写提升百分比；不以栈变短或单个热点消失代替整体收益。

继续根据最新 trace 和火焰图选择优化项，不以栈层数、排队次数或单个热点直接推算吞吐收益。

<a id="experiments"></a>

## 实验索引

每个实验的测试数据、火焰图、trace 结论和验证范围单独保存；原始采集文件继续放在 Git 忽略的 `benchmarks/` 下。

- [v3：初始基线与诊断](profile/v3-baseline.md)
- [v4：共享 WAL 缓冲与直接编码](profile/v4-wal-buffer.md)
- [v5：批量应用与借用参数](profile/v5-batched-apply.md)
- <a id="v6-drain-experiment"></a>[v6：直接聚合编码与 effect/WAL 共享](profile/v6-drain-sharing.md)
- <a id="v7-follower-experiment"></a>[v7：follower 复用接收批次](profile/v7-follower-cache.md)
- <a id="v8-frame-experiment"></a>[v8：Fetch 直接封帧](profile/v8-fetch-frame.md)
- <a id="v9-input-experiment"></a>[v9：SET 输入所有权转交](profile/v9-input-ownership.md)（当前保留实现）
- <a id="v10-receipt-experiment"></a>[v10：follower 接收确认快路径](profile/v10-follower-receipt.md)（最新实验，无收益，已回退）
