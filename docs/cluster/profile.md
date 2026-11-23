# raft.memory / 原 cluster profile 与优化项

| 版本 | 确认吞吐 MiB/s | 对比 |
| --- | ---: | ---: |
| 原始 `raft.memory`（profile 优化前） | 169.56 | — |
| 最新 `raft.memory`（以下优化全部包含） | **325.31** | **+91.9%** |
| 原 cluster（最新同轮对照） | 606.05 | 仍为最新 Raft 的 1.86 倍 |

最新版本包含的优化项：

- **P0-1：减少编码与缓冲复制**。共享不可变 WAL、单条 SET 直接编码、drain 一次预分配并直接聚合，effect 与 WAL 共用缓冲。
- **P0-2：有界批量应用与借用参数**。多笔已提交 effect 一次投递 owner，SET 同步借用 key/value，减少往返、复制和清零。
- **P1-4：follower 复用已验证的接收批次**。普通 SET/no-op 从共享缓冲应用，省去本轮负载的 WAL 读回、重复 CRC/解码和 value 副本。
- **P1-7：Fetch 直接封帧**。只计算字段长度后一次预留，去掉 body → frame 副本和尾部扩容复制。

均为 1 连接、pipeline 64、未插桩三轮中位数，计时包含 SET + 原写连接 WAIT 1。
**+91.9% 是原始基线到最新版本的跨轮累计对比**，包含上述全部优化及 WAIT 等待变化；各阶段增量不能相加。
本轮确认吞吐中位数 +8.6%，但范围重叠、第三组几乎持平；逐笔 shard 执行等成本仍存在。
逐轮数据与结论见[实验索引](#experiments)；最新火焰图与 trace 见 [v8 实验](profile/v8-fetch-frame.md)，完整口径见 [benchmark](replication-benchmark.md)。

## 1. 初始诊断依据（优化前 v3）

以下采用 2026-11-23 的 `memory-c1-p64-v3` / `cluster-c1-p64-v3` 初始采样，作为选择优化项的依据。
同机、同一插桩二进制（SHA-256 前缀 `72f222abed27`）、1 连接、pipeline 64、256 MiB、256 KiB/value；
三副本停一台，计时包含原写连接 `WAIT 1`。恢复、`WAIT 2` 和 1088 个值读回均通过。

| 指标 | raft.memory | 原 cluster |
| --- | ---: | ---: |
| 未插桩三轮确认吞吐中位数 | 169.56 MiB/s | 628.87 MiB/s |
| 插桩 SET + WAIT 1 | 1512.14 ms | 415.31 ms |
| leader CPU 样本 / 估计 CPU 时间 | 628 / 1256 ms | 191 / 382 ms |
| leader memcpy | 366 样本 / 732 ms | 116 样本 / 232 ms |
| leader memset | 66 样本 / 132 ms | 30 样本 / 60 ms |
| leader CRC32::Extend | 45 样本 / 90 ms | 未采到 |
| follower CPU 样本 / 估计 CPU 时间 | 253 / 506 ms | 70 / 140 ms |

- Raft leader 总 CPU 为 **3.29 倍**；`memcpy + memset` 的绝对差约 **572 ms**，占 CPU 差值的约 **65%**。
  两边 memcpy 百分比都接近 60%，只比较百分比会掩盖差距。
- `AppendSetBatch` 累计占 leader 样本 **36.0%**，其中 `MakeSetEffectBatch` 为 **23.1%**。
  父子栈包含关系不能相加，也不能把这些 CPU 比例直接换算为吞吐提升。
- trace 区间并集：leader 编码 **475 ms**、worker 排队 **588 ms**、应用 **237 ms**；follower 应用 **369 ms**。
  应用内部的 `journal.apply` 分别为 **111 / 98 ms**，原 cluster follower 为 **47 ms**。
  外层还包含日志读取、解码和跨线程等待；各阶段重叠，差值不能直接认定为某一种开销。
- 初始 1024 笔 SET 对应 leader **5106 次** worker 访问、**2650 次**等待 I/O 排空；
  次数也包含复制等操作，不能全部归因于 SET。
- CPU 采样为 500 Hz，时间是样本估计，包含多线程；火焰图没有测量分配字节、峰值 RSS 或实际内存带宽。
  “复制多”已有证据，“内存占用降低多少”仍需补测。
- `memory` 仍写 page cache、后台同步；每笔 SET 仍需多数接收并完成本地应用。原 cluster 的 SET 先回复本地执行结果，
  再由末尾 WAIT 确认副本应用。总计时包含副本确认，但两者逐笔确认和持久性语义仍有区别。

证据入口：[Raft 火焰图](http://172.31.23.2:3137/memory-c1-p64-v3/index.html)、
[原 cluster 火焰图](http://172.31.23.2:3137/cluster-c1-p64-v3/index.html)、
[trace 结论及链接](replication-observability.md)、[未插桩 benchmark](replication-benchmark.md)。
原始 `*.folded`、`stages.csv` 和 `summary.json` 位于 `benchmarks/observability-20261123/`，由 Git 忽略。

## 2. 路径差异：哪些工作可以减少

| 阶段 | 原 cluster | raft.memory | 优化方向 |
| --- | --- | --- | --- |
| 接收 | RESP 参数 → SET 执行 | RESP 参数 → 异步票据 → 排队 | 一份输入所有权，缩短大 value 存活期 |
| 复制准备 | shard journal 编码并流式发送 | effect → Kafka batch → drain 聚合 → WAL 缓冲 | 直接编码到最终缓冲，传递所有权 |
| 确认 | 本地 SET 回复；WAIT 追踪副本应用 | WAL 接收 → follower Fetch → HW → 本地应用 → SET 回复 | 保留确认条件，批量推进完成通知 |
| 应用 | replica flow 消费 journal 并执行 | 组 worker → owner 批量 → 借用 SET 参数 → 命令分发 → shard | 继续减少通用分发和逐笔 shard 等待 |
| 重放数据 | 接收的 journal 数据 | leader/follower 普通 effect 复用不可变 WAL 缓冲，缺口从 WAL 重放 | 保留有界缓存及生命周期校验，继续减少 Fetch 副本 |

Raft 的任期、日志顺序、HW 和恢复边界仍由 Raft 负责；借鉴原 cluster 的数据处理方式，保持两种集群设计独立。

## 3. 优化项与当前状态

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

### P0-1b：follower 接收确认快路径（待验证候选，未实现）

**问题：** `ReceiveFetchResponse` 已完成接收追加，但下一次报告接收位置的 Fetch 仍经过
`RunPeer → RunOnWorker → Next → Tick → Synchronize → apply_`。
`RunOnWorker` 的统一 pending 检查还会等待 memory 后台 fsync。
于是已经满足本模式确认条件的位置，要等待状态机应用或后台同步才有机会报告给 leader。

**目标：** 让 follower 及时发起携带已发布 `End()` 的下一次 Fetch；已提交数据的应用与网络等待重叠。
先限定 follower 普通数据路径，保持组内日志/任期/HW 状态串行更新；leader 的 SET 仍等本地应用完成才回复。
接收位置、持久位置、已应用位置分别使用，WAIT 继续根据 applied tag 确认副本应用。

- 准备 Fetch 时读取已发布的位置；已接收位置的报告不依赖后续 apply 或 memory 后台 fsync。
  不能直接给所有 `RunOnWorker` 设置 `allow_pending=true`，也不能提前报告尚未完成的追加。
- 将应用组织为有界、按日志顺序的任务；组 worker 不同步等待整个 owner/shard 应用批次。
  完成后由组 worker 推进连续 applied 前缀。换主、控制记录、截断、快照、迁移和存储失败保留屏障。
- 这与 P1-6“允许追加和 fsync 并行”不同：先优化已经接收的位置报告，不改变持久标记切点。
  P1-4 已独立验证接收缓冲复用，减少了读回、清零及重复解码；后续评估须使用最新实现作基线，不能重复计算收益。

**依据与收益边界：** 接收后的串行等待及其与 leader 工作的重叠，见[确认路径复核](profile/v5-batched-apply.md#confirmation-path)。
先补齐 Fetch 实际发送、leader 接纳位置与 HW 推进的关联事件，再决定是否完整异步化；不能把累计等待直接算成可兑现收益。

**验收：** 先补齐接收完成、Fetch 实际发出、leader 接纳 offset/HW 的关联事件，核对确认等待是否消失；
再做 1 连接 / pipeline 64 三轮未插桩对照及原 cluster 对照，保持 SET + 原连接 WAIT 1 口径。
验证 applied 不超过 HW、同 key 顺序、慢应用反压、换主/截断/快照和关闭；flush 模式仍按持久位置确认。
按唯一缓冲 capacity 核对内存，完成离线副本恢复、WAIT 2 和全部值读回。

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
调用次数与批次末尾回复等待的区别，见[确认路径复核](profile/v5-batched-apply.md#confirmation-path)。

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

### P1-5：统一内存预算，缩短副本生命周期

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


## 4. 验证顺序与收益记录

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

- [v4：共享 WAL 缓冲与直接编码](profile/v4-wal-buffer.md)
- [v5：批量应用与借用参数](profile/v5-batched-apply.md)

<a id="v6-drain-experiment"></a>

- [v6：直接聚合编码与 effect/WAL 共享](profile/v6-drain-sharing.md)

<a id="v7-follower-experiment"></a>

- [v7：follower 复用接收批次](profile/v7-follower-cache.md)

<a id="v8-frame-experiment"></a>

- [v8：Fetch 直接封帧](profile/v8-fetch-frame.md)
