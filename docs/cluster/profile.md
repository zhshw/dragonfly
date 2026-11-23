# raft.memory / 原 cluster profile 与优化项

| 版本 | 确认吞吐 MiB/s | 对比 |
| --- | ---: | ---: |
| 原始 `raft.memory`（profile 优化前） | 169.56 | — |
| 最新 `raft.memory`（以下优化全部包含） | **300.25** | **+77.1%** |
| 原 cluster（最新同轮对照） | 607.99 | 仍为最新 Raft 的 2.03 倍 |

最新版本包含的优化项：

- **P0-1：减少编码与缓冲复制**。共享不可变 WAL、单条 SET 直接编码、drain 一次预分配并直接聚合，effect 与 WAL 共用缓冲。
- **P0-2：有界批量应用与借用参数**。多笔已提交 effect 一次投递 owner，SET 同步借用 key/value，减少往返、复制和清零。
- **P1-4：follower 复用已验证的接收批次**。普通 SET/no-op 从共享缓冲应用，省去本轮负载的 WAL 读回、重复 CRC/解码和 value 副本。

均为 1 连接、pipeline 64、未插桩三轮中位数，计时包含 SET + 原写连接 WAIT 1。
**+77.1% 是原始基线到最新版本的跨轮累计对比**，包含上述全部优化及 WAIT 等待变化；各阶段增量不能相加。
最新一轮 P95 有回退样本，RSS 尚未改善；Fetch 封帧复制和逐笔 shard 执行仍存在。
逐轮对照保留在后文；最新火焰图与 trace 见 [第 10 节](#v7-follower-experiment)，完整口径见 [benchmark](replication-benchmark.md)。

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

### P0-1：一次编码，传递同一个不可变 WAL 缓冲（已实验，收益有限）

**代码依据：** [data_node.cc](../../src/server/cluster/raft/runtime/data_node.cc) 的 `AsyncPendingSet` / `AppendSetBatch`，
[set_effect.cc](../../src/server/cluster/raft/data/set_effect.cc) 的 `MakeSetEffectBatch`，
[record_batch.cc](../../src/server/cluster/raft/log/record_batch.cc) 的 `PutRecord` / `BuildBatch`，
[fetch_replication.cc](../../src/server/cluster/raft/quorum/fetch_replication.cc) 的 `Append`，
[partition_log.cc](../../src/server/cluster/raft/log/partition_log.cc) 的 `TryAppendAsync`。

优化前 value 经过票据、`SetEffect`、DFKV payload、record body、records、Kafka batch；drain 又拼接各 batch。
聚合后的 `records` 再复制到 `shared_ptr<string> payload` 和 `PendingAppend::bytes`。
首个 batch 已使用 move，但后续 append、扩容和异步提交仍可能复制整段。

- **已实现：** 聚合结果通过 move 向下传递，追加回调和在途 I/O 引用同一个 `shared_ptr<const string>`。
  回退同步追加仍持有该缓冲；短写及完成回调中可用，成功或失败结束后释放。
- **已实现：** `MakeSingleRecordBatch` 精确预留最终空间，DFKV 头、key、value 直接编码为一个 batch，
  去掉三个中间大字符串。保留旧通用编码器作为逐字节对照，日志格式和 CRC 不变。
- **本轮已实现：** drain 一次预留完整容量，直接编码到最终聚合缓冲，移除逐条 batch 的追加和扩容复制。
  effect 缓存持有该不可变缓冲的所有权及 key/value 视图，不再另存大 value；视图不引用请求临时数据。
- **v5 选定目标：** `SetEffect` 构造和 drain 追加/扩容占 leader 69 个 memcpy 样本，约 138 ms。
  实现后 v6 确认编码与复制下降，但吞吐仅 +4.2%、P95 变差，详见第 9 节。

**本轮实现：** 先按实际编码长度、段边界和预算选定连续前缀，直接编码进一个最终 WAL 缓冲；
冻结后由 I/O 与 effect 缓存共享所有权，effect 只保存元信息及 key/value 范围。
缓存按唯一 capacity 计入 8 MiB 预算，最后一条 pending 引用移除后扣除；超预算继续从 WAL 重放。
少量剩余 effect 仍可能保留整个大批次，正在应用和 I/O 的引用也另有生命周期，因此不能把缓存上限等同于全部临时内存。
每条日志的字节格式、CRC、段边界和确认顺序保持不变。

**原选择依据：** v5 的 effect 构造约 **70 ms**、drain 追加/扩容约 **68 ms**，合计对应 leader CPU 样本的 **15.6%**。
这些复制位于日志提交前的组 worker 路径，源码可明确定位；单条写入最终编码的约 46 ms 必要复制不计入目标。
这是待减少工作的采样规模，不能换算为吞吐提升。直接聚合与完整共享版已分别对照，单独直接聚合尚无稳定收益。

**验证：** 编码字节、CRC、段截取、短写和共享生命周期回归通过；编码 CPU 下降。
每 MiB 复制/分配字节尚未完整采集，进程峰值 RSS 未下降，内存优化目标尚未达成。

<a id="drain-preallocation"></a>

### P0-1a：drain 合批缓冲预分配（已由直接聚合覆盖）

- **原问题：** `records.append` 聚合已编码 batch 时反复扩容；v5 扩容搬运旧内容有 **24 个样本，约 48 ms CPU**，
  占 leader CPU 样本的 **5.4%**，不足以支持较大吞吐收益的预期。
- **已实现：** 按编码长度、预算和段边界选定连续前缀后一次 reserve，所有记录直接写入最终缓冲。
  同 key 顺序、条件写屏障和确认语义不变，预留后追加不移动缓冲的测试通过。
- **实验边界：** 本轮阶段一同时包含预分配和直接聚合，未单独测试“只加 reserve”。
  直接聚合两组对照分别为 +1.8% / −1.2%，不能认定稳定收益，更不能把它归为纯预分配的收益。

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
- 这与 P2-6“允许追加和 fsync 并行”不同：先优化已经接收的位置报告，不改变持久标记切点。
  P1-4 已独立验证接收缓冲复用，减少了读回、清零及重复解码；后续评估须以 v7 为基线，不能重复计算收益。

**证据：** v5 的 134 个有数据响应之后，到下一次 Fetch RPC 开始累计 **227.68 ms**；
其中 apply **140.67 ms**、后台 fsync **80.47 ms**，区间合并后覆盖 **97.1%**。
这是已定位的串行等待，尚不是可兑现的吞吐收益；与 leader 工作的重叠及最终 WAIT 仍会影响结果。

**收益判断：** 尚不能确认有较大吞吐收益。上述间隔有 **142.42 ms（62.6%）** 与 leader 的编码、
应用、worker 操作或 I/O 阶段重合，其中与 leader 自身 fsync 重合 **69.95 ms**。
余下 **85.26 ms** 也不能等同于 leader 空闲或可节省时间，尚缺少网络发送及 HW 推进的关联证据。
保留为待验证候选；先确认关键路径，再决定是否进行完整应用异步化，当前不作为首个实施项。

**验收：** 先补齐接收完成、Fetch 实际发出、leader 接纳 offset/HW 的关联事件，核对确认等待是否消失；
再做 1 连接 / pipeline 64 三轮未插桩对照及原 cluster 对照，保持 SET + 原连接 WAIT 1 口径。
验证 applied 不超过 HW、同 key 顺序、慢应用反压、换主/截断/快照和关闭；flush 模式仍按持久位置确认。
按唯一缓冲 capacity 核对内存，完成离线副本恢复、WAIT 2 和全部值读回。

### P0-2：批量应用，逐步缩短通用 SET 执行路径

**已实现并通过验证：** 连续已提交替换按最多 32 笔 / 1 MiB 投递 owner，一笔超过预算时单独执行；
owner 每完成一笔检查 1 ms 预算，超时返回已完成前缀；单笔执行不能被强行中断。控制、no-op、迁移记录先排空批次，日志游标与 slot generation
仍由组 worker 更新；执行失败只推进成功前缀。`JournalExecutor::ExecuteSet` 同步借用 key/value，
省去 `BackedArguments` 的大块复制和清零，继续执行原有 SET 事务。新增 owner 排队、批次、WAL 读取及解码 trace。
相对已优化的上一轮 `encoded-bin`，1 连接 / pipeline 64 三轮确认吞吐增量提高 8.8%。

**优化前的代码路径：** `DataNodeImpl::OpenPartition` 每条 effect 调用一次 `owner_->Await`；
[set_executor.cc](../../src/server/cluster/raft/data/set_executor.cc) 的 `ExecuteSetEffect` 构造 `CmdData`，
再经 `JournalExecutor`、`Service::DispatchCommand` 执行 SET。
[BackedArguments::Assign](../../src/common/backed_args.h) 会 resize 存储后再复制参数。

- **已完成第一阶段：** 一次投递有界连续 effect，在 owner 上顺序执行；原有 SET 的 TTL 清除、OOM、类型覆盖和 DB 选择均保留。
- **后续待评估：** 抽取可复用的内部“替换 value”入口，进一步省去命令分发和捕获 OK；复用现有 shard 操作，
  保留 TTL 清除、内存检查、统计、索引和变更通知，不能直接绕过 DbSlice/事务约束。
- 用笔数、字节数和执行时间限制每批占用；同 key 顺序、条件写、迁移记录及快照边界保持屏障。
  若进一步按 shard 并行，只能公布全部完成的连续 applied 前缀；部分失败不能推进游标或提前回复。

**验收：** Await/每千笔、应用 CPU、`raft.apply` 和临时参数分配下降；先核对同 key 覆盖、NX/XX、TTL、OOM 和重放一致性。

### P1-3：按 applied offset 批量完成票据，减少 worker 回查

**代码依据：** `TrySet` 为每笔创建确认 fiber；`WaitApplied → VisitUntil → RunOnWorker` 查询应用进度。
初始 v3 的 1024 笔 SET 对应 leader **5106 次** worker 访问、**2650 次**等待 I/O 排空。
这些次数也包含复制等操作，不能全部归因于 SET。当前已经是事件等待，不是忙轮询。
v5 的 `WaitApplied` 仍经默认 `allow_pending=false` 的 `RunOnWorker`；即使只是查询已应用前缀，
也要先等当前追加或后台同步结束。批量应用减少的是 owner 投递，并未移除此处等待。

- 按 `(epoch, end_offset)` 登记待完成票据，由应用进度一次完成连续前缀，减少每笔唤醒后重新排队查询。
- 角色/任期变化、存储失败、超时和关闭必须完成或取消相应票据；连接回复仍按原顺序。
- 数据复制和应用缩短之后再决定是否进一步拆组 worker；日志位置、任期和 HW 的修改继续串行。
  Fetch 已允许部分在途访问，不重复把“允许 Fetch”当成新优化。

**验收：** 按操作类型统计 worker 访问和唤醒次数、排队 P95、fiber 数及栈内存；确认吞吐改善，超时和换主无挂起票据。

<a id="follower-received-cache"></a>

### P1-4：复用近期日志批次，减少 follower 读回与重复解码（已实现并实测）

**代码依据：** `CommittedEffects::ApplyToBatched` 在未命中 effect 缓存时用 `log.Read(..., 1, ...)` 逐 batch 读回，
随后 `DecodeRecordBatch → DecodeSetEffect`；后者重新复制 key/value。
`PartitionLog::Read` 先创建清零字符串再 pread；leader Fetch 也走此入口。
leader 已有 parked Fetch 和在途 `WrittenRecords` 快路径；v5 中这两条路径的复制与 follower 读回更值得优先处理，
不能把 leader 的全部 Fetch 都算成磁盘重读。

**v6 选择依据：** 完整共享版 follower 仍执行 1024 次 WAL 读回和两层解码，区间并集 **161.33 ms**，
占外层 apply 的 **55.0%**。按日志 offset 重新关联后，其中 **112.90 ms** 位于收到新日志之后、
下一次报告接收位置的 Fetch RPC 发起之前。这项既延续 P0-1 的缓冲共享，也直接缩短 P0-1b 路径中的重复处理。
它不能代替接收确认流水化，但可以先复用现有有界缓存和应用路径，不必一并改造组 worker 调度。

**本轮实现与验证范围：**

1. 保留 follower 接收缓冲的不可变所有权；`AppendAsFollower` 完成完整批次校验和本模式的写入确认后，
   将同一缓冲的 batch/record 元信息交给数据组缓存。只接纳实际追加的完整前缀，排除半包、未来任期及失败追加。
2. 普通 SET/no-op effect 使用 `SetEffectView` 引用该缓冲，不再为应用创建第二份 key/value。
   应用时继续检查 group、slot generation 和迁移屏障；控制/迁移记录、缓存缺口和恢复保留现有 WAL 重放路径。
3. 复用缓存的唯一 capacity 预算；明确处理截断、换主和快照安装的失效，不能只用 offset 判断缓存有效。
   日志截断和快照替换增加内容代次，下一次缓存接入/应用时清理旧代次；任期改变仍保守清理缓存。
   少量未应用记录可能持有整块 Fetch 缓冲，记录缓存容量、回退次数和进程 RSS。
4. 增量对照使用本轮完整共享版 `c4b918f2a1ff…`；仍测 1 连接 / pipeline 64、三轮 SET + 原连接 WAIT 1，
   9 轮均通过原写连接 WAIT 1、离线副本恢复、WAIT 2 和全值读回。本次没有扩展连接数。

**实测结果：** 确认吞吐 **+19.1%**，SET 阶段 **+7.2%**，其余改善包含末尾 WAIT 少等一轮重查。
单次诊断的 follower 1024 笔全部命中缓存，WAL 读回/重复解码为零，CPU 估计 **−35.9%**；
峰值 RSS **+3.6%**，不能称为内存占用改善。正确性与尾延迟边界见 [第 10 节](#v7-follower-experiment)。

**原选择依据与收益边界：** 161.33 ms 和 112.90 ms 都是单次 trace 区间，不是可直接省下的端到端时间。
同一次采样中 leader/follower 后台 fsync 和实际 KV 应用仍有等待，不能承诺两位数吞吐提升。
选择它的依据是三类可定位的重复工作和本轮已具备的共享机制；不把 trace 墙钟时间与 P1-7 的 CPU 样本直接比大小。
P1-7 的 Fetch 封帧优化保留为后续独立项，本次选择不混入发送协议改动。
复核脚本与逐间隔数据在 ignored 的 `benchmarks/profile-drain-20261123/v6-critical-review/`。

- leader 原有 `RememberAppended` 与新增 follower 缓存的命中事件均已补齐；本次实测没有 WAL 回退。
- follower 已复用接收至应用期间的不可变 batch，慢副本、预算不足和重启继续从磁盘读取。
  leader 发送 Fetch 时的额外副本仍保留，属于后续独立改动。
- 已传递首次校验的 batch 元信息，网络输入、磁盘重读仍完整校验，没有任意字节接口的“跳过校验”开关。

**验收：** pread 次数/字节、缓存命中率、解码复制字节和 CRC CPU 下降；坏 CRC、部分 batch、换主替换尾部和恢复仍能检出。

### P1-5：统一内存预算，缩短副本生命周期

**现状：** 提交/排队已有 **8 MiB / 256 笔**限制；每个数据副本的 effect 缓存另有 **8 MiB / 4096 条**上限，
现在同时覆盖 leader 追加和 follower 接收缓冲。
这些是不同阶段的逻辑限制，有的允许空队列接收首个大请求；它们既不是总 RSS 上限，也不能直接相加推算实际占用。
WAL 副本、Fetch 响应、应用参数、确认 fiber 和 allocator 容量仍需另行统计。
共享 effect 缓存按唯一 WAL capacity 记账，诊断采集追加后的预算快照；这不是全流程内存预算。
v7 follower 观测到的 pending capacity 最大 7.75 MiB，但峰值 RSS 从 392.21 升至 406.50 MiB；
leader 峰值也从 454.08 升至 465.30 MiB，整体内存改善仍未实现。

- 分别记录逻辑在途字节和唯一缓冲的实际 capacity；共享引用只统计一次，票据/排队指标不能重复累计。
- 输入已转交 WAL 或应用后及时释放大 value，保留未完成票据的反压；缓存及可复用缓冲池均设字节上限和回收策略。
- 优先消除整个临时对象，再消除“清零后立刻覆盖”的初始化；只在能保证完整写入的专用缓冲使用未初始化存储。
  RESP parser 的清零两边都存在，先处理 Raft 额外路径，不贸然改公共参数容器。

**验收：** 分配次数/MiB、分配字节/MiB、在途唯一缓冲峰值、fiber 栈占用和峰值 RSS；慢副本及超时下内存能回落。
进程 RSS、allocator 保留内存、KV 数据和内核 page cache 分开报告，避免把缓存移动误称为内存减少。

### P2-6：将 memory 后台同步与普通追加的占用状态拆开

**代码依据：** 后台同步达到 **64 MiB 或 100 ms**触发，`TryFlushBufferedAsync` 使用同一个 `pending_`，
因此 `RunOnWorker` 的大多数操作仍会等排空。初始 v3 的 leader FSYNC 区间约 **138 ms**，等待排空约 **405 ms**；
后者包含调度及其他 I/O，不能全部算成刷盘时间。

- 评估独立的追加状态与 flush 状态：后台同步固定捕获 `(segment, file_end, offset)`，只发布该 cut 的持久标记，
  新追加留到下一轮；不能在旧 FSYNC 完成后把不断增长的当前尾部误报为已持久化。
- 换段、截断、控制记录、快照及存储错误仍需屏障；`flush` 模式的多数持久确认不变。
  此项改动风险较高，排在减少复制、应用和回查之后。

**验收：** 同步期间的追加排队下降；同时验证标记切点、短写、同步失败和故障恢复。不能只凭正常退出后的读回宣布安全。

### P1-7：Fetch 直接构造完整响应帧，减少发送前复制（v5 新增）

**代码依据：** [flexible_message.h](../../src/server/cluster/raft/protocol/flexible_message.h)
的 `Writer::Field(RecordBytes)` 将日志复制进消息 body；尾部 `Tags → Varint → push_back` 还可能触发 body 扩容。
[rpc_frame.cc](../../src/server/cluster/raft/protocol/rpc_frame.cc) 的 `WriteResponse → WriteFrame`
随后把完整 body 再复制进最终 frame。v5 合计 56 个 memcpy 样本，约 112 ms，占 leader CPU 样本的 12.6%。
其中单缓冲方案直接针对 body → frame 的 24 个样本及尾部扩容的 10 个样本，合计约 **68 ms / 7.7%**；
records 写入最终帧的 22 个样本仍对应必要复制，不能把全部 112 ms 都算作可删除开销。

- 先精确计算或可靠预留完整帧容量，保留长度前缀和 header 空间，让 body 直接写进最终帧，再回填长度。
  独立测量“避免扩容”和“去掉 body → frame 副本”，保留原接口在编码失败时的输出语义。
- 与 P1-4 配合，让 parked Fetch 引用在途 WAL 的不可变范围，避免发布和填充 response 时再各复制一次。
  进一步采用分散缓冲发送时，再处理 buffer 所有权、部分发送和背压；先完成单缓冲版本。

**验收：** Fetch 各协议版本逐字节一致，frame 长度、tag、空/null records、截断输入和错误返回正确；
发送前复制字节、分配和 leader CPU 下降，原连接 WAIT 与恢复检查通过。

## 4. 验证顺序与收益记录

1. 先增加按阶段的复制/分配计数及 apply 子阶段：WAL 读取、CRC/解码、owner 排队、实际 KV 应用。
   已增加进程 RSS 和应用读取/解码、owner 排队/批次 trace；复制/分配字节尚未完整采集。
   内存诊断单独运行，不把 profiler 开销混入正式吞吐。
2. 共享 WAL、单条直接编码及 owner 批量应用已完成第一阶段；P0-1 直接聚合与 P1-4 follower 缓存均已完成实验。
   v7 确认吞吐 +19.1%，但包含 WAIT 重查时机改善，P95 仍有回退、RSS 未下降。本轮只测 1 连接；
   P0-1b 的完整流水化仍未实施，后续按 v7 剩余路径选择，不能继续使用优化前的读回成本预测收益。
3. 正式性能仍用未插桩 Release，先测 **1 连接、pipeline 64**，相同数据量、value 大小及 SET + WAIT 1 口径，
   交错运行至少三轮，记录中位数和范围。没有收益就停止扩展连接数；确认收益后只扩到 **2、8**，不恢复 64。
4. 每轮保留原 cluster 对照，完成离线副本重启、WAIT 2 和全部值读回；另按改动覆盖同 key/条件写、段边界、
   慢副本、换主、截断和故障注入。共享路径改动也需确认 `flush` 模式正确性。
5. 记录：确认吞吐、批次 P95、CPU ms/MiB、复制/分配字节比、临时内存峰值、worker 排队及每千笔同步次数。
   只有实测后填写提升百分比；不以栈变短或单个热点消失代替整体收益。

暂不优先扩大 ring、继续增加连接数或仅替换 CRC：初始 v3 的 Folly 队列并集为 **2.57 ms**，
v5 leader / follower 为 **3.90 / 0.46 ms**，
数据 follower 未出现 Fetch 后 25 ms 退避，两边最终 WAIT 都约 101 ms。
CRC 指令优化仍可作为后续独立对照，但无法消除当前的多份缓冲和逐笔 worker 往返。

## 5. 上轮复采：复制下降，应用与 RSS 尚未改善（v4）

同一负载分别复采优化前、优化后及原 cluster，三轮均通过恢复和逐值读回。
旧诊断二进制为 `72f222abed27…`，新诊断二进制为 `9371d68cbce0…`；正式吞吐仍采用上面的三轮未插桩结果。
按角色对照：旧 Raft leader 为 node 11，新 Raft leader 为 node 12，不能直接比较同编号节点。

| 单次诊断指标 | 优化前 raft.memory | 优化后 raft.memory | 原 cluster |
| --- | ---: | ---: | ---: |
| leader CPU 估计时间 | 1220 ms | 924 ms（−24.3%） | 396 ms |
| leader memcpy | 732 ms | 420 ms（−42.6%） | 212 ms |
| leader memset | 130 ms | 132 ms | 58 ms |
| leader CRC32::Extend | 58 ms | 100 ms | 未采到 |
| leader 编码区间并集 | 451.06 ms | 231.41 ms（−48.7%） | — |
| leader worker 排队区间并集 | 567.22 ms | 358.21 ms（−36.8%） | — |
| leader 应用区间并集 | 240.70 ms | 245.33 ms | — |
| follower 应用区间并集 | 376.59 ms | 388.88 ms | journal.apply 42.82 ms |
| leader 进程峰值 RSS | 431.63 MiB | 434.06 MiB | 656.84 MiB |
| follower 进程峰值 RSS | 394.25 MiB | 393.53 MiB | 373.64 MiB |

- 两版都是 176 轮编码、每节点 1024 次 journal 应用。编码和 memcpy 明显下降，支持此次优化的作用方向；
  单次采样中的 CRC 波动不代表吞吐收益，不同阶段也不能相加。
- memset、应用耗时及进程峰值 RSS 没有明显下降。**本轮确认的是复制工作减少，尚不能宣称整体内存占用下降。**
  RSS 包含 KV、运行时及 allocator 保留内存，峰值覆盖启动、预热和测量；原 cluster 的较高 RSS 也不能直接归因于某个缓冲。
- 该版 leader 仍有 358 ms worker 排队和 245 ms 应用，follower 应用约 389 ms；因此本轮优先处理 P0-2 的批量应用，
  并继续拆分应用内部等待。Raft 整体应用与原 cluster 的 journal.apply 边界不同，不能直接按两者比值推算收益。

| 报告 | 火焰图、时间线与内存 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 优化前 raft.memory | [v4 baseline](http://172.31.23.2:3137/memory-baseline-c1-p64-v4/index.html) | [1626 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/5a837d3e34025f6d8d819db22ad17921) |
| 优化后 raft.memory | [v4 encoded](http://172.31.23.2:3137/memory-encoded-c1-p64-v4/index.html) | [1604 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/00c7a5bb2afc56628e8dff3542abdef1) |
| 原 cluster | [v4 cluster](http://172.31.23.2:3137/cluster-c1-p64-v4/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/4ab7ec022a93519f896c78824daea85e) |

完整事件分别为 23357 / 22135 / 2942 条，无丢失；Langfuse 展示各阶段最长 50 条，报告统计使用全部事件。
原始结果保存在本地和测试机 `docs/cluster/benchmarks/profile-buffer-20261123/`，已由 Git 忽略。

## 6. 上轮复采：应用往返下降，RSS 尚未下降（v5）

对照上一轮直接编码版与本轮批量应用版，仍为 1 连接、pipeline 64、256 MiB、256 KiB/value。
三个诊断负载都完成 WAIT 1、离线副本恢复、WAIT 2 和全部值读回；正式吞吐三轮未插桩结果为本轮增量 **+8.8%**，
相对原始 169.56 MiB/s 基线累计 **+32.5%**（跨轮比较）。
旧/新 Raft leader 均为 node 12，原 cluster leader 为 node 11。

| 单次诊断指标 | 上一版 raft.memory | 本轮 raft.memory | 原 cluster |
| --- | ---: | ---: | ---: |
| leader 应用区间并集 | 244.32 ms | **151.33 ms（−38.1%）** | — |
| follower 应用区间并集 | 386.30 ms | **306.41 ms（−20.7%）** | journal.apply 39.78 ms |
| owner 投递次数，leader / follower | 1024 / 1024¹ | **368 / 386** | — |
| leader worker 排队区间并集 | 388.24 ms | 301.37 ms | — |
| leader CPU 估计时间 | 902 ms | 886 ms | 386 ms |
| follower CPU 估计时间 | 444 ms | 394 ms | 126 ms |
| leader memcpy / memset | 438 / 122 ms | 440 / 86 ms | 218 / 56 ms |
| follower memcpy / memset | 92 / 106 ms | 100 / 50 ms | 44 / 54 ms |
| leader 进程峰值 RSS | 437.36 MiB | 434.07 MiB | 653.91 MiB |
| follower 进程峰值 RSS | 393.16 MiB | 395.93 MiB | 373.30 MiB |

¹ 旧版每条 effect 对应一次 `owner_->Await`，由旧调用点与每节点 1024 次 `journal.apply` 核对；
新次数直接来自 `raft.apply.owner_batch`。两版都实际执行 1024 笔 SET，减少的是投递次数，未合并或跳过 KV 写入。

- **每千笔 owner 投递降为 359 / 377 次**，减少约 64% / 62%。256 KiB/value 连同日志头后，1 MiB 批次最多装 3 笔。
  follower 有部分批次触及 1 ms 预算；`arg0` 是待执行笔数，剩余前缀再次投递会重复计入，不能累加它作为完成笔数。
- **清零工作减少，整体复制与 RSS 尚未明显改善。** leader memset 样本约下降 29.5%，follower 约下降 52.8%；
  memcpy 没有下降，leader 总 CPU 也基本持平。RSS 包含 KV 和 allocator 保留内存；批量暂存还会延长部分 effect 的存活期。
  32 笔 / 1 MiB 约束的是投递批次，读取时可额外持有一笔前探记录，不能当成全部临时内存的硬上限。
- **保留的瓶颈更清楚了。** 新 follower 仍有 1024 次 WAL 读回，读取、batch 校验/解码、effect 解码分别约
  64.71 / 41.47 / 57.14 ms。leader 编码约 243.28 ms，worker 访问从 5035 次变为 5400 次；
  批量 owner 投递没有消除确认票据的 worker 回查。P1-4 的近期不可变批次复用与 P1-3 的批量完成票据仍待实施，
  与剩余编码/发送复制一起排序，见第 7 节。
- 新 leader / follower 的 `journal.apply` 为 89.39 / 121.06 ms，旧版为 73.45 / 67.75 ms；
  这个内部阶段未缩短，不能把外层应用的全部改善归为 KV 执行加速。它包含等待，后续还需区分 shard 调度与实际执行。
- CPU 只是一轮 500 Hz 采样；原 cluster follower 仅 63 个样本，细分热点不稳定。新版本增加了 apply 子阶段插桩，
  完整事件数增加不代表业务调用增加；不同阶段存在包含/重叠，不能相加。

**正确性：** Debug、Release、ASAN/UBSAN 各 19 项单测通过，覆盖缓存缺口、笔数/字节预算、控制/no-op/迁移屏障、
部分失败前缀、时间预算返回、同 key 顺序、TTL、二进制/空值、DB 切换和 OOM。12 项集群回归覆盖 flush/memory pipeline、
条件写、断连/关闭、换主、崩溃恢复、快照追赶和迁移恢复；9 轮正式 benchmark 全部读回一致。

| 报告 | 火焰图与时间线 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 上一版 raft.memory | [v5 encoded](http://172.31.23.2:3137/memory-encoded-c1-p64-v5/index.html) | [1607 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/47cede02d05c56b698351ceb37528bf4) |
| 本轮 raft.memory | [v5 apply](http://172.31.23.2:3137/memory-apply-c1-p64-v5/index.html)、[leader 火焰图](http://172.31.23.2:3137/memory-apply-c1-p64-v5/leader-node-12.svg) | [1945 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/03172858182454468f874daca442a8e1) |
| 原 cluster | [v5 cluster](http://172.31.23.2:3137/cluster-c1-p64-v5/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/3ef83e65f05d51ce89767ee7fecdb3f1) |

完整事件分别为 22403 / 27938 / 2931 条，无丢失。生产候选二进制为 `f039be66100f…`，
旧/新诊断二进制为 `9371d68cbce0…` / `225520a1cba5…`；报告统计使用全部事件，Langfuse 每阶段保留最长 50 条。
原始数据、源码差异、二进制和复现脚本在测试机 `docs/cluster/benchmarks/profile-apply-20261123/`，数据已同步本地并由 Git 忽略。

## 7. v5 逐栈复核：剩余复制与等待

本节在提交 `40139414`（实现）、`ce5a8753`（观测与已有结论）后分析，未修改生产代码或重跑 benchmark。
使用第 6 节的同一轮原始采样，并对保留的 `225520a1cba5…` 二进制做 `addr2line -i` 内联展开。
模板和 `std::char_traits` 栈由此对应到实际调用点；下表按 memcpy **叶子样本互斥归类**，不会重复累计父子函数。

| 本轮 leader memcpy 来源 | 样本 / 估计 CPU | 对应优化 |
| --- | ---: | --- |
| 构造 `SetEffect` 的 key/value；drain 追加及扩容 | 35 + 34 / 138 ms | P0-1 收尾 |
| Fetch records → body、尾部扩容；body → frame | 32 + 24 / 112 ms | 新增 P1-7 |
| 异步请求取得输入所有权 | 29 / 58 ms | P1-5，需保持参数生命周期 |
| 单条 record 写入最终 batch | 23 / 46 ms | P0-1 已减少中间副本；此处仍需编码 |
| 发布 parked Fetch；parked 内容填入 response | 13 + 9 / 44 ms | P1-4 共享已有 WAL |
| 最终写入 KV 的 `LargeString::SetString` | 21 / 42 ms | 保留 SET 存储语义 |
| 合计 | **220 / 440 ms** | leader 全部 443 个 CPU 样本中的 49.7% |

- drain 的 34 个样本中，24 个来自扩容搬运旧内容，10 个来自追加新内容。单条 batch 已精确预留，
  聚合 `records.append` 仍会扩容；不能把“单条直接编码完成”写成“聚合复制已消除”。
- Fetch body 的 32 个样本中，22 个来自 records 追加，10 个来自尾部 `Tags` 写长度时触发的扩容。
  后者是搬运已经编码的大 body，并非 tag 本身有很大 payload；这里只改 CRC 或 ring 大小无法消除它。
- 原 cluster leader 的 109 个 memcpy 样本中，72 个属于 journal 编码/流式复制、35 个属于 KV 存储、
  2 个属于 RESP。两条路径都使用 Boost.Context，Raft 多出来的输入、effect、WAL、Fetch body/frame 中间形态
  才是此次可定位的额外工作；模板栈层数本身不能当作耗时。

**follower 读回/解码仍是明确目标。** 剩余 25 个 memset 样本全部来自 `PartitionLog::Read` 的清零字符串；
27 个 memcpy 样本来自 `DecodeSetEffect` 再复制 value，25 个 CRC 样本来自应用时重新校验。
另有 22 个 CRC 样本在接收追加时校验。读取、batch 解码、effect 解码三个 trace 阶段合并去重为
**163.32 ms，占 follower 外层 apply 的 53.3%**，均位于该 apply 区间内。
应复用接收时已验证的不可变批次及其元信息，保留网络输入和磁盘重读的校验；leader 追加时也仍有 25 个重复校验样本。
follower 追加本轮还同步调用了 **134 次 pwrite，共 36.64 ms**；当前并非全部文件操作都走 Folly 异步路径。
先处理读回和复制，再单独评估异步追加；`memory` 仍包含 page cache 写入和后台同步。

**确认路径仍有等待，但不能把排队全归为 fiber 调度。**

- leader 5400 次 worker 入口包含 2580 次 pending-I/O 等待后重试，并非 5400 个独立业务操作。
  `await_commit` 从 873 增至 1262 次；现有 trace 没有操作类型和票据 ID，不能将所有访问都归到 `WaitApplied`。
- worker 排队区间并集 301.37 ms 中，**255.77 ms（84.9%）与编码或应用区间重合**。
  组 worker 在应用期间同步等待 owner/shard，仍占着该组任务顺序；缩短组内数据处理和应用往返仍然重要。
  区间重合说明排队时系统正在做什么，不能直接推出删去某阶段后的吞吐提升。
- leader 的 I/O 等待与后台 fsync 重合 **118.27 ms**，follower 为 **93.91 ms**。
  P2-6 的统一 pending 状态确有影响；它与 worker 排队是不同等待区间，不能把所有等待都算成 fsync。
  P1-3 应让已应用前缀直接完成票据，P2-6 则另行处理追加与持久标记切点，避免把两种改动混在一起。

| 对照原计划 | v5 选择依据与当前实施状态 |
| --- | --- |
| P0-1 剩余部分（已实验） | 原目标约 138 ms 复制样本；v6 编码和 CPU 下降，但吞吐仅 +4.2%、P95 和 RSS 回退，见第 9 节 |
| P1-7 | 单缓冲完整帧，直接针对约 68 ms 的 body → frame 与尾部扩容复制；先不扩到分散缓冲发送 |
| P1-4（已实测） | v7 该负载的读回/重复解码为零；确认吞吐 +19.1%、SET 阶段 +7.2%，RSS 未改善，见第 10 节 |
| P0-1b | 已确认存在串行等待，先补齐对 leader HW 和批次完成的影响，暂不进行完整调度重构 |
| P0-2 | 补齐真正的 shard 批量执行评估；原 cluster 有 squasher，当前 Raft 仍逐笔同步执行 SET |
| P1-3 | 保留降低回查成本，但批次最后一笔 apply 完成后的票据等待很小，暂不作为大收益首选 |
| P0-1a（已覆盖） | 已包含于直接聚合；没有纯 reserve 的独立收益数据 |
| P1-5 | 贯穿以上改动记录唯一 capacity、分配字节与峰值；当前 RSS 仍未改善 |
| P2-6 | 允许追加与同步并行另做实验；P0-1b 先处理已有接收位置的报告，避免扩大持久性改动 |

以上 CPU 数字来自单轮 500 Hz 采样，不能作为可节省的准确时长或承诺收益。
复核脚本和区间交集结果保存在 ignored 的 `benchmarks/profile-apply-20261123/v5-review/`；
内联调用点记录在 `benchmarks/profile-apply-20261123/memory-apply-c1-p64-v5/*callers*.txt`，
原始基线仍为 **169.56 MiB/s**。

## 8. 按日志 offset 复核确认路径：串行等待与收益边界

前面的 CPU 热点表不能直接决定端到端收益。本节对 v5 两版的完整 trace 按 offset 重新关联，
核对 follower 接收完成、下一次 Fetch RPC、leader 应用前缀和 16 个客户端 pipeline 的末笔票据。
本次只分析既有数据，没有实施优化或新增性能结果。

| 确认路径指标 | 上一版 encoded | 本轮 apply |
| --- | ---: | ---: |
| SET 阶段墙钟时间，单次插桩诊断 | 1181.66 ms | 1104.43 ms |
| follower 有数据响应次数 | 135 | 134 |
| 有数据响应接收完成 → 下一次 Fetch RPC 开始，累计 | 233.64 ms | **227.68 ms** |
| 上述间隔中的 apply | 180.50 ms | **140.67 ms** |
| 上述间隔中的后台 fsync | 45.57 ms | **80.47 ms** |
| 上述两阶段区间并集 | 226.07 ms | **221.14 ms，占间隔 97.1%** |
| 接收完成 → 下次 RPC 间隔，与 leader 已测工作/I/O 的重合 | 144.99 ms | **142.42 ms，占间隔 62.6%** |
| 其中与 leader 自身 fsync 的重合，已包含在上一行 | 38.28 ms | 69.95 ms |
| 未被上述 leader 阶段覆盖的间隔，不能视作空闲或可节省时间 | 88.66 ms | 85.26 ms |
| follower 全部 apply，均在相邻 Fetch RPC 之间 | 386.30 ms | 306.41 ms |
| 16 批中最后完成票据：本地 apply 完成后的等待总和 | 0.93 ms | **0.22 ms** |

**具体例子，时间相对本轮测量开始：** follower 在 **37.777 ms** 已接收至 offset **89**，
随后于 **37.801–41.220 ms** 应用旧的已提交前缀 **71 → 86**，直到 **41.231 ms** 才发起报告 offset 89 的下一次 Fetch。
leader 对 **86 → 89** 的本地应用于 **41.780 ms** 才开始。
这里新日志的接收已经完成，旧前缀应用却排在新位置报告之前；该等待链由时间线和代码顺序共同支持。

- **确认路径流水化保留为候选，尚不能确认较大收益。** 处理整条 follower 接收位置报告路径，覆盖 apply 以及统一 pending 检查带来的后台同步等待。
  memory 模式允许报告 page cache 接收位置；这不授权 SET 跳过 leader 应用，也不授权 WAIT 跳过 follower 应用。
- **只做 follower 缓存也不能直接按 163 ms 估收益。** 读回/解码共 163.32 ms，其中 **94.38 ms**
  位于“收到新数据、尚未开始下一次 Fetch”的间隔内。其余部分可能与 leader 工作重叠；此前只用“占 apply 53.3%”
  判断收益大小的依据不够。缓存复用仍有 CPU 和内存价值，应与调度改动分别实验。
- **回查次数多不等于批次末尾回复慢。** 本轮每批最后完成票据在本地 apply 后最多等待 **0.031 ms**。
  P1-3 仍可能减少中间请求的排队、唤醒和内存占用，但现有数据不支持把“已 apply 后尽快通知”当成大幅吞吐提升的主因。

**原 cluster 的更短栈还来自真正的批量执行。** leader 的 193 个样本中，128 个位于
`MultiCommandSquasher` 调用链内；这是包含实际 SET、存储和 journal 工作的 inclusive 样本，绝不是 66% 的可删除开销。
[main_service.cc](../../src/server/main_service.cc) 的 `DispatchSquashedBatch` 在 `raft_data_commands_` 存在时直接返回，
而已提交 Raft effect 仍由 `JournalExecutor::ExecuteSet → DispatchCommand(ONLY_SYNC)` 逐笔执行。
原 cluster 的 squasher 按 shard 收集命令、一次投递多条；副本也按 shard journal flow 应用。
因此 P0-2 下一阶段应评估 shard 内批量执行和有序完成前缀，而非只为缩短栈另写一个 SET 入口。
当前缺少逐笔 shard 排队 trace，不能据此给出这一项的收益百分比。

**测量边界：** Fetch RPC span 从 `peer.Call` 前开始，不是网卡实际发送时刻；接收结束位置由下一次 receive 的起始 End 推得。
分析已核对单 follower RPC 顺序、End 单调、1024 条记录与 1024 个唯一票据的覆盖。空响应后的 apply 另计为
165.74 ms，没有混入 140.67 ms；后台 fsync 与 apply 通过区间合并去重。
227.68 ms 是同一 follower 路径的累计等待，不等于整次 benchmark 可省下的墙钟时间，不能宣称可直接提升约 20%。
leader 重合区间对 worker operation、encode、submit、apply、Folly queue/write/fsync 取并集后计算，
不累加父子 span；这些是阶段墙钟区间，不等同于 CPU 忙碌。未覆盖区间仍可能包含未插桩的发送、封帧等工作。
因此 142.42 ms 重合与 85.26 ms 未覆盖都不能直接用于预测改动后的吞吐，更不能据此承诺两位数提升。
下一次诊断需要精确关联接收完成、Fetch 实际发出与 leader HW 前移，正式吞吐仍用未插桩三轮对照。

可复核脚本和逐个间隔、批次末尾票据数据保存在 ignored 的
`benchmarks/profile-apply-20261123/v5-critical-review/`。

<a id="v6-drain-experiment"></a>

## 9. P0-1 实验：编码与 CPU 下降，吞吐增益有限，RSS 上升（v6）

本轮完成直接聚合编码和 effect/WAL 共享，分别保留改前、直接聚合、完整共享三份二进制。
正式性能采用未插桩 Folly Release、1 连接 / pipeline 64、256 MiB、256 KiB/value，
四版本轮换各三轮，计时包含 SET + 原写连接 WAIT 1；原 cluster 使用完整共享版的同一二进制。

| 版本 | 确认吞吐 MiB/s（范围） | 批次 P95 ms |
| --- | ---: | ---: |
| 改前 `f039be66100f…`，已包含上一轮优化 | 232.92（223.83–233.62） | 75.05 |
| 仅直接聚合 `6fdd7deecdd2…` | 230.04（227.93–245.02） | 79.67 |
| 直接聚合 + effect/WAL 共享 `c4b918f2a1ff…` | **242.79（225.90–247.48）** | **83.19** |
| 原 cluster | 605.40（604.48–605.74） | 35.49 |

- **完整改动仅 +4.2%，P95 +10.8%，没有达到预期的大幅收益。** 配对吞吐为 +3.9% / +0.9% / +6.3%，
  三轮范围重叠；共享这一步相对仅直接聚合 +5.5%，但其中一轮为 −7.8%。保留全部样本，不扩展到 2 / 8 连接。
- **直接聚合单独没有稳定收益。** 先行对照 231.32 → 235.41 MiB/s（+1.8%），上表复测为 −1.2%。
  阶段一已经包含预分配，不能把其结果算成纯 reserve 的独立收益。
- 原始 memory 基线仍为 **169.56 MiB/s**。224.69 是改前版的上一轮结果，232.92 是该版的本轮复测；
  此次增量只按本轮配对计算，不拿旧轮次放大改善。

以下为另跑的一次诊断，旧/新诊断二进制分别为 `225520a1cba5…` / `1de5b759be7a…`。
旧 leader 是 node 12，新 leader 是 node 11，按角色比较。CPU 为 500 Hz 样本估计，包含多线程；
trace 耗时用阶段区间并集，不能将重叠阶段相加。诊断吞吐不代替上面的正式结果。

| 单次诊断指标 | 改前 raft.memory | 完整共享版 raft.memory | 原 cluster |
| --- | ---: | ---: | ---: |
| leader CPU 估计时间 | 852 ms | **712 ms（−16.4%）** | 384 ms |
| leader CPU / 用户数据 MiB | 3.33 ms | 2.78 ms | 1.50 ms |
| leader memcpy / memset | 418 / 94 ms | **278 / 76 ms** | 208 / 66 ms |
| drain/编码路径的 memcpy 叶子样本 | 87 / 174 ms | **22 / 44 ms** | — |
| leader 编码区间并集 | 238.34 ms | **86.87 ms（−63.6%）** | — |
| leader worker 排队区间并集 | 315.01 ms | 196.12 ms | — |
| leader 应用区间并集 | 140.38 ms | 157.91 ms | — |
| follower CPU 估计时间 | 378 ms | 402 ms | 140 ms |
| follower 应用区间并集 | 307.49 ms | 293.57 ms | journal.apply 43.59 ms |
| follower WAL 读回/两层解码区间并集 | 157.31 ms | 161.33 ms | — |
| leader / follower 后台 fsync 区间并集 | 90.93 / 80.84 ms | 180.77 / 167.58 ms | — |
| leader 测量结束 RSS / 进程峰值 RSS | 414.54 / 429.00 MiB | **426.21 / 461.16 MiB** | 650.35 / 653.22 MiB |
| follower 测量结束 RSS / 进程峰值 RSS | 388.92 / 393.81 MiB | 389.35 / 390.91 MiB | 373.58 / 373.58 MiB |

**目标复制确实减少，内存目标尚未达成。** 编码轮数 176 → 177，执行的 SET 都是 1024 笔，
因此编码减少不是少做了工作。drain/编码路径的 memcpy 从 87 降到 22 个叶子样本，
后者仍包含将输入写入最终 WAL 的必要复制；leader 总 memcpy 减少约 33.5%。
但峰值 RSS 增加 **32.15 MiB（+7.5%）**，不能把共享所有权等同于整体内存减少。
RSS 包含 KV、运行时及 allocator 保留内存；峰值覆盖启动、预热和测量，未测分配字节，尚不能归因于某个缓冲。

新增 `raft.set.cache_bytes` 的 177 次追加后快照中，pending cache 的唯一 capacity 最大为 **7.75 MiB**，
单个 WAL 缓冲最大 **3.75 MiB**，未超过 8 MiB 缓存预算。它是瞬时记账值，不能累加，也不包含正在应用或 I/O 另持的引用。
新旧 leader 都没有 `raft.apply.read/decode` 事件，1024 笔均从缓存应用，未观察到本轮共享预算导致的 WAL 回退。
慢副本、部分应用时少数引用仍能保留整个聚合缓冲；本次观测不证明全流程内存硬上限或普遍下降。

**未改变的等待限制了整体收益。** follower 仍有 1024 次 WAL 读回和两层解码，共约 161 ms，
占其外层 apply 约 55%；应用仍位于下一轮 Fetch 之前。leader 应用未缩短，worker 入口仍有 4941 次。
新 leader 的 Fetch body/封帧复制仍采到 56 个样本（约 112 ms），请求取得输入所有权有 30 个样本（约 60 ms）。
这些仍对应 P1-4、P0-1b、P0-2、P1-7 和 P1-5，不能按各阶段耗时相加预测下一步收益。

本次诊断的后台 fsync 更慢：新版本最长 pipeline 为 100.88 ms，与 leader / follower fsync 分别重合约
48.65 / 49.09 ms；两边彼此也有重合，不能相加。本轮没有消除 memory 后台同步的等待，
但仅凭重合和单次诊断不足以解释正式三轮 P95 的全部回退，也不能扣掉 fsync 后宣称更高吞吐。
该阶段结论是：**局部 CPU 优化有效，综合收益有限。** 当时保留实验并继续实施 P1-4，后续结果见第 10 节。

**正确性：** Debug / Release 各 63 项、ASAN/UBSAN 62 项单测通过（sanitizer 未包含 `set_executor_test`），
覆盖逐字节编码、CRC、边界、共享生命周期、预算回退、部分失败、换任期、短写及存储失败；
11 项集群回归通过，覆盖 flush/memory pipeline、条件写、恢复及换主/快照。
两组正式 benchmark 共 21 轮及 3 次诊断均完成原连接 WAIT 1、离线副本恢复、WAIT 2 和 1088 个值读回。

| 报告 | 火焰图与时间线 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 改前 raft.memory | [v6 before](http://172.31.23.2:3137/memory-before-c1-p64-v6/index.html) | [1945 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/72d854c7eac45407875efc4156a843b2) |
| 完整共享版 raft.memory | [v6 shared](http://172.31.23.2:3137/memory-shared-c1-p64-v6/index.html)、[leader 火焰图](http://172.31.23.2:3137/memory-shared-c1-p64-v6/leader-node-11.svg) | [1974 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/aca08d5385f758778d3576d19b7b7cdd) |
| 原 cluster | [v6 cluster](http://172.31.23.2:3137/cluster-c1-p64-v6/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/2a04e2fbb63a513ba411603489b8b603) |

完整事件分别为 26824 / 26343 / 2930 条，无丢失；Langfuse 每阶段保留最长 50 条，报告统计使用全部事件。
原 cluster follower 仅 70 个 CPU 样本，细分比例不稳定。原始数据、脚本 `compare_steps.py` / `analyze_profiles.py`、
源码快照和二进制保存在测试机 `docs/cluster/benchmarks/profile-drain-20261123/`；报告与日志已同步本地并由 Git 忽略。

<a id="v7-follower-experiment"></a>

## 10. P1-4 实验：follower 复用接收批次，确认吞吐提高，RSS 未下降（v7）

沿用 P0-1 的共享缓冲和 `SetEffectView`，接收追加成功后缓存已验证的普通 SET/no-op，
应用时复用 key/value 视图；省去刚写入 WAL 的读回、清零、第二遍 CRC 和 value 副本。
原始网络校验、HW/应用顺序、迁移 fencing 与 flush/memory 确认条件保持不变。
缓存按唯一 capacity 限额，超预算/控制记录/恢复走 WAL；截断与快照内容代次、任期变化会使缓存失效。

正式性能仍为同机未插桩 Folly Release，**1 连接 / pipeline 64**，256 MiB、256 KiB/value，
三版本交错各三轮，计时包含 SET + 原写连接 WAIT 1。基线是完整 P0-1 版，本轮复测为 252.11；
不用上一轮 242.79 计算增量，原始基线 **169.56 MiB/s** 保留。

| 版本 | 确认吞吐 MiB/s（范围） | 仅 SET 吞吐 MiB/s | 末尾 WAIT ms | 批次 P95 ms |
| --- | ---: | ---: | ---: | ---: |
| 完整 P0-1 `c4b918f2a1ff…` | 252.11（244.06–256.09） | 280.04 | 101.27 | 71.12 |
| 加 follower 缓存 `c4460ba22ac7…` | **300.25（279.17–309.97）** | **300.31** | **0.16** | **69.79** |
| 原 cluster，同一新版二进制 | 607.99（604.13–608.24） | 799.35 | 101.39 | 34.96 |

- **确认吞吐 +19.1%**，三组配对 +23.0% / +9.0% / +23.0%，范围不重叠；原 cluster 仍快约 **2.03 倍**。
- **SET 阶段中位数 +7.2%**，墙钟 914.16 → 852.46 ms。末尾 WAIT 从约 101 ms 降至 0.16 ms（三轮均如此），
  是确认吞吐改善的重要组成，不能把 +19.1% 全解释为执行速度或 CPU 提升。
- **P95 仍不稳定**：三轮 87.07 / 67.22 / 71.12 → 69.79 / 103.22 / 63.25 ms。
  中位数仅 −1.9%，第二轮明显回退，该轮仅 SET 吞吐也从 285.02 降至 279.22 MiB/s。保留全部样本，不扩展连接数。

另跑一次诊断；旧/新诊断二进制为 `1de5b759be7a…` / `6c87855cebf0…`，leader 均为 node 11，
follower 均为 node 12。CPU 按 500 Hz 样本估计，trace 取区间并集；这组插桩数据不代替正式三轮性能。

| 单次诊断指标 | 完整 P0-1 | 加 follower 缓存 | 原 cluster |
| --- | ---: | ---: | ---: |
| follower WAL 读取 / batch 解码 / effect 解码次数 | 各 1024 | **各 0** | — |
| follower 读回与解码区间并集 | 154.20 ms | **0 ms** | — |
| follower 外层应用区间并集 | 294.29 ms | **83.95 ms（−71.5%）** | journal.apply 43.18 ms |
| follower CPU 估计 / 每 MiB CPU | 368 ms / 1.44 ms | **236 ms / 0.92 ms（−35.9%）** | 156 ms / 0.61 ms |
| follower memcpy / memset / CRC | 96 / 42 / 84 ms | **74 / 0 / 36 ms** | 22 / 70 / 未采到 |
| leader CPU 估计 | 660 ms | 688 ms | 394 ms |
| leader 编码区间并集 | 84.99 ms | 86.08 ms | — |
| leader 应用区间并集 | 168.59 ms | 175.32 ms | — |
| leader 测量结束 RSS / 进程峰值 RSS | 438.23 / 454.08 MiB | 428.87 / 465.30 MiB | 647.62 / 652.48 MiB |
| follower 测量结束 RSS / 进程峰值 RSS | 388.98 / 392.21 MiB | **406.47 / 406.50 MiB** | 373.58 / 373.58 MiB |

**消除了目标重复工作，没有少执行 SET。** 新版 leader/follower 各有 1024 次 cache hit 和 1024 次
`journal.apply`，没有 WAL 回退；cache hit 只表示从缓存取出，须结合实际应用事件与读回验证判断成功。
follower 应用时的 CRC 从 18 个叶子样本降至零，接收时仍采到 18 个 CRC 样本，保留首次校验。
118 次缓存准备合计约 0.10 ms，接收追加总耗时 74.45 → 74.00 ms，没有把 154 ms 的重复工作整体转移到接收阶段。
leader 编码和 CPU 未改善；原 cluster follower 仅 78 个样本，细分热点不宜过度比较。

**应用缩短也减少了接收位置报告前的串行等待。** 按 offset 关联后，128 → 118 个有数据响应的
“接收完成至下一次 Fetch RPC”累计间隔为 **256.26 → 96.85 ms**，其中应用为 **177.59 → 52.30 ms**。
应用仍在下一次 Fetch 之前，P0-1b 的完整流水化没有实现；后台 fsync、owner/shard 等待也仍存在。
这里是单次阶段墙钟，不与 CPU 或正式吞吐百分比相加。

**WAIT 的 100 ms 差异来自检查时机。** 两版最后目标均为 offset 1094：旧 follower 最后应用结束
比客户端 WAIT 开始晚 **0.219 ms**，下一次 Fetch 晚 **0.228 ms**；新版分别提前 **0.091 / 0.056 ms**。
[ServerFamily::Wait](../../src/server/server_family.cc) 在应用进度不足时睡眠 100 ms 再检查，
与诊断末尾 WAIT **101.36 → 0.23 ms** 一致。RPC span 表示进入 `peer.Call`，并非实际发包，
目前没有 leader 接纳 applied tag 的独立事件；不据此声称完整关键路径已全部追踪。
WAIT 仍要求副本 applied offset 达标，SET 仍需本模式多数接收及 leader 本地应用。

**内存占用没有下降。** follower 峰值 RSS **+14.28 MiB / +3.6%**，leader 峰值 **+2.5%**。
118 次接收追加后的缓存快照中，pending 唯一 capacity 最大 **7.75 MiB**，单个缓冲最大 **4.00 MiB**，
未超过 8 MiB 缓存预算；正在应用或 I/O 的引用另计，不能当作全流程内存上限。
follower 测量前 RSS 已从 120.89 变为 131.55 MiB；峰值还包含启动、预热、KV 和 allocator 保留内存，
本次没有分配字节追踪，不能把 RSS 增量精确归因于某个缓存。

**验证：** Debug / Release 各 72 项、ASAN/UBSAN 71 项单测通过（sanitizer 未包含 `set_executor_test`）；
覆盖共享生命周期、预算回退、坏 CRC、半包/未来任期、同任期截断替换、快照失效、迁移屏障和存储失败。
12 项集群回归覆盖 flush/memory pipeline、条件写、断连/关闭、换主、崩溃恢复及快照追赶。
9 轮正式性能与 3 次诊断均完成原连接 WAIT 1、离线副本恢复、WAIT 2、1088 个值读回。

| 报告 | 火焰图与时间线 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 完整 P0-1 | [v7 before](http://172.31.23.2:3137/memory-before-c1-p64-v7/index.html) | [1976 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/c2b9bff454e45d7d986f5b04a8540ada) |
| follower 缓存 | [v7 follower](http://172.31.23.2:3137/memory-follower-c1-p64-v7/index.html)、[follower 火焰图](http://172.31.23.2:3137/memory-follower-c1-p64-v7/follower-node-12.svg) | [1991 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/e0be3600de775f28a1f938ec9cd3491b) |
| 原 cluster | [v7 cluster](http://172.31.23.2:3137/cluster-c1-p64-v7/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/d45f7f1688e958a6a1aa07e127e50711) |

完整事件为 27642 / 25587 / 2951 条，无丢失；Langfuse 每阶段保留最长 50 条，报告统计使用全部事件。
原始数据、源码/二进制快照及 `analyze.py`、`analyze_profiles.py`、`v7-critical-review/analyze.py`
保存在本地及测试机 `docs/cluster/benchmarks/profile-follower-20261123/`，由 Git 忽略。
