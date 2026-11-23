# raft.memory / 原 cluster profile 与优化项

**可以把调用路径和内存一起优化：优先减少 value 的重复副本、逐笔应用往返及 worker 回查。**
栈更深本身不是性能结论；本次证据是 Raft 消耗更多绝对 CPU 时间，且大量复制、编码和应用占用同一个组 worker。
减少这些工作既可能降低内存流量和临时内存，也可能缩短排队。
已实现 WAL 缓冲共享、单条 SET 的直接编码、有界批量应用及同步借用 SET 参数；effect 缓存共享等仍待实施。
各项收益以分阶段对照为准，不能把计划中的优化算作已经完成。

**相对原始基线的累计提升：169.56 → 224.69 MiB/s（+32.5%）。**
两者均为 1 连接、pipeline 64、未插桩三轮 `raft.memory` 确认吞吐中位数，SET + WAIT 1 口径相同，但来自不同轮次。
**本轮增量对照：206.56 → 224.69 MiB/s（+8.8%）**，批次 P95 **90.91 → 75.08 ms（−17.4%）**。
206.56 是已包含共享缓冲和直接编码的上一版复测值，不能作为原始基线。
原 cluster 本轮为 607.34 MiB/s，仍快约 2.70 倍。各阶段提升不能相加，完整口径和范围见 [benchmark](replication-benchmark.md)。

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
| 重放数据 | 接收的 journal 数据 | leader 有 effect 缓存；follower 通常从刚写入的 WAL 读取、解码 | 复用近期已验证的不可变批次 |

Raft 的任期、日志顺序、HW 和恢复边界仍由 Raft 负责；借鉴原 cluster 的数据处理方式，保持两种集群设计独立。

## 3. 六个优化项

### P0-1：一次编码，传递同一个不可变 WAL 缓冲

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
- **待实施：** effect 缓存持有缓冲所有权及 key/value 范围，减少另一份大 value；不能保存临时 `string_view`。
  drain 聚合的追加复制也仍保留，本轮尚未做到全链路只持有一份数据。

**验收：** 每 MiB 用户数据的复制字节、分配字节及编码 CPU 下降；编码后的字节、CRC、段截取、短写重试均保持正确。

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

- 按 `(epoch, end_offset)` 登记待完成票据，由应用进度一次完成连续前缀，减少每笔唤醒后重新排队查询。
- 角色/任期变化、存储失败、超时和关闭必须完成或取消相应票据；连接回复仍按原顺序。
- 数据复制和应用缩短之后再决定是否进一步拆组 worker；日志位置、任期和 HW 的修改继续串行。
  Fetch 已允许部分在途访问，不重复把“允许 Fetch”当成新优化。

**验收：** 按操作类型统计 worker 访问和唤醒次数、排队 P95、fiber 数及栈内存；确认吞吐改善，超时和换主无挂起票据。

### P1-4：复用近期日志批次，减少 follower 读回与重复解码

**代码依据：** `CommittedEffects::ApplyTo` 在未命中 effect 缓存时用 `log.Read(..., 1, ...)` 逐 batch 读回，
随后 `DecodeRecordBatch → DecodeSetEffect`；后者重新复制 key/value。
`PartitionLog::Read` 先创建清零字符串再 pread；leader Fetch 也走此入口。

- leader 已有 `RememberAppended` 缓存，先补命中率，不能把新增同样的缓存当作收益。
- 让 follower 在写入确认至应用期间复用收到的不可变 batch；近期 Fetch 也可复用有界 WAL 缓冲。
  慢副本、缓存淘汰和重启继续从磁盘读取；缓存必须绑定任期、范围及截断生命周期。
- 对同一份已验证、未改变的内存对象传递 batch 元信息，减少内部重复 CRC/解析。
  网络输入、磁盘重读仍需完整校验，不能给任意字节接口增加“跳过校验”开关。

**验收：** pread 次数/字节、缓存命中率、解码复制字节和 CRC CPU 下降；坏 CRC、部分 batch、换主替换尾部和恢复仍能检出。

### P1-5：统一内存预算，缩短副本生命周期

**现状：** 提交/排队已有 **8 MiB / 256 笔**限制；leader effect 缓存另有 **8 MiB / 4096 条**上限。
这些是不同阶段的逻辑限制，有的允许空队列接收首个大请求；它们既不是总 RSS 上限，也不能直接相加推算实际占用。
WAL 副本、Fetch 响应、应用参数、确认 fiber 和 allocator 容量仍需另行统计。

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

## 4. 验证顺序与收益记录

1. 先增加按阶段的复制/分配计数及 apply 子阶段：WAL 读取、CRC/解码、owner 排队、实际 KV 应用。
   已增加进程 RSS 和应用读取/解码、owner 排队/批次 trace；复制/分配字节尚未完整采集。
   内存诊断单独运行，不把 profiler 开销混入正式吞吐。
2. P0-1 和 P0-2 第一阶段已经完成，接下来按新 profile 验证 P1 项；各阶段分别对照，避免跨轮叠加收益。
3. 正式性能仍用未插桩 Release，先测 **1 连接、pipeline 64**，相同数据量、value 大小及 SET + WAIT 1 口径，
   交错运行至少三轮，记录中位数和范围。没有收益就停止扩展连接数；确认收益后只扩到 **2、8**，不恢复 64。
4. 每轮保留原 cluster 对照，完成离线副本重启、WAIT 2 和全部值读回；另按改动覆盖同 key/条件写、段边界、
   慢副本、换主、截断和故障注入。共享路径改动也需确认 `flush` 模式正确性。
5. 记录：确认吞吐、批次 P95、CPU ms/MiB、复制/分配字节比、临时内存峰值、worker 排队及每千笔同步次数。
   只有实测后填写提升百分比；不以栈变短或单个热点消失代替整体收益。

暂不优先扩大 ring、继续增加连接数或仅替换 CRC：本轮 Folly 队列并集只有 **2.57 ms**，
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

## 6. 最新复采：应用往返下降，RSS 尚未下降（v5）

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
  批量 owner 投递没有消除确认票据的 worker 回查。下一步优先评估 P1-4 的近期不可变批次复用与 P1-3 的批量完成票据。
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
