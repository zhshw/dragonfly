# v5：批量应用与借用参数

[Profile 索引](../profile.md) · [benchmark](../replication-benchmark.md)

<a id="diagnostic-results"></a>

## 诊断结果

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
  与剩余编码/发送复制一起排序，见[逐栈复核](#stack-review)。
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

<a id="stack-review"></a>

## 逐栈复核：剩余复制与等待

本节在提交 `40139414`（实现）、`ce5a8753`（观测与已有结论）后分析，未修改生产代码或重跑 benchmark。
使用[诊断结果](#diagnostic-results)的同一轮原始采样，并对保留的 `225520a1cba5…` 二进制做 `addr2line -i` 内联展开。
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
  P1-6 的统一 pending 状态确有影响；它与 worker 排队是不同等待区间，不能把所有等待都算成 fsync。
  P1-3 应让已应用前缀直接完成票据，P1-6 则另行处理追加与持久标记切点，避免把两种改动混在一起。

| 对照原计划 | v5 选择依据与当前实施状态 |
| --- | --- |
| P0-1 剩余部分（已实验） | 原目标约 138 ms 复制样本；v6 编码和 CPU 下降，但吞吐仅 +4.2%、P95 和 RSS 回退，见 [v6 实验](v6-drain-sharing.md) |
| P1-7 | 单缓冲完整帧，直接针对约 68 ms 的 body → frame 与尾部扩容复制；先不扩到分散缓冲发送 |
| P1-4（已实测） | v7 该负载的读回/重复解码为零；确认吞吐 +19.1%、SET 阶段 +7.2%，RSS 未改善，见 [v7 实验](v7-follower-cache.md) |
| P0-1b | 已确认存在串行等待，先补齐对 leader HW 和批次完成的影响，暂不进行完整调度重构 |
| P0-2 | 补齐真正的 shard 批量执行评估；原 cluster 有 squasher，当前 Raft 仍逐笔同步执行 SET |
| P1-3 | 保留降低回查成本，但批次最后一笔 apply 完成后的票据等待很小，暂不作为大收益首选 |
| P0-1a（已覆盖） | 已包含于直接聚合；没有纯 reserve 的独立收益数据 |
| P1-5 | 贯穿以上改动记录唯一 capacity、分配字节与峰值；当前 RSS 仍未改善 |
| P1-6 | 允许追加与同步并行另做实验；P0-1b 先处理已有接收位置的报告，避免扩大持久性改动 |

以上 CPU 数字来自单轮 500 Hz 采样，不能作为可节省的准确时长或承诺收益。
复核脚本和区间交集结果保存在 ignored 的 `docs/cluster/benchmarks/profile-apply-20261123/v5-review/`；
内联调用点记录在 `docs/cluster/benchmarks/profile-apply-20261123/memory-apply-c1-p64-v5/*callers*.txt`，
原始基线仍为 **169.56 MiB/s**。

<a id="confirmation-path"></a>

## 按日志 offset 复核确认路径：串行等待与收益边界

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
[main_service.cc](../../../src/server/main_service.cc) 的 `DispatchSquashedBatch` 在 `raft_data_commands_` 存在时直接返回，
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
`docs/cluster/benchmarks/profile-apply-20261123/v5-critical-review/`。

## 实现范围与后续边界

**已实现并通过验证：** 连续已提交替换按最多 32 笔 / 1 MiB 投递 owner，一笔超过预算时单独执行；
owner 每完成一笔检查 1 ms 预算，超时返回已完成前缀；单笔执行不能被强行中断。控制、no-op、迁移记录先排空批次，日志游标与 slot generation
仍由组 worker 更新；执行失败只推进成功前缀。`JournalExecutor::ExecuteSet` 同步借用 key/value，
省去 `BackedArguments` 的大块复制和清零，继续执行原有 SET 事务。新增 owner 排队、批次、WAL 读取及解码 trace。
相对已优化的上一轮 `encoded-bin`，1 连接 / pipeline 64 三轮确认吞吐增量提高 8.8%。

v14b 将普通缓存前缀交给独立 ApplyLane，v15 再将相邻同 shard 的普通 SET 合并为一次投递，按实际成功前缀发布应用进度。 [v14b ApplyLane](v14b-batch-progress.md) · [v15 shard 合批](v15-shard-apply.md)

**优化前的代码路径：** `DataNodeImpl::OpenPartition` 每条 effect 调用一次 `owner_->Await`；
[set_executor.cc](../../../src/server/cluster/raft/data/set_executor.cc) 的 `ExecuteSetEffect` 构造 `CmdData`，
再经 `JournalExecutor`、`Service::DispatchCommand` 执行 SET。
[BackedArguments::Assign](../../../src/common/backed_args.h) 会 resize 存储后再复制参数。

- **已完成第一阶段：** 一次投递有界连续 effect，在 owner 上顺序执行；原有 SET 的 TTL 清除、OOM、类型覆盖和 DB 选择均保留。
- **后续待评估：** 抽取可复用的内部“替换 value”入口，进一步省去命令分发和捕获 OK；复用现有 shard 操作，
  保留 TTL 清除、内存检查、统计、索引和变更通知，不能直接绕过 DbSlice/事务约束。
- 用笔数、字节数和执行时间限制每批占用；同 key 顺序、条件写、迁移记录及快照边界保持屏障。
  若进一步按 shard 并行，只能公布全部完成的连续 applied 前缀；部分失败不能推进游标或提前回复。

**验收：** Await/每千笔、应用 CPU、`raft.apply` 和临时参数分配下降；先核对同 key 覆盖、NX/XX、TTL、OOM 和重放一致性。

## 补充等待复核

该版 `WaitApplied` 仍经默认 `allow_pending=false` 的 `RunOnWorker`；即使只是查询已应用前缀，
也要先等当前追加或后台同步结束。批量应用减少的是 owner 投递，并未移除此处等待。

初始 v3 的 Folly 队列并集为 **2.57 ms**，本轮 leader / follower 为 **3.90 / 0.46 ms**。
数据 follower 未出现 Fetch 后 25 ms 退避，两边最终 WAIT 都约 101 ms。
这些结果不支持优先扩大 ring 或仅替换 CRC；相关等待仍需与编码、应用路径一起分析。
