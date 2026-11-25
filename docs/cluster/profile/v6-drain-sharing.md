# v6：直接聚合编码与 effect/WAL 共享

[Profile 索引](../profile.md) · [benchmark](../replication-benchmark.md)

## P0-1 实验：编码与 CPU 下降，吞吐增益有限，RSS 上升（v6）

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
该阶段结论是：**局部 CPU 优化有效，综合收益有限。** 当时保留实验并继续实施 P1-4，后续结果见 [v7 实验](v7-follower-cache.md)。

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

## 实现依据与边界

**代码依据：** [data_node.cc](../../../src/server/cluster/raft/runtime/data_node.cc) 的 `AsyncPendingSet` / `AppendSetBatch`，
[set_effect.cc](../../../src/server/cluster/raft/data/set_effect.cc) 的 `MakeSetEffectBatch`，
[record_batch.cc](../../../src/server/cluster/raft/log/record_batch.cc) 的 `PutRecord` / `BuildBatch`，
[fetch_replication.cc](../../../src/server/cluster/raft/quorum/fetch_replication.cc) 的 `Append`，
[partition_log.cc](../../../src/server/cluster/raft/log/partition_log.cc) 的 `TryAppendAsync`。

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
  实现后 v6 确认编码与复制下降，但吞吐仅 +4.2%、P95 变差，详见 [v6 实验](v6-drain-sharing.md)。

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

### drain 合批缓冲预分配

P0-1a 的预分配随直接聚合落地。后续 v11 试过复用编码结果中的 CRC，三轮吞吐近似持平、批次 P95 上升，已回退该快路径。 [v11 回退记录](v11-leader-wal-append.md)

- **原问题：** `records.append` 聚合已编码 batch 时反复扩容；v5 扩容搬运旧内容有 **24 个样本，约 48 ms CPU**，
  占 leader CPU 样本的 **5.4%**，不足以支持较大吞吐收益的预期。
- **已实现：** 按编码长度、预算和段边界选定连续前缀后一次 reserve，所有记录直接写入最终缓冲。
  同 key 顺序、条件写屏障和确认语义不变，预留后追加不移动缓冲的测试通过。
- **实验边界：** 本轮阶段一同时包含预分配和直接聚合，未单独测试“只加 reserve”。
  直接聚合两组对照分别为 +1.8% / −1.2%，不能认定稳定收益，更不能把它归为纯预分配的收益。
