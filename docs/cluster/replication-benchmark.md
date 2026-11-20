# Dragonfly 数据组复制测试与优化

2026-11-21 在同一台 4 vCPU 的 Colima 虚拟机上依次测量 Dragonfly 和 Kafka，网络均为回环。Dragonfly 使用 Linux aarch64 Debug 构建，每个数据进程使用两个引擎分片和 `--force_epoll`，Raft WAL 使用同步文件 I/O。组 10 有一主两副本，组 20 有一主一副本。Raft 另有三个 controller。[异步集群配置](../../tests/integration/async_replication_fixture.py) · [Raft 集群配置](../../tests/integration/raft_data_group_test.py)

CSV 保留 78 条逐轮指标，含客户端写入、日志提交阶段和相邻请求间隔。独立的进程内复制测试另附 1,536 笔正式追加的时间戳。阻塞和快照中断分别在独立进程用例中计时。[优先优化项与验收方法](#优先优化日志追加前的准备开销) · [逐轮数据](replication-benchmark.csv) · [数据组复制优化项](#数据组复制优化项) · [组复制设计](raft-replication-design.md)

## 集群就绪与计时条件

Dragonfly 每轮从空目录建群，测试依次验证三个条件，全部通过才开始记录正式 SET。[配对负载](../../tests/integration/replication_comparison_test.py)

1. **全员就绪。** 五个数据节点的 `CLUSTER SLOTS` 必须一致。两个槽段 0–8191 和 8192–16383 各自公布预期的主节点和副本。异步模式还从主从两端核对三条复制链路均在线。Raft 将节点 13 提升为 voter，再从控制组和两个数据组的 leader 分别检查成员、epoch 和提交高水位（HW），每一名 voter 的日志末端（LEO）都须达到本组 HW。最后在两组各写一个标记，用原连接执行 `WAIT 2`、`WAIT 1` 并读回原值。
2. **停机后仍可复制。** 强杀组 10 的节点 13，等待进程退出。向两组分别写入新的标记，各自在同一连接上得到 `WAIT 1` 和正确的 GET 结果，确认剩余副本已经接收停机后的写入。
3. **预热正式写入路径。** 默认静置 5 秒，再用正式连接合计写 32 个 256 KiB 随机值。每条参与预热的连接都等待一个副本确认。预热键与正式键使用相同的 hash tag、不同的键名。三种预热条件下，正式 SET 都是新建键。

64 MiB 和 128 MiB 分别为 256 和 512 个 256 KiB 随机值。连接同时起跑，每条连接最多一笔 SET 在途。计时从第一条连接开始发送到最后一条连接收到 SET 确认。吞吐只计正式写入的 value 字节，数据生成、预热、额外的 WAIT 和恢复各自位于计时区间之外。每种配置独立建群三次，每列取三轮中位数，P50、P95 先按单轮全部正式 SET 计算。

## 落后一个副本时写入及恢复

| 载荷 | 复制模式 | 连接数 | 写入耗时 | 有效 MiB/s | SET P50 / P95 | 节点 13 重启至各连接 `WAIT 2` 返回 2 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 64 MiB | 异步 | 1 | 0.434 s | 147.64 | 1.57 / 2.56 ms | 0.309 s |
| 64 MiB | Raft | 1 | 2.182 s | 29.34 | 8.19 / 12.09 ms | 1.208 s |
| 64 MiB | 异步 | 8 | 0.281 s | 227.72 | 8.33 / 12.01 ms | 0.309 s |
| 64 MiB | Raft | 8 | 1.262 s | 50.72 | 38.80 / 56.94 ms | 1.206 s |
| 128 MiB | 异步 | 1 | 0.758 s | 168.85 | 1.54 / 2.20 ms | 0.510 s |
| 128 MiB | Raft | 1 | 4.084 s | 31.34 | 7.65 / 11.19 ms | 2.213 s |

异步 SET 在主节点执行后即可确认，各引擎分片随后通过 journal 向副本推送变更。Raft SET 要等多数持久化且 leader 已应用。最后一列从启动进程前计时，包含建连和客户端轮询。默认设置下，节点 13 停机期间分别累积 72 或 136 MiB 的 value（含 8 MiB 预热）和少量标记。异步副本没有本地 RDB，通过全量同步取得最终键值。Raft 通过 Fetch 补齐这段 WAL。追平后，两种模式都从节点 13 逐键校验预热和正式写入的原值。[异步同步与 ACK](../../src/server/replica.cc) · [Raft 写入与提交](../../src/server/cluster/raft/runtime/data_node.cc) · [恢复校验](../../tests/integration/replication_comparison_test.py)

## 预热对 8 连接写入的影响

每轮新建一个集群。三轮依次轮换三种条件的顺序，正式负载均为组 10 上的 256 次新建 SET，共 64 MiB。首笔 P50 来自每条连接的第一笔正式写入，共 8 个样本。

| 初始化及停机检查通过后 | 首笔 SET P50 | 全轮 SET P50 / P95 | 吞吐中位数 | 三轮吞吐范围 |
| --- | ---: | ---: | ---: | ---: |
| 就绪即写 | 35.13 ms | 39.82 / 50.33 ms | 50.98 MiB/s | 50.70–53.04 MiB/s |
| 静置 5 秒 | 41.80 ms | 40.79 / 58.78 ms | 48.90 MiB/s | 45.88–49.89 MiB/s |
| 静置 5 秒，再写 32 笔 | 43.23 ms | 38.80 / 56.94 ms | 50.72 MiB/s | 42.23–52.67 MiB/s |

就绪即写与静置加预热的吞吐中位数相差 0.5%，预热也未稳定降低 P95。同样静置和预热后，异步吞吐仍为 Raft 的 4.49 倍。在这台虚拟机的三轮对照里，预热对吞吐的影响不足以解释两种复制模式的差距。

## Kafka 复制实测与 Dragonfly 的实现差异

同级 Kafka `5b648ca9cb`（4.5.0-SNAPSHOT）在同一台 4 vCPU、Linux ARM64 Colima 虚拟机上运行，使用 OpenJDK 21 和回环网络。普通分区与 KRaft quorum 分开运行，均配置三副本。先验证所有副本确实复制同一条初始化记录，停一台后再次验证剩余两台可提交新记录，然后预热 32 笔。每次只发一条预先生成的 256 KiB 随机记录。普通分区复用已就绪的集群，KRaft 每档 linger 复用一个 quorum，每种规模各跑三轮取各列中位数。吞吐量只计正式记录的载荷。

| Kafka 路径与配置 | 64 MiB 耗时 / 吞吐（s / MiB/s） | 64 MiB P50 / P95（ms） | 128 MiB 耗时 / 吞吐（s / MiB/s） | 128 MiB P50 / P95（ms） |
| --- | ---: | ---: | ---: | ---: |
| 普通分区，`acks=all`，默认刷盘 | 0.578 / 110.72 | 2.03 / 3.54 | 1.817 / 70.44 | 2.95 / 8.60 |
| 普通分区，`acks=all`，`flush.messages=1` | 1.081 / 59.19 | 3.94 / 7.80 | 3.172 / 40.35 | 5.21 / 11.17 |
| KRaft，`controller.quorum.append.linger.ms=0` | 1.132 / 56.52 | 4.18 / 6.53 | 2.855 / 44.83 | 4.31 / 11.43 |
| KRaft，默认 `controller.quorum.append.linger.ms=25` | 7.727 / 8.28 | 29.66 / 32.98 | 15.527 / 8.24 | 29.69 / 33.43 |

**普通分区计到 producer 收到确认。** 单分区、`min.insync.replicas=2`。停机前后三次连续观测到 ISR 分别为 3、2，并分别用 `read_committed` 和每台在线 broker 的本地日志核对标记的 offset 和内容。每个 topic 在三轮测试前预热一次，共 32 条。用 `linger.ms=0`、无压缩的 producer 逐条执行 `send().get()`。本例两台 ISR 成员都追加记录，高水位覆盖目标 offset 后，producer 收到成功。默认追加到文件页缓存即可报告进度，`flush.messages=1` 使 leader 和 follower 每条追加都触发刷盘。消费端在计时后用 `read_committed` 校验 offset 顺序和每条记录的内容。128 MiB 默认刷盘三轮为 70.44、32.02、197.04 MiB/s，同机轮间波动较大。[分区确认](../../../kafka/core/src/main/scala/kafka/cluster/Partition.scala) · [副本追加](../../../kafka/core/src/main/scala/kafka/server/ReplicaFetcherThread.scala) · [刷盘阈值](../../../kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java)

**KRaft 计到 leader 收到提交回调。** 使用上游 `TestRaftServer` 和真实 Raft 存储、网络，仅调整其负载生成和计时。三名 voter 的提交监听器先看到同一 epoch、offset 和标记。停一台后，另外两名 voter 的监听器再确认新的标记。follower 刷盘后通过下一次 Fetch 报告进度。每档 linger 预热一次。32 笔预热后的标记也须经两端提交回调确认，然后交替写入 64、128 MiB。本次两档的三次标记依次位于 epoch 1 的 offset 1、2、35。用单调纳秒时钟从 leader 调用 `prepareAppend` 前计到 `handleCommit` 入口，逐条核对 offset 和内容后才发下一条。128 MiB 负载下，默认攒批的 P50 比关闭攒批高 25.38 ms。该差值对应一次只追加一条记录的负载。[上游负载工具](../../../kafka/core/src/main/scala/kafka/tools/TestRaftServer.scala) · [KRaft 默认攒批配置](../../../kafka/raft/src/main/java/org/apache/kafka/raft/QuorumConfig.java)

## Leader 日志追加与相邻写入间隔

两边都在 leader 进程内逐笔提交，每笔只取两个共同端点。**追加开始**取实际调用本地日志追加函数前的时刻。**多数提交**取本任期的 leader HW 首次覆盖该笔排他结束 offset 的时刻。Dragonfly 分别在 `log_.Append` 前和 `committed_` 更新后采样。KRaft 分别在 `log.appendAsLeader` 前和 `LeaderState` 更新 `highWatermark` 后采样。提交回调另行打点。[Dragonfly 追加与 HW](../../src/server/cluster/raft/quorum/fetch_replication.cc) · [KRaft 日志追加](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java) · [KRaft leader HW](../../../kafka/raft/src/main/java/org/apache/kafka/raft/LeaderState.java)

每种实现新建三轮集群。先核对三票全部就绪，停一台，验证剩余两票提交新记录，静置 5 秒并预热 32 笔。每轮正式追加 256 笔预生成的 256 KiB 随机值，任何时刻最多一笔在途，KRaft 关闭 linger。两个负载程序都等本组提交通知并校验完整值，再提交下一笔。Dragonfly 的专用监听器对测试记录跳过 KV 执行，保留组 worker、合法 SET 日志批、实际 Fetch 和持久化路径。离线节点 13 重启后按正常 KV 回放并读回全部 288 个值。KRaft 的两名在线 voter 也逐笔核验了全部值。

| 同一对端点，进程内单笔在途 | Dragonfly | KRaft |
| --- | ---: | ---: |
| 上一笔 HW → 下一笔本地日志追加，P50 / P95 | 0.668 / 1.031 ms | 0.343 / 0.716 ms |
| 全部相邻间隔占整轮时间 | 14.56% | 8.61% |
| 本笔本地日志追加 → 本笔 HW，P50 / P95 | 4.168 / 6.098 ms | 3.555 / 6.848 ms |
| 首笔追加 → 末笔 HW，64 MiB 耗时 / 吞吐 | 1.294 s / 49.47 MiB/s | 1.200 s / 53.35 MiB/s |

Dragonfly 的本笔提交还可分成两段。进入本地日志追加到本地持久化完成的 P50 / P95 为 1.477 / 3.519 ms，随后到 HW 覆盖为 2.133 / 4.140 ms。两段各自计算分位数。[逐轮数据](replication-benchmark.csv)

**复核方法。** 逐笔文件按实现和轮次编号，纳秒时间戳以本轮首笔提交入口为零点。每轮检查 epoch 一致、排他 offset 连续，并对每笔计算 `hw_ns[i] - append_ns[i]`，对相邻两笔计算 `append_ns[i+1] - hw_ns[i]`。256 段提交耗时和 255 段间隔之和必须精确等于首笔追加至末笔 HW 的时长。间隔占比使用这个时长作分母。表中各列均先按轮统计，再取三轮中位数。[两边的逐笔时间戳](raft-log-timings.csv)

## 数据组复制优化项

| 状态 | 优化项 | Dragonfly 实现与参照 | 验证结果或试验指标 |
| --- | --- | --- | --- |
| 优先优化 | 缩短日志追加前的准备时间 | 分项测量路由检查、SET 记录构造、值复制、编码和 CRC32C。 | 对照准备阶段和相邻写入间隔的 P50、P95，再用真实 SET 复测。[实测差距与验收方法](#优先优化日志追加前的准备开销) |
| 已优化 | 事件驱动 Fetch 与提交等待 | 参照 KRaft 长轮询，组内日志、HW 或角色变化唤醒相应等待方。每个 peer 一次一个请求，follower 落盘后立即续拉并报告持久化位置。 | 暂停两台副本，SET 保持等待。恢复一票后，同一请求完成并可读回。[组内调度](../../src/server/cluster/raft/runtime/group.cc) · [进程回归](../../tests/integration/replication_comparison_test.py) |
| 已优化 | leader 与 follower 并行刷盘 | 参照 KRaft 提前完成挂起的 Fetch。leader 校验并写全普通数据批后，可在本地刷盘前将其连同原 HW 发给正在日志尾等待的 follower。 | 阻塞 leader 的日志段刷盘，在线 follower 已收到该值，SET 仍在等待。解除阻塞后写入成功。[KRaft](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L679-L683) · [组内 Fetch](../../src/server/cluster/raft/quorum/fetch_replication.cc) |
| 已优化 | 既有日志段追加时省去目录同步 | Kafka 仅在刷盘范围含新段时同步父目录。Dragonfly 复用 DFE3 恢复标记文件，每批同步日志段一次、标记文件两次。 | [实测刷盘次数](#存储与网络回归) · [Kafka 存储](../../../kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java#L265-L277) · [分区追加](../../src/server/cluster/raft/log/partition_log.cc) |
| 已优化 | 并发普通 SET 自然合批 | KRaft 一次取出的多个批次共享刷盘。Dragonfly 首笔立即派发，worker 取走前已到达的同组普通 SET 可共享刷盘。每批最多 32 笔、键值合计 1 MiB，合法的超大单笔独立追加。 | 每笔保留自己的 offset，按连接执行 WAIT。NX/XX 独立求值，8 连接同时对一个键执行 NX，只有一笔成功。[KRaft 批次累积](../../../kafka/raft/src/main/java/org/apache/kafka/raft/internals/BatchAccumulator.java) · [数据组写入](../../src/server/cluster/raft/runtime/data_node.cc) · [条件写回归](../../tests/integration/raft_data_group_test.py) |
| 已优化 | leader 从内存应用已提交记录 | 参照 KRaft 提交回调复用内存记录。HW 覆盖后，leader 按 offset 应用当前 epoch 缓存的 SET 效果，每组上限 4,096 笔、8 MiB。 | 缓存未命中、换主或重启时从 WAL 重放。回归覆盖旧 epoch 缓存与同一 offset 被覆盖的情况。[KRaft 回调](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L380-L385) · [效果应用回归](../../src/server/cluster/raft/data/set_effect_test.cc) |
| 已实现 | 复用提交记录的执行器 | 同一数据组按提交顺序共用一个 `JournalExecutor`，避免每笔 SET 重做跨分片的数据库激活。 | 快照加增量恢复后，在 DB 0 和 DB 1 读回同名键各自的值。[数据组执行](../../src/server/cluster/raft/runtime/data_node.cc) · [跨 DB 恢复测试](../../tests/integration/raft_data_group_test.py) |
| 已实现 | 按帧长接收大 Fetch 响应 | peer 校验 Kafka 长度前缀及上限后，为完整响应分配缓冲并直接收取剩余字节。 | TCP 用例覆盖大帧后复用同一连接以及遇到超长响应后重连。这两项改动合并测试时，8 连接吞吐未见稳定改善。[接收逻辑](../../src/server/cluster/raft/transport/peer_connection.cc) · [TCP 回归](../../src/server/cluster/raft/transport/raft_listener_test.cc) |
| 可尝试 | WAL 使用 io_uring 异步写入和刷盘 | 网络已支持 io_uring，WAL 当前由组内 worker 同步写盘。试验保持日志段、标记文件的持久化顺序和多数确认条件。 | 固定磁盘和 io_uring 网络后端，对比同步与异步 WAL 的单连接提交 P95、8 连接吞吐，并复验刷盘中断后的恢复。[WAL 与 io_uring](#io_uring-能承接哪些操作) |
| 可尝试 | 调整普通 SET 合批上限 | 以现有 32 笔、1 MiB、无固定攒批等待为基线，分别调整笔数和字节上限。 | 用 1、8、32 条连接分别写小值和 256 KiB 值，记录每批实际笔数、每千笔日志段同步次数、吞吐和 SET P95。[合批入口](../../src/server/cluster/raft/runtime/data_node.cc) |
| 可尝试 | 分别调整日志 Fetch 与快照分块大小 | KRaft 分开配置两种请求的字节预算，默认均为 1 MiB。Dragonfly 共用一个 1 MiB 常量，日志 Fetch 至少返回完整的首批。 | 保持单请求在途，分别试 1、4、8 MiB。对保留 WAL 和需要快照的副本分别测追平时间，同时记录健康多数的 SET P95 和峰值内存。[KRaft 配置](../../../kafka/raft/src/main/java/org/apache/kafka/raft/QuorumConfig.java) · [Fetch 请求](../../src/server/cluster/raft/runtime/group.cc) |

### 优先优化日志追加前的准备开销

在上述 256 KiB、单笔在途的负载下，Dragonfly 相邻写入间隔的 P50 为 KRaft 的 **1.95 倍**，两者的 P50 相差 **0.325 ms**，P95 为 KRaft 的 **1.44 倍**。三轮中，Dragonfly 的间隔 P50 为 0.659–0.695 ms，KRaft 为 0.321–0.381 ms。优先优化下一笔从开始提交到进入本地日志追加的这段时间。

| 下一笔提交入口 → 本地日志追加入口 | Dragonfly | KRaft |
| --- | ---: | ---: |
| P50 / P95 | 0.607 / 0.923 ms | 0.163 / 0.423 ms |
| 累计耗时占全部相邻间隔 | 90.60% | 50.74% |
| 累计耗时占整轮时间 | 13.19% | 4.20% |

这一段 Dragonfly 的 P50 为 KRaft 的 **3.72 倍**。Dragonfly 在此检查组内路由、构造 SET 记录并编码日志批，KRaft 在此执行 `prepareAppend`、调度并取出批次。现有探针量出了整段开销，各步骤的耗时仍待分项测量。这组基线来自 Dragonfly 的 C++ Debug 构建和 Kafka 的 JDK 21 进程。[Dragonfly 数据组写入](../../src/server/cluster/raft/runtime/data_node.cc) · [SET 记录编码](../../src/server/cluster/raft/data/set_effect.cc) · [KRaft 批次累积](../../../kafka/raft/src/main/java/org/apache/kafka/raft/internals/BatchAccumulator.java)

**定位与验收。** 先分别测量路由检查、SET 记录构造、值复制、批次编码与 CRC32C 的耗时，再针对实测占时最多的一步做改动。用相同构建配置和预热条件交替运行基线与候选版本，各三轮。先看准备阶段及整个相邻间隔的 P50、P95 能否稳定下降，再复测 1、8 连接的真实 Redis SET 吞吐和 P95。两组对照都保留多数持久化确认及节点 13 重启后的逐键读回校验。[日志批编码与校验](../../src/server/cluster/raft/log/record_batch.cc) · [客户端写入与恢复测试](../../tests/integration/replication_comparison_test.py)

### io_uring 能承接哪些操作

**网络已支持。** Linux 启动时探测 io_uring，可用时 Raft 的 Fetch 和 FetchSnapshot 连接随 Helio proactor 使用它。通过数据节点的 `INFO SERVER` 可查到 `multiplexing_api:iouring`。[启动探测](../../src/server/dfly_main.cc) · [Raft 连接](../../src/server/cluster/raft/transport/peer_connection.cc) · [进程用例](../../tests/integration/replication_comparison_test.py)

**WAL 异步落盘仍待试验。** 当前每组的单线程 worker 依次执行日志写入和 `fsync`。可以将文件 I/O 交给使用 Helio `LinuxFile` 的 proactor fiber，使其在等磁盘时让出执行线程。一次追加仍须依次完成日志段同步、恢复标记备用记录同步和选择新记录的文件长度同步。整条链成功后，本节点才可公布新的持久化位置。单连接 SET 仍需等待多数持久化和 leader 应用，收益需在相同网络后端实测。[组内 worker](../../src/server/cluster/raft/runtime/group.cc) · [分区追加](../../src/server/cluster/raft/log/partition_log.cc) · [恢复位置发布](../../src/server/cluster/raft/log/log_files.cc) · [Helio 文件接口](../../helio/util/fibers/uring_file.h)

## 阻塞与快照恢复

副本用 `SIGSTOP` 暂停，保留连接。以下为单次故障注入的观测值。SET 和后续的 WAIT 使用同一条连接。[阻塞用例](../../tests/integration/replication_comparison_test.py)

| 组 10 暂停的副本数 | 模式 | SET 结果与耗时 | 随后的 `WAIT 2 500` |
| ---: | --- | --- | --- |
| 1 | 异步 | 成功，0.19 ms | 返回 1，500.93 ms |
| 1 | Raft | 成功，4.59 ms | 返回 1，501.56 ms |
| 2 | 异步 | 成功，0.20 ms，新值可读 | 返回 0，501.95 ms |
| 2 | Raft | 2917.68 ms 后返回 `TRYAGAIN Outcome of Raft write is unknown` | — |

失去两台副本时，Raft leader 的日志末端（LEO）达到 7，高水位（HW）停在 6。另一条连接仍读到旧值，组 20 仍可写读。在单独的恢复用例中，及时恢复一票使原 SET 成功，HW 从 5 推进至 6，含暂停的耗时为 8.13 ms。

**异步全量同步的反压。** 未遍历的 bucket 在覆盖写前需要序列化。副本停止读取且快照输出缓冲耗尽时，同分片读写可受反压。回归还验证了增加缓冲后覆盖写完成，以及断开慢副本后缓冲排空和重新同步。[全量同步反压回归](../../tests/dragonfly/replication_test.py)

**Raft 快照传输中断。** 集群通过全员拓扑和 HW 检查后，先让节点 13 落后到需要下载快照，收到部分快照后暂停该节点，再强杀、重启。两个规模各测一次。两次 `WAIT 2 500` 均返回 1，重启后 `WAIT 2` 均返回 2。测试校验快照的 SHA-256、全部键值及暂停和重启期间的尾部写入。[快照故障注入与读回](../../tests/integration/raft_snapshot_stall_test.py)

| 正式载荷 | 键数 | 快照文件大小 | 暂停时已下载 | 健康多数 SET | `WAIT 2 500` 耗时 | 再次启动至 `WAIT 2` 返回 2 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 MiB | 256 | 67,134,160 B | 2 MiB | 3.17 ms | 500.48 ms | 0.820 s |
| 128 MiB | 512 | 134,267,632 B | 1 MiB | 7.68 ms | 500.74 ms | 1.418 s |

## 存储与网络回归

以下为独立的存储升级、刷盘和网络后端回归记录。

**WAL 刷盘。** 8 连接向既有段写 64 笔 256 KiB 值，leader 的日志段、标记文件、组目录分别同步 44、88、0 次。[分区追加](../../src/server/cluster/raft/log/partition_log.cc)

**格式升级。** 七进程升级测试使用旧二进制生成数据组和 controller 状态，强杀所有进程并移除引导拓扑，再用新二进制从原目录启动。两个数据组都能读回旧值并确认新写，11 个原有 DFE2 文件升级为 DFE3。再次强杀并重启后，控制器恢复槽归属，两组新值仍可读回。[日志格式与恢复标记](raft-replication-design.md#日志恢复标记)

**io_uring 网络。** 在 Debug 构建中移除测试的 `--force_epoll` 后，已核实八个进程均持有 ring，五个数据节点均报告 `iouring`。暂停两台副本后，挂起的 SET 等到恢复一票才完成。另一个用例写入 4 MiB 数据，离线副本重启后通过 Fetch 追平，逐键读回通过。Dragonfly 异步与 Raft 的性能对照使用 epoll。[Raft 连接](../../src/server/cluster/raft/transport/peer_connection.cc)

## KRaft 用例对应

对照同级 Kafka `5b648ca9cb` 的 quorum 测试。Redis SET、WAIT、读回 KV 和进程停顿由 Dragonfly 直接验证。

| Kafka 上游类及方法 | Dragonfly 对应断言 |
| --- | --- |
| `LeaderStateTest.testUpdateHighWatermarkQuorumSizeTwo`、`testUpdateHighWatermarkQuorumSizeThree`、`testCheckQuorum` | 两票组需要两票，三票组需要两票。只有本组不同 voter 的有效进度能推进 HW、维持多数联系。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/LeaderStateTest.java) · [HW](../../src/server/cluster/raft/replication/high_watermark_test.cc) · [quorum 检查](../../src/server/cluster/raft/quorum/epoch_control_test.cc) |
| `KafkaRaftClientTest.testListenerCommitCallbackAfterLeaderWrite`、`testLeaderShouldResignLeadershipIfNotGetFetchRequestFromMajorityVoters`、`testFollowerReplication`、`testHandleCommitCallbackFiresAfterFollowerHighWatermarkAdvances` | 本地追加后、HW 推进前读到旧值且写入未确认。恢复一票使同一请求完成，持续失联导致退位。副本续拉后能应用并读回全部 KV。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientTest.java) · [进程对照](../../tests/integration/replication_comparison_test.py) |
| `KafkaRaftClientFetchTest.testFetchMaxBytesAlwaysReturnsAtLeastOneBatch`、`testFetchMaxBytesWithTwoBatches` | 预算为 1 字节仍可取得完整首批。另一例的预算容纳两个完整批次以及第三批的一个字节，副本只持久化完整前缀，下次 Fetch 从第三批开始。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientFetchTest.java) · [批次边界测试](../../src/server/cluster/raft/quorum/fetch_replication_test.cc) |
| `KafkaRaftClientSnapshotTest.testLeaderImmediatelySendsSnapshotId`、`testFetchSnapshotResponsePartialData`、`testFollowerListenerNotified` | 落后于可读起点时返回快照 ID，按字节位置分块读取，完整安装后从快照 cut 继续应用日志。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientSnapshotTest.java) · [快照协议](../../src/server/cluster/raft/quorum/snapshot_transfer_test.cc) · [数据恢复](../../tests/integration/raft_snapshot_stall_test.py) |
| `KafkaRaftClientTest.testPurgatoryFetch*`、`testResignWillCompleteFetchPurgatory` 与 `KafkaRaftClientFetchTest` 的 HW 长轮询用例 | 追加、HW 推进、超时或退位使挂起的 Fetch 返回。重新求值时仅按收到原请求的时刻登记 voter 联系。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientFetchTest.java) · [Fetch 回归](../../src/server/cluster/raft/quorum/fetch_replication_test.cc) · [监听器停机](../../src/server/cluster/raft/transport/raft_listener_test.cc) |

## 复现

在 Linux 的 Dragonfly 仓库根目录运行。配对测试和快照中断测试需要 Python 3.10+ 和 `redis` 包，异步反压测试还需要仓库的 pytest 依赖。批量写入用例输出每轮的静置秒数、预热笔数、各连接首笔 SET 的 P50、整轮 SET 分位数及重启追平时间。下列命令使用默认的 5 秒和 32 笔。将 `REPLICATION_TEST_SETTLE_SECONDS` 与 `REPLICATION_TEST_WARMUP_WRITES` 这对参数设为 `(0, 0)` 或 `(5, 0)` 即可复测就绪即写和只静置两种条件，集群就绪检查始终执行。

```sh
ninja -C build-dbg dragonfly raft_storage high_watermark_test epoch_control_test \
  fetch_replication_test snapshot_transfer_test raft_listener_test
ctest --test-dir build-dbg --output-on-failure \
  -R '^(high_watermark|epoch_control|fetch_replication|snapshot_transfer|raft_listener)_test$'
export DRAGONFLY_PATH="$PWD/build-dbg/dragonfly"
bulk() {
  REPLICATION_TEST_MIB="$1" REPLICATION_TEST_CLIENTS="$2" \
    python3 tests/integration/replication_comparison_test.py -k lagging_replica_catches_up_bulk -v
}
for trial in 1 2 3; do
  bulk 64 1
  bulk 64 8
  bulk 128 1
done
python3 tests/integration/replication_comparison_test.py -k stopped_replicas -k pending_set -v
for mib in 64 128; do
  REPLICATION_TEST_MIB=$mib python3 tests/integration/raft_snapshot_stall_test.py -v
done
python3 -m pytest --df force_epoll \
  tests/dragonfly/replication_test.py::test_replication_full_sync_writes_do_not_wait_for_replica \
  tests/dragonfly/replication_test.py::test_replication_full_sync_output_buffer_replica_stalls
```

以下命令在同级 Kafka 仓库用 JDK 21 运行对应的协议回归用例。Kafka 性能表的计时来自前述三节点 JVM 进程。

```sh
cd ../kafka
./gradlew :raft:test --rerun \
  --tests LeaderStateTest.testUpdateHighWatermarkQuorumSizeTwo \
  --tests LeaderStateTest.testUpdateHighWatermarkQuorumSizeThree \
  --tests LeaderStateTest.testCheckQuorum \
  --tests KafkaRaftClientTest.testListenerCommitCallbackAfterLeaderWrite \
  --tests KafkaRaftClientTest.testLeaderShouldResignLeadershipIfNotGetFetchRequestFromMajorityVoters \
  --tests KafkaRaftClientTest.testFollowerReplication \
  --tests KafkaRaftClientTest.testHandleCommitCallbackFiresAfterFollowerHighWatermarkAdvances \
  --tests KafkaRaftClientFetchTest.testFetchMaxBytesAlwaysReturnsAtLeastOneBatch \
  --tests KafkaRaftClientFetchTest.testFetchMaxBytesWithTwoBatches \
  --tests KafkaRaftClientSnapshotTest.testLeaderImmediatelySendsSnapshotId \
  --tests KafkaRaftClientSnapshotTest.testFetchSnapshotResponsePartialData \
  --tests KafkaRaftClientSnapshotTest.testFollowerListenerNotified \
  --tests 'KafkaRaftClientTest.testPurgatoryFetch*' \
  --tests KafkaRaftClientTest.testResignWillCompleteFetchPurgatory \
  --tests KafkaRaftClientFetchTest.testUnknownHwmDeferredWhenLeaderDoesNotKnowHwm \
  --tests KafkaRaftClientFetchTest.testOutdatedHwmCompletedWhenLeaderKnowsHwm \
  --tests KafkaRaftClientFetchTest.testUnchangedHighWatermarkDeferred \
  --tests KafkaRaftClientFetchTest.testUpdatedHighWatermarkCompleted
```
