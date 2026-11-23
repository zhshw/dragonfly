# Dragonfly 数据组复制测试与优化

## 结论

### 最新结论：pipeline 64 逐档测试，8 连接停止

本次每连接先连续发送 64 笔 SET，再按序收齐这一批回复；**没有逐笔等待回复**。
普通 SET 的提交与确认已解耦，但这是有界窗口，不代表所有等待点都已消除。
固定 256 MiB、256 KiB/value、64 笔预热、5 秒静置，在同一 Ubuntu/NVMe/ext4 主机上
分别对改前异步 Raft、最新 Raft 和原 cluster 各跑三轮，版本哈希仍为 `14a092bdf6d6…`
和 `71080870a731…`。原 cluster 与最新 Raft 使用同一未插桩二进制。

按 1 → 2 → 4 → 8 连接逐档运行；先汇总当前档，再决定是否增加连接数。
第一档要求最新 Raft 相对改前版本有收益，随后要求最新 Raft 的复制确认吞吐和 SET
返回窗口吞吐相对上一档都提高超过 5%。该门槛用于避免把 WAIT 检查时机波动误判为
扩展收益，是小样本筛选规则，不是长期稳态性能保证。各列为三轮中位数：

| 连接数（每连接 pipeline 64） | 改前 Raft 确认 MiB/s | 最新 Raft 确认 MiB/s | 原 cluster 确认 MiB/s | 最新 Raft SET MiB/s | 最新 Raft 批次 P95 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 85.71 | 166.75 | 625.48 | 178.47 | 114.45 |
| 2 | 131.52 | 178.51 | 616.15 | 192.08 | 242.25 |
| 4 | 150.89 | 189.51 | 626.87 | 202.26 | 613.65 |
| 8 | 165.28 | 186.90 | 656.79 | 201.91 | 1041.93 |

1 连接最新 Raft 的确认吞吐相对改前提高 94.6%，证明同连接提前提交有实际收益。
1 → 2 连接的确认/SET 吞吐分别提高 7.1% / 7.6%；2 → 4 为 6.2% / 5.3%，已接近门槛。
4 → 8 则分别下降 1.4% / 0.2%，批次 P95 从 613.65 增至 1041.93 ms，**因此停在 8 连接，
没有测试 16、32、64 连接**。批次 P95 是整批 64 笔的耗时，不能当作单笔 SET 延迟。
原 cluster 在各档仍显著更快；它的 WAIT 确认副本应用，Raft SET 还包含多数持久化。

36 轮均通过所有写连接的 WAIT 1、离线副本重启后的 WAIT 2，以及全部 1088 个值读回。
当前结论是“同连接提前提交有效，但吞吐在 4→8 连接已不再增长，排队延迟仍明显增加”。
尚不能仅据此把瓶颈归因于 io_uring、编码、应用或锁竞争，也不能宣称阻塞已全部消除。

源码可见的剩余等待包括：全组 8 MiB 票据保留到回复释放；`TrySet` 未获容量时，队首
可退回 `Set → QueueSet → WaitApplied`；组 worker 的普通操作等待在途 WAL 追加排空；
提交后的应用仍由 worker 等待 proactor 执行。上述路径的命中次数、等待时长及性能占比
还没有本轮 trace 证据。本次先提交实现及结论，后续再用 CPU/等待火焰图或请求 trace
定位这些路径，不新增连接档位或据猜测继续改实现。

[本次逐轮 CSV](benchmarks/replication-pipeline-stepwise-ubuntu-20261123.csv) ·
[各档环境与哈希](benchmarks/replication-pipeline-stepwise-ubuntu-20261123.environment.json) ·
[逐档增益与停止决定](benchmarks/replication-pipeline-stepwise-ubuntu-20261123.decisions.json)。
完整日志在远端隔离仓库 `results-pipeline-stepwise-20261123-c{1,2,4,8}/`。

### 对照数据：每连接一笔在途（不用于验证同连接提前提交收益）

2026-11-23 按 1/8/64 连接矩阵重新运行，以下均为本次新采样，不复用旧表数字。
两种模式使用同一个最新、未插桩 Release 二进制 `71080870a731…`，已包含同连接 SET
异步提交改动。测试在 `172.31.23.2` 的 i7-13700KF / Ubuntu 26.04 / Linux 7.0.0-43 /
WD_BLACK SN770 上执行，数据位于 `/dev/nvme0n1p4` 的 ext4。

负载为 256 MiB、256 KiB/value、64 笔预热、5 秒静置；1/8/64 连接各三轮，两种模式
轮换先后顺序，共 18 轮。明确设置 `REPLICATION_TEST_PIPELINE=1`，每条连接等上一笔
SET 回复后才发下一笔。Raft 使用 Folly / liburing 2.15、异步追加、自适应 1/4 MiB drain
和 4 MiB 数据 Fetch；网络为 epoll，每数据节点两个网络线程。三副本停掉第三台后测量，
离线副本重启与读回在写入、在线确认计时结束后单独执行。

**主指标为复制确认完成吞吐。** 从首笔正式 SET 发出开始，全部 SET 回复后，在原写连接
并发执行 `WAIT 1 5000`，只有每条连接都返回 1 才停止计时。没有在每笔 SET 后插入 WAIT。
下表各列独立取三轮中位数，因此耗时列的中位数相加不一定等于总耗时中位数。

| 连接数 | 实现 | 复制确认完成耗时 ms | 复制确认吞吐 MiB/s | SET 返回窗口 ms | 批末确认尾段 ms | 重启至 `WAIT 2` ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 原 cluster | 497.82 | **514.24** | 396.96 | 100.86 | 649.72 |
| 1 | Raft 异步 + 自适应 drain | 3515.39 | **72.82** | 3414.12 | 101.21 | 1103.31 |
| 8 | 原 cluster | 372.34 | **687.54** | 270.15 | 102.19 | 631.94 |
| 8 | Raft 异步 + 自适应 drain | 1502.78 | **170.35** | 1405.95 | 100.51 | 1202.31 |
| 64 | 原 cluster | 383.10 | **668.23** | 274.04 | 109.06 | 653.31 |
| 64 | Raft 异步 + 自适应 drain | 1106.65 | **231.33** | 1104.32 | 2.18 | 1113.05 |

批末确认尾段从最后一个 SET 返回计到全部 WAIT 返回，包含客户端调度及 WAIT 的检查间隔。
本次 8 连接 Raft 三轮尾段为 100.64 / 100.51 / 0.37 ms；64 连接为 2.18 / 2.01 / 2.33 ms。
不能统一扣掉 100 ms，也不能把这段时间全部解释为复制传输耗时。

原 cluster 的 WAIT 确认副本 journal 已执行；Raft 的 WAIT 检查 follower 应用位置，SET
本身还要求多数持久化。原 cluster 的 WAIT **不是磁盘 fsync 确认**。统一的是副本已应用
的可观察确认端点，两者持久性保证仍不同。原 cluster 仍更快，本次 Raft 在 1/8/64 连接
下分别达到其复制确认吞吐的 14.2% / 24.8% / 34.6%。

18 轮均通过所有写连接的 WAIT 1，并在节点 13 重启、WAIT 2 成功后逐个核对全部 1088 个
预热及正式值。原 cluster 验证全量同步，Raft 验证普通 Fetch 追平且没有依赖数据快照。
两种模式完整 SHA-256 均为 `71080870a7317f772da29d2c8aae50c82a1e5827b4efa47cb64a6bf759769d02`。

[本次逐轮 CSV](benchmarks/replication-confirmed-latest-ubuntu-20261123-rerun1.csv) ·
[环境、二进制及负载脚本哈希](benchmarks/replication-confirmed-latest-ubuntu-20261123-rerun1.environment.json) ·
[测试入口](../../tests/integration/raft_commit_benchmark/dragonfly/cluster_compare.py)。
完整日志在远端隔离仓库 `results-replication-confirmed-latest-20261123-rerun1/`。
截图所示的旧版 `14a092bdf6d6…` 数据保留为
[历史 CSV](benchmarks/replication-confirmed-ubuntu-20261123.csv) 和
[历史环境](benchmarks/replication-confirmed-ubuntu-20261123.environment.json)，不混入本次汇总。

### 本轮：同连接 SET 已能提前提交

以下保留前次 27 轮的服务端改动配对试验，独立于文首本次复测。该改动将普通 SET 的入队与确认等待拆开，在 `raft_drain_writes=true` 的 RESP V2
路径启用。2026-11-23 在同一 Ubuntu/NVMe/ext4 主机对旧异步 Raft、新 Raft、原 cluster
交替测试，三个负载各三轮，共 27 轮。负载均为 256 MiB、256 KiB/value、64 笔预热，
**下表以全部写连接的批末 WAIT 1 完成为终点，单位 MiB/s，取三轮中位数。**

| 负载 | 旧异步 Raft | 新 Raft | 原 cluster |
| --- | ---: | ---: | ---: |
| 1 连接，每笔等回复 | 77.34 | 69.99 | 514.03 |
| 1 连接，pipeline 64 | 81.48 | **163.75** | 613.00 |
| 64 连接，每连接一笔在途 | 215.69 | 217.53 | 666.70 |

单连接 pipeline 提高 **101.0%（2.01 倍）**，三轮范围从 81.39–81.86 变为
159.53–168.79 MiB/s，未重叠。SET 返回窗口吞吐为 84.21 → 175.10 MiB/s，
64 笔批次 P95 为 225.78 → 114.17 ms（降低 49.4%）；这是批次耗时，不是单笔延迟。

代价也存在：单连接逐笔确认吞吐下降 9.5%，SET P95 从 3.54 增至 4.03 ms。
64 连接的 SET 返回窗口吞吐从 235.88 降至 218.01 MiB/s（下降 7.6%），P95 从
88.48 增至 100.13 ms。其复制确认吞吐看似持平，但批末确认尾段中位数由 101.34
降至 4.68 ms，受到 WAIT 检查时机影响，不能据此说多连接没有回退。新路径增加输入
持有及每笔完成 fiber；尚未隔离复制、分配和调度各自的贡献，后续应在这一提交路径上
减少开销。开关仍默认关闭。本轮解决同连接提交阻塞，未消除组 worker、WAL 追加及应用的串行工作。

原 cluster 的 pipeline 确认吞吐仍约为新 Raft 的 3.74 倍；原 cluster 的 WAIT 确认副本应用，
Raft 的 SET 还包含多数持久化，两者持久性保证仍不同。27 轮均通过 WAIT 1、离线副本重启
后的 WAIT 2 及全部 1088 个值读回。旧 Raft SHA-256 为 `14a092bdf6d6…`，新 Raft 与
原 cluster 使用同一二进制 `71080870a731…`，均未插桩。

[逐轮 CSV](benchmarks/replication-pipeline-admission-ubuntu-20261123.csv) ·
[二进制哈希、负载脚本哈希及磁盘](benchmarks/replication-pipeline-admission-ubuntu-20261123.environment.json) ·
[正确性验证](#本轮服务端异步提交) ·
[验证计数、源码哈希及日志清单](benchmarks/replication-pipeline-admission-ubuntu-20261123.validation.json)。
完整日志及固定二进制保留于远端隔离仓库的
`results-pipeline-admission-20261123/` 和 `results-pipeline-admission-release/`。

## 为什么副本确认后原 cluster 仍更快

### 批末确认没有改变每笔写入的背压

两种模式的最后一个完成端点都包含在线副本应用，但到达终点的过程不同。负载每连接一笔在途：原 cluster 的 SET 在本地写入并发布 journal 后返回，下一笔可以与前一笔的副本传输、执行重叠；Raft 的 SET 要等多数持久化和 leader 应用，逐笔等待负载的客户端收到回复后才发送下一笔。服务端允许 pipeline 提前提交，但逐笔等待的客户端没有使用这个能力。末尾统一 WAIT 只能确认积压已清空，不能把前面的发送节奏变成相同的协议。不能用主表推导“原 cluster 每笔副本确认延迟只有 Raft 的几分之一”。

原 cluster 的连续 journal 流由分片发送队列推送，副本执行后报告 ACK；没有逐批 WAL 段和恢复标记刷盘。Raft 在同一数据组维护有序日志，每组仅一轮追加在途，leader 和 follower 都要完成日志段与 DFE4 标记两次持久化屏障。io_uring 使等待期间能处理 Fetch，并未消除屏障本身。[journal 推送](../../src/server/journal/streamer.cc) · [分片发送队列](../../src/server/journal/buffered_socket_writer.cc) · [WAL 状态机](../../src/server/cluster/raft/log/partition_log.cc)

### 同连接阻塞的旧版基线

下面旧版数据对应修改前的 `14a092bdf6d6`。这是实现限制，不是 Raft 要求每笔都串行等回复。旧版 `Service::DispatchCommand` 对 Raft 的 `ONLY_ASYNC` 返回 `WOULD_BLOCK`，也不设置 deferred reply；`DispatchSquashedBatch` 在 Raft 模式直接返回，不走 pipeline squashing。随后 `DataNodeImpl::ProposeSet` 等待本笔 applied offset。只给客户端添加 pipeline，旧版仍会在服务端逐笔等待，不能自动把一条连接变成多笔 Raft 在途。[命令分发](../../src/server/main_service.cc) · [提交及应用等待](../../src/server/cluster/raft/runtime/data_node.cc)

本轮据此为普通无条件 SET 增加有界提交窗口：按连接顺序分配日志位置，把提交与回复等待分开，让已经收到的多笔请求参与 drain；提交、应用完成后依次回复。不同 key 不需要等待前一笔完成才进入日志；同 key 的无条件写也可按日志顺序追加，但必须保持应用与响应顺序。GET、WAIT、NX/XX、连接状态变更等需要顺序屏障；不能直接移除现有保护而让 WAIT 的 `raft_last_write`、读己之写或条件求值失序。所有 OK 继续要求多数持久化，不提前成功。

8/64 连接负载已经提供多笔在途，仍然存在差距，说明只解除同连接限制不会消除全部成本。本轮分别比较 1 连接 × 64 笔客户端窗口与 64 连接 × 1 笔窗口，并测实际 batch 大小、内存反压和提交延迟，避免只比较连接数。

**旧版只改客户端 pipeline，没有修改服务端。** 同一个 Folly Release 二进制（`14a092bdf6d6`）、ext4、256 MiB、全部不同 key，三个负载按轮换顺序各跑三次。pipeline 在一条物理连接上打包发送 64 个 SET，逐个收齐回复后发下一批；并不表示当前服务端同时接受了 64 笔 Raft 追加。三轮中位数如下：

| 负载 | SET 返回窗口吞吐 MiB/s | 副本确认吞吐 MiB/s | 副本确认吞吐三轮范围 |
| --- | ---: | ---: | ---: |
| 1 连接，每笔等回复 | 82.06 | **79.48** | 76.04–83.03 |
| 1 连接，pipeline 64 个不同 key | 84.22 | **81.50** | 80.60–86.53 |
| 64 连接，每连接一笔在途 | 228.95 | **220.18** | 209.93–229.03 |

单连接 pipeline 的复制确认吞吐只增加 2.5%，三轮范围重叠，没有显示明显改善；独立连接并发约为单连接逐笔等待的 2.77 倍。结果支持“同连接仍被服务端串行路径限制”的源码判断。**这组旧版数据不包含本轮服务端提交与确认解耦的改动，不能当作解除阻塞后的性能结果。** 9 轮全部通过在线副本确认及离线副本恢复后的 1088 个值校验。[逐轮数据](benchmarks/replication-pipeline-ubuntu-20261123.csv) · [环境与二进制哈希](benchmarks/replication-pipeline-ubuntu-20261123.environment.json) · [负载入口](../../tests/integration/raft_commit_benchmark/dragonfly/pipeline_compare.py)。

### 本轮服务端异步提交

普通 SET 使用 `TrySet` 在当前连接执行流中按序入队，立即交回 facade 执行后续 SET。
每组异步票据最多 256 笔 / 8 MiB，保留至完成及回复释放；满窗口的非队首请求不入队，
等成为队首再重试。票据持有请求数据及独立完成计数，断连不会让组 worker 引用已释放参数。
每笔仍须多数持久化且 leader 应用完成才能 OK，回复按连接原顺序发出并更新 WAIT 位置。
同 key 的普通覆盖写也可提前提交；GET、WAIT、NX/XX、SELECT、RESET 等继续是顺序屏障。

验证直接暂停唯一在线 follower：同连接 64 笔全部进入 leader 日志时，HW 未推进，
客户端没有收到任何回复；恢复 follower 后才收到 64 个 OK。同一测试在旧二进制上
无法满足 64 笔追加断言。另覆盖未确认写入断连、在途请求关机、300 笔请求及 12 MiB
有效载荷越过窗口、9 MiB 单笔超限错误、条件写、跨 DB 及两票组崩溃恢复。
Debug 和 Release 的 9 项分发单测均通过；Debug 的基础数据组、成员变更、pipeline
及恢复回归通过（Kafka Java 用例因未配置 classpath 跳过）。三个新增 pipeline 用例在
Release 和 ASAN+UBSAN 下全部通过，ASAN 单测及集群回归启用 `detect_leaks=1`，未报告问题。
旧版本运行 64 笔提前提交断言按预期失败；未将该失败计入候选版本通过数。

本轮旧/新版本及原 cluster 的配对性能结果见文首。客户端 pipeline 64 不代表固定 64 笔
服务端在途：256 KiB 值受 8 MiB 票据预算约束，单连接同时持有约 31 笔普通 SET，
已完成但尚未回复的票据也计入预算。64 笔暂停副本证明使用小值。

### Fetch、应用与复制的剩余成本

follower 一次一个 Fetch，收到数据后写 WAL 并持久化，下一次 Fetch 才报告持久位置；leader 推进 HW，follower 再收到 HW 才应用，后续请求携带应用进度供 WAIT 检查。一次 Fetch 可以同时传下一批数据与确认上一批，不应将它理解成每笔固定几个独立 RTT，但当前 follower 仍串行执行接收、落盘和续拉。[Fetch 循环](../../src/server/cluster/raft/runtime/group.cc) · [持久位置与 HW](../../src/server/cluster/raft/quorum/fetch_replication.cc)

提交后的应用也在组 worker 的同步路径上：`Synchronize` 调用 `apply_`，通过 `owner_->Await` 回网络 proactor，由 `JournalExecutor` 执行 SET。此时 worker 仍等待应用完成。leader 的效果缓存省去 WAL 重读，但尚未解除逐笔应用调度；follower 还需要读取和解码已提交日志。[应用回调](../../src/server/cluster/raft/runtime/data_node.cc) · [效果执行](../../src/server/cluster/raft/data/set_executor.cc)

另有可见的数据搬运：`SetEffect` 持有键值，`MakeSetEffectBatch` 生成载荷和 Kafka 日志批；合批拼接后，`FetchReplication` 再保存共享 payload，`PendingAppend` 保有写缓冲，Fetch 响应及应用命令也有复制。原 cluster 的 journal 编码和 `iovec` 发送路径更短。源码能确认这些操作存在，具体贡献须由当前 Folly 的阶段测量判断，不能沿用已删除的旧同步版本百分比。[SET 编码](../../src/server/cluster/raft/data/set_effect.cc) · [日志批](../../src/server/cluster/raft/log/record_batch.cc) · [异步追加](../../src/server/cluster/raft/quorum/fetch_replication.cc)

## 当前实现与后续试验

### 真实磁盘与 tmpfs 的诊断对照

以下为旧版 `14a092bdf6d6…` 的诊断，未在本次重新测量。在同一主机用与旧版磁盘基线相同的未插桩二进制、负载和恢复断言，将结果及数据目录改到 `/tmp` 的 tmpfs，原 cluster 与异步 Raft 再各跑三轮。以下均为复制确认吞吐中位数，单位 MiB/s。tmpfs 不提供掉电持久性，只用于检查去掉真实块设备等待后还剩多少成本；不是可部署的性能方案，也不能把差值精确等同于 fsync 成本。

| 连接数 | 原 cluster / ext4 | 原 cluster / tmpfs | Raft / ext4 | Raft / tmpfs |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 521.74 | 515.36 | 79.90 | 171.62 |
| 8 | 688.57 | 688.34 | 167.63 | 216.93 |
| 64 | 672.15 | 673.17 | 232.36 | 241.37 |

单连接 Raft 提升 114.8%，8 连接提升 29.4%，64 连接仅提升 3.9%。原 cluster 基本不变。说明单笔在途时真实存储等待影响很大；并发合批摊薄刷盘后，当前 Raft 仍受协议、编码、校验及应用路径限制。即使使用 tmpfs，64 连接的 Raft 也只有原 cluster 的约 36%。两组按文件系统分阶段运行，未交替切换挂载点；批次大小也可能因处理速度改变，因此这些变化是诊断线索，不是独立成本分解。

18 轮全部通过批末确认及重启后的 1088 个值校验。证据：[逐轮 CSV](benchmarks/replication-tmpfs-diagnostic-ubuntu-20261123.csv) · [挂载点及同一二进制哈希](benchmarks/replication-tmpfs-diagnostic-ubuntu-20261123.environment.json)。

### 解除同连接阻塞前的 Folly 阶段测量

另建仅增加内存时间戳的 Release 探针，在 ext4 上对 1/8/64 连接各跑三轮，仍为 256 MiB、64 笔预热和相同恢复检查。时间戳在测量阶段缓存在内存中，进程退出时写出。下表分母为该插桩轮次的 SET 返回窗口，各列先按轮计算再取三轮中位数。它用于定位阶段，不替代未插桩主表。

| 连接数 | leader 编码占比 | leader 应用占比 | follower 应用占比 | 两端持久化区间并集占比 | leader / follower 追加轮数 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 18.97% | 7.10% | 16.01% | 51.22% | 1024 / 1024 |
| 8 | 30.36% | 13.65% | 22.54% | 53.30% | 513 / 487 |
| 64 | 42.31% | 22.96% | 31.99% | 25.53% | 122 / 75 |

编码包含路由检查、构造效果、Kafka 日志编码及 CRC32C、拼接；不包含后续 `TryAppendAsync` 的再次校验与复制。应用包含从 WAL/缓存取效果、解码以及 worker 等待 proactor 执行命令。持久化区间是同步调用或异步 FSYNC 提交到完成，包含调度，并非设备内部耗时。跨节点的应用和刷盘可能重叠，不能相加成 100%，也不能把任一占比当作删除该阶段后必然获得的加速比。

64 连接下同步轮数已经明显摊薄，leader 编码与应用合计占该轮窗口约 65%；此时单纯扩大 ring 或取消客户端等待无法解决这些串行工作。Fetch 入队到 worker 开始处理的 P95 中位数随 1/8/64 连接升至 0.70 / 1.41 / 7.31 ms，也提示批内编码和应用仍会阻塞组 worker。

9 轮全部通过恢复校验，trace 核对两端各覆盖本轮全部 1024 笔写入。[阶段 CSV](benchmarks/replication-phases-ubuntu-20261123.csv) · [探针哈希](benchmarks/replication-phases-ubuntu-20261123.environment.json) · [阶段打点](../../tests/integration/raft_commit_benchmark/dragonfly/phase_trace.patch) · [汇总入口](../../tests/integration/raft_commit_benchmark/dragonfly/phase_compare.py)。

### 优化优先级

主线是同连接 SET 的提交与确认解耦。阶段诊断发现的其他开销单独记录，不能拿它们的收益证明 SET 已解除阻塞。

| 项目 | 当前状态 | 后续验收 |
| --- | --- | --- |
| 异步 WAL 与 Fetch 重叠 | Folly io_uring；普通 leader 追加在刷盘期间可响应新 Fetch | 保持两端持久化、任期及失败边界 |
| 有界 drain 合批 | 积压自适应 1/4 MiB，最多 32 笔；队列 256 笔 / 8 MiB | 比较批数、刷盘次数、P95 与反压 |
| 数据 Fetch 大小 | 普通数据 4 MiB，元数据和快照 1 MiB | 独立评估 follower 批次和应用阻塞 |
| 同连接多笔普通 SET 在途 | 本轮已接入 RESP V2 的有界异步提交，随 drain 开关启用 | 按日志及回复顺序，保留读、WAIT、条件写屏障 |
| 编码、缓冲及应用调度 | 尚有复制与 worker/proactor 往返 | 分阶段测量后单变量优化 |
| WAL 预分配、共享 EventBase、多批 WRITE | 尚未实现 | 分别测吞吐、尾延迟及故障恢复 |

当前异步 I/O 和 drain 仍默认关闭。Folly 与 Helio 的同版依赖对照、批次筛选、Debug/Release 单测及故障回归见 [io_uring 文档](io_uring.md)。

### 独立 CRC 排查：收益有限

当前 Abseil 的 `crc_x86_arm_combined.cc` 编译命令没有 SSE4.2/PCLMUL 选项；源码以这两个宏决定是否编入 x86 加速路径，原目标文件的加速工厂反汇编为直接返回空指针。仅将该源文件以 `-msse4.2 -mpclmul` 重新编译并链接进独立二进制，其他对象、Folly、drain、持久化屏障和日志格式保持不变。该实验二进制只适用于已检查 CPU 指令支持的测试主机，未修改正式构建配置。

两种异步 Raft 各运行 1/8/64 连接三轮、交替顺序。SET 返回窗口吞吐从 81.59 / 177.01 / 236.31 变为 88.53 / 184.70 / 239.80 MiB/s，分别提高 8.5% / 4.3% / 1.5%。复制确认吞吐分别为 79.04 → 85.56、169.61 → 172.10、215.98 → 239.27 MiB/s；64 连接的确认尾段中位数同时由 101.83 降为 4.51 ms，不能把确认吞吐的 10.8% 差异全部归于 CRC 加速。三轮短窗口不足以证明小幅变化稳定，尤其不能据此把整个编码阶段占比归因于 CRC。

**该排查没有解除同连接 SET 的阻塞，也没有显示足以解释原 cluster 差距的收益。** 50 项日志批、效果、日志和 Fetch 单测通过；353 个不同长度及未对齐输入的 CRC 与原实现完全一致；18 轮均通过恢复值校验。[逐轮数据](benchmarks/crc-diagnostic-ubuntu-20261123.csv) · [运行环境](benchmarks/crc-diagnostic-ubuntu-20261123.environment.json) · [变更范围及校验记录](benchmarks/crc-diagnostic-ubuntu-20261123.experiment.json) · [独立构建工具](../../tests/integration/raft_commit_benchmark/dragonfly/crc_variant.py)。

## 数据保留范围

同步 Raft 的吞吐、延迟、刷盘计数和编码阶段测试数据已从本文、io_uring 文档及相关 CSV 删除，混合文件只保留原 cluster、异步 Raft 或独立 Kafka 样本。日志格式兼容、同步回退和故障测试代码继续保留；它们是正确性覆盖，不是性能比较项。

现存旧样本与当前表版本、负载和计时端点不同，不能混算：[原 cluster 的历史客户端数据](replication-benchmark.csv) · [原 cluster 的历史逐笔路径](benchmarks/cluster-set-path.csv) · [旧版 cluster / 异步 Raft 的 SET 返回窗口](benchmarks/cluster-comparison-ubuntu-20261123.csv) · [原 cluster / Kafka 准备阶段](benchmarks/append-preparation.csv) · [独立 KRaft 提交记录](raft-log-timings.csv) · [独立 KRaft 逐轮结果](benchmarks/raft-commit-rounds.csv)。

## 复现

### 当前 Release：原 cluster 与 Raft

在 Ubuntu/NVMe 主机的仓库根目录，以当前 Folly Release 构建运行配对用例。首次构建 Folly 的依赖准备见 [构建说明](io_uring.md#folly-编译变体)。结果目录必须在真实磁盘上，不能使用本机默认的 tmpfs `/tmp`。runner 为 1/8/64 连接各运行原 cluster 和异步 Raft 三轮，轮换先后顺序；没有同步 Raft 项。

```sh
ninja -C build-opt dragonfly raft_storage
findmnt -T "$PWD"
python3 tests/integration/raft_commit_benchmark/dragonfly/cluster_compare.py \
  --build-dir build-opt --output-dir "$PWD/cluster-replication-comparison" \
  --clients 1 8 64 --mib 256 --warmup 64
```

### 阶段诊断与同连接 pipeline

阶段探针只改结果目录内的源码副本，正式二进制保持不变：

```sh
python3 tests/integration/raft_commit_benchmark/dragonfly/probe.py prepare \
  --build-dir build-opt --work-dir "$PWD/replication-phases" --phases
python3 tests/integration/raft_commit_benchmark/dragonfly/phase_compare.py \
  --prepared "$PWD/replication-phases" --output-dir "$PWD/replication-phases/trials"
python3 tests/integration/raft_commit_benchmark/dragonfly/pipeline_compare.py \
  --build-dir build-opt --baseline-build /path/to/previous-async-build \
  --include-cluster --clients 1 --pipeline-depth 64 \
  --output-dir "$PWD/replication-pipeline-c1"
```

pipeline 用例在同一条保留连接上打包发送普通 SET，再读取该批全部回复；没有 MULTI，也不会换连接执行 WAIT。pipeline 的批耗时单独报告，不能冒充每笔 SET 延迟。

runner 默认仅运行 1 连接、pipeline 64；每次只接受一个 `--clients` 值。先检查该档三轮结果，
有收益才以新的输出目录运行下一档，不能直接套用逐笔等待的 1/8/64 矩阵。
