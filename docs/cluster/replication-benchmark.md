# Dragonfly 数据组复制测试与优化

## 结论

### 单连接恢复标记对照

**Raft 每个在线副本少刷一次恢复标记后，单连接 SET 吞吐中位数提高 9.4%，P95 略升。** 两个版本都等多数持久化、leader 应用后才回复 OK。在同一台 4 vCPU Linux ARM64 虚拟机上，使用 Debug、epoll、每数据节点两个线程，三票停一台。每轮从空目录建群，就绪后静置 5 秒并预热 32 笔，随后用一条 Redis 连接串行写入 256 个 256 KiB 值，共 64 MiB。表中各列分别取三轮中位数。[逐轮结果与顺序](benchmarks/wal-marker-comparison.csv) · [就绪条件](#集群就绪与计时条件)

| 同一条 Raft 写入路径 | 64 MiB 写入耗时 | 吞吐 MiB/s | SET P50 | SET P95 | 重启至 `WAIT 2` |
| --- | ---: | ---: | ---: | ---: | ---: |
| 优化前（DFE3） | 2.217 s | 28.87 | 8.16 ms | 11.40 ms | 1.306 s |
| 优化后（DFE4） | 2.028 s | 31.57 | 7.62 ms | 11.54 ms | 1.312 s |

三组配对中，两组吞吐提高，一组下降。DFE3 三轮为 28.24–32.12 MiB/s，DFE4 为 27.74–37.38 MiB/s，范围重叠。P50 中位数降低 6.6%，P95 升高 0.14 ms。六轮均在离线副本重启、`WAIT 2` 成功后，从该节点逐个读回 288 个值。基线为 `1a550353`，优化版只替换分区日志的恢复标记实现。受测 `dragonfly` 二进制 SHA-256 前缀分别为 `d55123fa1031` 和 `4ac0d461e337`。

独立的系统调用跟踪中，leader 和在线 follower 各自仍须对日志段同步 256 次。每端标记文件同步由 512 次降至 256 次，两端合计由 1536 次降至 1024 次。单连接等待上一个 OK 才发下一笔，当前批里没有第二个 SET 可以合并刷盘。[刷盘计数与恢复验证](#存储与网络回归) · [与 KRaft 的对照](#kraft-如何刷盘和确认复制)

### 优化前的耗时分布

**旧版 cluster 将 journal 放入内存发送队列后就能回 OK。Raft 的同一笔 SET 要等多数持久化。** 在 DFE3 的对照实验中，三票停一台，leader 和唯一在线的 follower 都要持久化，随后 follower 用下一次 Fetch 回报日志位置。单连接每次等 OK 才发下一笔，每批只有一个 SET。两种模式各 768 笔，客户端平均耗时分别为 1.59 和 8.14 ms，Raft 平均多等 6.55 ms。其中 5.07 ms 在进入 WAL 追加到多数提交之间，占额外耗时的 **77.5%**。此处用两组累计耗时的差值计算占比。[旧版与 Raft 的逐段对照](#以旧版-cluster-对齐真实-set) · [逐笔时间戳](benchmarks/cluster-set-path.csv)

**DFE3 的这段时间主要花在两端刷盘。** 同负载另做三轮，leader 和在线 follower 每笔各执行三次 `fsync`。将两端重叠的刷盘时间只计一次，仍占 WAL 追加到多数提交累计耗时的 **73.0%**。768 笔中有 686 笔的 Fetch 已到达 leader，但组 worker 要等自己的 WAL 持久化结束才能处理请求并向 follower 发数据。这 686 笔的两端 WAL 追加完全串行。[刷盘与 Fetch 的逐笔验证](#刷盘与-fetch-如何影响提交时间)

**多次搬运是日志准备阶段最大的已测开销。** 独立探针中，旧版 journal 编码与 Kafka `prepareAppend` 的 P50 都约为 43 µs，Dragonfly Raft 的整批编码为 537 µs。缓冲区构造、扩容和复制合计占此前 Raft 准备阶段累计耗时的 53.9%。直接编码原型使进程内准备 P50 降低 33%，未插桩的真实单连接吞吐从 28.67 变为 28.78 MiB/s，三轮范围重叠。[本地阶段逐轮数据](benchmarks/append-preparation.csv) · [编码路径与分项测量](#日志追加前的准备开销) · [未插桩的客户端逐轮数据](replication-benchmark.csv)

## 落后一个副本时写入及恢复

下表是 DFE3 与编码器原型的基线。一个 Redis 客户端与组 10 的主节点建立一条写连接，每笔 SET 收到回复后再发下一笔。预热后写 256 个新键，每个 value 为 256 KiB，正式载荷共 64 MiB。对照旧版异步复制、使用 DFE3 的 Raft，以及只替换 SET 日志编码器的实验版 Raft。[配对用例](../../tests/integration/replication_comparison_test.py)

2026-11-21 在同一台 4 vCPU、Linux ARM64 的 Colima 虚拟机上串行运行，各版本轮换顺序，每种新建集群三次。正式二进制对应 `1a550353` 的 Debug 构建。两种 Raft 都使用 epoll、每个数据进程两个线程、同步 WAL 和同一 CRC32C 后端，测试走完整 Redis 命令路径。各列先按单轮计算，再取三轮中位数。[逐轮结果与运行顺序](replication-benchmark.csv) · [实验版与正式版的源码差异](benchmarks/direct-set-encoder.patch)

| 实现 | 64 MiB 写入耗时 | 吞吐 MiB/s | SET P50 / P95 | 重启至 `WAIT 2` |
| --- | ---: | ---: | ---: | ---: |
| 旧版异步（`cluster_mode=yes`） | 0.422 s | 151.70 | 1.58 / 2.67 ms | 0.309 s |
| Raft（DFE3） | 2.232 s | 28.67 | 8.24 / 13.91 ms | 1.306 s |
| Raft 直接编码原型（未合入） | 2.224 s | 28.78 | 8.44 / 11.59 ms | 1.306 s |

**直接编码在真实单连接 SET 上尚未显示稳定的吞吐收益。** 吞吐中位数从 28.67 到 28.78 MiB/s（+0.4%），DFE3 基线的三轮范围为 24.92–29.49，原型为 27.41–29.43 MiB/s。P95 的三轮中位数降低 16.7%，三轮中有两轮下降、一轮升高。P50 从 8.24 变为 8.44 ms。

**确认与恢复。** 异步模式的 SET 在主节点执行后即可应答。Raft 的 SET 在多数持久化且 leader 已应用后应答。正式写完再用原连接执行一次 `WAIT 1`。恢复列从启动节点 13 前计时，到原连接重试 `WAIT 2 500` 返回 2 为止，包含进程启动、连接和轮询。异步副本走全量同步，Raft 测试保留整段 WAL 供副本通过 Fetch 追平。计时结束后，测试从节点 13 读回就绪标记，逐一核对 32 个预热值和 256 个正式值。Raft 用例先确认没有数据快照，再让节点 13 当选 leader 后读取其本地 KV。[异步夹具](../../tests/integration/async_replication_fixture.py) · [Raft 夹具](../../tests/integration/raft_data_group_test.py) · [恢复断言](../../tests/integration/replication_comparison_test.py)

### 集群就绪与计时条件

1. 每轮从空目录建群，五个数据节点须看到一致的 `CLUSTER SLOTS`。组 10 有一主两副本，组 20 有一主一副本。异步模式核对所有复制链路在线。Raft 模式另有三个 controller，并核对节点 13 已晋升为 voter，各组成员已追上 HW。
2. 两组均完成标记写入、WAIT 和 GET 后，强杀组 10 的节点 13。在两组分别再次写入标记，并确认一台在线副本收到写入。组 20 只做就绪检查。
3. 静置 5 秒，用正式写连接预热 32 笔并执行 `WAIT 1`。正式键和预热键均落在组 10 的 slot 42、43，键名不同。随机数据预先生成。SET 延迟从调用到收到回复，吞吐只计正式 value 字节。预热、WAIT、重启和逐键读回均在正式写入计时之外。

### 以旧版 cluster 对齐真实 SET

以 `--cluster_mode=yes` 为基线。两种模式接收相同的 Redis 请求，普通新键最终都经过 `SetCmd::Set` 写入主节点 KV，但这个动作在复制流程中的位置不同。

- **旧版异步**在分片执行 SET 后编码 journal，发布到副本发送队列和分片 backlog，随后向客户端返回 OK。发送队列可能产生反压，副本执行和回报进度可以晚于 OK。[分片 SET](../../src/server/string_family.cc) · [journal 发布](../../src/server/journal/journal_slice.cc)
- **Raft** 由数据组 worker 准备日志批。leader 追加 WAL，在线 voter 持久化并通过 Fetch 回报位置。多数提交位置（HW）覆盖这条记录后，leader 通过 `JournalExecutor` 调度同一个分片 SET，执行成功才向客户端返回 OK。挂起的 Fetch 可以在 leader 刷盘前拿到数据，两端持久化的耗时会重叠。[数据组写入与等待](../../src/server/cluster/raft/runtime/data_node.cc) · [Fetch 与 HW](../../src/server/cluster/raft/quorum/fetch_replication.cc) · [提交后的 SET](../../src/server/cluster/raft/data/set_executor.cc)

沿用上面的单连接、256 KiB 负载和就绪条件，在同一台 Linux ARM64 虚拟机上用 `1a550353` 的临时 Debug 插桩构建复测，三轮顺序分别为旧→新、新→旧、旧→新。每个模式取得 768 笔正式 SET。按每轮的 key 序号关联客户端、leader 11 和在线副本 12 的单调时钟，Raft 另核对同一任期的连续日志位置。六轮均从重启的节点 13 逐个读回 32 个预热值和 256 个正式值。上文的吞吐表保留未插桩结果。[配对用例](../../tests/integration/replication_comparison_test.py) · [逐笔时间戳](benchmarks/cluster-set-path.csv)

| 真实 SET 的计时区间 | 旧版异步 P50 / P95（ms） | Raft P50 / P95（ms） |
| --- | ---: | ---: |
| 客户端调用 SET → 收到 OK | 1.53 / 2.45 | 8.07 / 11.79 |
| leader 通过通用命令校验 → 准备向客户端发 OK | 0.29 / 1.18 | 6.92 / 10.61 |
| leader 分片进入 `SetCmd::Set` → 新键写入并完成记账 | 0.055 / 0.104 | 0.071 / 0.132 |
| 客户端调用 SET → 在线副本的同一新键写入并完成记账 | 2.44 / 3.30 | 9.14 / 12.78 |

副本在同一个 `SetCmd::PostEdit` 的 `post_updater.Run()` 返回后打点。各自 768 笔中，旧版有 755 笔、Raft 有 696 笔在客户端收到 OK **之后**才走到副本的这个位置。旧版异步的客户端只等主节点，Raft 的多数提交要求在线 voter 已经持久化，follower 随后根据 HW 应用。保留批末 `WAIT` 验证追平，逐笔传播延迟直接从副本取时钟。[分片写入与记账](../../src/server/string_family.cc) · [WAIT 实现](../../src/server/server_family.cc)

| Raft 应答路径 | P50 / P95（ms） | 占 Raft 客户端累计时间 |
| --- | ---: | ---: |
| 组 worker 开始处理 → 调用 leader WAL 追加 | 0.62 / 1.24 | 8.5% |
| 调用 leader WAL 追加 → 本地持久化返回 | 2.19 / 4.14 | 29.8% |
| 本地持久化返回 → leader HW 首次覆盖本笔 | 2.29 / 4.73 | 32.5% |
| HW 覆盖本笔 → leader 准备向客户端发 OK | 1.12 / 2.10 | 15.3% |

逐笔直接取 WAL 入口到 HW 的时间差，P50 / P95 为 **5.03 / 7.73 ms**。作为参照，旧版从开始生成 journal 到副本入队和 backlog 发布返回，P50 / P95 为 **0.14 / 0.92 ms**。前者包含多数持久化，后者量到 leader 的本地发布。表中 P50、P95 先对同笔区间取差，再取三轮相应分位数的中位数。占比按三轮该区间总耗时除以同三轮客户端往返总耗时计算。CSV 中所有 `_ns` 列均相对本轮首笔客户端调用，空列表示该模式没有采集这个端点。

提交后还有一段可单独分析。Raft 从应用回调到进入分片 `SetCmd::Set` 的 P50 为 **0.79 ms**，占完整客户端时间的 **10.6%**，其中包含 worker 切回 proactor、`CmdData::Assign` 复制键值、内部命令分发和分片调度。[应用回调](../../src/server/cluster/raft/runtime/data_node.cc) · [SET 参数构造](../../src/server/cluster/raft/data/set_executor.cc) · [参数复制](../../src/common/backed_args.h) · [内部命令分发](../../src/server/journal/executor.cc)

#### 刷盘与 Fetch 如何影响提交时间

在同一台 Linux ARM64 虚拟机上，用 DFE3 的 Debug 源码另建三轮 Raft 集群。保持单连接、256 KiB、每笔等 OK 的负载。按组、任期和日志结束位置关联两端的系统调用、传送本批数据的 Fetch、回报新位置的下一次 Fetch 以及多数提交位置。每笔在 leader 和在线 follower 上都观察到三次 `fsync`，依次同步日志段、同步恢复标记的备用记录、切换标记并同步新的选择。后两次用于让重启后的节点选中一条写完整的恢复记录。[日志追加](../../src/server/cluster/raft/log/partition_log.cc) · [恢复标记](../../src/server/cluster/raft/log/log_files.cc) · [崩溃边界测试](../../src/server/cluster/raft/log/partition_log_test.cc)

| 无人为间隔，768 笔 | P50 |
| --- | ---: |
| leader 进入 WAL 追加 → HW 覆盖本笔 | 4.93 ms |
| 两端至少一端正在执行 `fsync` 的时间，逐笔取并集 | 3.59 ms |
| follower 完成本地持久化 → leader HW 覆盖本笔，含下一次 Fetch 回报 | 0.16 ms |

leader 的组 worker 同时负责 SET 日志追加和处理 Fetch。只有已经在日志尾挂起的 Fetch，才能在 leader 写完日志段、开始刷盘前直接收到可发送数据的通知。**686/768 笔（89.3%）走了从已发布 WAL 读取的路径**。这些请求全部在 leader 持久化完成前到达网络监听器，394 笔到达时 worker 正在准备本笔日志批，277 笔到达时正在追加 WAL，另 15 笔在 worker 开始处理本笔 SET 前已经到达。686 笔全部等到持久化后才进入 Fetch 处理函数。监听器收到请求到 worker 处理的 P50 为 2.22 ms，实际读取 WAL 的 P50 为 0.041 ms。排队时间与 leader 刷盘重叠。余下 82 笔命中已挂起的 Fetch，其中 81 笔的两端 WAL 追加发生重叠。[组 worker 与挂起请求](../../src/server/cluster/raft/runtime/group.cc) · [Fetch 应答](../../src/server/cluster/raft/quorum/fetch_replication.cc)

同一二进制另跑一轮，每个 SET 调用前留 5 ms 空闲，把这段时间排除在 SET 延迟外。255/256 笔命中已挂起的 Fetch，253 笔的两端 WAL 追加发生重叠。WAL 到 HW 的 P50 从 4.93 变为 4.76 ms。两端各自三次 `fsync` 的合计 P50 则从 1.62 / 1.51 ms 变为 2.69 / 2.64 ms（leader / follower）。提前挂起使两端恢复并行。这台虚拟机上的单轮对照中，提交延迟只小幅下降。

四轮均通过节点 13 重启后全部 288 个值的读回校验。CSV 中 `client_pause_ms=0` 为三轮正式样本，`5` 为单轮间隔对照，所有时间戳相对本轮首笔客户端调用。73.0% 的分母是三轮 WAL 到 HW 的累计时间，分子是每笔六次 `fsync` 区间并集的时长之和。P50 沿用逐轮分位数取中位数的口径。[逐笔 WAL、Fetch 和刷盘时间戳](benchmarks/wal-fetch-path.csv)

#### KRaft 如何刷盘和确认复制

同级 Kafka `5b648ca9cb` 的元数据 quorum 同样使用 follower 拉取和多数确认。在三票停一台的场景，传送本批数据和确认本批持久化发生在两次 Fetch 上。

1. **Leader 先追加，唤醒已挂起的 Fetch，再刷盘。** Raft 线程取出当前准备好的批次，逐批追加，更新本地日志结束位置，唤醒在旧位置等待的 Fetch，然后对这一轮统一调用 `log.flush(false)`。已有请求可以在 leader 刷盘前拿到包含新记录的响应，两端落盘有机会重叠。刷盘期间新到的 Fetch 仍要等同一个 Raft 线程下次处理入站队列。[批次追加](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L3092) · [唤醒与刷盘次序](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L679) · [入站队列](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L3697)
2. **Follower 刷完盘，通过下一次 Fetch 确认。** voter 收到非空记录后追加并同步刷盘。下一次 Fetch 携带本地新的日志结束位置，leader 按 voter 的位置推进 HW。上游测试中，follower 从 offset 1 拉到三条记录时，HW 仍是 1。它从 offset 4 续拉时，leader 才将 HW 推到 4。真实 standby controller 将已提交元数据的回放放进 controller 事件队列，Raft 线程可以继续拉取。[voter 刷盘](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L1835) · [两次 Fetch 的测试](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientTest.java#L1750) · [控制器回放](../../../kafka/metadata/src/main/java/org/apache/kafka/controller/QuorumController.java#L969)

**Dragonfly 也能让多笔普通 SET 共享刷盘，差别在何时封批。** 首笔立即派发，组 worker 取走前到达的同组普通 SET 可以加入同一个请求批。worker 将这些 SET 各自的日志批拼成一次 WAL 追加，共享一轮持久化。下一次独立追加会再刷盘。KRaft 则能让同一次 `drain()` 取出的多个已完成日志批逐批追加，最后统一刷盘。此前 Dragonfly 的 8 连接测试写入 64 笔，leader 日志段同步了 44 次。本次单连接每笔等 OK 才发下一笔，两种实现都只有当前这笔业务写入可刷，扩大合批范围无法摊薄这笔等待。[Dragonfly 封批](../../src/server/cluster/raft/runtime/data_node.cc#L489) · [合并追加](../../src/server/cluster/raft/runtime/data_node.cc#L594) · [KRaft drain](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L3096) · [并发刷盘计数](#存储与网络回归)

**与 Dragonfly 最直接的存储差异是如何记住可恢复的日志末端。** Dragonfly 在日志段刷盘后显式发布持久化边界。DFE3 每批为备用记录和选择器各执行一次 `fsync`，DFE4 在同段窗口内只同步一次标记文件。KRaft 刷日志段和相关索引，成功后在内存中更新 `recoveryPoint`，只有覆盖新日志段时才同步父目录。重新打开元数据日志时，KRaft 扫描保留的日志段，校验批次并截断损坏或不完整的尾部。有效的本地日志末端和多数提交位置各自维护。一次逻辑 `flush` 可涉及多个文件或映射，系统调用次数需要实测。[刷盘范围](../../../kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java#L244) · [段和索引](../../../kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L628) · [元数据日志启动](../../../kafka/raft/src/main/java/org/apache/kafka/raft/internals/KafkaRaftLog.java#L712) · [恢复时校验与截断](../../../kafka/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L482)

在下方的单笔在途复测中，从 leader 开始追加到多数提交，Dragonfly DFE4 和 KRaft 的 P50 都在 4 ms 左右。这个区间包含日志追加、刷盘、Fetch 传输和进度回报。探针没有分别计量 KRaft 各文件的刷盘时间。[逐笔对照](#leader-日志追加与相邻写入间隔)

### prepareAppend 与旧版异步复制的本地准备

同样是 256 KiB、单笔在途，**KRaft 的 `prepareAppend` 和旧版 Dragonfly 的 journal 编码，P50 分别为 43.3 和 43.0 µs**。KRaft 将记录写入 leader 的内存批次并返回预计 offset，随后调用方用 `schedulePreparedAppend` 允许日志追加。旧版 `JournalWriter` 返回时，日志字节已经生成，副本发送队列和分片 backlog 的发布接下来才发生。[KRaft API 约定](../../../kafka/raft/src/main/java/org/apache/kafka/raft/RaftClient.java) · [异步 journal 发布](../../src/server/journal/journal_slice.cc)

| leader 本地计时区间 | P50（µs） | P95（µs） |
| --- | ---: | ---: |
| 旧版异步，`JournalWriter::Write` 调用 | 43.0 | 279.8 |
| 旧版异步，开始生成 journal 到发布返回，含副本入队及可能的反压 | 165.0 | 1061.4 |
| KRaft，`prepareAppend` 调用 | 43.3 | 125.8 |
| KRaft，开始提交到调用 `log.appendAsLeader` 前，含调度和封批 | 144.9 | 315.3 |
| Dragonfly Raft，`MakeSetEffectBatch` 调用，含整批 CRC32C | 537.0 | 1169.6 |
| Dragonfly Raft，从组内路由检查到调用 `replication.Append` 前，含构造 `SetEffect` 和编码 | 581.4 | 1285.8 |

每种实现各建三轮集群，停一票后验证仍能提交，静置 5 秒、预热 32 笔，再逐笔测 256 条。Dragonfly 在真实单连接 SET 经过的主节点上打点，KRaft 在 leader 进程内预先构造好参数，再围住方法调用打点。表中各行分开取分位数，时间区间有包含关系。[逐轮分位数](benchmarks/append-preparation.csv)

KRaft 的批次在 `BatchBuilder.build()` 中封头并计算 CRC32C，封批 P50 为 34.1 µs。768 条正式记录都在 `prepareAppend` 返回打点之后完成封批，只有 1 条在返回打点之前已经开始封批。当前 Dragonfly Raft 在编码函数内完成整批，这段 P50 比异步 journal 编码多 494 µs，两端都还没等待磁盘或副本。优化时应同时看编码和进入 WAL 前的完整耗时。[KRaft 批构造](../../../kafka/raft/src/main/java/org/apache/kafka/raft/internals/BatchBuilder.java) · [Dragonfly 日志编码](../../src/server/cluster/raft/log/record_batch.cc) · [Raft SET 追加](../../src/server/cluster/raft/runtime/data_node.cc)

## Kafka 复制实测与 Dragonfly 的实现差异

以下吞吐和端到端延迟来自此前同机 Kafka `5b648ca9cb`、JDK 21 的 64 MiB 单笔在途对照。每笔 256 KiB，三副本停一台并确认剩余副本可提交，再预热 32 笔。普通分区用 producer 逐条等待 `acks=all`，KRaft 在 leader 进程内等提交回调。这两个端点与上表的 Redis SET 回复各自衡量不同工作。[Kafka 负载工具](../../../kafka/core/src/main/scala/kafka/tools/TestRaftServer.scala)

| 历史测量路径 | 完成端点 | 吞吐 MiB/s | P50 / P95（ms） |
| --- | --- | ---: | ---: |
| 普通分区，默认刷盘 | producer 收到 ACK | 110.72 | 2.03 / 3.54 |
| 普通分区，`flush.messages=1` | producer 收到 ACK | 59.19 | 3.94 / 7.80 |
| KRaft，追加 linger 为 0 ms | leader 提交回调入口 | 56.52 | 4.18 / 6.53 |
| KRaft，追加 linger 为 25 ms | leader 提交回调入口 | 8.28 | 29.66 / 32.98 |

普通分区设置 `min.insync.replicas=2`。默认允许追加到文件页缓存后报告进度，`flush.messages=1` 对每条触发刷盘。KRaft follower 持久化后通过下一次 Fetch 报告日志位置。原有 Kafka、128 MiB 和多连接预热的逐轮数值可用 `git show 1a550353:docs/cluster/replication-benchmark.csv` 查阅。[Kafka 分区确认](../../../kafka/core/src/main/scala/kafka/cluster/Partition.scala) · [KRaft 配置](../../../kafka/raft/src/main/java/org/apache/kafka/raft/QuorumConfig.java)

## Leader 日志追加与相邻写入间隔

2026-11-21 在同一台 4 vCPU Linux ARM64 虚拟机上复测当前 DFE4（Debug、epoll，探针二进制 SHA-256 前缀 `cbd21f134c59`）和 KRaft（`5b648ca9cb`、JDK 21）。两边都在 leader 进程内逐笔提交。**追加开始**取实际调用本地日志追加函数前的时刻。**多数提交**取本任期的 leader HW 首次覆盖该笔排他结束 offset 的时刻。Dragonfly 分别在 `log_.Append` 前和 `committed_` 更新后采样。KRaft 分别在 `log.appendAsLeader` 前和 `LeaderState` 更新 `highWatermark` 后采样。提交回调另行打点。[Dragonfly 追加与 HW](../../src/server/cluster/raft/quorum/fetch_replication.cc) · [KRaft 日志追加](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java) · [KRaft leader HW](../../../kafka/raft/src/main/java/org/apache/kafka/raft/LeaderState.java)

每种实现新建三轮集群。先核对三票全部就绪，停一台，验证剩余两票提交新记录，静置 5 秒并预热 32 笔。每轮正式追加 256 笔预生成的 256 KiB 随机值，任何时刻最多一笔在途，KRaft 关闭 linger，用上游 `TestRaftServer` 驱动真实的日志、Fetch 和多数提交。两个负载程序都等本组提交通知并校验完整值，再提交下一笔。Dragonfly 的专用监听器对测试记录跳过 KV 执行，保留组 worker、合法 SET 日志批、实际 Fetch 和持久化路径。离线节点 13 重启后按正常 KV 回放并读回全部 288 个值。KRaft 的两名在线 voter 也逐笔核验了全部值。

| 进程内单笔在途，各列取三轮中位数 | Dragonfly DFE4 | KRaft |
| --- | ---: | ---: |
| **本笔本地日志追加 → 本笔 HW，P50 / P95** | **4.00 / 6.98 ms** | **4.11 / 7.30 ms** |
| 上一笔 HW → 下一笔本地日志追加，P50 / P95 | 0.683 / 1.526 ms | 0.338 / 0.627 ms |
| 全部相邻间隔占整轮时间 | 16.25% | 7.78% |
| 首笔追加 → 末笔 HW，64 MiB 耗时 / 吞吐 | 1.292 s / 49.55 MiB/s | 1.201 s / 53.30 MiB/s |

三轮配对依次为 Dragonfly→KRaft、KRaft→Dragonfly、Dragonfly→KRaft。按配对轮次，Dragonfly 追加到 HW 的 P50 为 4.14、3.94、4.00 ms，KRaft 为 3.99、4.71、4.11 ms。Dragonfly 有两轮 P50 较低、一轮 P95 较低。两边的多数提交延迟在这个负载下接近，生产多机延迟和高并发吞吐需另测。[逐轮结果](benchmarks/raft-commit-rounds.csv)

Dragonfly 从本地日志追加到本地持久化完成的 P50 / P95 为 1.427 / 3.157 ms，随后到 HW 覆盖为 2.166 / 4.580 ms。两段各自计算分位数。Redis 客户端实测的 SET P50 7.62 ms 还覆盖命令分发、提交后应用 KV 和客户端收包，详见[单连接恢复标记对照](#单连接恢复标记对照)。

**复核方法。** 逐笔文件按实现和轮次编号，纳秒时间戳以本轮首笔提交入口为零点。每轮检查 epoch 一致、排他 offset 连续，并对每笔计算 `hw_ns[i] - append_ns[i]`，对相邻两笔计算 `append_ns[i+1] - hw_ns[i]`。256 段提交耗时和 255 段间隔之和必须精确等于首笔追加至末笔 HW 的时长。间隔占比使用这个时长作分母。表中各列均先按轮统计，再取三轮中位数。[两边的逐笔时间戳](raft-log-timings.csv)

## 数据组复制优化项

| 状态 | 优化项 | Dragonfly 实现与参照 | 验证结果或试验指标 |
| --- | --- | --- | --- |
| 原型已测，未合入 | 减少 SET 日志编码中的数据搬运 | 预先算出记录长度，将值直接写入最终日志批，省去中间缓冲区的扩容和复制。 | 此前进程内准备 P50 下降 33%。本轮真实单连接 SET 吞吐中位数增加 0.4%，三轮范围重叠。[旧版复制与原型对照](#旧版-cluster-的复制如何减少搬运) |
| 已优化 | 事件驱动 Fetch 与提交等待 | 参照 KRaft 长轮询，组内日志、HW 或角色变化唤醒相应等待方。每个 peer 一次一个请求，follower 落盘后立即续拉并报告持久化位置。 | 暂停两台副本，SET 保持等待。恢复一票后，同一请求完成并可读回。[组内调度](../../src/server/cluster/raft/runtime/group.cc) · [进程回归](../../tests/integration/replication_comparison_test.py) |
| 已实现，取决于 Fetch 是否提前挂起 | leader 与 follower 并行刷盘 | 参照 KRaft 提前完成挂起的 Fetch。leader 校验并写全普通数据批后，可在本地刷盘前将其连同原 HW 发给正在日志尾等待的 follower。 | 阻塞 leader 的日志段刷盘，在线 follower 已收到该值，SET 仍在等待。解除阻塞后写入成功。连续单连接 SET 的三轮实测中，81/768 笔的两端 WAL 追加发生重叠。[命中率与间隔对照](#刷盘与-fetch-如何影响提交时间) · [KRaft](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L679-L683) · [组内 Fetch](../../src/server/cluster/raft/quorum/fetch_replication.cc) |
| 可尝试 | 提前处理已到达、仍在排队的 Fetch | DFE3 的 768 笔中有 686 笔在 leader 完成本地持久化后才把数据发给 follower。先在 DFE4 复测这一比例，再试让已排队的同任期 Fetch 在日志写全后、刷盘前取到数据。 | 固定单连接且不加请求间隔，对比两端持久化重叠率、WAL 到 HW 的耗时和 SET P95。故障注入须验证只有已持久化的 voter 进度能推进 HW。[旧版逐笔观测](#刷盘与-fetch-如何影响提交时间) |
| 已优化 | 减少既有日志段的恢复标记同步 | DFE4 先同步日志段，再同步一次标记文件发布新位置。换段或更换基准记录时才在写路径同步目录。 | 单连接 256 笔，两端同步总数从 1536 降到 1024。吞吐中位数提高 9.4%，P95 略升。[逐轮对照](#单连接恢复标记对照) · [恢复标记](raft-replication-design.md#日志恢复标记) |
| 已优化 | 并发普通 SET 自然合批 | KRaft 一次取出的多个批次共享刷盘。Dragonfly 首笔立即派发，worker 取走前已到达的同组普通 SET 可共享刷盘。每批最多 32 笔、键值合计 1 MiB，合法的超大单笔独立追加。 | 每笔保留自己的 offset，按连接执行 WAIT。NX/XX 独立求值，8 连接同时对一个键执行 NX，只有一笔成功。[KRaft 批次累积](../../../kafka/raft/src/main/java/org/apache/kafka/raft/internals/BatchAccumulator.java) · [数据组写入](../../src/server/cluster/raft/runtime/data_node.cc) · [条件写回归](../../tests/integration/raft_data_group_test.py) |
| 已优化 | leader 从内存应用已提交记录 | 参照 KRaft 提交回调复用内存记录。HW 覆盖后，leader 按 offset 应用当前 epoch 缓存的 SET 效果，每组上限 4,096 笔、8 MiB。 | 缓存未命中、换主或重启时从 WAL 重放。回归覆盖旧 epoch 缓存与同一 offset 被覆盖的情况。[KRaft 回调](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java#L380-L385) · [效果应用回归](../../src/server/cluster/raft/data/set_effect_test.cc) |
| 已实现 | 复用提交记录的执行器 | 同一数据组按提交顺序共用一个 `JournalExecutor`，避免每笔 SET 重做跨分片的数据库激活。 | 快照加增量恢复后，在 DB 0 和 DB 1 读回同名键各自的值。[数据组执行](../../src/server/cluster/raft/runtime/data_node.cc) · [跨 DB 恢复测试](../../tests/integration/raft_data_group_test.py) |
| 已实现 | 按帧长接收大 Fetch 响应 | peer 校验 Kafka 长度前缀及上限后，为完整响应分配缓冲并直接收取剩余字节。 | TCP 用例覆盖大帧后复用同一连接以及遇到超长响应后重连。这两项改动合并测试时，8 连接吞吐未见稳定改善。[接收逻辑](../../src/server/cluster/raft/transport/peer_connection.cc) · [TCP 回归](../../src/server/cluster/raft/transport/raft_listener_test.cc) |
| 可尝试 | WAL 使用 io_uring 异步写入和刷盘 | 网络已支持 io_uring，WAL 当前由组内 worker 同步写盘。试验保持日志段、标记文件的持久化顺序和多数确认条件。 | 固定磁盘和 io_uring 网络后端，对比同步与异步 WAL 的单连接提交 P95、8 连接吞吐，并复验刷盘中断后的恢复。[WAL 与 io_uring](#io_uring-能承接哪些操作) |
| 可尝试 | 调整普通 SET 合批上限 | 以现有 32 笔、1 MiB、无固定攒批等待为基线，分别调整笔数和字节上限。 | 用 1、8、32 条连接分别写小值和 256 KiB 值，记录每批实际笔数、每千笔日志段同步次数、吞吐和 SET P95。[合批入口](../../src/server/cluster/raft/runtime/data_node.cc) |
| 可尝试 | 分别调整日志 Fetch 与快照分块大小 | KRaft 分开配置两种请求的字节预算，默认均为 1 MiB。Dragonfly 共用一个 1 MiB 常量，日志 Fetch 至少返回完整的首批。 | 保持单请求在途，分别试 1、4、8 MiB。对保留 WAL 和需要快照的副本分别测追平时间，同时记录健康多数的 SET P95 和峰值内存。[KRaft 配置](../../../kafka/raft/src/main/java/org/apache/kafka/raft/QuorumConfig.java) · [Fetch 请求](../../src/server/cluster/raft/runtime/group.cc) |

### 日志追加前的准备开销

在本次 DFE4 与 KRaft 对照中，从下一笔提交入口到本地日志追加，Dragonfly 的 P50 / P95 为 **0.619 / 1.415 ms**，KRaft 为 **0.146 / 0.325 ms**。按轮计算，这一段占相邻两笔间隔累计时间的比例，中位数分别为 90.62% 和 46.35%。KRaft 的计时一直到调用 `log.appendAsLeader` 前，涵盖 `prepareAppend` 返回后的调度和封批。客户端 SET、进程内提交到追加、单次方法调用各有独立的计时端点。[此次逐笔时间戳](raft-log-timings.csv)

**同一个 256 KiB 值在日志编码中被反复搬运，是 Dragonfly 追加前最大的实测开销。** 此前在相同进程内负载下另建三轮集群分项计时，缓冲区构造、扩容和复制合计每笔 P50 为 **0.286 ms**，占三轮 Dragonfly 准备阶段累计耗时的 **53.9%**。leader 先把值复制到 `SetEffect`，再写入 SET 载荷、record body、records 和最终日志批。在该探针的 Debug 构建中，`body.append(value)` 刚好用满容量，随后追加 1 字节的 header count 就触发扩容，再搬运整个 body，单这一步 P50 为 **0.057 ms**。KRaft 预先计算记录长度，把记录写入可复用的批缓冲区，封批时用 `slice()` 共享这块内存。[Dragonfly 数据组写入](../../src/server/cluster/raft/runtime/data_node.cc) · [SET 载荷](../../src/server/cluster/raft/data/set_effect.cc) · [日志批编码](../../src/server/cluster/raft/log/record_batch.cc) · [KRaft 批编码](../../../kafka/raft/src/main/java/org/apache/kafka/raft/internals/BatchBuilder.java) · [KRaft 缓冲池](../../../kafka/raft/src/main/java/org/apache/kafka/raft/internals/BatchMemoryPool.java)

CRC32C 对两端差距的贡献仍待验证。此前单独给 Dragonfly Debug 构建启用 ARM 硬件 CRC32C 后，相邻写入间隔 P50 从 0.651 降到 0.542 ms，P95 从 1.121 变为 1.248 ms。KRaft 补测的封批 P50 为 34.1 µs，其中包含批头处理和 CRC32C，校验本身的耗时仍未单独测量。

#### 旧版 cluster 的复制如何减少搬运

旧版 cluster 的普通 SET 在分片内借用 key 和 value，`JournalWriter` 把字段依次写入同一个字符串。各副本的复制流看到同一条已编码的 journal 记录，随后分片把这块内存移入 backlog，留给断线续传。每个副本的发送队列仍复制一份记录以便异步发送。发送时 `BufferedSocketWriter` 用 `iovec` 引用队列中的各个块，直到写完成才释放，省去了组装连续发送缓冲区的一次搬运。[SET 记录](../../src/server/string_family.cc) · [JournalWriter](../../src/server/journal/serializer.cc) · [分片 backlog](../../src/server/journal/journal_slice.cc) · [副本流](../../src/server/journal/streamer.cc) · [发送队列](../../src/server/journal/buffered_socket_writer.cc)

Raft 可以借用旧版按字段写入同一块内存的做法。先算出整笔长度，计入批头、变长长度字段、SET 头、键值和末尾的 header count。为最终日志批预留足够容量，把键值写到最终位置，再回填 CRC。这样单笔 256 KiB 的值在追加前只需写入持有到提交的 `SetEffect` 和交给 WAL 的日志批。并发合批时也要为每条 SET 保留各自的单记录批和 offset，现有恢复路径按这个格式回放。[SET 编码](../../src/server/cluster/raft/data/set_effect.cc) · [组内合批](../../src/server/cluster/raft/runtime/data_node.cc) · [WAL 追加](../../src/server/cluster/raft/log/partition_log.cc)

**此前的进程内编码器对照。** 在相同软件 CRC32C 下，只替换普通 SET 编码器，两种版本各重建三轮集群。追加前准备 P50 / P95 从 0.591 / 1.224 ms 变为 0.395 / 0.709 ms，相邻写入间隔从 0.654 / 1.330 ms 变为 0.448 / 0.837 ms。150 组输入的新旧日志逐字节相同，包含二进制值、变长长度边界和空操作。该探针的监听器跳过测试记录的 KV 执行，本轮客户端结果见[单连接实测](#落后一个副本时写入及恢复)。

### io_uring 能承接哪些操作

**网络已支持。** Linux 启动时探测 io_uring，可用时 Raft 的 Fetch 和 FetchSnapshot 连接随 Helio proactor 使用它。通过数据节点的 `INFO SERVER` 可查到 `multiplexing_api:iouring`。[启动探测](../../src/server/dfly_main.cc) · [Raft 连接](../../src/server/cluster/raft/transport/peer_connection.cc) · [进程用例](../../tests/integration/replication_comparison_test.py)

**WAL 异步落盘仍待试验。** 当前每组的单线程 worker 依次执行日志写入和 `fsync`。可以将文件 I/O 交给使用 Helio `LinuxFile` 的 proactor fiber，使其在等磁盘时让出执行线程。DFE4 在同段窗口内仍须依次完成日志段同步和恢复标记长度同步。整条链成功后，本节点才可公布新的持久化位置。单连接 SET 仍需等待多数持久化和 leader 应用，收益需在相同网络后端实测。[组内 worker](../../src/server/cluster/raft/runtime/group.cc) · [分区追加](../../src/server/cluster/raft/log/partition_log.cc) · [恢复位置发布](../../src/server/cluster/raft/log/log_files.cc) · [Helio 文件接口](../../helio/util/fibers/uring_file.h)

## 阻塞与快照恢复

此前的故障注入覆盖两种不同于本轮重启追平的场景。暂停组 10 两台副本时，异步 SET 可以成功，随后 `WAIT 2` 超时返回 0。Raft 的 HW 停住，未提交的新值对 GET 不可见，恢复一票后等待中的 SET 可以完成。快照用例让一个副本落后到需要下载快照，在接收部分分块时强杀并重启，核对快照摘要、全部键值及尾部写入。本轮对 Raft 的单连接恢复测试覆盖保留 WAL 时的追平。[阻塞与恢复一票](../../tests/integration/replication_comparison_test.py) · [快照中断](../../tests/integration/raft_snapshot_stall_test.py) · [旧版全量同步反压](../../tests/dragonfly/replication_test.py)

## 存储与网络回归

正式计时外，两版各另跑一轮相同负载，按数据组 10 的节点和文件统计 256 笔 SET 期间的成功同步调用。leader 与在线 follower 各自的日志段 / 标记 / 目录次数从 256 / 512 / 0 变为 256 / 256 / 0。这里统计的是既有日志段、同一个标记窗口内的追加。窗口更换、换段、启动和快照保留另有目录同步。[分区追加](../../src/server/cluster/raft/log/partition_log.cc) · [磁盘格式与启动顺序](raft-replication-design.md#日志恢复标记)

故障注入先成功写入一批，再在第二批更新标记长度后、标记同步前强杀进程。重开时给标记同步注入 I/O 错误，分区拒绝打开。撤去故障后，重开进程同步标记，恢复到 offset 2，两批内容逐字节匹配。另一次在正常追加时让标记同步失败，内存中的持久化位置保持在 1，后续追加也被拒绝，直到重新打开。该验证覆盖进程崩溃和同步调用报错，整机断电未实测。[标记发布与重开](../../src/server/cluster/raft/log/log_files.cc) · [边界与损坏回归](../../src/server/cluster/raft/log/partition_log_test.cc)

跨版本测试用 DFE3 建立三个 controller、四个数据节点，在两个数据组写入 16 个键。强杀全群、移除初始配置后，新二进制直接用原目录启动，读回原值，再写入 16 个键。第二次强杀全群时，四个数据目录均已升级为 DFE4，重启后全部 32 个新旧值一致。分区日志、快照和 Fetch 的 34 项单测全部通过。进程回归还验证了落后副本安装快照后重放尾部日志，以及同时暂停两台副本时 SET 等待、恢复一票后 HW 从 5 推进到 6。[恢复和快照进程测试](../../tests/integration/raft_data_group_test.py) · [多数确认进程测试](../../tests/integration/replication_comparison_test.py) · [Fetch 单测](../../src/server/cluster/raft/quorum/fetch_replication_test.cc)

此前对 DFE3 既有段的并发写入跟踪观察到 44 次日志段同步、88 次恢复标记同步和 0 次目录同步（8 连接、64 笔）。这是独立的系统调用计数。DFE2 升级到 DFE3、io_uring 网络与快照恢复也有单独的验证记录。本轮使用 epoll 和同步 WAL。[分区追加](../../src/server/cluster/raft/log/partition_log.cc) · [恢复标记](raft-replication-design.md#日志恢复标记)

## KRaft 用例对应

下表列出同级 Kafka `5b648ca9cb` 与 Dragonfly 的协议回归用例，供核对测试覆盖。本轮执行的用例见[单连接复现命令](#复现)。

| Kafka 上游类及方法 | Dragonfly 对应断言 |
| --- | --- |
| `LeaderStateTest.testUpdateHighWatermarkQuorumSizeTwo`、`testUpdateHighWatermarkQuorumSizeThree`、`testCheckQuorum` | 两票组需要两票，三票组需要两票。只有本组不同 voter 的有效进度能推进 HW、维持多数联系。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/LeaderStateTest.java) · [HW](../../src/server/cluster/raft/replication/high_watermark_test.cc) · [quorum 检查](../../src/server/cluster/raft/quorum/epoch_control_test.cc) |
| `KafkaRaftClientTest.testListenerCommitCallbackAfterLeaderWrite`、`testLeaderShouldResignLeadershipIfNotGetFetchRequestFromMajorityVoters`、`testFollowerReplication`、`testHandleCommitCallbackFiresAfterFollowerHighWatermarkAdvances` | 本地追加后、HW 推进前读到旧值且写入未确认。恢复一票使同一请求完成，持续失联导致退位。副本续拉后能应用并读回全部 KV。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientTest.java) · [进程对照](../../tests/integration/replication_comparison_test.py) |
| `KafkaRaftClientFetchTest.testFetchMaxBytesAlwaysReturnsAtLeastOneBatch`、`testFetchMaxBytesWithTwoBatches` | 预算为 1 字节仍可取得完整首批。另一例的预算容纳两个完整批次以及第三批的一个字节，副本只持久化完整前缀，下次 Fetch 从第三批开始。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientFetchTest.java) · [批次边界测试](../../src/server/cluster/raft/quorum/fetch_replication_test.cc) |
| `KafkaRaftClientSnapshotTest.testLeaderImmediatelySendsSnapshotId`、`testFetchSnapshotResponsePartialData`、`testFollowerListenerNotified` | 落后于可读起点时返回快照 ID，按字节位置分块读取，完整安装后从快照 cut 继续应用日志。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientSnapshotTest.java) · [快照协议](../../src/server/cluster/raft/quorum/snapshot_transfer_test.cc) · [数据恢复](../../tests/integration/raft_snapshot_stall_test.py) |
| `KafkaRaftClientTest.testPurgatoryFetch*`、`testResignWillCompleteFetchPurgatory` 与 `KafkaRaftClientFetchTest` 的 HW 长轮询用例 | 追加、HW 推进、超时或退位使挂起的 Fetch 返回。重新求值时仅按收到原请求的时刻登记 voter 联系。[Kafka](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientFetchTest.java) · [Fetch 回归](../../src/server/cluster/raft/quorum/fetch_replication_test.cc) · [监听器停机](../../src/server/cluster/raft/transport/raft_listener_test.cc) |

## 复现

### Dragonfly 与 KRaft 的多数提交延迟

在同一台 Linux 主机准备 Dragonfly 的 CMake/Ninja Debug 构建、GNU patch、Python 3.10+（含 `redis` 包）和 JDK 21。Kafka 源码位于同级的 `../kafka`，版本为 `5b648ca9cbeed3cebe553fba86c516b08bcaeb40`。首次运行时，从 Dragonfly 仓库根目录构建两边的依赖，再启动测试。[统一入口](../../tests/integration/raft_commit_benchmark/run.py)

```sh
./helio/blaze.sh -DWITH_GCP=OFF
ninja -C build-dbg dragonfly raft_storage
(cd ../kafka && ./gradlew :core:jar :clients:jar)
python3 tests/integration/raft_commit_benchmark/run.py \
  --build-dir build-dbg --kafka-dir ../kafka --output-dir ../raft-commit-results
```

每次指定新的输出目录，也可省略 `--output-dir`，让脚本创建并打印临时目录。Kafka 默认使用上述 Gradle 构建生成的依赖和缓存中的 Scala 2.13.18 编译器。有同版本发行包时，可用 `--kafka-libs` 指定其 `libs` 目录，`--scala-compiler` 指定编译器 JAR。脚本从源码编译两边的测试探针，正式二进制和 Kafka checkout 继续供其他用例使用。[Dragonfly 探针](../../tests/integration/raft_commit_benchmark/dragonfly/probe.py) · [KRaft 探针](../../tests/integration/raft_commit_benchmark/kraft/probe.py)

脚本串行启动六个新集群，按三组配对运行。每轮须先通过就绪检查，完成 32 笔预热、256 笔单笔在途写入和全部 288 个值的核对。输出目录中的 `rounds.csv` 给出每轮的运行顺序、P50、P95 和吞吐，`raft-log-timings.csv` 给出双方共 1,536 笔正式记录的时间戳。各轮日志和原始采样一起保留。表中的 Dragonfly 结果来自应用 DFE4 日志恢复标记改动的工作树。脚本测量传入构建目录所对应的实现，可在 `dragonfly/sources.sha256` 核对 WAL 源文件、正式二进制和测试二进制。[统计与校验](../../tests/integration/raft_commit_benchmark/report.py) · [多数提交结果](#leader-日志追加与相邻写入间隔)

### Redis 单连接写入和副本恢复

在 Linux 的 Dragonfly 仓库根目录运行，需要 Python 3.10+ 及 `redis` 包。下列命令每种正式模式各跑一轮，每轮都会重建集群，输出 JSON 中包含 SET 分位数、吞吐和重启追平时间。Raft 用例从 Dragonfly 二进制所在目录查找 `raft_storage`。

```sh
ninja -C build-dbg dragonfly raft_storage
export DRAGONFLY_PATH="$PWD/build-dbg/dragonfly"
export REPLICATION_TEST_MIB=64 REPLICATION_TEST_CLIENTS=1
export REPLICATION_TEST_SETTLE_SECONDS=5 REPLICATION_TEST_WARMUP_WRITES=32
python3 tests/integration/replication_comparison_test.py -k lagging_replica_catches_up_bulk -v
```

复测恢复标记改动时，分别从 `1a550353` 和带 DFE4 改动的工作树以相同 Debug 配置构建 `dragonfly`、`raft_storage`，让两个二进制在各自的构建目录中配对。沿用上面的环境参数，每轮把 `DRAGONFLY_PATH` 指向对应目录的 `dragonfly`，执行同一个用例。

```sh
python3 tests/integration/replication_comparison_test.py RaftReplicationComparisonTest.test_lagging_replica_catches_up_bulk -v
```

要复测历史编码器原型，在 `1a550353` 的独立工作树应用[直接编码补丁](benchmarks/direct-set-encoder.patch)，重新构建并运行同一用例。旧版异步、DFE3 和编码器原型的逐轮数据保存在[编码器对照 CSV](replication-benchmark.csv)。
