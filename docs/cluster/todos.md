# Dragonfly Raft 集群实施任务

目标是在同一二进制中用 `--cluster_mode=yes` 运行现有集群，用拟新增的 `--cluster_mode=raft` 启用元数据 Raft 和按复制组运行的数据 Raft。以下复选框跟踪待实现和待验收的工作。每个模块给出接入位置、前置依赖和可复现的通过条件。[集群设计](raft-cluster-design.md) · [复制设计](raft-replication-design.md)

## 交付顺序

| 阶段 | 涉及模块 | 可演示的结果 |
| --- | --- | --- |
| 协议基线 | 01–04 | 旧配置回归通过。单个固定成员 quorum 在相同事件下与 Kafka 产生相同选票、批次和提交位置。 |
| 原图拓扑首次读写 | 05–09 的静态拓扑、SET/GET 和路由任务 | C1/C2/C3 管元数据。组 10 的 A、A-r1 服务 slots 0–8191，组 20 的 B、B-r1 服务 8192–16383。原 Redis 客户端经一个 seed 写入和读回数据。 |
| 故障与恢复 | 04、06–09 的成员变更、命令覆盖、快照和客户端回归 | 先验证两 voter 失一停写，再加入 A-r2 验证三 voter 失一可换主。落后副本能从快照追平。 |
| 迁移与切换 | 10–12 | 先只迁 slot 42，再用旧集群实测停写导入、原地址重连和回退。记录版本兼容和容量结果。 |

开发和测试固定使用集群设计所列的 Kafka 提交。协议对照的单位是一个 KRaft quorum；元数据组和各数据组分别拥有自己的任期、选票、成员集合和日志。以下 HW 均为排他提交边界，offset 小于 HW 的记录已提交。

## 01 启动配置与旧集群隔离

可先独立开展。现有模式判断决定请求能否走真实集群路由，也控制部分管理命令，需要逐个核对调用点。[模式解析](../../src/server/cluster_support.cc) · [模式判断](../../src/server/cluster_support.h) · [管理命令](../../src/server/cluster/cluster_family.cc)

- [ ] 01.1 增加启动值 `raft`。区分“启用真实集群路由”和“允许旧 manager 改拓扑”，核对直接判断 `kRealCluster` 的入口。覆盖空值、`emulated`、`yes`、`raft` 和非法值。
- [ ] 01.2 定义 Raft 启动配置，包括 cluster ID、节点及目录身份、角色、内部监听和 quorum 发现地址。区分新建、加入和重启恢复，错误身份或目录组合在开放业务端口前报错。
- [ ] 01.3 按启动模式选择管理与复制入口。对旧全量 CONFIG、REPLICAOF、REPLTAKEOVER 和迁移写命令逐项定义 Raft 模式的处理；未适配的操作明确报错。覆盖 CONFIG SET 尝试热切模式。
- [ ] 01.4 用同一份旧配置回归新旧二进制的 `yes` 模式。比对配置下发、主从握手、普通写回复、拓扑和 MOVED；同时回归单机和 emulated 模式。Raft 初始化未完成时不得接收业务写。[集群命令测试](../../src/server/cluster/cluster_family_test.cc) · [旧主从测试](../../tests/dragonfly/cluster_replication_test.py)

## 02 Kafka 协议编码与版本协商

可与 01 并行。由上游 schema 生成标准消息的 C++ 编解码器，并用同一提交的 Java 实现生成对照样本。[RPC schema](../../../kafka/clients/src/main/resources/common/message) · [元数据 schema](../../../kafka/metadata/src/main/resources/common/metadata)

- [ ] 02.1 建立逐 API 版本表，列出 header、flexible 版本、nullable 字段、tagged fields、错误码与默认值。实现 ApiVersions，只通告已实现的版本。
- [ ] 02.2 分批实现 Vote、Begin/EndQuorumEpoch、Fetch、FetchSnapshot、DescribeQuorum，再接入动态 voter、BrokerRegistration 和 BrokerHeartbeat 消息。为每批保留请求、成功响应和错误响应样本。
- [ ] 02.3 实现 record batch 头、CRC、控制批次、快照头尾和元数据记录。为 slot 归属、迁移阶段与 KV 效果定义独立记录版本及启用条件。
- [ ] 02.4 双向核对 Java 和 C++ 产物的字节与解码结果。覆盖未知 tagged field、未知元数据记录、截断消息、坏 CRC，以及 RPC 版本可用但 kraft.version 或 metadata.version 不满足的组合。

## 03 持久日志、身份与恢复

日志索引和故障注入可与 02 并行，磁盘格式对照依赖 02。优先复用已有写盘和原子替换逻辑。[Dragonfly AOF 写入](../../src/server/journal/aof_segment_writer.cc) · [Kafka 日志](../../../kafka/raft/src/main/java/org/apache/kafka/raft/internals/KafkaRaftLog.java) · [持久选票](../../../kafka/raft/src/main/java/org/apache/kafka/raft/FileQuorumStateStore.java)

- [ ] 03.1 实现目录格式化和恢复，持久保存 cluster ID、node ID、directory ID。重复格式化、磁盘接错节点或用错误模式打开旧目录时保护已有数据。
- [ ] 03.2 为每个 quorum 独立保存 epoch 和投票。原子更新后才应答正式 Vote；模拟更新前后断电，检查同一 term 不会投给两个候选者。
- [ ] 03.3 实现批量追加、持久化、按 offset 读取、按 epoch 查找共同前缀、尾部截断和 segment 回收。区分日志末端 LEO、已持久化位置、HW 和已应用位置。
- [ ] 03.4 实现有校验的快照文件、临时文件安装和续读起点。重启时检查日志连续性；测试半个尾批、损坏批次、未完成快照和快照后的日志缺口，验证恢复结果或明确的失败原因。

## 04 共享 KRaft 共识引擎

依赖 02、03。此模块移植一次，供元数据和数据组实例化。应能在不启动 KV 服务的情况下驱动时钟、磁盘和消息。[KafkaRaftClient](../../../kafka/raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java) · [QuorumState](../../../kafka/raft/src/main/java/org/apache/kafka/raft/QuorumState.java) · [上游测试](../../../kafka/raft/src/test/java/org/apache/kafka/raft)

- [ ] 04.1 在 proactor/fiber 上实现事件循环、请求超时及重试。给测试注入时钟、退避随机数、消息到达顺序和写盘完成事件。
- [ ] 04.2 移植 pre-vote、正式投票、leader 通告和退位。按日志末端 epoch、offset 及 voter 身份检查候选者。覆盖重复请求、过期响应、重启后重投、observer 不计票。
- [ ] 04.3 实现 follower 主动 Fetch、分歧检测和追赶。voter 持久化后在后续 Fetch 报告新 LEO；leader 按有效 voter 多数和当前任期记录推进 HW。覆盖仅复制旧任期记录、重复 Fetch 和失去多数后的退位。
  - [x] 移植 LeaderState 的排他 HW 计算。覆盖当前 term 首条记录、1/2/3 个 voter、多数门槛变化和同 offset 的 segment 位置更新。[实现与测试](../../src/server/cluster/raft/replication/high_watermark_test.cc)
- [ ] 04.4 实现元数据 FetchSnapshot 分块、校验、原子安装及从排他 endOffset 续拉。模拟传输中换主与重启，检查快照身份和保留前缀。
- [ ] 04.5 实现 Add/Remove/UpdateRaftVoter。observer 追平后一次增删一个 voter，按 VotersRecord 的生效、提交、截断规则更新成员；覆盖未提交变更时换主、删除当前 leader 和 ackWhenCommitted。
- [ ] 04.6 给 Java 和 C++ 输入同一事件序列，逐步比较角色、已投选票、输出 RPC、HW 和保留前缀。先移植 KafkaRaftClientPreVoteTest、LeaderStateTest，再移植 KafkaRaftClientReconfigTest 与 RaftEventSimulationTest。

## 05 元数据 controller 与 slot 目录

依赖 04 的静态 quorum；动态成员变更可随后接入。元数据组管理 slot 归属、节点身份和路由发现。[QuorumController](../../../kafka/metadata/src/main/java/org/apache/kafka/controller/QuorumController.java) · [MetadataLoader](../../../kafka/metadata/src/main/java/org/apache/kafka/image/loader/MetadataLoader.java) · [现有拓扑校验](../../src/server/cluster/cluster_config.cc)

- [ ] 05.1 建立单写事件队列，按稳定 offset 完成管理请求。回放 Begin/End/Abort 元数据事务，原子发布已提交 image。controller 在追加后退出时，恢复后应看不到半个事务。
- [ ] 05.2 实现节点注册、心跳、追平后解除 fencing 和受控退出。保存 Redis node ID 与 broker ID、RESP 地址的映射；以 incarnation 和 broker epoch 拒绝旧进程请求。
- [ ] 05.3 实现组登记与 SlotAssignmentRecord。校验目标组、旧 owner 和 generation，支持一个组多个不连续区间；服务前检查 16,384 个 slots 的完整覆盖、无重叠和有效引用。
- [ ] 05.4 接收数据组报告的 leader、term 和成员配置位置，校验来源并处理迟到通告。元数据更新只发布路由提示；向旧 leader 发写仍须由数据组检查任期与归属。
- [ ] 05.5 用 C1/C2/C3 启动元数据组。从全日志与快照分别重建 slot 表、节点映射及迁移阶段，比较结果。中断元数据多数，验证新归属不能发布，已配置的健康数据组仍可独立提交。

## 06 按组数据复制与换主

静态组实例依赖 03、04；自动发现和路由通告依赖 05。以原图的两组四节点为第一套用例。[原图与新图](raft-cluster-design.md#集群拓扑) · [组内复制与选举](raft-replication-design.md) · [现有复制管理](../../src/server/replica_manager.cc)

- [ ] 06.1 将 group ID 映射为内部数据 topic 的 partition ID。按 cluster、group 和目录分发 RPC；缺少分组字段的管理消息选用明确的组级端点或版本化封装。测试把组 20 的 Vote、Fetch 故意送入组 10。
- [ ] 06.2 每组独立创建、恢复和停止 Raft 实例。隔离持久日志、term、voter 配置和 HW；在相同 offset、不同组的日志上验证写完成回调不会串组。
- [ ] 06.3 在 A/A-r1 的组 10 和 B/B-r1 的组 20 验证 2/2 多数。断开 A，A-r1 无法单独接任，组 10 无新写确认；维持组 20 多数，验证其 HW 继续前进。
- [ ] 06.4 恢复 A 后让 A-r2 追平并完成 voter 变更。再断开 A，验证存活两票可选出合格 leader，组 10 保留已确认前缀，0–8191 的 owner group ID 不变。
- [ ] 06.5 实现旧主重新加入、对齐历史和故障请求清理。迟到的旧 term 回调不能完成新任期写；模拟恢复过程中再次换主，检查已提交记录仍然保留。

## 07 KV 写入、提交与执行

依赖 06 的组日志。首个端到端用例选用 SET/GET，随后扩展条件写、事务和后台写。[事务执行](../../src/server/transaction.cc) · [journal 回放](../../src/server/journal/journal_applier.cc) · [写确认时序](figures/write-ack.svg)

- [ ] 07.1 接入事务求值，生成带 DB、slot、generation 及确定性效果的批次。在暂存状态中求 Redis 回复；排队时保持同 slot 的操作顺序，等待网络时释放 shard 锁。
- [ ] 07.2 建立 append → 多数持久化 → HW → 按序 apply → RESP 的完成路径。测试跨线程完成顺序颠倒；读请求看不到未提交值，同一记录重复回放不会再次执行 INCR。
- [ ] 07.3 建立新 leader 对外服务前的恢复条件。继承的日志在当前 term 确认且本地 KV 应用到恢复边界后再接受条件写。用“旧主 INCR 已复制、未回复即宕机，随后新主再次 INCR”验证结果。
- [ ] 07.4 覆盖同 slot 的 MULTI/EXEC、脚本、阻塞命令、跨 DB 操作和节点范围的 FLUSHDB/FLUSHALL。记录随机结果、绝对过期时间与淘汰效果；逐项核对客户端回复、原子可见性和重放后的 KV。
- [ ] 07.5 分别在追加前、追加后、提交后、apply 后回复前注入失败。区分确定未写和结果未知；检查超时、断连和换主后的 waiter 清理，以及非幂等请求重试的可观察结果。

## 08 数据组 KV 快照与落后副本恢复

依赖 03、06、07 的日志与 apply 边界。使用 Dragonfly KV 镜像格式，并为组内恢复记录对应的 Raft 位置。[现有快照实现](../../src/server/snapshot.cc) · [快照并发写时序](figures/replica-snapshot.svg)

- [ ] 08.1 在组内所有参与线程已连续 apply 的边界 K 建立快照，记录组身份、epoch、排他 offset K 和当时的 slot generation。利用 bucket 写前保存覆盖并发 apply。
- [ ] 08.2 遍历该边界所属 slots 的所有 DB、类型及绝对 TTL。随迁移模块补齐冻结状态和暂存块；比较并发写期间的快照与同一 K 的日志回放结果。
- [ ] 08.3 实现镜像传输、校验、临时构建与原子安装。保留 K 起的连续日志，落后副本从 K 续拉；模拟中断、源 leader 改变和日志回收，验证无漏写或重复应用。

## 09 Redis 客户端、路由与复制状态

依赖 05、06；写回复和 WAIT 依赖 07。复用现有客户端端口和 RESP 编码。[请求路由](../../src/server/main_service.cc) · [集群命令](../../src/server/cluster/cluster_family.cc) · [WAIT 实现](../../src/server/server_family.cc)

- [ ] 09.1 从同一份已提交 image 派生 MYID、NODES、SLOTS、SHARDS，对齐 ROLE、INFO REPLICATION。保留每个数据节点的 ID 和通告地址；检查多段 slot、无主组、副本上游和独立 controller 的呈现。
- [ ] 09.2 接入 CheckKeysOwnership，覆盖 hash tag、MOVED 和 CROSSSLOT。在同 slot、同组不同 slot、不同组执行多键命令、事务及 pipeline，逐项核对错误和重定向地址。
- [ ] 09.3 按连接记录最后一次写在组内的位置。WAIT 按已执行到该位置的副本数及超时返回；制造“已 Fetch 持久化、尚未 apply”的副本，验证计数正确。
- [ ] 09.4 首版校验 leader 读和副本拒写。分别记录 READONLY/READWRITE、ASK/ASKING 的支持范围；如开放副本读或临时导流，补连接级状态和换主/迁移回归。若声明线性一致读，先定义多数确认读协议并测试隔离旧主。
- [ ] 09.5 用 redis-cli -c、redis-py RedisCluster 和独立拓扑缓存客户端按支持的 RESP 版本从原 seed 建连，经过组换主、重启、迁移后核对可达地址与原 node ID。[客户端测试](../../tests/dragonfly/cluster_client_test.py)

## 10 单 slot 跨组迁移

依赖 05–09。用 slot 42 从组 10 迁往组 20，slot 43 留在组 10 的场景验收。[迁移时序](raft-slot-migration.svg) · [源端实现参考](../../src/server/cluster/outgoing_slot_migration.cc) · [目标端实现参考](../../src/server/cluster/incoming_slot_migration.cc)

- [ ] 10.1 controller 记录 migration ID、源和目标、预期 owner/generation 及阶段。源组排空 slot 42 在途请求并提交冻结标记，覆盖 TTL、淘汰和管理写；slot 43 继续读写。
- [ ] 10.2 从冻结点导出镜像到目标组，按 migration ID 和序号去重暂存块。目标日志提交、apply 完整且数据校验通过后报告就绪；源、目标分别记录自己的 offset。
- [ ] 10.3 元数据事务原子修改 owner/generation 和阶段。目标看到切换并应用导入后开放写入。源仍冻结直到观察到切换，随后返回 MOVED；验证全程只有一个写入方。
- [ ] 10.4 从持久阶段恢复或取消迁移。注入冻结后源换主、导入后目标换主、切换响应丢失及回迁后重试旧清理，检查 generation 防止误删。覆盖冻结窗口的 TRYAGAIN 与切换后的 MOVED。[迁移故障测试](../../tests/dragonfly/cluster_migration_resilience_test.py)

## 11 存量集群导入、切换和回退

依赖 01、05–09。存量导入以稳定的全量拓扑和停写数据为输入，可独立于 10 的在线 slot 迁移开发。[切换步骤](raft-cluster-design.md#存量集群切换步骤) · [现有管理工具](../../tools/cluster_mgr.py)

- [ ] 11.1 实现导入前检查。结束在途迁移，冻结旧 manager、排空写入并隔离旧节点入口。读取实际生效的 topology、node ID、RESP 地址和全部 slot 范围，报告节点视图不一致。
- [ ] 11.2 将每个旧 master 及其 replicas 映射为稳定组。导出各 DB 的数据和绝对 TTL，在独立 Raft 目录恢复；重试同一导入得到相同组 ID 和恢复点，旧 per-shard LSN 保留其原有含义。
- [ ] 11.3 校验每组 voter、HW、apply 和镜像摘要，在同一过期时间基准下逐 DB、逐 slot 比较数据。模拟中途重启及配错模式，验证未完成导入不会开放业务入口。
- [ ] 11.4 新集群接管原 RESP 地址，使用已有客户端及缓存节点地址验证读写。分别演练首次新写之前切回，以及已有新写后停写、导回最终数据、校验再切回。

## 12 联合验收与容量

测试夹具可从 02 开始建立；最终报告依赖相应模块的实现。沿用现有 C++ 和 Python 测试入口，协议差分、多进程故障和客户端兼容分别记录结果。[集群配置测试](../../src/server/cluster/cluster_config_test.cc) · [Python 集群夹具](../../tests/dragonfly/cluster_test_utils.py) · [复制故障测试](../../tests/dragonfly/replication_resilience_test.py)

- [ ] 12.1 为固定 Kafka 提交保存可重放的事件序列、种子、批次和预期状态。差异报告指出第一个分歧事件及其组、term、投票、HW 或输出消息。
- [ ] 12.2 跑真实多进程矩阵：元数据失去多数、单数据组失去多数、A-r1 单票、扩为三 voter 后失主，以及整个集群断电重启。逐组核对已确认写和 slot 归属，核对另一健康组能继续提交。
- [ ] 12.3 暴露诊断所需的角色、term、voter 配置、日志末端、HW、apply、元数据 image offset 及迁移阶段。用同一次超时定位是选举未完成、复制未达多数、KV 未应用还是路由尚未刷新。
- [ ] 12.4 固定 16,384 slots 改变数据组数，测 FD、内存、冷启动、落后副本重建、吞吐及写延迟；分别记录慢 voter 对本组和同 proactor 其他组的影响，并与相同负载的 yes 模式对照。
- [ ] 12.5 保存版本化兼容结果：旧配置、Redis 客户端、管理命令、协议版本与扩展记录。测试不支持新记录的节点加入、降级和快照恢复；记录实际拒绝点及可回退的数据边界。
