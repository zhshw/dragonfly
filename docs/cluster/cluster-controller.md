# Cluster Controller

新建 Raft 集群为控制器保留 **group ID 0**。C1、C2、C3 复制节点身份、槽表和管理操作进度，组内选出的 leader 担任 active controller。数据节点回放已提交的元数据，生成本地路由表。[controller 入口](../../src/server/cluster/raft/runtime/controller.cc) · [数据节点回放和路由](../../src/server/cluster/raft/runtime/data_node.cc)

## 两套 Dragonfly 集群管理的取舍

| 场景 | `--cluster_mode=yes` | `--cluster_mode=raft` |
| --- | --- | --- |
| 变更只到了一半节点 | manager 生成完整 JSON，逐个发送 `DFLYCLUSTER CONFIG`。接口简单，已有编排工具可直接使用。漏发、乱序和重试由 manager 处理 | controller 将变更按序提交一次，各节点主动拉取。落后的节点按同一段历史追平，在应用前仍可能返回旧路由。一次变更须等待控制组多数持久化 |
| 管理进程或数据节点重启 | 数据节点的槽表只在内存。只要 manager 保存了权威配置，就能重新下发。manager 本身的高可用和配置备份取决于部署 | 三个 controller 中失去一个，其余两票可以选主并延续已提交历史。保留持久卷时，全群重启也可恢复。要为各副本配置磁盘和故障域，多数副本永久丢盘仍需灾备 |
| 控制面暂时不可用 | 已配置且仍运行的主节点可按原拓扑服务。新的归属、故障接管及重启节点重新配置要等 manager | 控制组失去多数时，已有路由及 leader 仍有效、且本组有多数的数据组可继续读写。新归属、重启节点注册和换主后的新路由发布要等控制组恢复 |
| 数据主节点故障 | 外部 manager 可按业务策略提升副本。默认异步复制允许主节点先答复，接管时可能缺少副本尚未收到的已确认写 | 数据组自行选主，已确认写经过本组多数。原图每个数据组只有两票，掉一台便停写。扩为三票才可容忍一台故障；增加 controller 票数不能改变这个门槛 |
| 运维工具与部署 | 节点端无需部署 controller quorum。已有平台可继续负责选址、扩容和流量切换，同时承担状态保存与下发失败处理 | 示例部署三个 controller 副本，每个数据节点跟随控制组。首次建群由控制组提交数据组清单，运维脚本通过管理 RESP 端口提交单槽迁移并查询进度 |

`--cluster_mode=yes` 下，manager 可以从自己的高可用存储恢复权威拓扑。`--cluster_mode=raft` 下，控制组日志记录已提交的变更，节点从各自的回放位置继续应用。两种模式下，运维平台都可以制定扩容和迁槽策略。[配置发布与恢复](../cluster-mode.md) · [Raft 重启与失主用例](../../tests/integration/raft_data_group_test.py)

## Kafka KRaft、Redis Cluster 和 Dragonfly 各由谁做决定

| 问题 | Kafka KRaft | 原生 Redis Cluster | Dragonfly 现有 `--cluster_mode=yes` |
| --- | --- | --- | --- |
| 谁维护拓扑 | 指定的 controller 组成元数据 quorum。active controller 根据注册、心跳和管理请求产生元数据记录，其他 controller 可接任 | 各节点通过 cluster bus 交换心跳、故障报告和 slot 配置。节点按配置纪元（configEpoch）解决 slot 归属冲突，集群没有专职 controller | 外部 manager 决定 slot 归属，生成完整拓扑，通过 `DFLYCLUSTER CONFIG` 下发到每个 master 和 replica |
| 故障时谁选主 | controller 由元数据 quorum 选举。普通 topic 的分区 leader 由 controller 按副本状态选定，数据复制采用 leader/follower 和 ISR 机制 | 主节点故障时，符合条件的副本向负责 slots 的主节点请求选票，获得多数授权后接管原 slots。普通写默认采用异步主从复制 | manager 决定提升哪个副本，并更新拓扑。配置里的 `replicas[]` 描述对外关系，实际复制另用 `REPLICAOF` 建立 |
| 重启从哪里恢复 | 从本地元数据日志、快照和持久选票恢复，跟随当前 leader 追平，broker 应用已提交元数据 | 各节点读取自己的集群配置文件（默认 `nodes.conf`），保留节点 ID、已知成员、slot 归属和纪元，再通过总线收敛 | 节点内存里的 `ClusterConfig` 随进程退出而消失。manager 须重新下发，RDB、AOF 和主从复制都不会重建这份拓扑 |
| 得到什么、付出什么 | 管理变更有统一顺序，换主后可续做；发布新元数据依赖 controller 多数派 | 无需单独部署 controller，故障转移由集群完成；传播期间节点视图可短暂不同，异步复制下故障转移可能丢失已确认写 | 工具能用一条 Redis 协议命令安装拓扑；权威状态的持久化、跨节点下发和失败重试交给外部 manager |

Kafka 对照同级仓库提交 `5b648ca9`。原生 Redis 先将本地超时标为 PFAIL，汇集足够的主节点故障报告后确认 FAIL；候选副本再向全群持槽的主节点争取过半选票。这些票授权副本接管旧主的槽。普通写可以在副本收到之前返回，`WAIT` 能降低已确认写丢失的风险。[Kafka KRaft 部署](../../../kafka/docs/operations/kraft.md) · [Kafka 分区控制](../../../kafka/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java) · [Redis Cluster 规范](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/) · [Redis 8.2 选举实现](https://github.com/redis/redis/blob/4fa8bf356b4bf0f1ce345286a4badcb/src/cluster_legacy.c#L3998-L4027) · [Dragonfly 现有管理模型](../cluster-mode.md)

## 控制器与数据组的职责

active controller 校验管理请求，在三名 voter 中取得两票后提交拓扑变更。数据组自行选主、确认业务写入，并向控制器提供迁移进度。增加 controller 只改变控制组的成员和多数门槛。[集群拓扑与组映射](raft-cluster-design.md#集群拓扑)

**部署建议。** C1、C2、C3 分布在三个故障域，先采用独立 controller 进程，便于单独滚动升级、扩缩容，也能隔离数据节点的内存压力和磁盘负载。代价是额外的进程、持久卷和内部监听端口。若将来支持同一进程兼任 controller 和数据节点，可减少小集群的部署成本，但一次停机会同时拿走两个组的票。部署前应按故障域计算每组剩余的票数。[Kafka 对独立和混合角色的取舍](../../../kafka/docs/operations/kraft.md)

## 怎样通过 Redis 管理集群

在 `--cluster_mode=raft --raft_role=controller` 的节点上，设置非零 `--admin_port` 开启 RESP 管理端口。例如，`--admin_bind=127.0.0.1 --admin_port=6380` 允许同机脚本用 `redis-cli -p 6380` 连接。管理端口复用 `requirepass` 和管理端口的 TLS 配置。脚本配置各 controller 的管理地址，收到 standby 的 `TRYAGAIN Contact the active controller` 时换一个地址重试。查询返回 active controller 本机已应用的提交视图，目前没有为单次读取另设 quorum 读屏障。[管理监听](../../src/server/cluster/raft/runtime/admin_listener.cc) · [管理命令](../../src/server/cluster/raft/runtime/admin_command.cc)

以原图中组 10 的业务槽 42 为例。先读取当前 owner 和 generation，将返回的值作为变更前提。下例假设得到 `10` 和 `1`。

```redis
HMGET ctl:slot:42 owner generation
SET ctl:req:move-42-001 '{"slot":42,"from":10,"to":20,"generation":1}' NX
GET ctl:req:move-42-001
HGETALL ctl:op:move-42-001
```

| 操作状态 | 管理端和业务端能观察到什么 |
| --- | --- |
| `preparing` | 控制组多数持久化并应用请求后，`SET … NX` 返回 `OK`。回复丢失可用原 ID 重试，返回 nil 时用 `GET` 核对已存的原始 JSON。同一个 ID 始终指向同一笔操作。 |
| `frozen` | 组 10 已冻结该槽并在本组日志中提交完整镜像的 SHA-256 和位置。组 20 按块复制并验证镜像。此时 owner 仍是 10，冻结期间源 leader 对该槽的 GET 和 SET 返回 `TRYAGAIN`，相邻槽继续服务。 |
| `cutover` | 目标 leader 报告已安装的日志位置后，controller 在一次元数据事务中将归属从 `(10,1)` 改为 `(20,2)`。目标同时观察到新归属并应用对应镜像后开放读写。源组更新路由后返回 `MOVED`。 |
| `finished` | 双方均已提交清理标记。用 `HGETALL ctl:op:move-42-001` 查到两项 `*_finished` 均为 1，用 `HMGET ctl:slot:42 owner generation` 查到 20 和 2。 |

这些管理对象来自控制组的提交记录。业务端口上的同名键仍是普通业务数据。当前请求记录和原始 JSON 随元数据快照保留，尚无清理期限。接口仅开放单槽迁移和上述查询。[迁移时序](raft-cluster-design.md#4-单个-slot-跨组迁移)

Redis 经典逐键迁槽允许 `MIGRATING/IMPORTING` 期间使用 `ASK/ASKING` 临时导流。Redis 8.4 起还提供 `CLUSTER MIGRATION` 原子迁槽接口。本方案先选冻结单槽来简化换主和重试，代价是该槽从冻结至目标开放写入期间暂停写入，时长取决于数据量和两组的复制进度。管理员直接 `HSET` 改 owner，或者用一次 `MULTI/EXEC` 改完几个管理键，都无法证明另一个组已经接收数据。[经典迁槽规范](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#ask-redirection) · [Redis 原子迁槽接口](https://redis.io/docs/latest/commands/cluster-migration/)

### 各 cluster_mode 的管理入口

| 启动配置 | 谁决定拓扑，配置怎样生效 |
| --- | --- |
| `--cluster_mode=yes` | 外部 manager 生成完整拓扑，用 `DFLYCLUSTER CONFIG` 下发到每个主节点和副本。重启的节点需再次接收配置。 |
| `--cluster_mode=raft` | active controller 校验变更并提交给控制组。数据节点主动拉取已提交元数据，拒绝逐节点安装 `DFLYCLUSTER CONFIG`。管理端口接受带当前 owner 和 generation 的单槽迁移请求。 |
| `--cluster_mode=emulated` | 单个实例对客户端展示模拟的槽拓扑，无需发布多节点配置。 |
| 省略 `--cluster_mode` 或取空值 | 以单机模式运行。 |

manager 管理的集群把完整槽表下发给节点。Raft 集群在独立目录中初始化，通过 `--raft_bootstrap_topology` 声明初始数据组、成员和槽范围。两套系统分别部署和恢复。[Raft 建群示例](raft-cluster-design.md#独立建群) · [manager 配置下发](../cluster-mode.md)

## 重启、换主和增加控制器

**当前实现。** 每个 controller 副本在自己的 `--dir/__cluster_metadata-0/` 中保存选举状态、日志和元数据快照。该目录由内部 topic `__cluster_metadata` 和 partition 编号生成。节点身份文件 `meta.properties` 位于 `--dir` 根目录。[controller 打开的分区](../../src/server/cluster/raft/runtime/controller.cc) · [组目录生成](../../src/server/cluster/raft/runtime/group.cc) · [节点身份](../../src/server/cluster/raft/bootstrap/raft_directory.cc)

**重启恢复。** controller 校验组身份并恢复选举状态，从快照的应用位置继续回放已提交记录，重建槽表和管理操作状态。控制组恢复多数后即可提交新变更。管理请求的原文、ID 和阶段也从这份日志恢复。[组内选举与持久状态](raft-replication-design.md)

| 场景 | 恢复行为与约束 |
| --- | --- |
| 版本 18 已提交，A 已应用，B 仍在 17，C1 此时宕机 | 新 active controller 从已提交记录恢复。B 向新 leader 继续 Fetch，回放到 18 后更新本地路由。路由版本取元数据稳定日志位置，每个 slot 的 generation 单独校验 |
| 组 10 已冻结，controller 还没记下回执就宕机 | 组 10 的 leader 根据本组日志向新 active controller 重报同一操作的冻结位置，再继续导入。若归属已切换，则从清理阶段继续。恢复所需状态包括操作进度、两组标记及最终归属 |
| 控制组只剩一票 | 无法提交新归属、注册或迁移。原 leader、路由仍有效且保有多数的数据组可继续服务。数据组若在此时换主，新路由的发布要等控制组恢复。组 10 丢掉 A-r1 时只有一票，即使三台 controller 健康也无法确认新的组 10 写入 |
| 增加 C4 | 在动态 voter 模式下，以原 cluster ID、新 node ID 和独立格式化目录启动 C4，使其作为 observer 追平，再向控制组提交 AddRaftVoter 并确认成员记录已提交。3→4 后多数从 2 变成 3，仍容忍 1 台故障；若要容忍 2 台，逐台扩至 5 台。数据组及 slot 分配无需变更 |
| 全部进程退出或持久卷损坏 | 保留节点目录时，从快照和已提交日志重建状态。多数 controller 卷永久丢失属于灾难恢复，需要独立备份和恢复流程；初始 topology JSON 只能描述引导时的布局 |

[Kafka 动态 controller 成员管理](../../../kafka/docs/operations/kraft.md) · [controller 恢复与成员变更用例](../../tests/integration/raft_controller_test.py)

## 当前分支与待验证项

七节点用例运行三个 controller 和两个各有两票的数据组。目标组失去一票时，请求仍可受理，归属保持在源组。杀掉 active controller 后，新 controller 按同一操作 ID 继续处理。恢复目标组后，槽 42 的多 DB 二进制值迁入组 20，槽 43 继续由组 10 读写。移除引导文件并重启全部进程后，新的归属、操作结果和迁移完成后的新写入均能恢复。[七节点集成测试](../../tests/integration/raft_data_group_test.py)

另一次故障注入把两个数据组都扩为三票，在源端分块尚未封存、目标端分块尚未装完时分别终止各自的 leader。新主接续迁移，目标读回的 4 MiB 随机数据与源数据逐字节一致。回迁演练把同一槽从 generation 2 迁回到 generation 3，原操作 ID 仍可查询，携带旧 generation 的新请求被拒绝。

首次启用在线迁槽前，应将所有 controller 和数据节点升级到能回放迁移记录的版本。首次建群使用正整数数据组 ID。已有 Raft 集群从原盘恢复已提交的组编号和槽表。[新建拓扑校验](../../src/server/cluster/raft/metadata/bootstrap_topology.cc) · [目录格式化与恢复](../../src/server/cluster/raft/bootstrap/raft_directory_test.cc)

迁槽会额外占用内存和日志空间。源 leader 在内存中生成整槽 RDB，源和目标副本分别暂存镜像，日志按 256 KiB 分块复制。大槽的冻结时长和额外内存需求取决于槽内数据量、磁盘和组内复制进度。[数据组迁槽驱动](../../src/server/cluster/raft/runtime/slot_migration_driver.cc)

在线扩容还要验证地址发现。当前进程用例覆盖预配地址的 observer 晋升；数据节点向 controller 注册和发送心跳时，仍从启动地址表查找 leader。若原节点只配置了 C1–C3，C4 当选后应能根据动态成员记录找到 C4，并在原数据节点无需重启的情况下恢复心跳。这个场景是将新增 controller 视为完整支持前的验收项。[成员变更用例](../../tests/integration/raft_controller_test.py) · [broker 注册与心跳](../../src/server/cluster/raft/runtime/data_node.cc)
