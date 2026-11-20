# Raft 集群任务与验收

`--cluster_mode=yes` 的集群由 manager 下发槽表，数据节点通过主从复制同步。`--cluster_mode=raft` 的集群从自己的拓扑和节点目录建群。本文验收 Raft 的七节点部署。[原模式架构](../cluster-mode.md) · [Raft 拓扑图](raft-cluster-design.md#集群拓扑)

跑通的标志是，Redis 客户端能向两个数据组写入并读回数据，管理员能把槽 42 从组 10 迁到组 20，七个进程全部重启后仍能查到迁移结果并读回已确认值。

## 建群与路由

- [x] 控制组由 C1、C2、C3 三个 voter 选主。四个数据节点分别属于组 10（A、A-r1）和组 20（B、B-r1）。每个节点有独立的持久目录和 node ID，各数据组自行选主。[控制组测试](../../tests/integration/raft_controller_test.py) · [数据组测试](../../tests/integration/raft_data_group_test.py)
- [x] 用 Raft 原生引导文件从七个空目录建群。`groups` 中每个数据组声明正整数 `group_id`、`members` 和 `slot_ranges`。四个数据节点的 `CLUSTER SLOTS` 均应列出 0–8191 和 8192–16383，成员及端口分别对应组 10 和组 20。引导文件有重叠或缺失槽时须报告错误。[引导拓扑测试](../../src/server/cluster/raft/metadata/bootstrap_topology_test.cc)

## 数据组读写

- [x] 用 Redis Cluster 客户端向两组各写一个二进制值并读回。向错误的 owner 发请求得到 `MOVED`，`redis-cli -c` 可以跟随重定向访问正确节点。[七节点读写测试](../../tests/integration/raft_data_group_test.py)
- [x] 停止组 10 的一名 voter，该组剩余一票无法确认新写，组 20 仍可读写。恢复该 voter 后，组 10 恢复写入。[两票数据组故障测试](../../tests/integration/raft_data_group_test.py)

## 管理端口与迁槽

- [x] 对 active controller 的管理端口发送 `SET ctl:req:move-42-001 <json> NX`。请求指定槽 42、源组 10、目标组 20 和 generation 1。首次提交返回 `OK`，同 ID 重试返回 nil，可用 `GET` 查回请求原文。[管理命令及示例](cluster-controller.md#怎样通过-redis-管理集群)
- [x] 停止目标组的一名 voter。观察到 `frozen` 时归属仍为组 10，源 leader 对槽 42 的 GET 和 SET 返回 `TRYAGAIN`，相邻槽 43 继续读写。恢复目标 voter 后，`HGETALL ctl:op:move-42-001` 显示 `finished`，`HMGET ctl:slot:42 owner generation` 返回 20、2，目标能读回迁移前各 DB 的二进制值并接受新写。[迁槽集成测试](../../tests/integration/raft_data_group_test.py)

## 换主与重启

- [x] 请求已受理且迁移未完成时终止 active controller。新 active 可以查到相同操作 ID 和原文，并接续完成迁移。另以八个进程验证动态加入的 C4 当选，原数据节点发现 C4 后完成迁槽，重启的数据副本能向 C4 重新注册。[换主与 C4 发现测试](../../tests/integration/raft_data_group_test.py)
- [x] 终止全部进程，保留七个节点的磁盘，删除引导文件后重新启动。检查槽 42 的归属、`finished` 状态和迁移后的新写入，确认槽 43 的数据仍由组 10 提供。[整群重启测试](../../tests/integration/raft_data_group_test.py)

Dragonfly 在 Linux Debug 上通过 controller 集成测试 6/6、数据组集成测试 8/8。数据组套件中的 Java 客户端真实连接元数据组和两个数据组的 leader，调用 `ApiVersions` v4 和 `DescribeQuorum` v2，检查 leader、HW 和 voter 数量。[控制组用例](../../tests/integration/raft_controller_test.py) · [数据组用例](../../tests/integration/raft_data_group_test.py) · [Java 探针](../../src/server/cluster/raft/protocol/testdata/LiveQuorum.java)

同级 Kafka 仓库 `5b648ca9` 的上游对照测试通过 704 项。raft 模块的 593 项覆盖选举、Fetch、快照和 voter 变更，对应 Dragonfly 的组内协议。metadata 模块的 111 项覆盖 controller 激活、broker 心跳和元数据回放，对应 Dragonfly 的控制面。[上游 Raft 测试](../../../kafka/raft/src/test/java/org/apache/kafka/raft) · [上游元数据测试](../../../kafka/metadata/src/test/java/org/apache/kafka) · [Dragonfly quorum 测试](../../src/server/cluster/raft/quorum) · [Dragonfly 元数据测试](../../src/server/cluster/raft/metadata)

## 数据组复制与快照恢复

KRaft 用排他 offset 和 leader epoch 标识快照。副本缺少可读日志前缀时，leader 在 Fetch 响应中给出快照 ID，副本按字节位置执行 FetchSnapshot，安装后从该 offset 继续 Fetch。Dragonfly 在这套传输中保存本组的 KV 和迁槽状态。[Kafka 快照测试](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientSnapshotTest.java) · [数据组复制设计](raft-replication-design.md)

- [x] **状态机**　在已提交并应用的完整批次边界保存各 DB 的 KV、实际持有的槽及 generation、冻结与导入进度。恢复同一 cut 后，旧 generation 继续受限，已应用记录不重复执行。[状态机测试](../../src/server/cluster/raft/data/set_effect_test.cc)
- [x] **本地快照与日志清理**　按新提交的日志字节数或时间生成快照，持久化成功后推进可读日志起点。保留快照及其后的日志，重启时先恢复快照，再回放已提交的后缀。[快照运行时](../../src/server/cluster/raft/runtime/data_snapshot.cc) · [RDB 恢复测试](../../src/server/cluster/raft/data/kv_rdb_test.cc)
- [x] **落后副本追赶**　落后位置仍在日志内时沿用普通 Fetch，早于保留起点时通过 FetchSnapshot 分块补齐。检查快照身份、大小和内容，安装后继续复制新写入。[FetchSnapshot 测试](../../src/server/cluster/raft/quorum/snapshot_transfer_test.cc)
- [x] **进程验收**　让一台 voter 离线至组内清掉旧前缀，验证它追平、能接任 leader 并读回快照和后缀中的值。迁槽冻结期间重启数据节点，再验证操作恢复、归属和邻槽数据。[进程测试](../../tests/integration/raft_data_group_test.py)
- [x] **复制对比**　在旧版异步和 Raft 两种集群写入相同的大值，停止一台副本后恢复并逐键校验。暂停一台和两台副本，分别检查 SET、WAIT、另一组的可用性，以及少数派日志的可见性。[配对测试](../../tests/integration/replication_comparison_test.py)
- [x] **传输阻塞与恢复**　旧版全量同步时暂停副本读取，验证写入反压和缓冲排空。Raft 副本下载部分快照时将其暂停并强杀，验证多数仍可提交，恢复后完整快照和尾部写入均可读回。与上游 KRaft 的多数提交、Fetch 批次边界、快照分块用例逐项对照。[实测结果与复现命令](raft-replication-design.md#7-数据组复制对比实测)
- [x] **复制延迟**　记录落后副本的持久化 LEO 与主副两端刷盘耗时。让日志追加唤醒空 Fetch，让提交进度唤醒写入方，在相同 64 MiB、128 MiB 负载上复测，并验证少数派写入和并发 NX。[时间分解](raft-replication-design.md#8-串行写入和副本追平的时间花在哪里) · [优化前后对照](raft-replication-design.md#9-事件驱动复制的复测)

双引擎分片的进程用例逐字节比较了 leader 与落后 voter 的快照，文件超过 1 MiB，需要多次 FetchSnapshot。尾部新写达到 `WAIT 2` 后，让恢复的 voter 当选并再次重启，验证两个引擎分片上的值和增量写入。七节点用例还验证了冻结时两个源 voter 从快照恢复、随后迁槽完成及整群重启。Kafka 对照执行了 `MockLogTest` 和 `KafkaRaftClientSnapshotTest` 中的快照提示、分块响应与监听器通知用例。[上游日志测试](../../../kafka/raft/src/test/java/org/apache/kafka/raft/MockLogTest.java) · [上游快照测试](../../../kafka/raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientSnapshotTest.java)

## 复现命令

在 Linux 的仓库根目录执行。测试需要 `redis-cli` 和 Python 的 `redis` 包。多进程用例自行分配本地端口，并用与 `dragonfly` 同目录的 `raft_storage` 格式化临时节点目录。运行完整的 8 项数据组用例时，需设置 `KAFKA_TEST_CLASSPATH`，包含编译后的 `LiveQuorum.class` 所在目录、Kafka 客户端及其运行依赖。省略这个变量会跳过 Java 联调用例。[Python 依赖](../../tests/dragonfly/requirements.txt) · [Java 探针源码](../../src/server/cluster/raft/protocol/testdata/LiveQuorum.java)

```sh
./helio/blaze.sh -DWITH_GCP=OFF
ninja -C build-dbg dragonfly raft_storage bootstrap_topology_test admin_command_test slot_migration_test \
  set_effect_test kv_snapshot_test kv_rdb_test snapshot_transfer_test partition_snapshot_test
ctest --test-dir build-dbg --output-on-failure -R '^(bootstrap_topology|admin_command|slot_migration|set_effect|kv_snapshot|kv_rdb|snapshot_transfer|partition_snapshot)_test$'
export DRAGONFLY_PATH=build-dbg/dragonfly
python3 tests/integration/raft_controller_test.py -v
python3 tests/integration/raft_data_group_test.py -v
```

单独复现 C4 当选后的迁槽和重新注册，可运行

```sh
DRAGONFLY_PATH=build-dbg/dragonfly python3 tests/integration/raft_data_group_test.py -v \
  DynamicControllerDiscoveryTest.test_data_nodes_follow_controller_outside_bootstrap_list
```
