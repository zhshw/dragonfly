# Dragonfly 组复制设计

`--cluster_mode=yes` 的主节点按引擎线程推送 journal，副本用 LSN 报告执行进度。`--cluster_mode=raft` 的每个数据复制组拥有独立的分区日志，voter 在组内选主，按多数持久化位置确认写入。下文沿用同一组 A、A-r1 节点对照两套复制协议。Raft 示例另将 A-r2 加入组 10，使多数从 2/2 变为 2/3。slot 42 和 43 共用组 10 的日志。[集群拓扑与 slot 映射](raft-cluster-design.md) · [主从复制原图](../replication.md) · [历次复制实验](profile.md#experiments)

本文描述默认 `--raft_data_ack_mode=flush`。实验选项 `memory` 改为多数页缓存确认，
控制记录和元数据仍需落盘；确认条件及恢复限制见 [io_uring 文档](io_uring.md)。

## 1. 复制状态机

原图跟踪主节点上的复制会话。Raft 图跟踪组 10 的一个 follower。组内换主时，follower 根据新 leader 的 epoch 和本地日志末端查找共同前缀。本地仍有共同日志前缀时继续增量 Fetch；落后位置早于 leader 的日志起点时，follower 安装整组 KV 快照后续拉。

![副本加载分配、恢复共同前缀或安装快照后进入 Fetch](figures/replica-states.svg)

<details>
<summary>查看新版 Mermaid 源码</summary>

```text
flowchart TD
    start((启动)) --> PREPARATION["加载组分配与本地日志"]
    PREPARATION -->|历史仍在保留窗口| ALIGN["比较 epoch 与 offset"]
    PREPARATION -->|所需前缀已清理| FULL_SYNC["安装组 KV 快照"]
    ALIGN -->|保留共同前缀| STABLE_SYNC["按组 Fetch"]
    FULL_SYNC -->|从 cut 续拉| STABLE_SYNC
    STABLE_SYNC -->|leader epoch 变化| PREPARATION
    STABLE_SYNC -->|组副本被移除| CANCELLED["结束该组复制"]
```

</details>

<details>
<summary>展开原图 · PREPARATION / FULL_SYNC / STABLE_SYNC</summary>

```mermaid
flowchart LR
    start((start)) --> PREPARATION
    PREPARATION -->|DFLY SYNC| FULL_SYNC
    PREPARATION -->|all flows partial| STABLE_SYNC
    FULL_SYNC -->|DFLY STARTSTABLE| STABLE_SYNC
    PREPARATION --> CANCELLED
    FULL_SYNC --> CANCELLED
    STABLE_SYNC --> CANCELLED
```

</details>

## 2. 握手、全量恢复与增量拉取

`yes` 的副本与主节点协商每线程的 flow，完成同步后接收主节点推送的 journal。`raft` 的副本从元数据发现数据组的入口，由本组协议确认 leader 和 term，按日志位置拉取。两端历史一致时直接进入 Fetch 循环。图中“所需前缀已清理”分支走 KRaft FetchSnapshot。leader 在 Fetch 中给出快照 ID，follower 从字节位置 0 开始分块拉取，安装后从快照的排他 offset 继续 Fetch。

![按组 Fetch、截断分歧与安装 KV 快照](figures/replica-fetch.svg)

<details>
<summary>查看新版 Mermaid 源码</summary>

```text
sequenceDiagram
    participant R as Follower A-r1
    participant M as Leader A
    Note over R,M: 已确认组 10 的 Raft leader 和 term
    R->>M: Fetch，group 10、fetch offset、last fetched epoch
    alt 发现日志分歧
      M-->>R: diverging epoch 与 end offset
      R->>R: 对齐共同前缀，截断冲突尾部
    else 所需前缀已清理
      R->>M: 请求组 10 的 KV 镜像（Dragonfly 扩展）
      M-->>R: 校验信息、镜像数据、cut 的 epoch 与 offset
      R->>R: 完整校验后原子安装
    end
    loop 从对齐后的 offset 继续
      R->>M: Fetch，携带本地 LEO
      M-->>R: 连续批次与 leader HW
      R->>R: 持久化批次，仅应用已提交前缀
    end
```

</details>

<details>
<summary>展开原图 · 控制连接、DFLY FLOW 和 journal 推送</summary>

```mermaid
sequenceDiagram
    participant R as Replica
    participant M as Master

    R->>M: PING / REPLCONF ... (control connection)
    M-->>R: master_replid, sync_id, num_flows, version
    R->>M: REPLCONF CLIENT-ID / CLIENT-VERSION
    par per shard
    R->>M: DFLY FLOW flow_id [lsn] (new connection)
    M-->>R: FULL or PARTIAL, eof_token
    end
    R->>M: DFLY SYNC (control connection)
    M-->>R: +OK (streaming starts async)
    par per flow negotiated FULL
    M->>R: RDB snapshot bytes + interleaved journal changes
    M->>R: full sync cut marker
    end
    Note over R: waits for cut on every flow
    R->>M: DFLY STARTSTABLE
    M->>R: journal offset, eof_token (full flows only)
    par per flow
    M->>R: live journal stream
    R->>M: REPLCONF ACK (periodic)
    end
```

</details>

## 3. 快照与并发写入

| 对象 | 当前怎样生成和恢复 |
| --- | --- |
| 控制组元数据 | controller 按大小或时间阈值在稳定 offset 生成快照，恢复时从快照和后续已提交记录重建元数据。[控制器](../../src/server/cluster/raft/runtime/controller.cc) · [元数据快照](../../src/server/cluster/raft/metadata/metadata_snapshot.cc) |
| 迁槽中的单槽 RDB | 源组冻结该槽后生成 RDB，将分块和摘要写入源组日志。目标组按块拉取、在本组日志中提交并应用安装记录。恢复时重放迁槽记录即可接续。[迁槽驱动](../../src/server/cluster/raft/runtime/slot_migration_driver.cc) · [单槽 RDB](../../src/server/cluster/raft/data/kv_rdb.cc) |
| 整个数据组的 KV | 每个数据副本从本组已提交、已应用的完整批次取 cut，记录 DB 中的值、已物化槽的 generation、冻结与导入进度。快照落在该副本的 `--dir/__dragonfly_data-<group_id>/` 中，文件名记录排他 offset 和 epoch。重启时先装载已提交的本组快照，再从 cut 回放后续日志。尚无快照的旧目录仍可从保留日志恢复。[快照运行时](../../src/server/cluster/raft/runtime/data_snapshot.cc) · [数据组回放](../../src/server/cluster/raft/data/set_effect.cc) |

数据节点默认在新增已应用日志达到 20 MiB 或距上次快照达到一小时且有新提交时生成快照。可通过 `--raft_data_snapshot_bytes` 和 `--raft_data_snapshot_interval_ms` 调整，时间阈值为 0 时仅按字节数触发。各执行线程在同一 cut 注册写前保存回调，随后恢复 apply，快照 fiber 继续遍历。槽及 generation 来自本组已应用的写入和迁槽记录，能覆盖控制器已发布新归属、本副本仍在回放旧记录的情况。

快照发布并持久化后，副本推进日志起点，清理已覆盖的旧段。跨过 cut 的物理段保留其末尾可读的批次。下载中断时，未发布的文件可丢弃并重新拉取。接收端在替换日志前检查成员配置、组身份、cut、分块内容和 SHA-256、迁槽进度及 RDB；恢复整个数据进程时还会清除该副本先前持有而当前镜像已移出的槽。快照捕获及装载期间会在内存中持有整组 RDB，应用层还需容纳迁槽暂存数据。写盘、校验和装载占用本组 worker，大镜像可能延迟心跳或触发重新选主。若实际装载时内存不足，节点停止对该组提供服务；释放资源并重启后，会从已持久化的快照重新装载。[日志保留](../../src/server/cluster/raft/log/partition_snapshot.cc) · [快照接收](../../src/server/cluster/raft/quorum/snapshot_transfer.cc)

已清理数据日志前缀的副本需要能装载整组快照的版本才能重启；落后于该起点的副本需要支持数据组 FetchSnapshot。

下图沿用原图的遍历 fiber、bucket 和写 fiber，展示在 cut 后继续应用写入时怎样保留旧值。

![组快照与并发 apply 的 bucket 保存顺序](figures/replica-snapshot.svg)

<details>
<summary>查看新版 Mermaid 源码</summary>

```text
sequenceDiagram
    participant T as 快照遍历 fiber
    participant B as KV bucket
    participant W as 已提交写的 apply fiber
    Note over T,W: 组 10 在完整 apply 边界 K 建立快照，开启版本 N
    W->>B: 准备修改当前 bucket
    alt 遍历尚未保存该 bucket
      W->>B: 先保存修改前内容，标记版本 N
    end
    W->>B: 应用本次变更
    T->>B: 遍历到该 bucket
    alt 已经保存
      T->>T: 跳过重复序列化
    else 仍未保存
      T->>B: 保存属于本组的键与 DB、绝对 TTL
    end
    T->>T: 完成组快照，记录排他 offset K
    Note over T,W: 保留从 K 开始的日志，待接收端安装后续拉
```

</details>

<details>
<summary>展开原图 · 同一 bucket 上的遍历与写前保存</summary>

```mermaid
sequenceDiagram
    participant T as Traversal fiber
    participant B as Bucket (version v)
    participant W as Writer fiber

    Note over T,W: snapshot_version = N, both racing on the same bucket
    W->>B: about to mutate a key in this bucket
    alt bucket not yet visited by traversal (v < N)
    W->>B: side-save pre-mutation bucket contents, stamp version = N
    W->>B: apply mutation
    else bucket already visited (v == N)
    W->>B: apply mutation directly, no side-save needed
    end
    T->>B: reach this bucket during traversal
    alt version already == N
    T->>B: skip - already serialized by a side-save
    else version still < N
    T->>B: serialize now, stamp version = N
    end
```

</details>

## 4. 客户端写入何时确认

![三个 voter 中两个持久化当前任期的写入即可提交](figures/write-ack.svg)

<details>
<summary>查看新版 Mermaid 源码</summary>

```text
sequenceDiagram
    participant U as Redis 客户端
    participant A as Leader A
    participant R as Voter A-r1
    participant S as Voter A-r2
    R->>A: Fetch 100，等待新批次
    U->>A: SET，slot 42 属于组 10
    A->>A: 写入批次 100，开始刷盘
    A-->>R: 返回批次 100 和原 HW
    R->>R: 持久化批次
    R->>A: 下一次 Fetch 报告 LEO 101
    Note over A,S: A 和 A-r1 均已持久化，达到 2/3
    A->>A: 数据 HW 到 101，应用写入
    A-->>U: OK
    S->>A: 稍后 Fetch 100
    A-->>S: 批次 100 与 HW 101
```

</details>

本例假设 voter 配置稳定。组内每个 voter 持久保存本组的任期、选票和日志。leader 用每个不同 voter 的持久化日志末端计算多数位置，过滤旧 term、错误组、错误目录和重复响应。采用 KRaft 的排他 HW：当前 term 的记录到达多数后，才可据此推进提交边界，连带提交此前的前缀。应用到本地 KV 后返回原始 RESP 结果。Fetch 确认复制位置，副本 apply 位置单独用于 `WAIT`。[Kafka LeaderState](../../../kafka/raft/src/main/java/org/apache/kafka/raft/LeaderState.java)

若 A 只能联系自己，仍可能短暂认为自己是 leader，但无法为新的写入凑齐多数并返回成功。失联期间已追加但未确认的请求结果可能未知。恢复后按共同的 term/offset 截断冲突尾部，已提交前缀受 Raft 选举与日志规则保护。元数据 quorum 不可用时，健康且已配置的数据组仍能在本组选举和提交；新 slot 归属的发布需要元数据 quorum 恢复。

**成员调整。** 按动态 voter 模式格式化的数据组可以独立增删成员。新节点先以 observer 追平，再按上游的 VotersRecord 生效、提交和截断规则一次增删一个 voter。多数门槛基于协议有效的 voter 配置。更换组副本复制整组日志，迁移单个 slot 则在两个组之间切换归属。[跨组所有权切换](raft-cluster-design.md#4-单个-slot-跨组迁移)

## 5. 失主、选主与重新追赶

现有计划内 takeover 需要旧主配合排空。下图的 A 已不可用，A-r1 和 A-r2 在组 10 内交换选票。候选者的日志必须至少与投票者一样新。投票者在同一 term 只投一个候选，先持久化选票再应答。控制器用 `DescribeQuorum` 向数据组轮询选举结果，确认新 leader 和 epoch 后提交路由变更。[controller 探测数据组](../../src/server/cluster/raft/runtime/controller.cc)

![组三 voter 中剩余两票选出 A-r1，控制面随后更新路由](figures/leader-change.svg)

<details>
<summary>查看新版 Mermaid 源码</summary>

```text
sequenceDiagram
    participant A as 旧 leader A
    participant R as 候选者 A-r1
    participant S as Voter A-r2
    participant Q as 元数据 controller
    Note over A,S: A 失联，A-r1 日志满足选举条件，预投票通过
    R->>R: 新 term，持久化自投票
    R->>S: Vote，携带 term 和日志末端
    S-->>R: 持久化选票，同意
    R->>R: 得到 2/3，追加本任期控制批次
    S->>R: Fetch
    R-->>S: 本任期批次
    S->>R: 持久化后报告新 LEO
    R->>R: 建立本任期 HW，恢复 KV
    Q->>R: DescribeQuorum，查询组 10
    R-->>Q: 返回组 10 的 leader 与 term
    Note over Q: 发布已观察到的路由
    A->>R: 重连，按新 term 对齐并追赶
```

</details>

<details>
<summary>展开原图 · REPLTAKEOVER 与旧主排空</summary>

```mermaid
sequenceDiagram
    participant R as Replica (requesting takeover)
    participant M as Master
    participant O as Other replicas

    R->>M: DFLY TAKEOVER seconds [SAVE]
    M->>M: flip global state ACTIVE -> TAKEN_OVER
    M->>M: drain in-flight dispatches, disable expiration
    M->>R: PING (force ACK)
    M->>M: wait until R's last_acked_lsn == current LSN
    M-->>R: +OK
    M-)O: best-effort wait for other replicas to catch up (no forced PING)
    M->>M: optional SAVE, then shut down / reconcile cluster slots
    R->>R: StartJournalAtOwnLSN, become master
```

</details>

## 6. KV 执行、快照与客户端故障语义

`yes` 由各引擎线程记录已执行的变更并独立分配 LSN。普通 SET 由 `SetCmd::RecordJournal` 显式写入 journal，副本跟随指定的主节点。`raft` 的数据组 leader 将 SET 的效果追加到本组日志。各副本按组内 offset 应用已提交前缀，leader 在本地 KV 应用后回复客户端。[SET 的 journal 入口](../../src/server/string_family.cc) · [主从复制管理](../../src/server/replica_manager.cc) · [Raft 数据节点](../../src/server/cluster/raft/runtime/data_node.cc)

| 接入点 | 当前数据路径 |
| --- | --- |
| 分区日志 | 每个数据组在自己的分区目录保存批次。leader 和 follower 在持久化后推进各自的 LEO，用 epoch 和 offset 查找共同前缀，发现分歧时截断未提交的冲突尾部。[分区日志](../../src/server/cluster/raft/log/partition_log.h) |
| 写入与条件求值 | leader 在追加前检查 slot 归属、generation 和本组角色。普通 SET 产生替换记录。NX/XX 等待此前追加的记录全部应用，再求值并记录替换或空操作。条件不满足时也等这次决定提交和应用后才返回 nil。[写入入口](../../src/server/cluster/raft/runtime/data_node.cc) |
| 确定性重放 | 数据批次携带 group ID、DB ID、slot、generation、键和值。副本按提交顺序应用相同效果。迁槽也以本组记录保存冻结、分块、安装和完成进度，切主或重启后可继续执行。[效果回放](../../src/server/cluster/raft/data/set_effect.cc) · [迁槽记录](../../src/server/cluster/raft/data/slot_transfer_state.cc) |
| 读取与 WAIT | GET 由已追上本组 HW 的当前 leader 提供。WAIT 按该连接记录的写入位置，统计本组副本在当前 epoch 报告的已应用位置。[副本应用进度](../../src/server/cluster/raft/data/apply_progress.cc) · [集成测试](../../tests/integration/raft_data_group_test.py) |

**SET 编码的取舍。** 当前数据组保留 Kafka 日志批内的 `DFKV v0` 载荷，编码器的优化方向是一次预留、直接写入最终批。Raft 已复用旧版的 `JournalExecutor` 来执行提交后的普通 SET。保留现有日志字节就能减少搬运，也能继续用同一解码器回放已有 WAL。[SET 执行](../../src/server/cluster/raft/data/set_executor.cc) · [复制与应用路径分析](profile/v5-batched-apply.md#stack-review)

旧版 journal 先用 `SELECT` 指定 DB，再用 `COMMAND` 记录事务 ID 和已经求值的 SET 命令。独立验证把这些字节装入 Kafka 数据批，取出后用原版 `JournalReader` 正确读回了 DB 0 和 DB 11 的空值及 256 KiB 二进制值。条件未满足时，旧版跳过这条 SET，Raft 则提交一个占用 offset 的空操作。旧 journal 也没有记录数据组、slot 和归属代际，Raft 回放时要用这些信息阻止旧代际的写入。将来扩展到 DEL、带绝对过期时间的 SET 等写入时，共用 journal 命令体可以省去逐条定义参数编码的工作，届时仍需保留 Raft 的归属校验、空操作和旧 WAL 解码。[Journal 编解码](../../src/server/journal/serializer.cc) · [组归属与代际校验](../../src/server/cluster/raft/data/set_effect.cc) · [条件失败后的重启回放](../../src/server/cluster/raft/data/set_effect_test.cc)

**换主后的条件写。** 假设 A 已将一次 `SET k v NX` 复制给 A-r1，客户端收到回复前 A 宕机。A-r1 当选时，继承的日志可能领先于本地 KV。新 leader 在前序记录按新任期的 HW 提交并应用后，才对下一次 NX/XX 求值。失去本组多数时，节点不能确认新的条件写。[条件写实现](../../src/server/cluster/raft/runtime/data_node.cc)

**客户端故障语义。** 追加前被拒的请求尚未写入。追加成功后遇到超时、换主或丢失回复，客户端看到的结果未知。重试 NX 可能返回 nil，此时客户端无法仅凭该回复判断是哪次请求写入了值。GET 检查本地归属、角色及已应用的 HW，尚无逐请求的 quorum 读屏障。[命令分派](../../src/server/main_service.cc) · [数据组读写](../../src/server/cluster/raft/runtime/data_node.cc)

后续若扩展 TTL、淘汰、事务和脚本，日志还需记录绝对过期时间、已决定的变更和原子批次边界，使副本重放得到相同结果。这些命令不在当前数据组的读写验收范围内。

## 7. 组内调度与日志恢复

### 组内唤醒与上游实现

有效的空 Fetch 可以在普通数据完整写入后提前返回。其他挂起的 Fetch 在组内日志位置、提交位置（HW）或领导权变化时重新检查。follower 落盘后立即用下一次 Fetch 报告已持久化的日志末端（LEO），取得快照分块后也立即续拉。每个 peer 同时只发一条请求，错误或异常的无进展响应仍会退避。SET 等待本组提交事件，唤醒后先应用已提交前缀，再校验原写入的 epoch 和 offset。并发条件写在前序效果应用后求值，8 个独立连接同时对一个键执行 SET NX，只有一个成功。[组内调度](../../src/server/cluster/raft/runtime/group.cc) · [Fetch](../../src/server/cluster/raft/quorum/fetch_replication.cc) · [条件写回归](../../tests/integration/raft_data_group_test.py)

etcd/raft v3.6.0 在落盘后释放 follower 的追加确认。它的 leader 通过有界在途窗口发送后续日志，收到确认才推进副本的 `Match`。[etcd 落盘后确认](https://github.com/etcd-io/raft/blob/f35a02212416d045c574e30aa59d4b07c4b8d732/raft.go#L544-L575) · [在途窗口](https://github.com/etcd-io/raft/blob/f35a02212416d045c574e30aa59d4b07c4b8d732/tracker/inflights.go#L42-L46)

### 日志恢复标记

数据组和控制器的分区日志都通过 `dragonfly-log-end` 记录本地持久化边界。DFE4 在文件开头的 44 字节内保存带 CRC32C 的基准记录，包含快照起点、日志段编号、已验证的字节末端和对应的排他 offset。同一段内最多可从这个基准向前发布 4095 个 offset。稀疏文件的逻辑长度编码已发布的增量，并带有能发现单比特长度错误的校验。[磁盘格式](../../src/server/cluster/raft/log/log_files.cc)

在这个窗口内，节点先写全并同步日志段，再用 `ftruncate` 更新标记长度并同步标记文件，随后才公布新的本地 LEO。换段、超出窗口、截断或改变快照保留边界时，节点写入并同步新的基准文件，原子替换旧文件，再同步目录。[追加与截断](../../src/server/cluster/raft/log/partition_log.cc) · [快照处理](../../src/server/cluster/raft/log/partition_snapshot.cc)

如果进程在更新标记长度后、同步返回前退出，重启进程可能从操作系统缓存中读到新的长度。启动时先同步 DFE3 或 DFE4 的标记文件，再同步目录，确保持久化了用于恢复的文件和选择。随后核对基准字节位置与 offset，逐批校验到所选边界，成功后才向 quorum 报告本地进度。已发布范围出现短写、坏 CRC 或断裂的批次会导致打开失败。选定边界之后的字节会被截掉，即使它们恰好构成有效日志批。新版本可以读取 DFE1、DFE2 和 DFE3，下一次发布边界时原子升级到 DFE4。已升级的分区目录需要支持 DFE4 的二进制才能重新打开。[边界恢复与升级回归](../../src/server/cluster/raft/log/partition_log_test.cc)

[回归与复现](todos.md#复现命令) · [Profile 实验索引](profile.md#experiments)
