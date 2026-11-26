# RAFT-001：Follower 接受 READONLY 后，GET 仍返回 MOVED

## 1. 状态与影响

- 状态：**已修复，回归通过**。
- 复现时间：2026-11-26，ubuntu-bj。
- 模式：`cluster_mode=raft`、`raft_data_ack_mode=memory`。
- 代码归档：`393544f2`；复现使用 v16 二进制，SHA256 为 `69d1b9333bfdc541e38c05fd956be7e0da256ca5b03fed0948028bef157c6b08`。

Follower 已应用到 Leader 的末尾位置，本地 key 数也符合预期；但同一连接发送 `READONLY` 收到 `OK` 后，`GET` 仍被重定向到 Leader。客户端无法按已接受的只读连接语义读取 Follower 本地数据，也无法通过 GET 主动核验其复制内容。

本 case 的触发条件包含 **READONLY 已成功返回**，不能只凭普通连接的 GET 重定向判断故障。

## 2. 复现过程

本次使用与 v16 相同的二进制启动独立校验集群，data group 10 选举 node 12 为 Leader；node 11 保持在线，node 13 在写入期间停止，随后恢复。未将 Follower 提升为 Leader。

1. 写入 1,024 条正式数据、64 条预热数据及 2 个标记 key，正式数据为 256 MiB、pipeline 64。
2. 直接连接各节点，查询 `INFO replication`；确认 Follower 的 `slave_repl_offset` 等于 Leader 的 `master_repl_offset`。
3. 在 Follower 查询 `ROLE` 和 `INFO keyspace`，确认角色、连接状态和本地 key 数。
4. 在同一条 Follower 连接上执行以下命令；客户端不自动跟随重定向：

```text
READONLY
+OK
GET {12561}:0
-MOVED 42 127.0.0.1:45879
```

使用 `redis-cli` 重查时，应进入 Follower 的交互连接，再依次执行 `READONLY` 和 `GET`。不要使用 `-c`，也不要拆成两次独立的 `redis-cli` 调用。

## 3. 修复前结果

| 节点 | 角色 | 已应用 offset | 本地 keys | 状态 |
| --- | --- | ---: | ---: | --- |
| 12 | Leader | 1094 | 未查询 | 两个 Follower 均在线、lag=0 |
| 11 | Follower | 1094 | 1090 | online，master_link_status=up |
| 13 | Follower，停机后恢复 | 1094 | 1090 | online，master_link_status=up |

node 11 与 node 13 的 `READONLY` 均返回 `OK`，随后 `GET {12561}:0` 均返回上述 `MOVED`。本地 key 数为 1,024 + 64 + 2；offset 与 key 数说明复制进度已追平，**本次没有完成 Follower 值内容的逐字节读取校验**。

[原始查询结果](evidence/RAFT-001-active-follower-query.json) 包含各节点直接返回的 ROLE、replication、keyspace 和 GET 响应。证据中的端口为本次临时集群端口，校验结束后集群已清理；不是长期服务地址。

## 4. 修复前的代码原因

1. [Service::DispatchRaftCommand](../../src/server/main_service.cc) 对 `READONLY` / `READWRITE` 直接返回 `OK`，没有记录连接读取模式，也没有把该模式传给 GET。
2. [DataNodeImpl::Route](../../src/server/cluster/raft/runtime/data_node.cc) 要求当前节点等于路由的 Leader；Follower 在这里直接返回 `MOVED`。
3. 同文件的 `DataNodeImpl::Get` 还要求本地角色为 `kLeader`。只修改 Route 的重定向判断仍不足以支持 Follower 读取。

因此，原始失败发生在读取模式与路由检查阶段，不能把该响应解释为 Follower 没有同步数据。

## 5. 修复验收

- 同一连接执行 `READONLY` 后，已追平且持有目标数据组的 Follower 能返回本地值；保持 Follower 身份，客户端不跟随 MOVED。
- 在线 Follower 和停机恢复后的 Follower 均执行逐 key、逐字节核验。
- `READWRITE` 恢复默认路由；读取模式按连接隔离，不影响写命令和其他连接。
- 错误数据组、slot 迁移、节点失效等原有路由与隔离检查仍然生效。
- 增加覆盖真实 Follower 的回归测试，再更新本记录的状态与修复提交。

## 6. 修复与回归结果

连接保存 `READONLY` 状态，GET 通过 `DataReadMode` 将读取模式传入 Raft；`READWRITE` 和 `RESET` 清除该状态。只有属于目标数据组、处于 Follower 角色并已从 Leader Fetch 的本地副本可以接受只读请求，仍检查本地已应用进度、broker incarnation、fencing 和 slot 迁移状态。写入继续使用 Leader 路由。

`READONLY` 允许读取本地已应用的已提交数据，可能落后于 Leader，不提供最新值保证。节点重启时，数据追平可能早于控制器完成新 incarnation 注册；回归测试先等待本节点 `CLUSTER SLOTS` 可用和 applied offset 追平，再直接查询 GET，不通过重试 GET 隐藏读取错误。

### 6.1 单元测试先复现再修复

新增 [DataDispatchTest.ReadOnlyAllowsFollowerReadsOnlyOnThisConnectionUntilReadWriteOrReset](../../src/server/cluster/raft/data/data_dispatch_test.cc)：修复前 4 处断言失败，已有 key 和不存在的 key 在 READONLY 后均返回 MOVED；修复后通过。

测试覆盖二进制值、缺失 key、连接隔离、READWRITE、RESET、Follower 写入重定向和 Leader 写入。同时在已有异步 SET 测试中加入 READONLY / READWRITE 顺序屏障检查。

```bash
ninja -C build-dbg dragonfly data_dispatch_test dragonfly_connection_test
./build-dbg/data_dispatch_test
./build-dbg/dragonfly_connection_test --gtest_filter='RaftAdmissionConnectionTest.*'
```

### 6.2 真实 Follower 读取与恢复

新增 [RaftFollowerReadTest](../../tests/integration/raft_data_group_test.py)，数据组 10 配置三个 voter，从实际选举结果确定 Leader。写入 32 个 32 KiB 二进制值及 DB 1 数据，直接读取两个 Follower；随后停止其中一个 Follower，在其离线期间更新全部值，重启后再次逐 key、逐字节验证。每轮查询前后均检查角色，客户端固定连接且不自动跟随 MOVED。

同一测试在修复前的二进制上，于 READONLY 后第一个 GET 复现 MOVED；修复后在线副本 offset 为 34，更新及恢复后为 66，四轮各 32 个值全部匹配。跨数据组 GET、普通连接 GET、READWRITE / RESET 后 GET 以及 Follower 的普通和条件 SET 仍重定向。

```bash
python3 -m unittest tests.integration.raft_data_group_test.RaftFollowerReadTest
RAFT_TEST_DATA_FLAGS=--raft_data_ack_mode=memory \
  python3 -m unittest tests.integration.raft_data_group_test.RaftFollowerReadTest
```

### 6.3 验证汇总

2026-11-26 在 ubuntu-bj 完成，客户端 I/O 为 io_uring：

| 检查 | 结果 |
| --- | --- |
| Debug / Release 构建 dragonfly、data_dispatch_test | 通过 |
| data_dispatch_test | Debug、Release 各 14/14 通过 |
| RaftAdmissionConnectionTest | Debug 1/1 通过 |
| Follower 读取与恢复 | Debug flush、Debug memory、Release memory 均通过 |
| ControllerAdminTest.test_submit_query_and_recover_management_request | 通过；增加冻结 slot 的 Follower 读取拒绝与迁移完成后目标 Follower 值校验 |
| pre-commit、git diff --check | 通过 |

Release 构建有现有 Folly/glog 模板的 signedness 警告；本次未运行 ASAN/UBSAN 或全仓测试。验证使用独立临时集群，未替换 v16 实验归档。
