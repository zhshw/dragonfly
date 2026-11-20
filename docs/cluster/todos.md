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

- [x] 请求已受理且迁移未完成时终止 active controller。新 active 可以查到相同操作 ID 和原文，并接续完成迁移。[管理请求恢复测试](../../tests/integration/raft_data_group_test.py)
- [x] 终止全部进程，保留七个节点的磁盘，删除引导文件后重新启动。检查槽 42 的归属、`finished` 状态和迁移后的新写入，确认槽 43 的数据仍由组 10 提供。[整群重启测试](../../tests/integration/raft_data_group_test.py)

本轮用原生引导格式在 Linux Debug 上跑完了 controller 和数据组两个集成测试文件，共 12 项通过。

## 复现命令

在 Linux 的仓库根目录执行。测试需要 `redis-cli` 和 Python 的 `redis` 包，支持的包版本见[测试依赖](../../tests/dragonfly/requirements.txt)。多进程用例自行分配本地端口，并用与 `dragonfly` 同目录的 `raft_storage` 格式化临时节点目录。

```sh
./helio/blaze.sh -DWITH_GCP=OFF
ninja -C build-dbg dragonfly raft_storage bootstrap_topology_test admin_command_test slot_migration_test
ctest --test-dir build-dbg --output-on-failure -R '^(bootstrap_topology|admin_command|slot_migration)_test$'
DRAGONFLY_PATH=build-dbg/dragonfly python3 tests/integration/raft_data_group_test.py -v \
  RaftDataGroupTest.test_two_voters_per_group \
  RaftDataGroupTest.test_recover_confirmed_values_without_bootstrap_after_cluster_crash \
  ControllerAdminTest.test_submit_query_and_recover_management_request
```
