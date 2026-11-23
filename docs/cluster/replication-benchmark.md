# Raft 复制优化与 benchmark

最明显的收益来自同连接 SET 提前提交；最新段边界与任期保护改动收益有限。
以下均为三轮中位数，各项使用不同阶段的基线，提升比例不能相加。

<a id="本轮同连接-set-已能提前提交"></a>

## 做了哪些优化、提升多少

- **SET 提交与确认解耦**：普通 SET 可按序提前入队，仍等多数落盘及应用后回复。单连接 pipeline 64 的确认吞吐 **81.48 → 163.75 MiB/s（+101.0%）**，批次 P95 降低 **49.4%**；逐笔等回复的负载反而下降 **9.5%**。
- **异步 WAL 与 Fetch 重叠**：leader 刷盘期间允许 follower 提前取日志。已实现并通过故障回归，性能收益未单独量化。
- **并发 drain 合批**：收集在途 I/O 期间已就绪的写，共享下一轮刷盘。历史 Helio 插桩、8 连接下，相对原合批的 SET 吞吐 **170.67 → 180.89 MiB/s（+6.0%）**；每千笔的段同步、标记同步各减少约 **9.1%**。
- **Raft 文件 I/O 接入 Folly**：提交、完成及重试使用 Folly EventBase。相同 liburing 2.15 下，1 / 8 连接 SET 吞吐分别 **+8.2% / −2.2%**，未显示稳定整体优势。
- **自适应 1/4 MiB drain + 4 MiB 数据 Fetch**：相对固定 1/1 MiB，1 / 8 连接 SET 吞吐分别 **+3.3% / −4.4%**，有回退，不能认为大批次普遍更快。
- **按当前段容量截取前缀、绑定入队任期**：保持 FIFO，拒绝旧任期排队写。最新 1 / 2 / 8 连接确认吞吐分别 **+4.2% / −0.3% / +2.4%**；同时默认开启异步 I/O 和 drain，但两版测试均显式开启，不把默认值变化计作收益。
- **CRC 指令优化（独立试验，未合入）**：1 / 8 连接 SET 吞吐分别 **+8.5% / +4.3%**，收益有限，也没有解决提交阻塞。

历史 SET 吞吐只计到 SET 回复；“确认吞吐”还计入批末 WAIT。Folly 对照为 64 MiB、
自适应和 CRC 对照为 256 MiB，均每连接一笔在途；不能与 pipeline 结果直接混比。
实现、故障边界和历史测试细节见 [io_uring 文档](io_uring.md)。

<a id="latest-pipeline"></a>

## 最新对比：1、2、8 连接

2026-11-23，`172.31.23.2`，Ubuntu 26.04 / i7-13700KF / NVMe ext4，Folly Release。
每轮 256 MiB、256 KiB/value、不同 key、64 笔预热、5 秒静置；每连接发送 64 笔再收齐回复。
三副本停掉一台，**从首笔 SET 发出计到所有原写连接的 `WAIT 1` 成功**，单位 MiB/s。
改前版 `71080870a731…` 已支持提前提交；当前版 `5978ec01ba18…` 包含段边界与任期改动。
原 cluster 使用当前同一二进制。

| 连接数 | 改前 Raft | 当前 Raft | 变化 | 原 cluster |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 162.22 | 169.08 | +4.2% | 623.66 |
| 2 | 180.47 | 179.86 | −0.3% | 634.36 |
| 8 | 191.11 | 195.75 | +2.4% | 652.86 |

<a id="当前实现与后续试验"></a>
<a id="fetch应用与复制的剩余成本"></a>

## 结论与剩余问题

- **最新改动收益有限**：2 / 8 连接的三轮范围重叠；8 连接 SET 吞吐仅提高 0.2%，批次 P95 略升。WAIT 检查时机也影响确认吞吐。
- **原 cluster 仍明显更快**：其 WAIT 确认副本应用；Raft SET 还要求多数落盘，两者持久性保证不同。
- **正确性通过**：27 轮均通过各写连接 WAIT 1、离线副本重启后的 WAIT 2 和全部 1088 个值读回。
- **仍需定位等待**：票据容量、WAL 排空和 worker 等待应用尚未消除，下一步用 trace / 火焰图量化。最新测试未刻意跨越 1 GiB 段边界，也没有新刷盘计数，不能单独量化段截取收益。

## 复现与原始数据

保留改动前的 `dragonfly`、`raft_storage`，在真实磁盘目录运行；[Folly 构建方法](io_uring.md#folly-编译变体)。

```sh
ninja -C build-opt dragonfly raft_storage
for clients in 1 2 8; do
  python3 tests/integration/raft_commit_benchmark/dragonfly/pipeline_compare.py \
    --build-dir build-opt --baseline-build /path/to/previous-pipeline-build \
    --include-cluster --clients "$clients" --pipeline-depth 64 \
    --output-dir "$PWD/docs/cluster/benchmarks/replication-pipeline-c$clients"
done
```

原始数据仅保存在本地 `docs/cluster/benchmarks/` 及测试机，已由 `.gitignore` 排除；
64 连接样本和同步 Raft 性能数据已删除。最新日志位于测试机隔离仓库
`/home/zhangshenwang/workspace/db/dragonfly-io-uring-20261123/results-segment-drain-pipeline-20261123-c{1,2,8}/`。
