# v3：初始基线与诊断

[Profile 索引](../profile.md#experiments)

<a id="ack-modes"></a>

## raft.flush、raft.memory 与原 cluster 正式结果

2026-11-23 在 `172.31.23.2`（Ubuntu 26.04、i7-13700KF、NVMe ext4）运行同一未插桩 Folly Release 二进制 `5f185e62b82d…`。1 连接、pipeline 64、256 MiB、256 KiB/value，使用不同 key，预热 64 笔并静置 5 秒。三副本停一台，计时从首笔 SET 发出至原写连接 `WAIT 1` 成功。9 轮均通过离线副本恢复、`WAIT 2` 和全部 1088 个值读回。表中为三轮中位数。

| 模式 | 确认吞吐 MiB/s | 批次 P95 ms |
| --- | ---: | ---: |
| raft.flush | 162.38 | 114.15 |
| raft.memory | 169.56 | 113.75 |
| 原 cluster | 628.87 | 22.89 |

`raft.memory` 确认吞吐较 `raft.flush` 提高 **4.4%**，原 cluster 为 `raft.memory` 的 **3.71 倍**。`flush` 默认要求多数持久化，`memory` 改为多数写入 page cache 后确认并后台刷盘。逐轮刷盘只解释了当时的一小部分差距。独立采样中 leader 的后台 fsync 区间并集约 138 ms，`memcpy/memset` 占 leader CPU 样本的 68.8%。当时先在 1 连接 / pipeline 64 负载下验证减少复制与批量应用。 [确认模式语义](../io_uring.md#当前实现默认开启) · [v4 共享 WAL 缓冲](v4-wal-buffer.md) · [v5 批量应用](v5-batched-apply.md)

用同一份 Release 二进制复测三个模式。原始结果位于测试机隔离仓库的 `docs/cluster/benchmarks/ack-modes-20261123-c1/`，由 Git 忽略。

```sh
mkdir -p "$PWD/docs/cluster/benchmarks"
profile benchmark \
  -build-dir build-opt -ack-modes flush,memory -include-cluster \
  -clients 1 -pipeline-depth 64 \
  -output-dir "$PWD/docs/cluster/benchmarks/ack-modes-c1"
```

## 火焰图与 Trace 诊断

以下采用 2026-11-23 的 `memory-c1-p64-v3` / `cluster-c1-p64-v3` 初始采样，作为选择优化项的依据。
同机、同一插桩二进制（SHA-256 前缀 `72f222abed27`）、1 连接、pipeline 64、256 MiB、256 KiB/value；
三副本停一台，计时包含原写连接 `WAIT 1`。恢复、`WAIT 2` 和 1088 个值读回均通过。

| 指标 | raft.memory | 原 cluster |
| --- | ---: | ---: |
| 插桩 SET + WAIT 1 | 1512.14 ms | 415.31 ms |
| leader CPU 样本 / 估计 CPU 时间 | 628 / 1256 ms | 191 / 382 ms |
| leader memcpy | 366 样本 / 732 ms | 116 样本 / 232 ms |
| leader memset | 66 样本 / 132 ms | 30 样本 / 60 ms |
| leader CRC32::Extend | 45 样本 / 90 ms | 未采到 |
| follower CPU 样本 / 估计 CPU 时间 | 253 / 506 ms | 70 / 140 ms |

- Raft leader 总 CPU 为 **3.29 倍**；`memcpy + memset` 的绝对差约 **572 ms**，占 CPU 差值的约 **65%**。
  两边 memcpy 百分比都接近 60%，只比较百分比会掩盖差距。
- `AppendSetBatch` 累计占 leader 样本 **36.0%**，其中 `MakeSetEffectBatch` 为 **23.1%**。
  父子栈包含关系不能相加，也不能把这些 CPU 比例直接换算为吞吐提升。
- trace 区间并集：leader 编码 **475 ms**、worker 排队 **588 ms**、应用 **237 ms**；follower 应用 **369 ms**。
  应用内部的 `journal.apply` 分别为 **111 / 98 ms**，原 cluster follower 为 **47 ms**。
  外层还包含日志读取、解码和跨线程等待；各阶段重叠，差值不能直接认定为某一种开销。
- 初始 1024 笔 SET 对应 leader **5106 次** worker 访问、**2650 次**等待 I/O 排空；
  次数也包含复制等操作，不能全部归因于 SET。
- CPU 采样为 500 Hz，时间是样本估计，包含多线程；火焰图没有测量分配字节、峰值 RSS 或实际内存带宽。
  “复制多”已有证据，“内存占用降低多少”仍需补测。
- `memory` 仍写 page cache、后台同步；每笔 SET 仍需多数接收并完成本地应用。原 cluster 的 SET 先回复本地执行结果，
  再由末尾 WAIT 确认副本应用。总计时包含副本确认，但两者逐笔确认和持久性语义仍有区别。

平台已关联两组历史采集：[火焰图对照](http://127.0.0.1:8787/#/v3/flamegraphs) · [Trace 对照](http://127.0.0.1:8787/#/v3/trace)。v3 没有上一轮，使用已选的 `v3:cluster` 作为诊断对照；原 cluster 的 2920 条事件、221 条 Langfuse 展示样本和双节点火焰图均来自当时的采集。

原始证据入口：[Raft 火焰图](http://172.31.23.2:3137/memory-c1-p64-v3/index.html)、
[原 cluster 火焰图](http://172.31.23.2:3137/cluster-c1-p64-v3/index.html)、
[trace 结论及链接](../replication-observability.md)。
原始 `*.folded`、`stages.csv` 和 `summary.json` 位于 `docs/cluster/benchmarks/observability-20261123/`，由 Git 忽略。
