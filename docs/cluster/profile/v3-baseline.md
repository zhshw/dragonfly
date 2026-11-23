# v3：初始基线与诊断

[返回总览与优化项](../profile.md) · [benchmark](../replication-benchmark.md)

以下采用 2026-11-23 的 `memory-c1-p64-v3` / `cluster-c1-p64-v3` 初始采样，作为选择优化项的依据。
同机、同一插桩二进制（SHA-256 前缀 `72f222abed27`）、1 连接、pipeline 64、256 MiB、256 KiB/value；
三副本停一台，计时包含原写连接 `WAIT 1`。恢复、`WAIT 2` 和 1088 个值读回均通过。

| 指标 | raft.memory | 原 cluster |
| --- | ---: | ---: |
| 未插桩三轮确认吞吐中位数 | 169.56 MiB/s | 628.87 MiB/s |
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

证据入口：[Raft 火焰图](http://172.31.23.2:3137/memory-c1-p64-v3/index.html)、
[原 cluster 火焰图](http://172.31.23.2:3137/cluster-c1-p64-v3/index.html)、
[trace 结论及链接](../replication-observability.md)、[未插桩 benchmark](../replication-benchmark.md)。
原始 `*.folded`、`stages.csv` 和 `summary.json` 位于 `docs/cluster/benchmarks/observability-20261123/`，由 Git 忽略。
