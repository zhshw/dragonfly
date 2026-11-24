# v10：memory 后台同步与普通追加并行

[返回总览与优化项](../profile.md#memory-flush-separation) · [benchmark](../replication-benchmark.md)

**P1-6：1 连接确认吞吐 +16.1%，批次 P95 −11.8%。** 三组吞吐均提高、范围不重叠，保留实现。
基线是保留的 v9；[提前 Fetch 试验](p0-1b-follower-receipt.md)无收益，仍保持回退。

- 追加与后台同步分开管理；同任期、同一段的普通 memory 写可在 fsync 期间继续编码、追加、复制和确认。
- 同步只发布启动时捕获的日志位置；新追加保留自己的字节和时间计数，任一 I/O 失败后持续拒绝写入。
- 保持 **64 MiB / 100 ms** 同步条件、队列预算和应用后回复语义；换主、控制记录、换段、截断、快照及关闭保留屏障，`flush` 模式不变。

沿用 v9 的单组负载：Ubuntu 同机未插桩 Folly Release，**memory、1 连接 / pipeline 64**，
256 MiB、256 KiB/value，64 笔预热、5 秒静置；改前 / 改后 / 原 cluster 交错各三轮，
计时包含 **SET + 原写连接 WAIT 1**，下表为中位数。本轮总结只采用此组数据，不扩展连接数结论。

| 版本 | 确认吞吐 MiB/s（范围） | 仅 SET MiB/s | 末尾 WAIT ms | 批次 P95 ms |
| --- | ---: | ---: | ---: | ---: |
| 改前（v9 本轮复测）`694ef9835c9c…` | 377.21（374.08–391.31） | 377.29 | 0.14 | 60.19 |
| P1-6 `b8a53a9fcbfa…` | **437.97（423.44–438.17）** | 438.07 | 0.16 | **53.06** |
| 原 cluster，同一候选二进制 | 605.47（461.14–623.90） | 794.39 | 101.25 | 34.63 |

三组确认吞吐配对 **+12.3% / +11.9% / +17.1%**；P95 第二组略升 **2.5%**，并非每轮都下降。
原始基线仍为 **169.56 MiB/s**；v9 历史 **389.95** 与本轮 **377.21** 是不同轮次，不能将复测差异归因于改动。

单次独立诊断，CPU 按 **500 Hz** 采样估算；按角色比较，不替代正式三轮结果。

| 诊断指标 | v9 本轮复测 | P1-6 | 原 cluster |
| --- | ---: | ---: | ---: |
| 本节点 fsync 在途时的新 leader 编码 / follower 追加 | 0 / 0 次 | **27 / 17 次** | — |
| follower fsync 与等待 I/O 重叠 | 111.85 ms | **26.87 ms** | — |
| 两节点 fsync 区间并集 | 133.76 ms | 137.29 ms | — |
| 窗口内 leader / follower fsync 次数 | 10 / 10 | 12 / 11 | — |
| leader worker 排队区间并集 | 204.39 ms | 280.15 ms | — |
| leader CPU / 每 MiB CPU | 552 / 2.16 ms | 486 / 1.90 ms | 400 / 1.56 ms |
| follower CPU / 两节点 CPU 合计 | 254 / 806 ms | 236 / 722 ms | 140 / 540 ms |
| leader / follower 峰值 RSS | 437.17 / 407.39 MiB | 434.98 / 412.95 MiB | 648.70 / 373.32 MiB |

**追加与同步已实际重叠，收益没有依靠减少同步次数。** fsync 是 Folly 发起至完成的经过时间，包含提交/完成处理；
窗口可能截断一次同步，操作次数不等于完整刷新轮数。leader worker 排队仍增加，内存占用未显示整体改善；
这次没有消除 memcpy/memset，也没有将应用改为独立异步队列。

**验证：** Debug / Release / ASAN+UBSAN 各 **86 项**相关单测，Debug 和 sanitizer 各 **14 项**集群回归通过。
覆盖并发追加/同步的完成顺序、短写、两类 I/O 失败、固定切点崩溃恢复、同步后新尾部的字节/时间计数，
以及 flush/memory、同 key、条件写、断连、反压、关闭、换主、恢复和快照。
本组 **9 轮**正式测试与 **3 次**诊断均通过原连接 WAIT 1、离线副本恢复、WAIT 2 和全部 **1088** 个值读回。

| 报告 | 火焰图与时间线 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 改前 v9 | [改前报告](http://172.31.23.2:3137/memory-before-c1-p64-v11/index.html) | [2171 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/831d87fef0925e799460273efbceec3e) |
| P1-6 | [P1-6 报告](http://172.31.23.2:3137/memory-overlap-c1-p64-v11/index.html) · [leader 火焰图](http://172.31.23.2:3137/memory-overlap-c1-p64-v11/leader-node-11.svg) | [2156 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/da2541414b0a573fa8d9ff6d1af41d39) |
| 原 cluster | [原 cluster 报告](http://172.31.23.2:3137/cluster-c1-p64-v11/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/034cbc264f3a51b39be42b09c17bfa71) |

完整事件 **29346 / 29818 / 2947** 条，无丢失；Langfuse 每阶段展示最长 50 条，统计使用全部事件。

数据、补丁、源码哈希、候选二进制和复现脚本在本地及测试机
`docs/cluster/benchmarks/profile-flush-overlap-20261124/`（Git 忽略）。

报告 URL 沿用采集时的 `v11` 标识，文档与保留版本统一记为 **v10**；额外连接数的原始样本仅归档，不纳入本页结论。
