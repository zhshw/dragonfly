# 未保留试验：follower 接收确认快路径（无收益，已回退）

[Profile 索引](../profile.md#p0-1b-receipt-experiment)

原采集使用 `v10` 试验代号；本次代码未保留。保留版本 v10 的 P1-6 实验见[后台同步与普通追加并行](v10-memory-flush-overlap.md)。

**结论：未证明端到端收益，实验代码已回退到 v9（`29565323`）。** 保留实验补丁、原始数据、指标和火焰图/trace，不扩展到 2、8 连接。

后续的 v14b 在批次事件循环中使用独立 ApplyLane 推进应用，并分别报告接收与应用水位。 [v14b 实现与验证](v14b-batch-progress.md)

P0-1b 本次尝试在同 leader、同任期的普通 SET 响应后准备下一笔 Fetch，先报告已发布接收位置，
再由原 worker 串行 apply；每个 peer 仍只有一笔 RPC 在途。控制记录、换主、截断和快照保留应用屏障，
WAIT 继续按 applied tag 确认。只尝试一步提前发送，未实现独立异步应用队列。

Ubuntu 同机未插桩 Folly Release，**memory ack、1 连接 / pipeline 64**，256 MiB、256 KiB/value，
64 笔预热、5 秒静置；三版本交错各三轮，计时包含 **SET + 原写连接 WAIT 1**，表中为中位数。
v9 本轮复测 **400.62 MiB/s**，历史 **389.95** 和原始基线 **169.56** 保留原含义。

| 版本 | 确认吞吐 MiB/s（范围） | 仅 SET 吞吐 MiB/s | 末尾 WAIT ms | 批次 P95 ms |
| --- | ---: | ---: | ---: | ---: |
| 改前（v9 复测）`694ef9835c9c…` | **400.62（367.54–401.43）** | 400.70 | 0.15 | **58.66** |
| 提前 Fetch，已回退 `ac4928b5cf92…` | 385.40（369.79–402.57） | 385.48 | 0.15 | 70.09 |
| 原 cluster，同一候选二进制 | 618.30（557.79–623.72） | 818.57 | 101.19 | 24.49 |

- **确认吞吐 −3.8%，批次 P95 +19.5%**。两版 WAIT 均约 0.15 ms，差异发生在 SET 阶段。
- 三轮确认吞吐 **401.43 / 400.62 / 367.54 → 385.40 / 369.79 / 402.57 MiB/s**，
  配对 **−4.0% / −7.7% / +9.5%**，范围重叠、方向不一致；不认定稳定收益，也不将中位数下降解释成固定回退幅度。

单次独立诊断，CPU 按 **500 Hz** 采样估算；旧/新 leader 为 node 11 / 12，以下按角色比较。
插桩二进制为 `6e382a9e0799…` / `b3bd0cfa487b…`，诊断结果不替代正式三轮 benchmark。

| 单次诊断指标 | 改前（v9 复测） | 提前 Fetch | 原 cluster |
| --- | ---: | ---: | ---: |
| 接收完成 → 请求写入 socket，累计 / P95 | 88.62 / 1.542 ms | **12.01 / 0.358 ms** | — |
| 接收完成 → leader HW，累计 | 134.38 ms | 73.33 ms | — |
| 数据组 Fetch 发送次数 | 200 | 256 | — |
| leader worker 排队区间并集 | 217.73 ms | 295.80 ms | — |
| leader CPU / 每 MiB CPU | 564 ms / 2.20 ms | 490 ms / 1.91 ms | 364 ms / 1.42 ms |
| follower CPU / 两节点 CPU 合计 | 258 / 822 ms | 252 / 742 ms | 140 / 504 ms |
| leader memcpy / memset / CRC CPU | 158 / 64 / 118 ms | 152 / 50 / 88 ms | 190 / 64 / 未采到 |
| leader 峰值 RSS | 430.79 MiB | 438.50 MiB | 652.36 MiB |
| follower 峰值 RSS | 410.44 MiB | 404.89 MiB | 373.17 MiB |

**局部等待缩短，整体收益没有兑现。** 按 offset 关联的含新数据响应 **133 → 140** 次，均覆盖 1024 笔；
接收至发送累计等待减少 **86.4%**，其中 apply 重叠 **63.82 → 0.51 ms**、fsync 重叠 **19.35 → 0.47 ms**。
同时 Fetch 次数增加 **28%**、leader worker 排队变长；应用和后续追加仍串行等待，未完成全路径解耦。
这些现象不能直接确定唯一瓶颈，重叠区间不能相加推算吞吐。单次 CPU 下降也不足以推翻正式对照；RSS 未显示整体改善。
`fetch.sent` 是完整请求写入 socket 的时间，并非网卡时间戳；候选版 `fetch.rpc` 包含发送后的 apply，不能当纯网络耗时。

**验证：** 候选版 Debug / Release / ASAN+UBSAN 各 **45 项**相关单测、Debug **14 项**和 sanitizer **9 项**集群回归通过，
覆盖 flush/memory、同 key、条件写、断连、反压、关闭、换主、恢复和快照。
**9 轮**正式测试和 **3 次**诊断均通过原连接 WAIT 1、离线副本恢复、WAIT 2 和全部 **1088** 个值读回。
回退后 Release 重建及 **44 项**基线单测通过，二进制 SHA 与本轮 v9 基线 `694ef9835c9c…` 完全一致。

| 报告 | 火焰图与时间线 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 改前（v9 复测） | [v10 before](http://172.31.23.2:3137/memory-before-c1-p64-v10/index.html) | [2386 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/3d0b38f4cc7f5061a84edd6daf495ce2) |
| 提前 Fetch（已回退） | [v10 receipt](http://172.31.23.2:3137/memory-receipt-c1-p64-v10/index.html) · [leader 火焰图](http://172.31.23.2:3137/memory-receipt-c1-p64-v10/leader-node-12.svg) | [2399 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/322a1a0dabd55ded8c24bdf9e8319874) |
| 原 cluster | [v10 cluster](http://172.31.23.2:3137/cluster-c1-p64-v10/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/94b9606ef13d5be0a187957f39105cb9) |

完整事件为 **31211 / 30692 / 2939** 条，无丢失；Langfuse 每阶段保留最长 50 条，统计使用全部事件。
数据与验证日志在本地及测试机 `docs/cluster/benchmarks/profile-receipt-20261123/`（Git 忽略），
包含 `compare-c1/`、`receipt-analysis.json`、`source-final.patch`、`experimental-src/`、源码/二进制哈希和复现脚本；
候选及诊断二进制仍保留在测试机。新增插桩代码也已回退，复现时在隔离的 v9 checkout 应用归档补丁。
