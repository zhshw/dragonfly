# v4：共享 WAL 缓冲与直接编码

[Profile 索引](../profile.md#wal-buffer-remaining) · [v3 基线](v3-baseline.md) · [v5 批量应用](v5-batched-apply.md) · [v6 直接聚合](v6-drain-sharing.md)

## 三轮未插桩结果

2026-11-23，沿用 v3 的机器和负载。未插桩 Folly Release，1 连接、pipeline 64、256 MiB、256 KiB/value，计时到原写连接 `WAIT 1` 成功。Raft 两版均显式使用 `memory`，与原 cluster 交错各跑三轮。表中为三轮中位数及吞吐范围。

| 版本 | 确认吞吐 MiB/s（范围） | 批次 P95 ms |
| --- | ---: | ---: |
| v3 本轮复测 `5f185e62b82d…` | 164.59（162.18–168.49） | 129.00 |
| 共享缓冲 + 直接编码 `b8362f34fc6e…` | **203.44（196.54–223.93）** | **83.38** |
| 原 cluster，同一新版二进制 | 607.46（605.95–626.61） | 35.51 |

- 同轮确认吞吐 **+23.6%**、批次 P95 **−35.4%**，新旧吞吐范围不重叠。原 cluster 的吞吐是本轮新版的 **2.99 倍**。v3 首次测得 169.56 MiB/s，本轮增量使用同版复测值 164.59 MiB/s。
- 单独共享缓冲的先行对照为 170.20 → 172.36 MiB/s（+1.3%），三轮范围重叠。直接编码和共享缓冲的组合收益以本轮对照为准。
- 聚合 WAL 通过 move 和不可变共享所有权交给 I/O。SET 的 DFKV 头、key、value 直接写入最终 batch，去掉中间 payload/body/records。两版使用相同的日志格式、CRC、提交和应用确认条件。effect 缓存副本、drain 聚合复制和逐笔应用仍存在。
- 9 轮均通过 `WAIT 1`、离线副本恢复、`WAIT 2` 和全部 1088 个值读回。Debug、Release 单元回归，57 项 ASAN/UBSAN 检查和 11 项集群回归通过。

复测时将 A/B 示例中的 `-baseline-build` 指向冻结的 v3 构建，`-output-dir` 可用 `docs/cluster/benchmarks/profile-buffer-c1`。正式原始数据和两阶段二进制在测试机 `docs/cluster/benchmarks/profile-buffer-20261123/`。 [本地 A/B 命令](../profile.md#local-benchmark)

## 上轮复采：复制下降，应用与 RSS 尚未改善（v4）

同一负载分别复采优化前、优化后及原 cluster，三轮均通过恢复和逐值读回。
旧诊断二进制为 `72f222abed27…`，新诊断二进制为 `9371d68cbce0…`。
按角色对照：旧 Raft leader 为 node 11，新 Raft leader 为 node 12，不能直接比较同编号节点。

| 单次诊断指标 | 优化前 raft.memory | 优化后 raft.memory | 原 cluster |
| --- | ---: | ---: | ---: |
| leader CPU 估计时间 | 1220 ms | 924 ms（−24.3%） | 396 ms |
| leader memcpy | 732 ms | 420 ms（−42.6%） | 212 ms |
| leader memset | 130 ms | 132 ms | 58 ms |
| leader CRC32::Extend | 58 ms | 100 ms | 未采到 |
| leader 编码区间并集 | 451.06 ms | 231.41 ms（−48.7%） | — |
| leader worker 排队区间并集 | 567.22 ms | 358.21 ms（−36.8%） | — |
| leader 应用区间并集 | 240.70 ms | 245.33 ms | — |
| follower 应用区间并集 | 376.59 ms | 388.88 ms | journal.apply 42.82 ms |
| leader 进程峰值 RSS | 431.63 MiB | 434.06 MiB | 656.84 MiB |
| follower 进程峰值 RSS | 394.25 MiB | 393.53 MiB | 373.64 MiB |

- 两版都是 176 轮编码、每节点 1024 次 journal 应用。编码和 memcpy 明显下降，支持此次优化的作用方向；
  单次采样中的 CRC 波动不代表吞吐收益，不同阶段也不能相加。
- memset、应用耗时及进程峰值 RSS 没有明显下降。**本轮确认的是复制工作减少，尚不能宣称整体内存占用下降。**
  RSS 包含 KV、运行时及 allocator 保留内存，峰值覆盖启动、预热和测量；原 cluster 的较高 RSS 也不能直接归因于某个缓冲。
- 该版 leader 仍有 358 ms worker 排队和 245 ms 应用，follower 应用约 389 ms；因此本轮优先处理 P0-2 的批量应用，
  并继续拆分应用内部等待。Raft 整体应用与原 cluster 的 journal.apply 边界不同，不能直接按两者比值推算收益。

| 报告 | 火焰图、时间线与内存 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 优化前 raft.memory | [v4 baseline](http://172.31.23.2:3137/memory-baseline-c1-p64-v4/index.html) | [1626 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/5a837d3e34025f6d8d819db22ad17921) |
| 优化后 raft.memory | [v4 encoded](http://172.31.23.2:3137/memory-encoded-c1-p64-v4/index.html) | [1604 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/00c7a5bb2afc56628e8dff3542abdef1) |
| 原 cluster | [v4 cluster](http://172.31.23.2:3137/cluster-c1-p64-v4/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/4ab7ec022a93519f896c78824daea85e) |

完整事件分别为 23357 / 22135 / 2942 条，无丢失；Langfuse 展示各阶段最长 50 条，报告统计使用全部事件。
原始结果保存在本地和测试机 `docs/cluster/benchmarks/profile-buffer-20261123/`，已由 Git 忽略。
