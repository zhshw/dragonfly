# raft.memory 火焰图与 trace

本页保留优化前 v3 诊断。后续 v5 实现共享 WAL 缓冲与直接编码基础上的批量应用和参数借用，`raft.memory` 从 v3 的 **169.56** 提高到 **224.69 MiB/s（+32.5%，同口径跨轮比较）**，较上一版的 **206.56 MiB/s** 提高 **8.8%**。 [v5 实验](profile/v5-batched-apply.md) · [Profile 实验索引](profile.md#experiments)

**仍有阻塞，优先处理 worker 内的数据复制、编码与应用路径。**
memory 取消逐轮持久化确认后，未插桩吞吐只提高 4.4%。此次采样将剩余成本定位到具体阶段，
还没有通过单变量优化实验确认每项能带来多少提升。

- [raft.memory 报告与火焰图](http://172.31.23.2:3137/memory-c1-p64-v3/index.html)
- [原 cluster 报告与火焰图](http://172.31.23.2:3137/cluster-c1-p64-v3/index.html)
- [raft.memory Langfuse trace](http://172.31.23.2:3007/project/dragonfly-performance/traces/7833a94e00d55cfabb8ab8758766240c)
- [原 cluster Langfuse trace](http://172.31.23.2:3007/project/dragonfly-performance/traces/ed28fa05c47550fcbf81e9fb9bf592bc)

服务监听 `0.0.0.0:3007` / `0.0.0.0:3137`；部署、登录和复现方法见
[工具说明](../../tools/raft-observability/README.md)。持久数据在 `/data`，原 NTFS 和已有文件保留。

## 负载与证据

2026-11-23，`172.31.23.2`，Folly Release 的独立插桩版本，同一二进制运行两种模式。
**1 连接、pipeline 64**，256 MiB、1024 个不同 key、256 KiB/value，预热 64 笔、静置 5 秒。
三副本停掉一台，计时到原写连接 `WAIT 1` 完成；两种模式都通过离线副本恢复、`WAIT 2` 和 1088 个值读回。

| 本次诊断 | raft.memory | 原 cluster |
| --- | ---: | ---: |
| SET + WAIT 1 总时间 | 1512.14 ms | 415.31 ms |
| 确认吞吐 | 169.30 MiB/s | 616.41 MiB/s |
| 最后 SET 回复至 WAIT 完成 | 101.58 ms | 101.26 ms |
| leader CPU 样本 / 对应 CPU 时间 | 628 / 1256 ms | 191 / 382 ms |
| follower CPU 样本 | 253 | 70 |

这些是插桩诊断值，不替代 [三轮未插桩结果](replication-benchmark.md)。CPU 500 Hz，
仅预热后至最终 WAIT 采样；span 严格裁剪到客户端计时窗口，完整事件没有丢失。
数据组与元数据组分别标记，不能把元数据组的空闲长轮询算作数据复制瓶颈。

## 火焰图：复制路径消耗的 CPU 明显更多

| leader CPU 热点 | raft.memory | 原 cluster |
| --- | ---: | ---: |
| memcpy | 58.28%（约 732 ms） | 60.73%（约 232 ms） |
| memset | 10.51%（约 132 ms） | 15.71%（约 60 ms） |
| CRC32::Extend | 7.17%（约 90 ms） | 未采到该热点 |

- 两边都以复制为主，但 Raft leader 总 CPU 约为原 cluster 的 **3.29 倍**，不能只比较百分比。
- `AppendSetBatch` 调用栈累计占 leader 样本 **36.0%**，其内 `MakeSetEffectBatch` 占 **23.1%**；
  与 trace 的编码阶段相符。减少 effect、日志批和聚合缓冲之间的重复复制值得优先验证。
- follower 的 CRC 占 **15.4%**，复制与清零合计 **43.1%**；应用路径也需要继续拆分。
- CRC 有成本，但单独替换 CRC 无法消除 worker 内的串行工作与等待。

## Trace：等待尚未消除

下表为同一阶段重叠区间的并集，不是把所有请求耗时相加；**不同阶段仍有重叠，不能相加**。

| raft.memory 阶段 | leader | follower | 含义 |
| --- | ---: | ---: | --- |
| worker 排队 | 588.35 ms | 51.23 ms | 入队至 worker 开始执行 |
| 等待在途 I/O 排空 | 405.20 ms | 101.87 ms | 含 I/O 完成后的 worker 调度，不是纯设备时间 |
| SET 编码与聚合 | 474.96 ms | — | 176 轮，运行在组 worker 内 |
| 提交后应用 | 237.23 ms | 368.79 ms | 含日志/effect 处理和跨线程应用 |
| Folly WRITE | 50.50 ms | — | leader 176 次异步写，提交至 CQE |
| Folly FSYNC | 137.64 ms | 118.87 ms | 两端各 24 次后台同步，提交至 CQE |
| Folly EventBase 排队 | 2.57 ms | 0.38 ms | I/O 库接收队列本身占比较小 |

- **阻塞仍存在**：`RunOnWorker` 在 `AppendPending()` 时等待；编码和应用也占用同一个组 worker。
  单连接已能提前提交，不等于后续提交、确认和应用全部并行。
- leader 1024 笔 SET 的“等追加”和“等应用”区间并集分别为 **879 / 952 ms**，二者彼此重叠。
  原 cluster follower 的 journal 应用区间并集约 **47 ms**；两条路径的整体应用阶段不等价，不能直接将差值全归为线程切换。
- **未观察到数据 follower 在 Fetch 后进入 25 ms 退避**。出现的 25 ms 空闲 peer 睡眠和元数据 100 ms 长轮询不能作为此次慢的原因。
- 两种模式最后 WAIT 都约 **101 ms**；差距已经出现在 SET 回复前，单改最终 WAIT 无法解决主要差距。
- `raft.memory` 仍写 page cache 并后台刷盘；本轮 FSYNC 及排空等待说明刷盘影响仍在，只是不足以解释全部差距。

## v3 诊断提出的优化方向

1. 减少 `AppendSetBatch → MakeSetEffectBatch → 日志聚合` 的重复复制，缩短 worker 占用；保持不可变缓冲生命周期和日志字节完全一致。 [v6 直接聚合](profile/v6-drain-sharing.md)
2. 拆分应用阶段的日志读取、解码、跨线程等待和实际 KV 写入，评估批量应用能否减少逐笔往返。 [v5 批量应用](profile/v5-batched-apply.md)
3. 再评估能否缩小在途 I/O 阻塞的操作范围；换主、截断、快照和恢复标记仍须保持顺序。 [v10 后台同步与普通追加并行](profile/v10-memory-flush-overlap.md)

每次只改一个变量，先复测 1 连接 pipeline 64，不扩展连接数。
完整 CSV、CPU profile、Perfetto JSON 保存在隔离仓库 `docs/cluster/benchmarks/observability-20261123/`，
由 `.gitignore` 排除。Langfuse 展示每节点、阶段最长 50 个 span，附总数；完整统计来自全部事件。
