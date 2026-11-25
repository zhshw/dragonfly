# v9：SET 输入所有权转交

[Profile 索引](../profile.md) · [benchmark](../replication-benchmark.md)

<a id="input-ownership"></a>

## P1-5 实验：转交 SET 输入，缩短缓冲生命周期（v9）

普通异步 SET 通过 `TrySetOwned` 接管已解析的 `BackedArguments`，消除入队时的 key/value 副本；
准入拒绝不改变原参数，外部参数保留复制回退。drain 完成后释放输入，effect/WAL 独立持有共享缓冲。
票据销毁时才归还 `reserved_bytes`，保持 **8 MiB / 256 笔**反压；同步修正连接队列字节统计和旧参数视图，
支持命令提前销毁及断连，确认条件和回复顺序不变。这是 P1-5 的第一步，尚未统一全流程内存预算。

每个数据副本的 effect 缓存另有 **8 MiB / 4096 条**上限。复核在途内存时按唯一缓冲的实际 capacity 统计，并分别观察分配、fiber 栈与进程 RSS。 [effect 缓存计费](../../../src/server/cluster/raft/data/set_effect.cc)

Ubuntu 同机未插桩 Folly Release，Raft 使用 **memory ack**，**1 连接 / pipeline 64**，256 MiB、256 KiB/value，
64 笔预热、5 秒静置；三版本交错各三轮，计时均包含 **SET + 原写连接 WAIT 1**，表中为中位数。
增量基线是 `f6b8c9d5` 的 v8 实现，本轮复测 **322.41 MiB/s**；v8 历史值 **325.31** 与原始基线 **169.56** 保留原含义。

| 版本 | 确认吞吐 MiB/s（范围） | 仅 SET 吞吐 MiB/s | 末尾 WAIT ms | 批次 P95 ms |
| --- | ---: | ---: | ---: | ---: |
| 改前（v8 复测）`71979b246b5f…` | 322.41（318.96–332.53） | 322.47 | 0.15 | 74.94 |
| 输入转交 `694ef9835c9c…` | **389.95（382.47–403.93）** | **390.04** | **0.15** | **65.32** |
| 原 cluster，同一新版二进制 | 580.94（486.54–621.18） | 754.54 | 101.38 | 33.58 |

- **确认吞吐 +20.9%，SET 吞吐 +21.0%**，SET 墙钟 **793.86 → 656.34 ms**；两版 WAIT 均约 0.15 ms，收益来自 SET 阶段。
  相对原始 **169.56 MiB/s** 累计 **+130.0%**，属于跨轮比较，不能相加各阶段增量。
- 三轮确认吞吐 **322.41 / 318.96 / 332.53 → 389.95 / 382.47 / 403.93 MiB/s**，
  配对 **+20.9% / +19.9% / +21.5%**，范围不重叠。仅测此负载，尚不外推多连接或小 value。
- **P95 中位数 −12.8%**：三轮 **77.25 / 74.94 / 61.81 → 56.83 / 72.28 / 65.32 ms**，第三组上升 **5.7%**。
  原 cluster 仍快 **1.49 倍**，首轮偏低，全部样本保留。

另跑单次诊断；旧/新插桩二进制为 `6685d26f5cc2…` / `8dc386509669…`。
旧版 leader / follower 为 node 12 / 11，新版为 node 11 / 12，以下按角色比较。
CPU 按 **500 Hz** 样本估算；诊断结果不替代正式三轮 benchmark。

| 单次诊断指标 | 改前（v8 复测） | 输入转交 | 原 cluster |
| --- | ---: | ---: | ---: |
| 入队 key/value memcpy 样本 / 估计 CPU | 27 / 54 ms | **0 / 0 ms** | — |
| leader CPU / 每 MiB CPU | 612 ms / 2.39 ms | **506 ms / 1.98 ms（−17.3%）** | 382 ms / 1.49 ms |
| follower CPU | 240 ms | 238 ms | 146 ms |
| leader + follower CPU | 852 ms | **744 ms（−12.7%）** | 528 ms |
| leader memcpy / memset / CRC | 230 / 78 / 70 ms | 134 / 54 / 106 ms | 184 / 74 / 未采到 |
| leader 测量前 RSS | 162.23 MiB | 140.93 MiB | 128.29 MiB |
| leader 测量结束 RSS / 进程峰值 RSS | 444.62 / 459.72 MiB | **403.55 / 445.07 MiB** | 650.63 / 653.57 MiB |
| follower 测量结束 RSS / 进程峰值 RSS | 410.74 / 410.74 MiB | **400.98 / 407.04 MiB** | 373.04 / 373.04 MiB |

**目标输入副本已消除，反压仍保留。** 1024 笔全部走 `input_take`，`input_copy=0`，释放后参数堆容量均为零。
输入持有时间中位数 / P95 为 **3.12 / 11.11 ms**，票据为 **13.30 / 32.65 ms**；
接管输入的并发堆容量峰值为 **7.75 MiB**，这是输入阶段容量，不是全流程峰值或相比旧版的节省量。

**RSS 有下降迹象，不能全部归因。** leader 峰值下降 **14.65 MiB / 3.2%**，测量结束 RSS 下降 **41.07 MiB**，
但测量前已低 **21.30 MiB**，且角色节点不同。RSS 包含启动、预热、KV 和 allocator 保留内存，不能宣称每轮节省 41 MiB。

**trace 显示排队缩短，应用执行本身基本持平。** leader worker 排队区间并集 **209.05 → 190.91 ms**、
owner 排队 **39.91 → 34.56 ms**，follower Fetch RPC **508.30 → 387.49 ms**；
leader `journal.apply` **112.93 → 112.16 ms**、SET 编码 **86.01 → 84.67 ms**，编码批次 **173 → 136**。
这些区间相互重叠，不能相加解释吞吐；两版每节点均应用 **1024** 笔，全部命中共享 effect，WAL 读取/重复解码各为零。

<a id="v9-confirmation-review"></a>

**确认路径复核：** 16 批最后票据在对应 leader apply 后的等待合计 **0.069 ms**、最大 **0.008 ms**，暂不支持优先改 P1-3。
follower receive 到下一 Fetch RPC 间隔合计 **115.36 ms**，其中 **91.48 ms** 与 leader 已测工作 / I/O 重叠。
P0-1b 串行等待仍存在，但缺少实际发包和 leader 接纳 offset/HW 的事件，不能将差值当作可兑现收益。

**验证：** Debug / Release / ASAN+UBSAN 各 **30 项**相关单测通过；Debug **14 项**、ASAN+UBSAN **9 项**集群回归通过，
覆盖参数所有权、拒绝重试、断连、反压、同 key、条件写、关闭、换主、恢复与快照。
**9 轮**正式测试和 **3 次**诊断均通过原连接 WAIT 1、离线副本恢复、WAIT 2 和全部 **1088** 个值读回。

| 报告 | 火焰图与时间线 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 改前（v8 复测） | [v9 before](http://172.31.23.2:3137/memory-before-c1-p64-v9/index.html) | [1993 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/996cbe4048a45c38ad0192c05a8148af) |
| 输入转交 | [v9 input](http://172.31.23.2:3137/memory-input-c1-p64-v9/index.html) · [leader 火焰图](http://172.31.23.2:3137/memory-input-c1-p64-v9/leader-node-11.svg) | [2159 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/9c163cf3cac1502a8a4dafa17b54db44) |
| 原 cluster | [v9 cluster](http://172.31.23.2:3137/cluster-c1-p64-v9/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/6d21b700dd275fd1a0a7e0ca3f26a43b) |

完整事件为 **25779 / 29441 / 2948** 条，无丢失；Langfuse 每阶段保留最长 50 条，统计使用全部事件。
完整数据、二进制校验信息、诊断明细、验证日志及复现脚本保留在本地和测试机
`docs/cluster/benchmarks/profile-input-20261123/`（Git 忽略）；确认路径明细见其中 `v9-review/`。
