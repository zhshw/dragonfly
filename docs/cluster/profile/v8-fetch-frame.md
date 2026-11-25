# v8：Fetch 直接封帧

[Profile 索引](../profile.md) · [benchmark](../replication-benchmark.md)

## P1-7 实验：Fetch 直接封帧，减少 leader 复制（v8）

本轮只改 Fetch response 的输出缓冲：用同一 schema 预计算长度，一次 reserve 后直接编码完整帧，
省去 body → frame 副本和 records 后面追加 tags 时的扩容复制。预计算只检查字段和长度，不复制大 payload。
保留必要的 records → 最终帧复制；其他 RPC 仍走原路径，协议与确认条件不变。

同机未插桩 Folly Release，**1 连接 / pipeline 64**，256 MiB、256 KiB/value、64 笔预热和 5 秒静置，
三版本交错各三轮，计时包含 SET + 原写连接 WAIT 1。基线是 `88f83cd7` 的 P1-4 完整版本，
使用本轮复测 **299.55** 计算增量；原始 **169.56 MiB/s** 与上一轮 **300.25** 都保留原含义。

| 版本 | 确认吞吐 MiB/s（范围） | 仅 SET 吞吐 MiB/s | 末尾 WAIT ms | 批次 P95 ms |
| --- | ---: | ---: | ---: | ---: |
| 改前 `c4460ba22ac7…` | 299.55（287.32–326.08） | 299.60 | 0.15 | 71.96 |
| 直接封帧 `71979b246b5f…` | **325.31（317.49–326.24）** | **325.38** | **0.16** | **69.47** |
| 原 cluster，同一新版二进制 | 606.05（580.56–620.97） | 797.11 | 101.25 | 33.33 |

- **确认与 SET 吞吐中位数均 +8.6%**，SET 墙钟 854.47 → 786.78 ms；本轮两版 WAIT 均接近立即完成，
  没有上一轮少等 100 ms 的因素。原 cluster 仍快 **1.86 倍**。
- **范围重叠，第三组近乎持平**：确认吞吐三轮 299.55 / 287.32 / 326.08 → 317.49 / 325.31 / 326.24，
  配对为 **+6.0% / +13.2% / +0.05%**。本轮支持局部复制优化有效，不能把 +8.6% 当作稳定保证；未扩连接数。
- **P95 中位数 −3.5%**：三轮 71.96 / 120.02 / 68.11 → 69.47 / 70.89 / 68.84 ms，第三组略升 **1.1%**。
  所有样本保留，没有删除旧版第二轮的较慢结果。

另跑单次诊断；旧/新插桩二进制为 `6c87855cebf0…` / `6685d26f5cc2…`，两版 leader 均为 node 12、
follower 均为 node 11。CPU 按 500 Hz 样本估算；以下结果不替代正式三轮 benchmark。

| 单次诊断指标 | 改前 | 直接封帧 | 原 cluster |
| --- | ---: | ---: | ---: |
| Fetch body → frame / 尾部扩容 memcpy 样本 | 21 / 11 | **0 / 0** | — |
| records 写入输出缓冲 memcpy 样本 | 12 | 16 | — |
| 上述 Fetch 复制合计 | 44 样本 / 88 ms | **16 样本 / 32 ms（−63.6%）** | — |
| leader CPU / 每 MiB CPU | 668 ms / 2.61 ms | **614 ms / 2.40 ms（−8.1%）** | 402 ms / 1.57 ms |
| follower CPU | 226 ms | 242 ms | 124 ms |
| leader + follower CPU | 894 ms | **856 ms（−4.3%）** | 526 ms |
| leader memcpy / memset / CRC | 288 / 86 / 68 ms | 226 / 94 / 86 ms | 210 / 72 / 未采到 |
| leader 测量结束 RSS / 进程峰值 RSS | 443.07 / 460.99 MiB | **438.74 / 455.41 MiB** | 649.82 / 653.73 MiB |
| follower 测量结束 RSS / 进程峰值 RSS | 409.85 / 409.85 MiB | **401.13 / 410.62 MiB** | 373.38 / 373.38 MiB |

**目标复制已消除，内存占用没有明显改善。** 同轮旧版可删除路径为 32 个样本；新版只剩 records 写最终帧，
必要复制从 12 变为 16 个样本，不能把样本差当作精确复制字节。leader 峰值 RSS 仅下降 **5.58 MiB / 1.2%**，
follower 峰值上升 **0.2%**。RSS 包含启动、预热、KV 和 allocator 保留内存；没有分配字节追踪，
不能宣称相应比例的总内存节省。原 cluster follower 只有 62 个样本，细分比例不宜过度解读。

**trace 显示排队缩短，应用执行本身未变快。** follower Fetch RPC 区间并集 **600.02 → 537.52 ms**，
leader worker 排队 **257.46 → 193.40 ms**、owner 排队 **74.37 → 34.22 ms**；
leader `journal.apply` **103.17 → 104.41 ms**、SET 编码 **85.47 → 86.15 ms**，均未下降。
这些区间相互重叠，也受批次和调度影响，不能相加或全部归因于封帧优化。
两版 leader/follower 各实际应用 **1024** 笔，全部命中共享 effect，WAL 读取/重复解码各为零。
仍有输入所有权复制、聚合编码、CRC、逐笔 shard 执行和后台同步等待；本轮没有实现 P0-1b 的完整流水化。

**验证：** Debug / Release / ASAN+UBSAN 各 **79 项**相关单测通过；包括 Fetch v4–v18 Java golden、
4 MiB records、null/empty、各层 tags、known/unknown tag 冲突、长度溢出、截断帧和失败时输出不变。
**12 项**集群回归通过，覆盖 flush/memory pipeline、条件写、断连/关闭、换主、崩溃恢复及快照追赶。
**9 轮**正式测试与 **3 次**诊断均完成原连接 WAIT 1、离线副本恢复、WAIT 2 和全部 **1088** 个值读回。

| 报告 | 火焰图与时间线 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 改前 | [v8 before](http://172.31.23.2:3137/memory-before-c1-p64-v8/index.html) | [1997 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/cf1eea995e86598683ad2018f92080c4) |
| 直接封帧 | [v8 frame](http://172.31.23.2:3137/memory-frame-c1-p64-v8/index.html)、[leader 火焰图](http://172.31.23.2:3137/memory-frame-c1-p64-v8/leader-node-12.svg) | [1985 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/a465e04f0f9955f9a3613f53ca6400c1) |
| 原 cluster | [v8 cluster](http://172.31.23.2:3137/cluster-c1-p64-v8/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/ca7f5c0f1cf55c9f94c9b83c8905cafc) |

完整事件为 **26529 / 24989 / 2950** 条，无丢失；Langfuse 每阶段保留最长 50 条，统计使用全部事件。
脚本、源码/二进制快照与原始结果在 `docs/cluster/benchmarks/profile-frame-20261123/`，由 Git 忽略；
测试机保留两版正式与诊断二进制。

## 选择依据与实现范围

**实施前 v7 复核：** 当时 leader 的 Fetch 编码仍有 42 个 memcpy 叶子样本（约 84 ms）：
body → frame 为 16 个、尾部 tags 导致扩容为 11 个、records 写入 body 为 15 个。
本次针对前两项，合计约 **54 ms / leader CPU 的 7.8%**；必要的 records 复制仍保留，不据此承诺吞吐提升。
先按相同 schema 计算完整帧长度，再一次 reserve 并直接编码；只切换 Fetch response，其他 RPC 保留原路径。
预计算不复制 records 或 unknown tag payload，保持全部版本、null/empty、tag 冲突及失败时输出不变。
基线为 `88f83cd7` / `c4460ba22ac7…`，1 连接 / pipeline 64 三轮未插桩对照为
**299.55 → 325.31 MiB/s（+8.6%）**，SET 阶段同为 +8.6%，两版末尾 WAIT 均约 0.15 ms。
三组配对为 +6.0% / +13.2% / +0.05%，范围重叠；保留原 cluster 对照、恢复和全值读回，未扩连接数。
CPU、RSS 与完整验证见本文实验结果。

**代码依据：** [flexible_message.h](../../../src/server/cluster/raft/protocol/flexible_message.h)
的 `Writer::Field(RecordBytes)` 将日志复制进消息 body；尾部 `Tags → Varint → push_back` 还可能触发 body 扩容。
[rpc_frame.cc](../../../src/server/cluster/raft/protocol/rpc_frame.cc) 的 `WriteResponse → WriteFrame`
随后把完整 body 再复制进最终 frame。v5 合计 56 个 memcpy 样本，约 112 ms，占 leader CPU 样本的 12.6%。
其中单缓冲方案直接针对 body → frame 的 24 个样本及尾部扩容的 10 个样本，合计约 **68 ms / 7.7%**；
records 写入最终帧的 22 个样本仍对应必要复制，不能把全部 112 ms 都算作可删除开销。

- 已使用同一 schema 预计算长度，一次 reserve 后将长度、header 和 body 写进最终帧。
  本轮合并验证“避免扩容”和“去掉 body → frame 副本”，未分别量化；编码失败时保留原输出。
- 后续可与 P1-4 配合，让 parked Fetch 引用在途 WAL 的不可变范围，避免发布和填充 response 时再各复制一次。
  分散缓冲发送涉及 buffer 所有权、部分发送和背压。v12 在 Leader 试验分散发送，收益待确认，代码暂回退。v13 独立验证并保留了 Follower 借用解码。 [v12 实验](v12-fetch-scatter.md) · [v13 实验](v13-fetch-borrow.md)

**验收：** Fetch 各协议版本逐字节一致，frame 长度、tag、空/null records、截断输入和错误返回正确；
发送前复制字节、分配和 leader CPU 下降，原连接 WAIT 与恢复检查通过。
