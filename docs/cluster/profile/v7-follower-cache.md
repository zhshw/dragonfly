# v7：follower 复用接收批次

[Profile 索引](../profile.md#v7-follower-experiment) · [v6 effect/WAL 共享](v6-drain-sharing.md)

## P1-4 实验：follower 复用接收批次，确认吞吐提高，RSS 未下降（v7）

沿用 P0-1 的共享缓冲和 `SetEffectView`，接收追加成功后缓存已验证的普通 SET/no-op，
应用时复用 key/value 视图；省去刚写入 WAL 的读回、清零、第二遍 CRC 和 value 副本。
原始网络校验、HW/应用顺序、迁移 fencing 与 flush/memory 确认条件保持不变。
缓存按唯一 capacity 限额，超预算/控制记录/恢复走 WAL；截断与快照内容代次、任期变化会使缓存失效。

正式性能仍为同机未插桩 Folly Release，**1 连接 / pipeline 64**，256 MiB、256 KiB/value，
三版本交错各三轮，计时包含 SET + 原写连接 WAIT 1，表中为三轮中位数及确认吞吐范围。基线是完整 P0-1 版，本轮复测为 252.11；
不用上一轮 242.79 计算增量，原始基线 **169.56 MiB/s** 保留。

| 版本 | 确认吞吐 MiB/s（范围） | 仅 SET 吞吐 MiB/s | 末尾 WAIT ms | 批次 P95 ms |
| --- | ---: | ---: | ---: | ---: |
| 完整 P0-1 `c4b918f2a1ff…` | 252.11（244.06–256.09） | 280.04 | 101.27 | 71.12 |
| 加 follower 缓存 `c4460ba22ac7…` | **300.25（279.17–309.97）** | **300.31** | **0.16** | **69.79** |
| 原 cluster，同一新版二进制 | 607.99（604.13–608.24） | 799.35 | 101.39 | 34.96 |

- **确认吞吐 +19.1%**，三组配对 +23.0% / +9.0% / +23.0%，范围不重叠；原 cluster 仍快约 **2.03 倍**。
- **SET 阶段中位数 +7.2%**，墙钟 914.16 → 852.46 ms。末尾 WAIT 从约 101 ms 降至 0.16 ms（三轮均如此），
  是确认吞吐改善的重要组成，不能把 +19.1% 全解释为执行速度或 CPU 提升。
- **P95 仍不稳定**：三轮 87.07 / 67.22 / 71.12 → 69.79 / 103.22 / 63.25 ms。
  中位数仅 −1.9%，第二轮明显回退，该轮仅 SET 吞吐也从 285.02 降至 279.22 MiB/s。保留全部样本，不扩展连接数。

另跑一次诊断；旧/新诊断二进制为 `1de5b759be7a…` / `6c87855cebf0…`，leader 均为 node 11，
follower 均为 node 12。CPU 按 500 Hz 样本估计，trace 取区间并集；这组插桩数据不代替正式三轮性能。

| 单次诊断指标 | 完整 P0-1 | 加 follower 缓存 | 原 cluster |
| --- | ---: | ---: | ---: |
| follower WAL 读取 / batch 解码 / effect 解码次数 | 各 1024 | **各 0** | — |
| follower 读回与解码区间并集 | 154.20 ms | **0 ms** | — |
| follower 外层应用区间并集 | 294.29 ms | **83.95 ms（−71.5%）** | journal.apply 43.18 ms |
| follower CPU 估计 / 每 MiB CPU | 368 ms / 1.44 ms | **236 ms / 0.92 ms（−35.9%）** | 156 ms / 0.61 ms |
| follower memcpy / memset / CRC | 96 / 42 / 84 ms | **74 / 0 / 36 ms** | 22 / 70 / 未采到 |
| leader CPU 估计 | 660 ms | 688 ms | 394 ms |
| leader 编码区间并集 | 84.99 ms | 86.08 ms | — |
| leader 应用区间并集 | 168.59 ms | 175.32 ms | — |
| leader 测量结束 RSS / 进程峰值 RSS | 438.23 / 454.08 MiB | 428.87 / 465.30 MiB | 647.62 / 652.48 MiB |
| follower 测量结束 RSS / 进程峰值 RSS | 388.98 / 392.21 MiB | **406.47 / 406.50 MiB** | 373.58 / 373.58 MiB |

**消除了目标重复工作，没有少执行 SET。** 新版 leader/follower 各有 1024 次 cache hit 和 1024 次
`journal.apply`，没有 WAL 回退；cache hit 只表示从缓存取出，须结合实际应用事件与读回验证判断成功。
follower 应用时的 CRC 从 18 个叶子样本降至零，接收时仍采到 18 个 CRC 样本，保留首次校验。
118 次缓存准备合计约 0.10 ms，接收追加总耗时 74.45 → 74.00 ms，没有把 154 ms 的重复工作整体转移到接收阶段。
leader 编码和 CPU 未改善；原 cluster follower 仅 78 个样本，细分热点不宜过度比较。

**应用缩短也减少了接收位置报告前的串行等待。** 按 offset 关联后，128 → 118 个有数据响应的
“接收完成至下一次 Fetch RPC”累计间隔为 **256.26 → 96.85 ms**，其中应用为 **177.59 → 52.30 ms**。
应用仍在下一次 Fetch 之前，P0-1b 的完整流水化没有实现；后台 fsync、owner/shard 等待也仍存在。
这里是单次阶段墙钟，不与 CPU 或正式吞吐百分比相加。

**WAIT 的 100 ms 差异来自检查时机。** 两版最后目标均为 offset 1094：旧 follower 最后应用结束
比客户端 WAIT 开始晚 **0.219 ms**，下一次 Fetch 晚 **0.228 ms**；新版分别提前 **0.091 / 0.056 ms**。
[ServerFamily::Wait](../../../src/server/server_family.cc) 在应用进度不足时睡眠 100 ms 再检查，
与诊断末尾 WAIT **101.36 → 0.23 ms** 一致。RPC span 表示进入 `peer.Call`，并非实际发包，
目前没有 leader 接纳 applied tag 的独立事件；不据此声称完整关键路径已全部追踪。
WAIT 仍要求副本 applied offset 达标，SET 仍需本模式多数接收及 leader 本地应用。

**内存占用没有下降。** follower 峰值 RSS **+14.28 MiB / +3.6%**，leader 峰值 **+2.5%**。
118 次接收追加后的缓存快照中，pending 唯一 capacity 最大 **7.75 MiB**，单个缓冲最大 **4.00 MiB**，
未超过 8 MiB 缓存预算；正在应用或 I/O 的引用另计，不能当作全流程内存上限。
follower 测量前 RSS 已从 120.89 变为 131.55 MiB；峰值还包含启动、预热、KV 和 allocator 保留内存，
本次没有分配字节追踪，不能把 RSS 增量精确归因于某个缓存。

**验证：** Debug / Release 各 72 项、ASAN/UBSAN 71 项单测通过（sanitizer 未包含 `set_executor_test`）；
覆盖共享生命周期、预算回退、坏 CRC、半包/未来任期、同任期截断替换、快照失效、迁移屏障和存储失败。
12 项集群回归覆盖 flush/memory pipeline、条件写、断连/关闭、换主、崩溃恢复及快照追赶。
9 轮正式性能与 3 次诊断均完成原连接 WAIT 1、离线副本恢复、WAIT 2、1088 个值读回。

| 报告 | 火焰图与时间线 | 已核对的 Langfuse trace |
| --- | --- | --- |
| 完整 P0-1 | [v7 before](http://172.31.23.2:3137/memory-before-c1-p64-v7/index.html) | [1976 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/c2b9bff454e45d7d986f5b04a8540ada) |
| follower 缓存 | [v7 follower](http://172.31.23.2:3137/memory-follower-c1-p64-v7/index.html)、[follower 火焰图](http://172.31.23.2:3137/memory-follower-c1-p64-v7/follower-node-12.svg) | [1991 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/e0be3600de775f28a1f938ec9cd3491b) |
| 原 cluster | [v7 cluster](http://172.31.23.2:3137/cluster-c1-p64-v7/index.html) | [221 observations](http://172.31.23.2:3007/project/dragonfly-performance/traces/d45f7f1688e958a6a1aa07e127e50711) |

完整事件为 27642 / 25587 / 2951 条，无丢失；Langfuse 每阶段保留最长 50 条，报告统计使用全部事件。
原始数据、源码/二进制快照及 `analyze.py`、`analyze_profiles.py`、`v7-critical-review/analyze.py`
保存在本地及测试机 `docs/cluster/benchmarks/profile-follower-20261123/`，由 Git 忽略。

## 选择依据与实现范围

**代码依据：** `CommittedEffects::ApplyToBatched` 在未命中 effect 缓存时用 `log.Read(..., 1, ...)` 逐 batch 读回，
随后 `DecodeRecordBatch → DecodeSetEffect`；后者重新复制 key/value。
`PartitionLog::Read` 先创建清零字符串再 pread；leader Fetch 也走此入口。
leader 已有 parked Fetch 和在途 `WrittenRecords` 快路径；v5 中这两条路径的复制与 follower 读回更值得优先处理，
不能把 leader 的全部 Fetch 都算成磁盘重读。

**v6 选择依据：** 完整共享版 follower 仍执行 1024 次 WAL 读回和两层解码，区间并集 **161.33 ms**，
占外层 apply 的 **55.0%**。按日志 offset 重新关联后，其中 **112.90 ms** 位于收到新日志之后、
下一次报告接收位置的 Fetch RPC 发起之前。这项既延续 P0-1 的缓冲共享，也直接缩短 P0-1b 路径中的重复处理。
它不能代替接收确认流水化，但可以先复用现有有界缓存和应用路径，不必一并改造组 worker 调度。

**本轮实现与验证范围：**

1. 保留 follower 接收缓冲的不可变所有权；`AppendAsFollower` 完成完整批次校验和本模式的写入确认后，
   将同一缓冲的 batch/record 元信息交给数据组缓存。只接纳实际追加的完整前缀，排除半包、未来任期及失败追加。
2. 普通 SET/no-op effect 使用 `SetEffectView` 引用该缓冲，不再为应用创建第二份 key/value。
   应用时继续检查 group、slot generation 和迁移屏障；控制/迁移记录、缓存缺口和恢复保留现有 WAL 重放路径。
3. 复用缓存的唯一 capacity 预算；明确处理截断、换主和快照安装的失效，不能只用 offset 判断缓存有效。
   日志截断和快照替换增加内容代次，下一次缓存接入/应用时清理旧代次；任期改变仍保守清理缓存。
   少量未应用记录可能持有整块 Fetch 缓冲，记录缓存容量、回退次数和进程 RSS。
4. 增量对照使用本轮完整共享版 `c4b918f2a1ff…`；仍测 1 连接 / pipeline 64、三轮 SET + 原连接 WAIT 1，
   9 轮均通过原写连接 WAIT 1、离线副本恢复、WAIT 2 和全值读回。本次没有扩展连接数。

**实测结果：** 确认吞吐 **+19.1%**，SET 阶段 **+7.2%**，其余改善包含末尾 WAIT 少等一轮重查。
单次诊断的 follower 1024 笔全部命中缓存，WAL 读回/重复解码为零，CPU 估计 **−35.9%**；
峰值 RSS **+3.6%**，不能称为内存占用改善。正确性与尾延迟边界见本文实验结果。

**原选择依据与收益边界：** 161.33 ms 和 112.90 ms 都是单次 trace 区间，不是可直接省下的端到端时间。
同一次采样中 leader/follower 后台 fsync 和实际 KV 应用仍有等待，不能承诺两位数吞吐提升。
选择它的依据是三类可定位的重复工作和本轮已具备的共享机制；不把 trace 墙钟时间与 P1-7 的 CPU 样本直接比大小。
P1-7 的 Fetch 封帧优化保留为后续独立项，本次选择不混入发送协议改动。
复核脚本与逐间隔数据在 ignored 的 `docs/cluster/benchmarks/profile-drain-20261123/v6-critical-review/`。

- leader 原有 `RememberAppended` 与新增 follower 缓存的命中事件均已补齐；本次实测没有 WAL 回退。
- follower 已复用接收至应用期间的不可变 batch，慢副本、预算不足和重启继续从磁盘读取。
  leader 发送 Fetch 时的额外副本仍保留，属于后续独立改动。
- 已传递首次校验的 batch 元信息，网络输入、磁盘重读仍完整校验，没有任意字节接口的“跳过校验”开关。

**验收：** pread 次数/字节、缓存命中率、解码复制字节和 CRC CPU 下降；坏 CRC、部分 batch、换主替换尾部和恢复仍能检出。
