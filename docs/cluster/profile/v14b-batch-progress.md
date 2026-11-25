# v14b Folly io_uring 批次事件循环（首版修正）

**延续 v14 的 Folly io_uring 新架构，保留实现，上一轮选择 v13。** 普通 SET 使用有界准入、固定编码执行器、独立 ApplyLane 和共享回复水位，移除逐笔确认 fiber、applied 回查及逐笔定时器。节点 RPC 使用 Folly io_uring；测试已关闭强制 epoll，并断言 RESP 实际后端为 `iouring`。 [P1-3 票据完成](p1-3-review.md#v14--v14b-落地)

v14b 修复首版的 applied 上报和 WAIT 唤醒。Follower 将普通缓存前缀交给独立、有界的 ApplyLane，完成事件只发布实际成功的前缀。应用在途时先报告接收位置。遇到空响应时等待本地批次完成事件，再报告 applied，避免新的 applied 水位滞留在长轮询期间。Leader WAIT 订阅副本进度变化。连接关闭、超时和条件写仍受原有检查与屏障约束。平台结果同时覆盖共享回复水位、Folly RPC 和此处的进度上报。 [P0-1b 早期尝试（已回退）](p0-1b-follower-receipt.md) · [首版 v14 记录](v14-batch-event-loop.md) · [设计与实现边界](../raft-batch-event-loop.md)

## 实验结果

Raft memory、1 连接、pipeline 64、256 MiB、256 KiB/value，SET + 原连接 `WAIT 1`，正式三轮。平台的上一轮、运行 Diff、Trace 和火焰图均引用 v13；没有重跑 v13。后端同时由 epoll 改为 io_uring，数据是组合改动的对照。 [平台](http://127.0.0.1:8787/#/v14b/overview)

| 指标 | v13 | v14b | 变化 |
| --- | ---: | ---: | ---: |
| 吞吐中位数（MiB/s） | 486.54 | 499.85 | +2.7% |
| 吞吐算术均值（MiB/s） | 470.84 | 501.83 | +6.6% |
| 批次 P95 中位数（ms） | 45.25 | 38.21 | −15.6% |
| 批次 P95 算术均值（ms） | 48.68 | 44.66 | −8.2% |

三轮吞吐 **511.38 / 494.25 / 499.85 MiB/s**，P95 **58.42 / 38.21 / 37.36 ms**。首版 v14 的末尾 WAIT 中位数 **200.42 ms**，本轮为 **0.15 ms**；第一轮 P95 仍偏高，不能宣称所有轮次尾延迟都改善。

## 独立诊断

- Leader worker 排队区间并集 **263.28 → 25.78 ms**；CPU **506 → 504 ms**，Follower CPU **200 → 258 ms**。调度阻塞减少，整体 CPU 尚未下降。
- Leader 编码 **82.55 ms / 257 批**、追加准备 **41.13 ms**、ApplyLane 执行 **86.53 ms / 379 批**。相邻 SET 的 shard 内合批仍未实现。
- **内存作为后续优化的观测与约束。** Leader 峰值 RSS **424.54 → 668.01 MiB（+243.47 MiB）**，Follower **407.52 → 420.39 MiB**。需查编码/WAL 缓冲跨线程释放、分配器保留页和每 peer 的 Folly loop/接收缓冲；当前证据不能区分这些来源，不能宣称本轮内存已优化。
- memcpy 仍占 Leader **65/252** 个叶子样本，CRC 为 **58/252**。WAL 提交 **176 → 257 批**，需观察就绪队列和批次大小；不因局部热点减少而直接扩大测试连接数。各阶段区间有重叠，不能相加。

## 下一步：批次传递到 shard

**吞吐优化优先看 shard 合批与跨线程投递、等待，再配合调整应用批次大小。** 当前外层已合批，批内仍逐笔同步分发 SET。

| 层次 | 本轮 1024 条写的情况 | 待解决问题 |
| --- | --- | --- |
| WAL 合批 | 257 批，平均 **4.0 条/批**；v13 为 **5.8 条/批** | 核对就绪队列和取批时机，解释批次变小 |
| ApplyLane 合批 | 379 批，平均 **2.7 条/批** | **1 MiB** 编码字节上限，当前 **256 KiB/value** 加上编码开销，一批最多装 **3 条** |
| shard 执行 | 批内逐条 `ExecuteSetEffect → ExecuteSet → DispatchCommand(ONLY_SYNC)` | 非本线程 shard 仍需逐笔投递并等待，尚未摊薄内部调度成本 |

1. **先做 shard 合批。** 一次投递携带多条普通 SET，在所属 shard 内执行并批量返回完成进度。保留同 key 日志顺序、事务约束、控制记录屏障及失败处理，组内 applied 只推进实际完成的连续前缀。

   **原 cluster 的跨线程做法（源码核查）：**

   | 路径 | 实际执行方式 |
   | --- | --- |
   | 主节点客户端 pipeline | `DispatchSquashedBatch → MultiCommandSquasher` 将已解析、可合批的单 shard 命令按目标 shard 分组；每个参与 shard 一次 `AddL2`，协调端用一个 `FiberBlockingCounter` 等本批完成。批大小取决于已就绪命令和上限，不能把 pipeline 64 当成实际每批 64 条。 |
   | shard 内执行 | 每个 shard 复用一个 `SHARD_LOCAL` 事务和命令上下文，在 `SquashedHopCb` 中依序 `InvokeCmd`，保留每条命令的锁检查与执行语义；同线程且满足调度条件时走 inline，避免逐条跨线程往返。 |
   | Dragonfly 副本流 | 主节点将复制连接迁到对应 shard 线程；副本将各 flow 分配到线程，由常驻读 fiber 逐条 `JournalApplier → JournalExecutor` 重放。布局与线程匹配时，单 shard 命令可 inline；普通命令无需全 flow 屏障，全局命令才经 `MultiShardExecution` 汇合。副本这条路径靠线程就近执行，未使用上述 pipeline squasher。 |

   源码：[分组与批量投递](../../../src/server/multi_command_squasher.cc)、[shard 队列](../../../src/server/engine_shard_set.h)、[inline 条件](../../../src/server/transaction.cc)、[主节点 flow 迁移](../../../src/server/dflycmd.cc)、[副本 flow](../../../src/server/replica.cc)、[重放与全局屏障](../../../src/server/journal/journal_applier.cc)。

   **Raft 可借鉴的实现方向：** 在已提交的 `ApplyBatch` 内借用 key/value 视图，按 shard 分组，每个 shard 一次投递并复用本地事务执行，最后汇总完成进度。当前 `DispatchSquashedBatch` 在 `raft_data_commands_` 存在时直接返回 0，需要从下层复用执行机制；保持提交后应用的顺序。跨 shard 并行还需按日志 offset 记录完成结果，处理失败后的已执行后缀、重试和快照屏障，不能只按成功条数推进 applied。当前仅确认这些结构差异，未测量各自的吞吐贡献。

2. **配合调整 ApplyBatch 预算。** 减少 1 MiB 上限切碎批次带来的 GroupLoop 与 ApplyLane 交接，同时约束批内笔数、字节数和执行时间，观察公平性与 RSS。
3. **再查 WAL batch。** 记录取批时就绪请求数量、实际批大小及预算、段边界、任期等截断原因，判断是预算限制还是唤醒时机导致小批次。

本轮 ApplyLane 执行 **86.53 ms**，其中 `journal.apply` 为 **85.85 ms**，包含实际写入及 shard 等待，不能全部视为可节省时间。下一轮补充 **shard 排队时间、执行时间、投递次数和批次笔数/字节数**，结合正式吞吐、P95 与内存判断收益。以上为 v14b 结束时的计划。第 1 项的后续实现与测量见 [v15 shard 合批](v15-shard-apply.md)。

## 验证与归档

首版 Debug、Release、ASAN/UBSAN 的 **42 项**相关单测及 **16 项**集成验证已通过；本轮新增进度唤醒单测后，三个构建配置均通过 **13 项 dispatch/进度测试**，另通过 **4 项**集成回归。平台完成 **30 项单测、1 项集成验证**，三轮均通过副本确认、恢复和全量读回；独立诊断 **2265 条观测**已在 Langfuse 核验。

源码快照、二进制、指标及验证日志保存在执行机 `~/.local/state/dragonfly-raft-workflow/runs/v14b/v14b-run-001/`。保留新架构继续优化，原始数据不提交 Git。
