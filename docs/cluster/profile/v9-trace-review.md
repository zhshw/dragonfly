# v9 trace 复核：优先 P1-6，解除后台同步对追加的阻塞

[返回优化项](../profile.md#memory-flush-separation) · [v9 实验](v9-input-ownership.md)

**下一优先项是 P1-6：拆开 memory 后台同步与普通追加的占用状态。**
第二项再看 P0-2 的串行应用；P1-3 票据通知保留为备选。本次只分析已有数据，没有修改运行代码或新增性能测试。
使用两份相同 v9 实现、1 连接 / pipeline 64 的完整 trace；“复测”指提前 Fetch 失败试验的改前版（采集标识 `v10 before`），不含已回退候选。

## 1. 最明确的阻塞：后台 fsync 占住追加通道

| 单次诊断指标 | v9 原诊断 | v9 复测 |
| --- | ---: | ---: |
| SET 测量时间 | 649.24 ms | 670.21 ms |
| leader / follower fsync 区间 | 95.12 / 79.04 ms | 128.24 / 109.43 ms |
| 两节点 fsync 区间并集 | **131.05 ms（20.2%）** | **133.79 ms（20.0%）** |
| leader fsync 与等待追加重叠 | 74.23 ms | 77.34 ms |
| follower fsync 与等待 I/O 重叠 | 74.54 ms | 103.88 ms |
| 本节点 fsync 在途时的新 leader 编码 / follower 追加 | **0 / 0 次** | **0 / 0 次** |
| 有 >1 ms fsync 重叠的批次，耗时中位数 | **53.55 ms（5 批）** | **61.46 ms（5 批）** |
| 其余批次，耗时中位数 | **32.29 ms（11 批）** | **32.38 ms（11 批）** |

**代码与 trace 对得上：** `TryFlushBufferedAsync` 把后台同步放入同一个 `pending_`；
`AppendPending()` 因而为真，`RunOnWorker` 与日志追加入口都会挡住新的普通写。
直到段文件 fsync、恢复标记更新及标记 fsync 完成，才清掉这个状态。memory 已允许多数接收确认，但后台同步仍周期性阻塞追加。

这是比单纯提前发 Fetch 更完整的处理方向：提前 Fetch 试验只提前报告位置，下一响应的追加仍要等这个屏障。
P1-6 应保持当前 **64 MiB / 100 ms** 刷新条件，分别管理追加与后台同步；
开始同步时固定 `(segment, file_end, offset)`，仅发布该 cut 的持久标记，同步期间新写的字节和时间继续计入下一轮。
换段、截断、控制记录、快照、换主、关闭和失败须等待相应 I/O，`flush` 的多数持久确认保持不变。

**收益边界：** 20% 是观测到的重叠时间比例，不能直接当作提速比例；fsync 与其他节点工作重叠，并发追加也可能改变 I/O 延迟。
Folly span 从操作发起计到完成回调，包含提交/完成处理，并非纯设备时间。该改动风险高于票据通知，但阻塞证据更直接、影响范围更广。

## 2. 第二个大头：worker 同步等待应用

leader `raft.apply` 两次分别 **156.60 / 155.50 ms**；worker 排队 **190.91 / 217.73 ms**，
其中 **125.95 / 138.83 ms（约 66% / 64%）** 与 apply 重叠。说明大量 worker 排队发生在它等待状态机应用时。

当前虽已按 **32 笔 / 1 MiB** 投递 owner，内部仍逐笔 `ExecuteSetEffect → JournalExecutor → DispatchCommand → shard`。
应继续评估 P0-2 的 shard 内批量执行及逐笔跨线程等待；但 owner 排队本身只有 **34.56 / 7.32 ms**，
仅扩大 owner 批次不能按全部 155 ms 估算收益。KV 写入、类型/TTL、索引、事务和连续 applied 前缀仍需保留。

## 3. 下一轮应验证什么

先验证 P1-6：同一段普通 memory 写在后台同步期间能够继续编码和追加，下一轮持久标记仍严格止于捕获的 cut。
随后沿用未插桩三版本、1 连接 / pipeline 64 三轮对照，同时检查吞吐/P95、同步期间追加等待及内存预算。
短写、fsync/标记失败、边界操作和崩溃恢复必须验证；无稳定收益即回退，不扩连接数。

完整逐批计算与脚本保存在 Git 忽略的 `docs/cluster/benchmarks/profile-input-20261123/trace-bottleneck-review/`。
[v9 trace 与火焰图](http://172.31.23.2:3137/memory-input-c1-p64-v9/index.html) ·
[v9 复测 trace 与火焰图](http://172.31.23.2:3137/memory-before-c1-p64-v10/index.html)。
