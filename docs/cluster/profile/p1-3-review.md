# P1-3 按 applied offset 批量完成票据

[Profile 索引](../profile.md#ticket-completion-review) · [v9 实验](v9-input-ownership.md)

## v14 / v14b 落地

Folly io_uring 批次事件循环首版移除了普通异步 SET 的逐笔 `raft-set-confirm` fiber、applied 回查和逐笔定时器。追加成功后，票据绑定到当前组实例的 `(epoch, end_offset)`。组发布共享回复水位 `R = min(本地确认位置, HW, applied)`，以连续成功应用的前缀完成同任期、已绑定且受水位覆盖的票据。连接只订阅队首，按 FIFO 回复并归还准入额度。组内用一个定时器跟踪最早的截止时间，超时、换主、存储失败和关闭沿用各自的收尾路径，每笔只完成一次。

组 I/O 和节点 RPC 用 Folly io_uring，测试 RESP 后端也从 epoll 切到 io_uring。宿主引擎的连接唤醒沿用原有通知接口。平台结果衡量这批组合改动。 [v14 首版](v14-batch-event-loop.md) · [v14b 修正](v14b-batch-progress.md) · [回复流程与屏障](../raft-batch-event-loop.md#3-从提交到回复)

## v9 与 v13 历史复核

v9 时这项优化列为备选，优先处理后台同步阻塞。以下 v9 数据来自两份已保存的诊断。 [P1-6 优先依据](v9-trace-review.md)

v13 仍逐笔启动确认 fiber。1024 笔写匹配到 124 次 apply，应用后确认 P95 为 **0.744 ms**。16 个 pipeline 批尾额外等待最大 **95.3 µs**、合计 **0.604 ms**。这些数据给出了当时逐笔回查的等待规模，尚无 P1-3 单项吞吐对照。 [v13 Trace 复核](v13-fetch-borrow.md#trace-复核)

### v9 的批尾与逐笔等待

`WaitApplied → VisitUntil → Visit → RunOnWorker` 即使只查询 applied，也先经过 `AppendPending()` 屏障。
因此本笔已应用后，回查仍可能排在后续追加或 memory 后台 fsync 后面。
每笔 `raft-set-confirm` fiber 等完回查才完成票据，票据又须等连接按序回复销毁后才归还 **8 MiB / 256 笔**准入额度。

以下按 offset 对齐全部 1024 笔的对应 apply 批次结束与 `WaitApplied` 返回；时间区间合并去重。
第二列来自提前 Fetch 失败试验的 **改前 v9** 诊断（采集标识 `v10 before`），不含已回退候选。

| 指标 | v9 原诊断 | v9 复测诊断（v10 before） |
| --- | ---: | ---: |
| SET 测量时间（插桩） | 649.24 ms | 670.21 ms |
| apply 后票据回查等待，中位数 / P95 | 0.125 / 1.726 ms | 0.088 / 1.701 ms |
| apply 后仍等 >1 ms 的票据 | 141 / 1024 | 61 / 1024 |
| apply 后等待区间并集 / 最大单笔 | 71.19 / 23.24 ms | 98.24 / 27.92 ms |
| 16 个批尾额外等待，合计 / 最大 | 0.069 / 0.008 ms | **28.10 / 27.92 ms** |
| 额度已满且有已应用未完成票据，区间并集 | 54.36 ms | 23.48 ms |
| 上述区间扣除已观测 write/fsync 的重叠 | 18.25 ms | 17.21 ms |
| 同时存活的票据峰值 | 31 | 31 |

复测的**第 9 批最后票据 offset 646**，apply 后等 **27.92 ms**，其中 **27.87 ms** 与两段后台 fsync 重叠；
该批客户端时间为 **61.48 ms**。这比只统计 worker 排队次数更能支持实验。
v9 原诊断的 16 个批尾几乎立即完成，因此先前的“批尾没有明显等待”只适用于那份样本，不能覆盖全部票据或其他采样。

额度占用按输入/票据生命周期与单连接 FIFO offset 顺序关联，已核对笔数和时间边界；
“额度满”不等于当时一定有已解析请求等待准入。上述区间还与其他必要工作重叠，**不能当作可兑现的吞吐提升**。

### v9 火焰图中的回查开销

v9 leader 共 **253 个样本 / 506 ms**；可识别的 `WaitApplied` 调用侧为 **5 个 / 10 ms（2.0%）**，
复测为 **3/282（1.1%）**。原诊断确认 fiber 结束时的 `CheckStackMargin` 另有 5 个样本；
这些只是可识别路径，不能覆盖被内联或转交 worker 后的全部确认成本。
worker 的 **93 个**样本中 **70 个**属于 `AppendSetBatch`；不能把整个 worker 火焰都算成 P1-3 收益。

每轮源码确实创建 1024 个确认 fiber；当前负载最多 31 个票据，每个 fiber 默认栈 64 KiB，
对应约 **1.94 MiB** 的栈容量上限量级，不能称为实测 RSS 节省。输入释放和日志/应用工作仍必须保留。

### v9 阶段的试验设想

- 先将票据按 `(epoch, end_offset)` 登记，在连续 applied 前缀推进时批量完成，避免完成通知再过 I/O 屏障。
  只改变完成通知；SET 仍等多数确认和本地应用，WAIT 仍确认副本 applied。
- 第一版保留现有确认 fiber 的收尾，先验证减少回查能否改善吞吐/P95；移除逐笔 fiber 可作为后续单独增量。
  保留有序回复、票据销毁后归还额度，以及换主、错误、超时、关闭的完成/取消语义。
- 只测 **1 连接 / pipeline 64**：先区分确认查询、drain、Fetch 的 worker 访问，再核对已应用票据等待、
  额度满时长及正式三版本三轮确认吞吐/P95。收益不稳定就回退，不扩连接数。

脚本与计算结果保存在 Git 忽略的 `docs/cluster/benchmarks/profile-input-20261123/p1-3-review/`。
[v9 leader 火焰图](http://172.31.23.2:3137/memory-input-c1-p64-v9/leader-node-11.svg) ·
[v9 复测诊断](http://172.31.23.2:3137/memory-before-c1-p64-v10/index.html)。
