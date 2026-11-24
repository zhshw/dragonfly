# Raft 复制 Profile

正式 benchmark、逐轮日志、正确性验证、Trace 和火焰图以 `profile` 平台的归档为准。本文保留当前结果和优化结论。

## 当前结果

取平台总览的三轮中位数。Raft 使用 `memory` ack，负载为 1 连接、pipeline 64、256 MiB、256 KiB/value，计时包含 SET 和原写连接上的 `WAIT 1`。

| 平台记录 | 吞吐（MiB/s） | 批次 P95（ms） |
| --- | ---: | ---: |
| 基线 v3 | 169.56 | 113.75 |
| 上一轮 v13（P1-7） | 486.54 | 45.25 |
| 本轮 v14b（Folly io_uring 首版修正） | **499.85** | **38.21** |
| 对比参照 `v10:cluster` | 605.47 | 34.63 |

v14b 较 v3 累计吞吐 **+194.8%**，达到原 cluster 参照的 **82.6%**。较 v13 吞吐中位数 **+2.7%**、算术均值 **+6.6%**，批次 P95 中位数 **−15.6%**。v14b 延续 v14 的 Folly io_uring 批次事件循环架构，仅修复首版的 applied 上报和 WAIT 唤醒 bug；上一轮明确选择 **v13**，v14 保留首版记录。相较 v13，测试网络后端同时从 epoll 改为 io_uring，不能拆分归因。**Leader 峰值 RSS 从 424.54 升至 668.01 MiB，内存仍需优化。** [v14b 平台](http://127.0.0.1:8787/#/v14b/overview) · [实验结论](profile/v14b-batch-progress.md)

## 使用 profile

1. 在仓库根目录运行 `make profile` 启动 Langfuse 和 Web。 [打开 profile](http://127.0.0.1:8787/)
2. 点击「创建实验」，选择上一轮，并确认基线与同负载的对比参照。点击「运行 Benchmark」，平台运行本轮三次正式测试和独立诊断，复用已选对照的归档。
3. 在「运行记录」查看三轮算术均值 Diff、合并日志和本轮验证。在「火焰图」和「Trace」对照上一轮与本轮，原 cluster 火焰图从总览的「查看基准诊断」进入。用「复制实验摘要」提取分析所需的指标。

### 用 CLI 运行 benchmark

创建新实验 `v15`，选 v14b 为上一轮，并沿用其基线与对比参照。

```sh
profile create -id v15 -title '本轮优化' -previous v14b
profile run -id v15
profile show -id v15
```

`run` 等待本轮 benchmark 和诊断结果入库，`show` 查看三轮正式数据。需要独立测量时，在执行机用 `profile run -config FILE`，配置中的 `output` 指向新目录。 [CLI 参数](../../tools/profile/README.md#cli)

### Trace 和火焰图分析示例

查看 v10 应用完成到写入确认返回的延迟。 [v10 Trace](http://127.0.0.1:8787/#/v10/trace)

```sh
profile trace -id v10 -analysis apply-to-confirm
```

先看 v10 Leader 的叶子热点，再用 `-symbol` 按上游调用路径拆分同一个热点。

```sh
profile flamegraph -id v10 -run diagnostic-current -role leader -view leaf -limit 5
profile flamegraph -id v10 -run diagnostic-current -role leader \
  -symbol 'CRC32::Extend' -caller-depth 8
```

`CRC32::Extend` 的 55 个样本中，**31 个**来自 `AppendSingleRecordBatch` 编码，**24 个**来自 `TryAppendAsync` 的 WAL 追加校验。换成 `-symbol '__memcpy_avx'` 可用同样的方法定位复制来源。 [v10 火焰图](http://127.0.0.1:8787/#/v10/flamegraphs)

复核本地 A/B 归档时，用 `profile summarize` 一次汇总正式指标、关键等待和各节点 CPU 热点。 [CLI 汇总示例](../../tools/profile/README.md#汇总本地归档)

## 路径差异

| 阶段 | 原 cluster | raft.memory |
| --- | --- | --- |
| 接收 | RESP 参数 → SET 执行 | RESP 参数 → 异步票据 → 排队 |
| 复制准备 | shard journal 编码并流式发送 | effect → Kafka batch → drain 聚合 → WAL 缓冲 |
| 确认 | 本地 SET 回复，WAIT 追踪副本应用 | WAL 接收 → follower Fetch → HW → 本地应用 → SET 回复 |
| 应用 | replica flow 消费 journal 并执行 | GroupLoop → 独立 ApplyLane → 命令分发 → shard |
| 重放数据 | 接收的 journal 数据 | 普通 effect 复用不可变 WAL 缓冲，缺口从 WAL 重放 |

两套集群设计独立。Raft 的任期、日志顺序、HW 和恢复边界仍由 Raft 负责。

## 下一轮优先项

1. **先做 shard 合批，减少逐笔跨线程投递与等待。** v14b ApplyLane 内仍逐笔同步分发 SET；参考原 cluster 按 shard 分组、一次投递并复用本地事务的方式，补充 shard 排队、执行和投递次数。当前 **86.53 ms** 应用区间包含实际写入及等待，不能全部当作可节省时间。 [v14b 源码对照与计划](profile/v14b-batch-progress.md#下一步批次传递到-shard)
2. **配合调整应用批次预算，再查 WAL batch。** 1 MiB 上限使当前 256 KiB/value 的应用批次最多装 3 条；WAL 提交也从 **176 增到 257 批**。记录批次笔数、字节数、就绪数量与截断原因。历史失败结论继续保留，避免重复试验。 [v11 复用编码](profile/v11-leader-wal-append.md)
3. **同步约束内存增量。** v14b Leader 峰值 RSS 较 v13 增加 **243.47 MiB**。检查编码/WAL 缓冲跨线程释放、分配器保留页及 Folly 每 peer loop/接收缓冲，用分配证据区分来源。当前吞吐有收益，尚未兑现内存优化。 [v14b 诊断](profile/v14b-batch-progress.md)

## 优化项与当前状态

<a id="wal-buffer-remaining"></a>
<a id="drain-preallocation"></a>

### P0-1 共享 WAL 缓冲与直接编码

单条 SET 直接编码。drain 按长度、段边界和预算选定连续前缀，一次预留并写入最终缓冲，冻结后由 I/O 与 effect 共享。P0-1a 的 drain 合批缓冲预分配随直接聚合实现，收益按组合改动记录。 [v4 共享缓冲](profile/v4-wal-buffer.md) · [v6 直接聚合](profile/v6-drain-sharing.md)

**Leader WAL 追加复用编码结果：v11 已试验并回退。** 追加准备区间并集 **41.95 → 0.99 ms**，但正式吞吐 **−0.3%**，批次 P95 **+19.1%**。相同 1024 笔写的提交次数 **153 → 177**，平均合批 **6.69 → 5.79 笔**，worker 排队 **211.95 → 213.22 ms**。处理变快可能改变 drain 收集窗口，单次诊断尚不能证明因果；各阶段时间也不能相加。**热点消失不等于吞吐收益，复试前先核实合批变小的原因，同时检查提交次数、关键排队和正式 P95。** [v11 实验](profile/v11-leader-wal-append.md)

<a id="follower-receipt-progress"></a>

### P0-1b follower 接收确认快路径

早期仅提前 Fetch 的试验未获吞吐和 P95 收益，已经回退。v14b 在新结构中使用独立、有界的 ApplyLane，普通应用在途时继续报告接收位置；遇到空响应等待本地批次事件，再报告 applied，避免旧水位被长轮询扣留。它与共享回复水位、Folly RPC 一起测量，不能单独归因。 [早期试验](profile/p0-1b-follower-receipt.md) · [v14b](profile/v14b-batch-progress.md)

### P0-2 批量应用与借用 SET 参数

v5 实现最多 32 笔 / 1 MiB 的 owner 批量投递和借用 key/value；v14b 将普通缓存前缀交给独立 ApplyLane，完成事件只发布实际成功前缀。下一步评估 shard 内合批，减少逐笔分发和跨线程等待。验证需覆盖 DbSlice 与事务行为、同 key 顺序、控制记录、迁移与快照边界及部分失败。 [v5](profile/v5-batched-apply.md) · [v14b](profile/v14b-batch-progress.md)

v13 的 owner 排队仅 **10.41 ms**；owner 批内执行 **138.23 ms**，其中逐笔 `journal.apply` 为 **137.53 ms**。进一步合批应针对 SET 内的 shard 投递与等待，先区分其与实际写入的耗时，不能把整个 apply 或 worker 排队作为可节省预算。 [v13 复核](profile/v13-fetch-borrow.md#trace-复核)

<a id="ticket-completion-review"></a>

### P1-3 按 applied offset 批量完成票据（已进入新架构首版）

v13 的普通异步 SET 每笔创建一个 `raft-set-confirm` fiber，醒来后逐笔回查 worker。其 **1024 笔写对应 124 次 apply**，应用后确认 P95 为 **0.744 ms**；16 个 pipeline 批尾额外等待最大 **95.3 µs**、合计 **0.604 ms**。这些历史数据支持减少执行上下文和回查，尚不能单独证明吞吐收益。 [v13 Trace](http://127.0.0.1:8787/#/v13/trace)

1. 追加成功后，组内队列按 `(epoch, end_offset)` 登记并持有票据。登记与检查 applied 进度由同一组 worker 串行处理，覆盖应用先完成的情况。
2. 组仍为同任期 Leader 时，状态机推进连续 applied 前缀就立即批量完成已覆盖的票据。按连接所在 proactor 合并通知，连接按 FIFO 回复，票据释放时归还准入额度。
3. 每组只为最早的截止时间安排一个定时器。超时、换主、存储失败和关闭统一收尾，每个票据只完成一次。

**新架构首版已移除普通 SET 的逐笔确认 fiber 和 applied 回查。** 每组发布 `R=min(本地确认位置, HW, applied)`，同任期记录按共享水位完成，连接只订阅队首并按 FIFO 回复；组内一个定时器处理最早截止时间。Folly 直接处理组 I/O 和节点 RPC，测试 RESP 后端也改为 io_uring，不再沿用 epoll。宿主引擎的连接唤醒仍使用原有通知接口，未增加逐笔 `MSG_RING`。这是架构组合改动，性能变化不能单独归因于 P1-3。 [新结构与实现边界](raft-batch-event-loop.md) · [P1-3 历史复核](profile/p1-3-review.md)

<a id="follower-received-cache"></a>

### P1-4 follower 复用已验证批次

校验和追加完成后将不可变接收缓冲交给有界 effect 缓存。普通 SET/no-op 借用视图，缺口、控制记录、恢复与超预算走 WAL 重读。截断、换任期和快照使缓存失效。 [v7 接收缓存](profile/v7-follower-cache.md)

### P1-5 SET 输入所有权转交

普通异步 SET 接管已解析参数，drain 完成后释放大缓冲，准入额度在有序回复后归还。提交排队按 8 MiB / 256 笔约束，每个数据副本的 effect 缓存另有 8 MiB / 4096 条上限。全流程预算尚待统一，按唯一缓冲的实际 capacity 统计在途内存，并分别观察分配、fiber 栈和 RSS。 [v9 所有权转交](profile/v9-input-ownership.md#input-ownership)

<a id="memory-flush-separation"></a>

### P1-6 memory 后台同步与普通追加并行

同任期、同段的普通写可在后台同步期间继续编码和追加。每轮同步按 64 MiB / 100 ms 条件触发，持久标记只发布开始时捕获的恢复切点。换主、控制记录、换段、截断、快照、关闭与 I/O 失败保留相应屏障。 [v10 实验](profile/v10-memory-flush-overlap.md) · [v9 Trace 复核](profile/v9-trace-review.md)

### P1-7 Fetch 直接封帧与借用解码

按 schema 计算字段长度后一次预留，直接写完整响应，去掉 body 到 frame 的中间复制和尾部扩容复制。共享 parked Fetch 缓冲或分散发送仍需评估所有权、部分发送、反压和内存预算。 [v8 Fetch 封帧](profile/v8-fetch-frame.md)

**v12：Leader 分散发送 records，收益待确认，代码暂回退。** 三轮吞吐 **467.87 / 266.90 / 464.05 MiB/s**，中位数较 v10 **+0.7%**；均值 **−10.7%** 主要被第二轮拉低。另两轮略高于 v10 中位数，但 P95 一轮改善、一轮变差，不能直接判定方案导致退化，也不能排除慢轮后宣称稳定收益。第二轮 SET 阶段为 **958.84 ms**，末尾 WAIT 仅 **0.30 ms**；正式轮没有独立 Trace，不能用另跑的诊断解释该轮慢因。

独立诊断的 Leader CPU **510 → 470 ms**，但 WAL 提交 **153 → 176 次**、worker 排队 **211.95 → 265.10 ms**。再次出现局部 CPU 改善与整体收益不一致。后续复试优先查慢轮与批次节奏；Follower 解码单独在 v13 测试。补丁、二进制、三轮原始数据和诊断均保留。 [v12 实验](profile/v12-fetch-scatter.md)

**v13：Follower 借用 records 解码，保留实现。** 大 Fetch 的 records 引用不可变接收缓冲，WAL 校验与 effect 缓存共用其所有权，按整个接收缓冲容量计入缓存预算；小响应保留原路径。相对 v10，三轮吞吐中位数 **460.88 → 486.54 MiB/s（+5.6%）**、均值 **+5.2%**，P95 中位数 **44.01 → 45.25 ms（+2.8%）**。独立诊断 Follower CPU **256 → 200 ms**、memcpy 样本 **36 → 20**，1024 笔写全部命中缓存；Leader 排队 **211.95 → 263.28 ms**，仍需优化。结论是减少复制带来了本负载的吞吐收益，不能宣称尾延迟改善或把单次诊断变化全归因于此改动。 [v13 实验](profile/v13-fetch-borrow.md)

## 验证口径

- 正式测量用未插桩 Release，默认 1 连接、pipeline 64。独立诊断用来定位等待和 CPU 热点，正式吞吐和 P95 用来判断收益。
- 正确性核对原连接 `WAIT 1`、离线副本重启、`WAIT 2` 和全量读回。按改动覆盖同 key、条件写、段边界、慢副本、换主、截断与故障注入。共享路径同时验证 `flush`。
- 小范围优化试验无稳定收益时回退代码，保留平台指标与实验结论。[批次事件驱动新架构](raft-batch-event-loop.md#9-实验推进约定)按用户约定持续迭代：先出第一版测试，性能不佳继续在当前实验实现上分析、优化，不自动回退代码。

<a id="experiments"></a>

## 实验索引

各实验文件保留实现与历史验证结论。逐轮指标、日志和诊断在平台按实验 ID 查询。 [实验列表](http://127.0.0.1:8787/)

- [v3 初始基线与诊断](profile/v3-baseline.md)
- [v4 共享 WAL 缓冲与直接编码](profile/v4-wal-buffer.md)
- [v5 批量应用与借用参数](profile/v5-batched-apply.md)
- <a id="v6-drain-experiment"></a>[v6 直接聚合编码与 effect/WAL 共享](profile/v6-drain-sharing.md)
- <a id="v7-follower-experiment"></a>[v7 follower 复用接收批次](profile/v7-follower-cache.md)
- <a id="v8-frame-experiment"></a>[v8 Fetch 直接封帧](profile/v8-fetch-frame.md)
- <a id="v9-input-experiment"></a>[v9 SET 输入所有权转交](profile/v9-input-ownership.md)
- <a id="p0-1b-receipt-experiment"></a>[P0-1b follower 接收确认快路径（已回退）](profile/p0-1b-follower-receipt.md)
- <a id="v10-flush-overlap-experiment"></a>[v10 memory 后台同步与普通追加并行](profile/v10-memory-flush-overlap.md)
- [v11 Leader WAL 追加复用编码结果（已回退）](profile/v11-leader-wal-append.md)
- [v12 Fetch records 分散发送（收益待确认，暂回退）](profile/v12-fetch-scatter.md)
- [v13 Follower Fetch records 借用解码](profile/v13-fetch-borrow.md)
- [v14 Folly io_uring 批次事件循环首版](profile/v14-batch-event-loop.md)
- [v14b Folly io_uring 批次事件循环（首版修正）](profile/v14b-batch-progress.md)
