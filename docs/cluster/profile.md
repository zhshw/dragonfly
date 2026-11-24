# Raft 复制 Profile

正式 benchmark、逐轮日志、正确性验证、Trace 和火焰图以 `profile` 平台的归档为准。本文保留当前结果和优化结论。

## 当前结果

取平台总览的三轮中位数。Raft 使用 `memory` ack，负载为 1 连接、pipeline 64、256 MiB、256 KiB/value，计时包含 SET 和原写连接上的 `WAIT 1`。

| 平台记录 | 吞吐（MiB/s） | 批次 P95（ms） |
| --- | ---: | ---: |
| 基线 v3 | 169.56 | 113.75 |
| 上一轮 v10（P1-6） | 460.88 | 44.01 |
| 本轮 v13（P1-7） | **486.54** | **45.25** |
| 对比参照 `v10:cluster` | 605.47 | 34.63 |

v13 较 v3 累计吞吐 **+186.9%**，达到原 cluster 参照的 **80.4%**。较 v10 归档吞吐中位数 **+5.6%**、算术均值 **+5.2%**，批次 P95 中位数 **+2.8%**，尾延迟未改善。v11/v12 代码未合入，本轮是对 v10 的独立增量；三轮跨轮对照不代表所有负载。 [v13 平台总览](http://127.0.0.1:8787/#/v13/overview)

## 使用 profile

1. 在仓库根目录运行 `make profile` 启动 Langfuse 和 Web。 [打开 profile](http://127.0.0.1:8787/)
2. 点击「创建实验」，选择上一轮，并确认基线与同负载的对比参照。点击「运行 Benchmark」，平台运行本轮三次正式测试和独立诊断，复用已选对照的归档。
3. 在「运行记录」查看三轮算术均值 Diff、合并日志和本轮验证。在「火焰图」和「Trace」对照上一轮与本轮，原 cluster 火焰图从总览的「查看基准诊断」进入。用「复制实验摘要」提取分析所需的指标。

### 用 CLI 运行 benchmark

创建新实验 `v14`，选 v13 为上一轮，并沿用其基线与对比参照。

```sh
profile create -id v14 -title '本轮优化' -previous v13
profile run -id v14
profile show -id v14
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
| 应用 | replica flow 消费 journal 并执行 | 组 worker → owner 批量 → 命令分发 → shard |
| 重放数据 | 接收的 journal 数据 | 普通 effect 复用不可变 WAL 缓冲，缺口从 WAL 重放 |

两套集群设计独立。Raft 的任期、日志顺序、HW 和恢复边界仍由 Raft 负责。

## 下一轮优先项

1. **P0-2 评估 shard 内合批。** v13 Leader worker 排队区间并集为 **263.28 ms**，apply 为 **159.61 ms**，两者会重叠。owner 已经批量接收，批内仍逐笔走 `ExecuteSetEffect` 和 shard 调度。先验证减少逐笔调度能否缩短 apply 和 worker 排队。 [v13 Trace](http://127.0.0.1:8787/#/v13/trace)
2. **Leader 分散发送复试前先查慢轮。** v12 吞吐中位数 **+0.7%**，第二轮明显偏慢，稳定收益尚未确认，代码暂回退。Follower 借用解码已在 v13 实现，吞吐 **+5.6%**，P95 未改善；继续减少复制不能替代对排队和合批节奏的分析。 [v12 实验](profile/v12-fetch-scatter.md) · [v13 实验](profile/v13-fetch-borrow.md)

## 优化项与当前状态

<a id="wal-buffer-remaining"></a>
<a id="drain-preallocation"></a>

### P0-1 共享 WAL 缓冲与直接编码

单条 SET 直接编码。drain 按长度、段边界和预算选定连续前缀，一次预留并写入最终缓冲，冻结后由 I/O 与 effect 共享。P0-1a 的 drain 合批缓冲预分配随直接聚合实现，收益按组合改动记录。 [v4 共享缓冲](profile/v4-wal-buffer.md) · [v6 直接聚合](profile/v6-drain-sharing.md)

**Leader WAL 追加复用编码结果：v11 已试验并回退。** 追加准备区间并集 **41.95 → 0.99 ms**，但正式吞吐 **−0.3%**，批次 P95 **+19.1%**。相同 1024 笔写的提交次数 **153 → 177**，平均合批 **6.69 → 5.79 笔**，worker 排队 **211.95 → 213.22 ms**。处理变快可能改变 drain 收集窗口，单次诊断尚不能证明因果；各阶段时间也不能相加。**热点消失不等于吞吐收益，复试前先核实合批变小的原因，同时检查提交次数、关键排队和正式 P95。** [v11 实验](profile/v11-leader-wal-append.md)

<a id="follower-receipt-progress"></a>

### P0-1b follower 接收确认快路径（已回退）

提前 Fetch 缩短了接收到发出下一次请求的等待，正式吞吐和 P95 未获益。普通追加完成后，下一次 Fetch 仍经过 group worker 串行应用。独立、有界且按顺序推进 applied 的异步应用队列待验证，需覆盖换主、控制记录、截断、快照、迁移和存储失败。 [提前 Fetch 实验](profile/p0-1b-follower-receipt.md)

### P0-2 批量应用与借用 SET 参数

已实现最多 32 笔 / 1 MiB 的 owner 批量投递，按 1 ms 预算返回已完成前缀，SET 同步借用 key/value。下一步评估 shard 内合批，减少逐笔分发和跨线程等待。验证需覆盖 DbSlice 与事务行为、同 key 顺序、控制记录、迁移与快照边界，以及部分失败时连续 applied 前缀。 [v5 批量应用](profile/v5-batched-apply.md)

<a id="ticket-completion-review"></a>

### P1-3 按 applied offset 批量完成票据（备选）

普通异步 SET 每笔创建一个 `raft-set-confirm` fiber，订阅提交变化、按截止时间等待，醒来后逐笔回查 worker。v10 的 **1024 笔写对应 139 次 apply**。按 offset 对齐后，apply 完成到 `WaitApplied` 返回的 P95 为 **1.63 ms**，16 个 pipeline 批尾的额外等待最大为 **31 µs**。`WaitApplied` 在 Leader 栈中可识别到 **2 / 255 个 CPU 样本**。P1-3 的主要目标是减少 fiber、回查和唤醒。 [确认路径](../../src/server/cluster/raft/runtime/data_node.cc#L598) · [v10 Trace](http://127.0.0.1:8787/#/v10/trace)

1. 追加成功后，组内队列按 `(epoch, end_offset)` 登记并持有票据。登记与检查 applied 进度由同一组 worker 串行处理，覆盖应用先完成的情况。
2. 组仍为同任期 Leader 时，状态机推进连续 applied 前缀就立即批量完成已覆盖的票据。按连接所在 proactor 合并通知，连接按 FIFO 回复，票据释放时归还准入额度。
3. 每组只为最早的截止时间安排一个定时器。超时、换主、存储失败和关闭统一收尾，每个票据只完成一次。

**第一版复用现有 proactor 唤醒即可。** applied 事件触发成功确认，组级定时器处理超时。`MSG_RING` 需要发送方也在支持该操作的 io_uring proactor 上，普通 worker 发出的唤醒可走现有 eventfd 路径。先沿用 v10 的 epoll 连接路径，测每千笔回查与唤醒次数、应用后确认 P95 和正式吞吐。合批后若唤醒仍是热点，再测 io_uring 的增量。 [应用进度](../../src/server/cluster/raft/runtime/group.cc#L412) · [唤醒实现](../../helio/util/fibers/uring_proactor.cc#L1072) · [P1-3 历史复核](profile/p1-3-review.md)

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
- 无稳定收益的试验回退代码，保留平台指标与实验结论。

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
