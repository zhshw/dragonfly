# Raft 复制 Profile

正式 benchmark、逐轮日志、正确性验证、Trace 和火焰图以 `profile` 平台的归档为准。本文保留当前结果和优化结论。

## 当前结果

取平台总览的三轮中位数。Raft 使用 `memory` ack，负载为 1 连接、pipeline 64、256 MiB、256 KiB/value，计时包含 SET 和原写连接上的 `WAIT 1`。

| 平台记录 | 吞吐（MiB/s） | 批次 P95（ms） |
| --- | ---: | ---: |
| 基线 v3 | 169.56 | 113.75 |
| 本轮 v10（P1-6） | **460.88** | **44.01** |
| 对比参照 `v10:cluster` | 605.47 | 34.63 |

v10 较 v3 累计吞吐 **+171.8%**，达到原 cluster 参照的 **76.1%**。较上一轮 v9 归档吞吐 **+18.2%**、批次 P95 **−32.6%**，为跨轮对照。 [v10 平台总览](http://127.0.0.1:8787/#/v10/overview)

## 使用 profile

1. 在仓库根目录运行 `make profile` 启动 Langfuse 和 Web。 [打开 profile](http://127.0.0.1:8787/)
2. 点击「创建实验」，选择上一轮，并确认基线与同负载的对比参照。点击「运行 Benchmark」，平台运行本轮三次正式测试和独立诊断，复用已选对照的归档。
3. 在「运行记录」查看三轮算术均值 Diff、合并日志和本轮验证。在「火焰图」和「Trace」对照上一轮与本轮，原 cluster 火焰图从总览的「查看基准诊断」进入。用「复制实验摘要」提取分析所需的指标。

### 用 CLI 运行 benchmark

创建新实验 `v11`，选 v10 为上一轮，并沿用其基线与对比参照。

```sh
profile create -id v11 -title '本轮优化' -previous v10
profile run -id v11
profile show -id v11
```

`run` 等待本轮 benchmark 和诊断结果入库，`show` 查看三轮正式数据。需要独立测量时，在执行机用 `profile run -config FILE`，配置中的 `output` 指向新目录。 [CLI 参数](../../tools/profile/README.md#cli)

### Trace 和火焰图分析示例

比较平台中 v9 与 v10 的本轮独立诊断。两轮使用相同负载，先筛选 leader 的写入等待，再按叶子函数查看 CPU 热点。`trace` 和 `flamegraph` 默认读取本轮诊断，可用 `-run` 指定诊断运行 ID。

```sh
for version in v9 v10; do
  profile trace -id "$version" -run diagnostic-current -role leader \
    -stage raft.set.await -sort p95 -limit 5
  profile flamegraph -id "$version" -run diagnostic-current -role leader \
    -view leaf -limit 5
done
```

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

## 优化项与当前状态

<a id="wal-buffer-remaining"></a>
<a id="drain-preallocation"></a>

### P0-1 共享 WAL 缓冲与直接编码

单条 SET 直接编码。drain 按长度、段边界和预算选定连续前缀，一次预留并写入最终缓冲，冻结后由 I/O 与 effect 共享。P0-1a 的 drain 合批缓冲预分配随直接聚合实现，收益按组合改动记录。 [v4 共享缓冲](profile/v4-wal-buffer.md) · [v6 直接聚合](profile/v6-drain-sharing.md)

<a id="follower-receipt-progress"></a>

### P0-1b follower 接收确认快路径（已回退）

提前 Fetch 缩短了接收到发出下一次请求的等待，正式吞吐和 P95 未获益。普通追加完成后，下一次 Fetch 仍经过 group worker 串行应用。独立、有界且按顺序推进 applied 的异步应用队列待验证，需覆盖换主、控制记录、截断、快照、迁移和存储失败。 [提前 Fetch 实验](profile/p0-1b-follower-receipt.md)

### P0-2 批量应用与借用 SET 参数

已实现最多 32 笔 / 1 MiB 的 owner 批量投递，按 1 ms 预算返回已完成前缀，SET 同步借用 key/value。下一步评估 shard 内合批，减少逐笔分发和跨线程等待。验证需覆盖 DbSlice 与事务行为、同 key 顺序、控制记录、迁移与快照边界，以及部分失败时连续 applied 前缀。 [v5 批量应用](profile/v5-batched-apply.md)

<a id="ticket-completion-review"></a>

### P1-3 按 applied offset 批量完成票据（备选）

v10 的 P1-6 已允许 `WaitApplied` 在后台同步期间查询本地 applied 进度。每笔票据仍独立回查。可按 `(epoch, end_offset)` 登记票据，在连续 applied 前缀推进时批量完成，重点验证完成等待、准入额度和调度成本，以及换主、失败、超时与关闭时的票据收尾。 [P1-3 复核](profile/p1-3-review.md)

<a id="follower-received-cache"></a>

### P1-4 follower 复用已验证批次

校验和追加完成后将不可变接收缓冲交给有界 effect 缓存。普通 SET/no-op 借用视图，缺口、控制记录、恢复与超预算走 WAL 重读。截断、换任期和快照使缓存失效。 [v7 接收缓存](profile/v7-follower-cache.md)

### P1-5 SET 输入所有权转交

普通异步 SET 接管已解析参数，drain 完成后释放大缓冲，准入额度在有序回复后归还。提交排队按 8 MiB / 256 笔约束，每个数据副本的 effect 缓存另有 8 MiB / 4096 条上限。全流程预算尚待统一，按唯一缓冲的实际 capacity 统计在途内存，并分别观察分配、fiber 栈和 RSS。 [v9 所有权转交](profile/v9-input-ownership.md#input-ownership)

<a id="memory-flush-separation"></a>

### P1-6 memory 后台同步与普通追加并行

同任期、同段的普通写可在后台同步期间继续编码和追加。每轮同步按 64 MiB / 100 ms 条件触发，持久标记只发布开始时捕获的恢复切点。换主、控制记录、换段、截断、快照、关闭与 I/O 失败保留相应屏障。 [v10 实验](profile/v10-memory-flush-overlap.md) · [v9 Trace 复核](profile/v9-trace-review.md)

### P1-7 Fetch 直接封帧

按 schema 计算字段长度后一次预留，直接写完整响应，去掉 body 到 frame 的中间复制和尾部扩容复制。共享 parked Fetch 缓冲或分散发送仍需评估所有权、部分发送、反压和内存预算。 [v8 Fetch 封帧](profile/v8-fetch-frame.md)

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
