# Raft 复制 Profile

逐轮 benchmark、验证日志、Trace 和火焰图按实验 ID 查询。 [实验平台](http://172.31.23.2:8787/)

[Raft 原生 OTEL traces](otel/trace.md)：C++ 现场计算请求属性，记录等待与 I/O 证据，直接上报 Langfuse，CLI/Web 共用查询；保留准入空档和 flush gate 两个强制复现场景。[OTEL Metrics](otel/metrics.md) 延后。

<a id="experiments"></a>

## 实验索引

- [v3 初始基线与诊断](profile/v3-baseline.md)
- <a id="wal-buffer-remaining"></a>[v4 P0-1 共享 WAL 缓冲与直接编码](profile/v4-wal-buffer.md)
- [v5 P0-2 批量应用与借用参数](profile/v5-batched-apply.md)
- <a id="v6-drain-experiment"></a><a id="drain-preallocation"></a>[v6 P0-1 直接聚合编码与 effect/WAL 共享](profile/v6-drain-sharing.md)
- <a id="v7-follower-experiment"></a><a id="follower-received-cache"></a>[v7 P1-4 Follower 复用接收批次](profile/v7-follower-cache.md)
- <a id="v8-frame-experiment"></a>[v8 P1-7 Fetch 直接封帧](profile/v8-fetch-frame.md)
- <a id="v9-input-experiment"></a>[v9 P1-5 SET 输入所有权转交](profile/v9-input-ownership.md)
- <a id="p0-1b-receipt-experiment"></a><a id="follower-receipt-progress"></a>[P0-1b Follower 接收确认快路径（已回退）](profile/p0-1b-follower-receipt.md)
- <a id="v10-flush-overlap-experiment"></a><a id="memory-flush-separation"></a>[v10 P1-6 memory 后台同步与普通追加并行](profile/v10-memory-flush-overlap.md) · [v9 Trace 复核](profile/v9-trace-review.md)
- [v11 Leader WAL 追加复用编码结果（已回退）](profile/v11-leader-wal-append.md)
- [v12 Fetch records 分散发送（暂回退）](profile/v12-fetch-scatter.md)
- [v13 P1-7 Follower Fetch records 借用解码](profile/v13-fetch-borrow.md)
- [v14 Folly io_uring 批次事件循环首版](profile/v14-batch-event-loop.md)
- [v14b Folly io_uring 批次事件循环首版修正](profile/v14b-batch-progress.md)
- [v15 P0-2 已提交 SET 的 shard 合批](profile/v15-shard-apply.md)
- [v16 Raft SET 批量补发与 Follower 并发 flush](profile/v16-wal-encode-overlap.md)
- [v17 P1-8 数据组按 I/O 就绪事件推进](profile/v17-group-scheduling.md)

## 专题索引

- <a id="ticket-completion-review"></a>[P1-3 按 applied offset 批量完成票据](profile/p1-3-review.md)
- [批次事件循环的设计与验证](raft-batch-event-loop.md)
- [历史 benchmark](replication-benchmark.md) · [初始火焰图与 Trace](replication-observability.md)

## 使用 profile

1. 在实验主机的仓库根目录运行 `make profile`，启动 Web、Langfuse 和宿主机执行器。 [打开 profile](http://172.31.23.2:8787/)
2. 点击「创建实验」，选择上一轮，并确认基线与同负载的对比参照。点击「运行 Benchmark」，平台运行本轮三次正式测试和独立诊断，复用已选对照的归档。
3. 在「运行记录」查看三轮算术均值 Diff、合并日志和本轮验证。在「火焰图」和「Trace」对照上一轮与本轮，原 cluster 火焰图从总览的「查看基准诊断」进入。用「复制实验摘要」提取分析所需的指标。

### 用 CLI 运行 benchmark

以 v14b 为上一轮，沿用其基线与对比参照。执行前将示例 ID 换成未使用的实验 ID。

```sh
experiment_id=raft-set-example
profile create -id "$experiment_id" -title 'Raft 已提交 SET 的 shard 合批' -previous v14b
profile run -id "$experiment_id"
profile show -id "$experiment_id"
```

`run` 等待本轮 benchmark 和诊断结果入库，`show` 查看三轮正式数据。需要独立测量时，在执行机用 `profile run -config FILE`，配置中的 `output` 指向新目录。 [CLI 参数](../../tools/profile/README.md#cli)

### Trace 和火焰图分析示例

查看 v10 应用完成到写入确认返回的延迟。 [v10 Trace](http://172.31.23.2:8787/#/v10/trace)

```sh
profile trace -id v10 -analysis apply-to-confirm
```

先看 v10 Leader 的叶子热点，再用 `-symbol` 按上游调用路径拆分同一个热点。

```sh
profile flamegraph -id v10 -run diagnostic-current -role leader -view leaf -limit 5
profile flamegraph -id v10 -run diagnostic-current -role leader \
  -symbol 'CRC32::Extend' -caller-depth 8
```

`CRC32::Extend` 的 55 个样本中，**31 个**来自 `AppendSingleRecordBatch` 编码，**24 个**来自 `TryAppendAsync` 的 WAL 追加校验。换成 `-symbol '__memcpy_avx'` 可用同样的方法定位复制来源。 [v10 火焰图](http://172.31.23.2:8787/#/v10/flamegraphs)

### 汇总本地归档

指定正式 A/B 的输出目录和两份独立诊断报告，汇总吞吐、批次 P95、关键等待和各节点 CPU 热点。 [CLI 汇总参数](../../tools/profile/README.md#汇总本地归档)

```sh
profile summarize -compare "$compare_dir" \
  -before "$baseline_report" -after "$current_report" -out "$summary_dir"
```
