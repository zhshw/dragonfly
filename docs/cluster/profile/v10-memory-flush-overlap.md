# v10：memory 后台同步与普通追加并行

[Profile 索引](../profile.md#memory-flush-separation) · [v9 输入所有权转交](v9-input-ownership.md)

**P1-6 已保留。** 将追加与后台同步分开管理，同任期、同一段的普通 memory 写可在 fsync 期间继续编码、追加、复制和确认。

- 同步只发布启动时捕获的日志位置，新追加保留自己的字节和时间计数。任一 I/O 失败后持续拒绝写入。
- 维持 64 MiB / 100 ms 同步条件、队列预算和应用后回复语义。换主、控制记录、换段、截断、快照及关闭保留相应屏障。

拆分后台同步与普通追加前的等待证据见 v9 诊断。 [v9 Trace 复核](v9-trace-review.md)

## 平台归档

平台的 v10 本轮采用重测数据，上一轮直接引用已归档的 v9。正式吞吐、P95、逐轮日志和诊断在平台核对。 [v10 总览](http://127.0.0.1:8787/#/v10/overview) · [运行记录](http://127.0.0.1:8787/#/v10/runs) · [Trace](http://127.0.0.1:8787/#/v10/trace) · [火焰图](http://127.0.0.1:8787/#/v10/flamegraphs)

## 首次同轮验证

Ubuntu 同机未插桩 Folly Release，1 连接、pipeline 64、256 MiB。三轮中位数对照 v9 当时的复测，确认吞吐 **377.21 → 437.97 MiB/s（+16.1%）**，批次 P95 **60.19 → 53.06 ms（−11.8%）**。三轮吞吐均提高，第二轮 P95 略升。

独立诊断在本节点 fsync 期间观察到 **27 次 leader 编码和 17 次 follower 追加**，改前均为零。两节点 fsync 区间并集接近，leader worker 排队仍较多，峰值 RSS 未见整体改善。 [P1-6 原始诊断](http://172.31.23.2:3137/memory-overlap-c1-p64-v11/index.html)

Debug / Release / ASAN+UBSAN 各 86 项相关单测，Debug 和 sanitizer 各 14 项集群回归通过。覆盖并发完成顺序、短写、两类 I/O 失败、固定切点崩溃恢复及新尾部计数。首次同轮测试与诊断均通过原连接 `WAIT 1`、离线副本恢复、`WAIT 2` 和全量读回。

原始 benchmark、验证日志和复现资料在本地及测试机的 `docs/cluster/benchmarks/profile-flush-overlap-20261124/`，由 Git 忽略。 [本地 A/B 命令](../profile.md#local-benchmark)
