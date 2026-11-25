# Raft OTEL Metrics（后续阶段）

更新日期：2026-11-26。工作分支：`profile/v15-replication-trace`。

当前 v15 的准入空档与 Follower flush 阻塞分析采用 **OTEL traces**。数据与查询契约见 [trace.md](trace.md)，分析过程见 [v16 文档](v16-wal-encode-overlap.md)。

本阶段不实现标准 OTEL Metrics：不接入 Meter/Counter/Histogram/MetricReader，不部署 metrics Collector，不新增 ClickHouse metrics 表。请求的 wait、overlap、bytes、counts 是 **span attributes**，不称为 OTEL Metrics。未完成的 MetricsSink 接入草稿已移除。

后续需要持续统计吞吐、错误率、耗时分布时，再单独定义 metrics instruments、单位、低基数标签、聚合方式和存储方案。Langfuse 的 Metrics API 是 Trace/Score 聚合接口；其 OTEL metrics 接口接受但丢弃数据，不能作为标准 metrics 存储端。支持范围见 [Trace 文档](trace.md#langfuse-与-metrics)。

后续 metrics 应复用 C++ 已有完成边界和数值，避免重新从 Trace 文件推导；无论采用何种聚合，都不能替代单个慢请求的 spans/events 证据。
