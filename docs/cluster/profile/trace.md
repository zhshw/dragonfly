# Raft OTEL traces

C++ 在 Raft 执行现场记录 spans/events，计算请求属性，直接上报 Langfuse；CLI 和 Web 共用查询服务。具体分析命令、查询结果与优化依据见 [v16 分析](v16-wal-encode-overlap.md#1-分析查询)。

## 数据流

```text
Raft C++ → 有界内存缓冲与延后定稿 → OTEL trace exporter → Langfuse
                                                           ↓
                                              Go HTTP API → CLI / Web
```

- [trace.h / trace.cc](../../../src/server/cluster/raft/trace.h) 定义事件、属性及计算边界；[otel](../../../src/server/cluster/raft/otel) 负责标准 traces 编码与发送。
- 请求 wait、overlap、bytes、counts 在 C++ 完成处计算一次。查询层读取已存属性，过滤、排序和关联证据；事件耗时使用自身起止边界，不重新配对生成请求数值。
- Langfuse 保存 Trace，Profile PostgreSQL 管理实验和 capture 引用，无需 CSV、timeline JSON 或中间导入步骤。
- CLI 和 Web 均调用 `/api/v1/experiments/{id}/captures`、`/requests`、`/events`，共用 `pkg/httpapi` → `pkg/langfuse.QueryNative/RankNative` 和同一 DTO。

## 名称、单位与关联

保留已有 `raft.fetch.rpc`、`raft.fetch.receive`、`raft.await_io`、`raft.folly.fsync`、`raft.set.admission.wait` 等名称。schema 3 的耗时属性统一为 **ms**，字节和计数为整数；原始 `start_ns/end_ns` 仅保留精确边界。CLI/Web 按完整精度筛选，默认显示三位小数。定义和单位随 capture manifest 发布。

| 事件或属性 | 边界 / 含义 |
| --- | --- |
| `raft.set.admission.wait` | 首次额度拒绝到成功准入 |
| `raft.set.admission.retired_tickets` / `full_wave_retired` | 等待期间实际退役的票据数 / 当时在途票据是否全部退役 |
| `raft.set.admission.wal_idle` | max(0, wait.end − max(first_retirement, last_wal_completion))，限定本 connection/group |
| `raft.fetch.rpc` | RPC 发出到返回 |
| `raft.fetch.return_wait` | RPC 返回到进入响应处理，包含没有活动事件的空档 |
| `raft.fetch.receive` / `raft.fetch` | 响应处理耗时 / RPC 发出到处理完成的总耗时 |
| `raft.fetch.flush_gate.ready/blocked` | 实际 gate 判断、拒绝原因和 flush ID |
| `raft.fetch.await_flush` | gate 首次拒绝到允许；重试不重置起点 |
| `raft.await_io` | 返回空档内，请求真正等待 I/O 的区间并集 |
| `raft.fetch.fsync_overlap` / `io_fsync_overlap` | 返回空档与同 group fsync 的交集 / 再与 await_io 相交；重叠区间不重复累计 |
| `raft.folly.fsync` / `raft.flush` | 同步提交到完成回调 / 整个后台 flush 生命周期 |
| `raft.fetch.payload_bytes` / `appended_bytes` / `appended_records` | 响应字节 / 实际成功追加的完整前缀字节 / 记录数 |
| `raft.fetch.partial` | 成功追加且 appended_bytes < payload_bytes |
| `raft.fetch.report` / `raft.hw.advance` / `raft.reply.advance` | 报告追加位置 / Leader HW 推进 / 回复位置推进 |

`raft.fetch = rpc + return_wait + receive`；gate、await_io 和 fsync overlap 可互相重叠，不能再加到总耗时。`return_wait`、`await_flush` 和 gate 原因是新增显式证据，不是历史事件的改名。

关联使用 capture、process instance、group、epoch、request、event、io、flush 等明确身份；共享 fsync 只保存一次。时间相交与真实等待依赖分别标记，不能通过“时间最近”猜配对，也不能相减不同机器的单调时钟。缺失或未完成值保留 `Unsupported`、`Incomplete`，只有完整观测到的零值才写 0。

## 采集与完整性

诊断构建启用 `WITH_RAFT_OTEL=ON`、`WITH_RAFT_FOLLY=ON`。启动前配置 `RAFT_TRACE_CAPTURE_ID`、`RAFT_TRACE_EXPERIMENT`、`RAFT_TRACE_RELEASE`、`RAFT_TRACE_BINARY_SHA256`、`RAFT_TRACE_WORKLOAD`、`RAFT_TRACE_NODES`；通过 `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` 和 `OTEL_EXPORTER_OTLP_TRACES_HEADERS` 指定 traces 地址与认证，凭据由服务配置提供。未配置 capture 时不启用采集。

现场只记录边界和数值；请求完成后定稿，关联 I/O 稍后完成时补齐原事件。当前 session 覆盖进程生命周期，每进程最多 16 个 stream，每 stream 最多 32 MiB。退出时先排空连接和 Raft/I/O，再通过既有 fiber HTTP client 批量上报，完成清单最后发送。尚无动态开始/停止命令；缓冲溢出、进程崩溃或发送失败可能留下不完整 capture。

查询读完全部分页并校验预期 stream、事件数量、丢弃数及 schema 后才返回 `complete=true`。Top 10 在完整读取后计算，不能分别截取 RPC、receive 和 I/O 再拼请求。原始大整数与毫秒小数按十进制字符串保留精度。

## 查询约定

- **Slow path**：整轮所有事件按 duration 降序取 Top 10；event name、node、group 和 `-min-duration-ms` 均为可选过滤。点击编号或事件名展开关联时序。
- **Fetch delay**：按已存 `raft.fetch.return_wait` 排序，展示 RPC、返回空档、gate 和 I/O 证据。页面初始过滤 ≥ 10 ms；CLI 使用 `-min-return-wait-ms`，默认 0。
- `-capture` 固定轮次，`-selected TRACE_ID:SPAN_ID` 选择详情，`-json` 输出同一 API DTO。可复查命令集中在 [v16 文档](v16-wal-encode-overlap.md#1-分析查询)。
- Node / Event name 下拉来自整轮 capture。节点显示角色和编号，优先采用显式角色，否则依据 Fetch 的 group/epoch 身份；无法确定时显示 `Unknown`。角色切换且缺少任期时保留本轮全部角色，不将 fsync 文件类型当节点角色。

## Langfuse 与 Metrics

当前使用 **OTEL traces**，标准 [OTEL Metrics](metrics.md) 延后。Langfuse [官方说明](https://langfuse.com/faq/all/opentelemetry-logs-metrics-span-links) 明确 metrics 请求会被接受但丢弃；2026-11-26 核对的部署版 v4.83.0、发布版 v4.86.0 和当时 main 均未实现 metrics 存储。其 Metrics API 是已存 Trace/Score 的聚合查询。当前请求数值属于 span attributes。
