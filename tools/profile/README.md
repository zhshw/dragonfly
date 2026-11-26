# profile

`profile` 提供 benchmark 归档、Trace 查询和 CPU 火焰图分析。当前内置测量流程为 Dragonfly Raft benchmark。

请求分析采用 [Raft OTEL traces](../../docs/cluster/otel/trace.md)：C++ 记录事件并计算请求属性，直接上报 Langfuse，CLI/Web 共用查询。分析示例见 [v16](../../docs/cluster/profile/v16-wal-encode-overlap.md#1-分析查询)。OTEL Metrics 延后。

实验、对照选择、benchmark 轮次、验证结果和诊断指标保存在 PostgreSQL。
原生 Trace 保存在 Langfuse；capture 引用、火焰图和验证日志关联到实验，文件存放在数据盘。历史时间线归档仍可读取。

| 比较 | 计算 | 来源 |
| --- | --- | --- |
| 较上一轮 | 本轮 ÷ 上一轮 − 1 | 上一轮实验的正式指标 |
| 累计收益 | 本轮 ÷ 基线 − 1 | 选定的历史 benchmark |
| 参照达成率 | 本轮 ÷ 对比参照 | 选定的同负载 benchmark，例如原 Cluster |

创建实验时选定上一轮，可沿用该实验的基线与对比参照。每项选择会保存测试口径、版本和指标快照。
v10 的上一轮使用 v9 的历史数据。旧实验按原有版本关系关联上一轮归档。总览的耗时卡片突出批次 P95 较上一轮的变化百分比，下方显示两轮各自的 P95 三轮中位数。
总览、运行记录、火焰图、Slow 分析和 Trace 都使用选定版本的数据。首个版本没有上一轮时，火焰图和 Trace 展示本轮与所选对比参照。
Slow 分析默认打开 Slow path，可切换 Fetch delay；先排序事件或请求，再展开关联时序。Trace 页按事件名和压测 data group 的节点角色对齐两轮数据。每行展示事件数、耗时并集和组内所有事件的 P95。统计裁剪到各轮测量窗口；耗时并集按进程计算后相加，零耗时事件也计数。
运行记录的 Diff 表按指标展示 `baseline`、上一轮、本轮的三轮算术平均值，以及本轮与上一轮的均值差。表底每组有一个入口，可读取合并后的三轮原始日志。本轮的正确性验证和独立诊断放在同一张卡片。
总览的「查看基准诊断」打开所选参照的独立页面，展示 CPU 火焰图、Trace 和阶段耗时。

## 启动

在实验主机的仓库根目录运行，有 Docker 权限的账号可以直接启动。

```sh
make profile
```

`make profile` 启动 Compose 服务，再启动宿主机的 `profile-executor` 用户服务并等待就绪。容器由 Docker 的 `unless-stopped` 策略管理，只有宿主机执行器使用 systemd。首次在实验账号下运行 `make -C tools/profile build`，构建 `profile`、`profile-web` 和前端。Web 容器使用该账号的 UID/GID，默认发布 `0.0.0.0:8787`。可用 `BIN_DIR`、`WEB_DIST` 和 `PROFILE_LISTEN` 覆盖安装路径、前端目录和发布地址。 [Compose 配置](compose.yaml) · [打开实验平台](http://172.31.23.2:8787/)

`profile-web` 默认从 DaoCloud 拉取 Ubuntu 26.04，镜像固定到 Docker 官方发布的多架构索引摘要。可在 Compose 的环境文件中用 `PROFILE_WEB_IMAGE` 指定其他完整镜像地址。

原生 Trace 首次读取用 Langfuse 确认项目和 capture 时间窗口，再通过 ClickHouse HTTP 只读查询加载完整 span。校验失败时回退到 Langfuse API。在 Compose 的环境文件中将 `PROFILE_CLICKHOUSE_URL` 设为空可关闭直读。使用单独的 ClickHouse 查询账号时，设置 `PROFILE_CLICKHOUSE_USER` 和 `PROFILE_CLICKHOUSE_PASSWORD`，并授予该账号读取 `default.events_full` 的权限。

更新 Web 后，在仓库根目录重建容器。正在执行的 benchmark 由独立的宿主机进程继续运行。

```sh
make profile restart
```

从旧部署切换时，先等当前 benchmark 结束。若仍在使用旧的宿主机 `profile-web.service`，先运行一次 `sudo systemctl stop profile-web.service` 释放端口。只启动 Langfuse 和火焰图服务用 `make -C tools/profile langfuse`。 [部署脚本](ops/setup_host.sh)

构建、benchmark、Trace 采集和发布由宿主机的 `profile executor` 完成，使用配置中的 checkout、工具链和归档路径。Web 通过 `config/executor/executor.sock` 转发运行、进度、日志和删除请求，实验归档只读挂载入容器。执行器以实验账号的 systemd 用户服务运行，日志用 `journalctl --user -u profile-executor` 查看。更新执行器或修改工作流配置后，等本轮结束，运行 `systemctl --user stop profile-executor`，再用 `make -C tools/profile executor` 启动。 [工作流步骤](pkg/workflow/plan.go)

执行器随用户的 systemd 实例运行。`loginctl show-user "$USER" -p Linger` 为 `Linger=no` 时，最后一个登录会话退出后，执行器会随用户实例停止。Web 仍可读取已归档的实验。

## 数据库与回填

Compose 的一次性服务 `profile-init` 在 Langfuse 的 PostgreSQL 中准备 `dragonfly_experiments` 数据库和连接文件，沿用已有实验数据。 [迁移定义](models/schema.sql)

```sh
export PROFILE_DSN_FILE=/data/dragonfly-observability/config/raft-workflow.dsn
experiment_data=/data/dragonfly-observability/state/raft-workflow/experiments
profile backfill -data "$experiment_data" -archive docs/cluster/benchmarks
```

`backfill` 读取既有 benchmark、验证日志和诊断归档，将 v3–v10 与已回退的 P0-1b 直接写入 PostgreSQL。
重复执行会跳过已完成的实验，并为缺失上一轮关联的旧实验补上经过校验的归档引用。

## 在实验台运行

创建实验并选择对照后，点击「运行 Benchmark」。执行机验证构建、运行本轮三轮正式测试，再用同一份 Release 二进制独立采集 Leader 和 Follower 的原生 Trace 与 CPU 栈。C++ 退出排空后直接上报 Langfuse，校验 capture 的版本、负载、节点、stream 和事件完整性后，归档结果、日志、CPU 火焰图及 capture 引用。上一轮、基线和对比参照沿用创建时选定的归档。

执行状态记录在 PostgreSQL，CPU 原始采样和续跑检查点保存在执行机。采集与校验分步执行：入库延迟时重试只核对已完成的 capture；采集失败重跑使用新 capture ID。同一执行机一次运行一个实验。

`make profile` 读取 `config/profile.json`，可用 `WORKFLOW_CONFIG` 指定文件。文件使用 `plan` / `run` 的 JSON 配置，`output` 是执行归档根目录。

```json
{
  "repo": "~/workspace/db/dragonfly-io-uring-20261123",
  "output": "~/.local/state/dragonfly-raft-workflow/runs",
  "release_build": "build-opt",
  "validation": [{
    "build": "build-dbg",
    "unit_tests": ["trace_test", "trace_exporter_test", "partition_log_test", "folly_io_test"],
    "integration_tests": ["tests.integration.raft_data_group_test.RaftMemoryAckTest.test_idle_memory_writes_publish_recovery_boundary"]
  }],
  "profiler": "~/.local/opt/dragonfly-observability/profiler-build/libprofiler.so",
  "go": "~/.local/go/bin/go",
  "flamegraph_dir": "~/.local/opt/dragonfly-observability/downloads",
  "jobs": 12
}
```

构建目录须启用 `-DWITH_RAFT_OTEL=ON`（Raft 使用 Folly I/O）。workflow 不修改 CMake 配置；正式压测清除 Trace/CPU 采集环境，诊断才设置 capture 身份与凭据。Langfuse 默认使用本机 3007 和受保护的 `/data/dragonfly-observability/config/langfuse.env`，可用 `langfuse.url` / `langfuse.env_file` 覆盖。

压测、集群管理、恢复校验和 CPU 采样控制由 `pkg/benchmark` 的 Go 实现完成，workflow 直接调用。Python 仅用于配置中选定的集成回归。Go 压测器使用独立的源码哈希；历史 Python 归档仍可选为对照，版本差异会显示在比较结果中。

正式 benchmark 使用 Raft memory、1 连接、pipeline 64、256 MiB。创建时选定的归档须使用相同负载。`output` 放在 benchmark 使用的本地磁盘上，`-data` 指向 PostgreSQL 关联的产物目录。采集机需要 gperftools libprofiler、Go pprof、addr2line 和 flamegraph.pl；配置了 Python 集成回归时还需要 Python Redis。压测输出不能放在 tmpfs。

`cmd/cli` 构建 `profile`，`cmd/web` 构建 `profile-web`。`models` 包含实验数据模型和 GORM 持久化，沿用 PostgreSQL 的显式 SQL 迁移。`pkg` 提供测量、执行、HTTP 路由和 Langfuse Trace 处理。

## 原生 Trace 查询

Web 从 `LANGFUSE_URL` 和 `LANGFUSE_ENV_FILE` 读取查询配置，Compose 已配置内部 traces 服务与只读凭据文件。CLI 和页面通过 `pkg/httpapi` 共用 `pkg/otel` 的 capture 查询、校验和排序。

```sh
profile captures -id v15
profile fetch-delay -id v15 -min-return-wait-ms 10
profile slow-path -id v15 -limit 5
```

默认最近一轮 capture、降序 Top 10。耗时属性与输出统一为 ms，沿用原 `raft.*` trace 名称。`slow-path -min-duration-ms 10` 筛选事件耗时；`fetch-delay -min-return-wait-ms 10` 筛选 RPC 返回后的等待。Fetch 的其他字段可用 `-min-rpc-ms`、`-min-io-wait-ms`、`-min-appended-bytes` 等参数，一次选择一个阈值并按该字段排序；无阈值时可用 `-attribute` 选择排序字段。`-capture` 固定轮次，`-selected TRACE_ID:SPAN_ID` 查看指定请求，`-start-ns/-end-ns` 用原始单调时钟筛选请求起点，`-json` 输出 Web 使用的同一 DTO。`complete=false` 表示记录缺失或未排空，不能当作完整排名。此处 JSON 是 HTTP 查询响应，不是导入文件。

`Slow 分析` 页的 `Slow path` 和 `Fetch delay` 使用相同轮次 tabs，默认打开 `Slow path`。前者默认对全部事件按 duration 排序，不需要选择 attribute；Node 和 Event name 使用整轮采样生成下拉选项，不受当前筛选或 Top 10 限制。列表标题为 `Slow path`，左侧为编号列，点击编号或事件名打开时序。节点显示角色和编号；同一轮换主时，下拉列出该节点出现过的角色。后者默认按 `raft.fetch.return_wait` 排序，页面初始条件为 ≥ 10 ms：RPC 已返回到 Follower 开始处理响应之间的延迟。可修改阈值或切换其他 Fetch 属性。两者先看 Top 10，再看关联时序；事件详情不按特定事件名定制。`Event order` 支持开始时间或耗时排序。`Trace` 页展示采集概览与阶段耗时，并提供到 Slow 分析的入口。

## CLI

CLI 和页面使用同一份实验记录。CLI 自动读取系统环境变量 `PROFILE_URL`；未配置时连接 `http://127.0.0.1:8787`，`-url` 可临时覆盖。

```sh
profile create -id v11 -title '本轮优化' -previous v10
profile run -id v11
profile show -id v11
profile comparisons -id v16 -baseline v3 -previous v15 -reference v0:cluster
profile trace -id v11 -run diagnostic-current -role follower -stage raft. -sort p95 -limit 5
profile flamegraph -id v11 -run diagnostic-current -role leader -view inclusive -limit 5
profile delete -id v11
```

已完成实验可在总览点击「配置对照」，或使用 `comparisons` 修改基准、上一轮和参照。CLI 未指定的字段保持原值，显式传空值可清除；不同压测版本仍展示历史数值和差值，并标明差异。配置仅更新对照关系，不重跑实验。

Cluster 对照统一使用 `v0:cluster`；火焰图页可在上一轮和此参照之间切换。

`run -id` 等到入库后退出，失败后再次运行可继续。`show` 输出三轮指标。页面和 CLI 都能删除 PostgreSQL 实验及其平台归档。正在运行或被其他实验用作对照时，删除请求返回具体原因。对外发布的诊断由各自服务管理。

`trace` 和 `flamegraph` 默认读取所选实验的本轮独立诊断，也可用 `-run` 指定诊断运行 ID。三轮正式 benchmark 保留各自的测量和日志，Trace 和火焰图来自独立诊断。

- `trace` 支持节点角色、阶段前缀和 `-min-union-ms`。`-sort` 可选 `union`、`sum`、`p50`、`p95`、`max`、`count`，`-order` 决定升降序，`-offset` 和 `-limit` 用于分页。`-json` 返回当前页和匹配的阶段总数。
- `flamegraph` 从选定节点的 CPU 栈统计热点，`-view leaf` 按叶子函数统计，`-view inclusive` 包含调用链上的样本。`-limit 0` 可只读摘要和原图链接。

```sh
profile trace -id v10 -analysis apply-to-confirm
profile flamegraph -id v10 -role leader -symbol 'CRC32::Extend' -caller-depth 8
```

`apply-to-confirm` 按日志 offset 关联 Leader 上的 apply 批次和写入确认，统计匹配数、应用后的延迟、worker 排队与 apply 的重叠，以及票据峰值。只有确认 offset 连续且完整覆盖客户端批次时，才按 `client.pipeline` 的写入序号计算批尾耗时。输出同时列出有效批尾数和批次数。

`-symbol` 按函数名子串查询 CPU 调用路径，`-caller-depth` 限定上游帧数；`-view inclusive` 每个样本只计一次。

历史 timeline 归档仍支持 `profile trace` 查询；新实验使用上面的 `captures`、`slow-path` 和 `fetch-delay`，不经过文件导入。

v10 归档的查询结果见文档示例。 [Trace 和火焰图分析示例](../../docs/cluster/profile.md#trace-和火焰图分析示例)

在执行机上，`run -config FILE` 也可直接生成独立测量，`trace -verify-run DIR` 直接向 Langfuse 核对这轮观测。要更新既有实验的本轮数据，使用 `refresh -id ID -run DIR -data DIR -dsn-file FILE` 导入已完成的重测。重测需使用同一二进制和负载，并保留已归档的对照与其它负载结果。

### 汇总本地归档

`profile summarize` 读取 `profile benchmark` 的输出目录和已有的诊断 `report` 目录。

```sh
profile summarize -compare "$compare_dir" \
  -before "$baseline_report" -after "$current_report" -out "$summary_dir"
```

只有本轮诊断时可只传 `-after`。`-out` 指定新目录，`summary.md` 包含吞吐、批次 P95、CPU 和 capture 引用，`summary.json` 另含各节点的 CPU 热点与构建校验信息。

## API

| 方法 | 路由 | 返回 |
| --- | --- | --- |
| GET | `/api/v1/experiments` | 实验列表 |
| POST | `/api/v1/experiments` | 保存待测实验与已选对照 |
| GET | `/api/v1/comparison-targets` | 可选的正式 benchmark |
| GET | `/api/v1/comparison-targets/{target}/diagnostics` | 一条基准的独立诊断和产物 ID |
| GET | `/api/v1/experiments/{id}` | 摘要、轮次、诊断与产物 ID |
| PUT | `/api/v1/experiments/{id}/comparisons` | 保存已完成实验的基准、上一轮及参照，空字段清除对应选择 |
| DELETE | `/api/v1/experiments/{id}` | 删除实验及其平台归档 |
| GET | `/api/v1/experiments/{id}/summary` | 对照结果、CPU 热点、关键等待和 Langfuse 回执 |
| GET | `/api/v1/experiments/{id}/execution` | 执行状态和阶段进度 |
| POST | `/api/v1/experiments/{id}/execution` | 启动或继续执行，`{"restart":true}` 从头运行 |
| GET | `/api/v1/experiments/{id}/execution/log` | 当前执行的进度日志 |
| GET | `/api/v1/experiments/{id}/diagnostic-comparison` | 本轮与上一轮的阶段统计和产物 ID，每份诊断保留各自的实验 ID |
| GET | `/api/v1/experiments/{id}/event-summary?capture=ID` | 测量窗口内的事件统计。Trace 对比按事件与节点角色对齐，P95 从组内事件重新计算；`scope=capture` 查看完整采集 |
| GET | `/api/v1/experiments/{id}/trace` | 指定诊断运行的阶段统计和分页，支持 `run`、`role`、`stage`、`sort`、`order`、`min_union_ms`、`limit`、`offset`。`analysis=apply-to-confirm` 增加逐 offset 的应用后确认分析 |
| GET | `/api/v1/experiments/{id}/flamegraph` | 指定诊断运行的 CPU 热点和原图 ID，支持 `run`、`role`、`view`、`limit`。`symbol` 和可选的 `caller_depth` 按调用路径返回匹配样本 |
| GET | `/api/v1/experiments/{id}/run-comparison` | baseline、上一轮与本轮的运行记录 |
| GET | `/api/v1/experiments/{id}/run-logs?group=current` | 主负载三轮日志，`group` 可选 baseline、previous 或 current，省略时返回所有可用组 |
| GET | `/api/v1/experiments/{id}/runs/{run}/measurement` | 一轮的完整测量字段 |
| GET | `/api/v1/experiments/{id}/artifacts/{artifact}` | 日志、交互 SVG 或时间线 |

`POST` 可用 `{"id":"v11","title":"实验标题","targets":{"previous":"v10"}}`，
也可用 `cumulative_baseline` 和 `reference` 显式指定对照。页面的「复制实验摘要」调用同一个摘要接口。

`plan -config FILE` 查看测量步骤，`run -config FILE` 运行，`-resume` 核验输入与产物后继续。
默认只测本轮。选定的上一轮直接读取归档，显式提供 `baseline_build` 才会追加 A/B 测量。启用 diagnostics 时，冻结的 baseline 也须包含原生 Trace 能力和 CMakeCache.txt；before capture 使用 experiment_id 加 -baseline，避免混入本轮。include_cluster 只额外采集 CPU，不要求 Raft Trace。独立 run -config 可设置 experiment_id，默认使用 output 的目录名；平台执行自动采用实验 ID。
原生诊断由 workflow 自动生成 CPU 火焰图并保存 capture 回执；旧 report 文件生成入口已移除。
`import -config FILE` 可为数据库中的待测实验补上本轮结果。配置提供实验 ID、benchmark 目录和可选诊断路径。
