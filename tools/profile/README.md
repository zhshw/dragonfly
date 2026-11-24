# profile

`profile` 提供 benchmark 归档、Trace 查询和 CPU 火焰图分析。当前内置测量流程为 Dragonfly Raft benchmark。

实验、对照选择、benchmark 轮次、验证结果和诊断指标保存在 PostgreSQL。
火焰图、时间线和验证日志通过产物表关联到实验，文件存放在数据盘。

| 比较 | 计算 | 来源 |
| --- | --- | --- |
| 较上一轮 | 本轮 ÷ 上一轮 − 1 | 上一轮实验的正式指标 |
| 累计收益 | 本轮 ÷ 基线 − 1 | 选定的历史 benchmark |
| 参照达成率 | 本轮 ÷ 对比参照 | 选定的同负载 benchmark，例如原 Cluster |

创建实验时选定上一轮，可沿用该实验的基线与对比参照。每项选择会保存测试口径、版本和指标快照。
v10 的上一轮使用 v9 的历史数据。旧实验按原有版本关系关联上一轮归档。总览分别展示吞吐与批次 P95 耗时。
总览、运行记录、火焰图和 Trace 都使用选定版本的数据。首个版本的上一轮数据留空。
运行记录的 Diff 表按指标展示 `baseline`、上一轮、本轮的三轮算术平均值，以及本轮与上一轮的均值差。表底每组有一个入口，可读取合并后的三轮原始日志。本轮的正确性验证和独立诊断放在同一张卡片。
总览的「查看基准诊断」打开所选参照的独立页面，原 Cluster 参照页展示 CPU 火焰图。

## 启动

在实验主机的仓库根目录运行。

```sh
make profile
```

`make profile` 启动 Langfuse 和报告服务，再以当前用户身份启动宿主机上的 `profile-web`。只启动 Langfuse 和报告服务用 `make -C tools/profile langfuse`。 [Compose 配置](compose.yaml)

首次部署可用 `make -C tools/profile build` 构建 CLI 和 Web。默认使用 `/data/dragonfly-observability` 的配置和存储，工作流配置为 `config/profile.json`。 [数据盘准备](../raft-observability/README.md#部署与启动)

## 数据库与回填

`make profile` 在 Langfuse 的 PostgreSQL 中准备 `dragonfly_experiments` 数据库和连接文件，沿用已有实验数据。 [迁移定义](models/schema.sql)

```sh
export PROFILE_DSN_FILE=/data/dragonfly-observability/config/raft-workflow.dsn
experiment_data=/data/dragonfly-observability/state/raft-workflow/experiments
profile backfill -data "$experiment_data" -archive docs/cluster/benchmarks
```

`backfill` 读取既有 benchmark、验证日志和诊断归档，将 v3–v10 与已回退的 P0-1b 直接写入 PostgreSQL。
重复执行会跳过已完成的实验，并为缺失上一轮关联的旧实验补上经过校验的归档引用。

## 在实验台运行

创建实验并选择对照后，点击「运行 Benchmark」。执行机验证构建、运行本轮三轮正式测试，再用单独的插桩二进制采集 Leader 和 Follower 的 Trace 与 CPU 栈。Langfuse 确认可读后，结果、日志、火焰图和时间线自动关联到实验。上一轮、基线和对比参照沿用创建时选定的归档。

执行状态记录在 PostgreSQL，原始采集和续跑检查点保存在执行机。失败后可在校验已完成的产物后继续，或从头启动一次新的执行。同一执行机一次运行一个实验。

`make profile` 读取 `config/profile.json`，可用 `WORKFLOW_CONFIG` 指定文件。文件使用 `plan` / `run` 的 JSON 配置，`output` 是执行归档根目录。

```json
{
  "repo": "~/workspace/db/dragonfly-io-uring-20261123",
  "output": "~/.local/state/dragonfly-profile/runs",
  "release_build": "build-opt",
  "validation": [{
    "build": "build-dbg",
    "unit_tests": ["partition_log_test", "folly_io_test"],
    "integration_tests": ["tests.integration.raft_data_group_test.RaftMemoryAckTest.test_idle_memory_writes_publish_recovery_boundary"]
  }],
  "profiler": "~/.local/opt/dragonfly-observability/profiler-build/libprofiler.so",
  "go": "~/.local/go/bin/go",
  "flamegraph_dir": "~/.local/opt/dragonfly-observability/downloads",
  "jobs": 12
}
```

正式 benchmark 使用 Raft memory、1 连接、pipeline 64、256 MiB。创建时选定的归档须使用相同负载和测试脚本。`output` 放在 benchmark 使用的本地磁盘上，`-data` 指向 PostgreSQL 关联的产物目录。采集机的 Python 和 Langfuse 依赖见 [采集配置](../raft-observability/README.md)。

`cmd/cli` 构建 `profile`，`cmd/web` 构建 `profile-web`。`models` 包含实验数据模型和 GORM 持久化，沿用 PostgreSQL 的显式 SQL 迁移。`pkg` 提供测量、执行、HTTP 路由和 Langfuse Trace 处理。

## CLI

CLI 和页面使用同一份实验记录。默认连接 `http://127.0.0.1:8787`，可用 `-url` 或 `PROFILE_URL` 指定平台地址。

```sh
profile create -id v11 -title '本轮优化' -previous v10
profile run -id v11
profile show -id v11
profile trace -id v11 -run diagnostic-current -role follower -stage raft. -sort p95 -limit 5
profile flamegraph -id v11 -run diagnostic-current -role leader -view inclusive -limit 5
profile delete -id v11
```

`run -id` 等到入库后退出，失败后再次运行可继续。`show` 输出三轮指标。页面和 CLI 都能删除 PostgreSQL 实验及其平台归档。正在运行或被其他实验用作对照时，删除请求返回具体原因。对外发布的诊断由各自服务管理。

`trace` 和 `flamegraph` 默认读取所选实验的本轮独立诊断，也可用 `-run` 指定诊断运行 ID。三轮正式 benchmark 保留各自的测量和日志，Trace 和火焰图来自独立诊断。

- `trace` 支持节点角色、阶段前缀和 `-min-union-ms`。`-sort` 可选 `union`、`sum`、`p50`、`p95`、`max`、`count`，`-order` 决定升降序，`-offset` 和 `-limit` 用于分页。`-json` 返回当前页和匹配的阶段总数。
- `flamegraph` 从选定节点的 CPU 栈统计热点，`-view leaf` 按叶子函数统计，`-view inclusive` 包含调用链上的样本。`-limit 0` 可只读摘要和原图链接。

```sh
profile trace -id v10 -analysis apply-to-confirm
profile flamegraph -id v10 -role leader -symbol 'CRC32::Extend' -caller-depth 8
```

`apply-to-confirm` 按日志 offset 关联 Leader 上的 apply 批次和写入确认，统计匹配数、应用后的延迟、worker 排队与 apply 的重叠，以及票据峰值。只有确认 offset 连续且完整覆盖客户端批次时，才按 `client.pipeline` 的写入序号计算批尾耗时。输出同时列出有效批尾数和批次数。

`-symbol` 对函数名做区分大小写的子串匹配，按调用路径列出样本数及其占该节点总样本的比例。`-caller-depth` 为参与分组的上游帧数，默认使用完整路径。`-view inclusive` 使用靠近叶子的匹配帧，每个样本只计一次。两个查询都支持 `-json`。

v10 归档的查询结果见文档示例。 [Trace 和火焰图分析示例](../../docs/cluster/profile.md#trace-和火焰图分析示例)

在执行机上，`run -config FILE` 也可直接生成独立测量，`trace -verify-run DIR` 直接向 Langfuse 核对这轮观测。要更新既有实验的本轮数据，使用 `refresh -id ID -run DIR -data DIR -dsn-file FILE` 导入已完成的重测。重测需使用同一二进制和负载，并保留已归档的对照与其它负载结果。

### 汇总本地归档

`profile summarize` 读取 `pipeline_compare` 的输出目录和已有的诊断 `report` 目录。

```sh
profile summarize -compare "$compare_dir" \
  -before "$baseline_report" -after "$current_report" -out "$summary_dir"
```

只有本轮诊断时可只传 `-after`。`-out` 指定新目录，`summary.md` 包含吞吐、批次 P95 和关键等待，`summary.json` 另含各节点的 CPU 热点与构建校验信息。

## API

| 方法 | 路由 | 返回 |
| --- | --- | --- |
| GET | `/api/v1/experiments` | 实验列表 |
| POST | `/api/v1/experiments` | 保存待测实验与已选对照 |
| GET | `/api/v1/comparison-targets` | 可选的正式 benchmark |
| GET | `/api/v1/comparison-targets/{target}/diagnostics` | 一条基准的独立诊断和产物 ID |
| GET | `/api/v1/experiments/{id}` | 摘要、轮次、诊断与产物 ID |
| DELETE | `/api/v1/experiments/{id}` | 删除实验及其平台归档 |
| GET | `/api/v1/experiments/{id}/summary` | 对照结果、CPU 热点、关键等待和 Langfuse 回执 |
| GET | `/api/v1/experiments/{id}/execution` | 执行状态和阶段进度 |
| POST | `/api/v1/experiments/{id}/execution` | 启动或继续执行，`{"restart":true}` 从头运行 |
| GET | `/api/v1/experiments/{id}/execution/log` | 当前执行的进度日志 |
| GET | `/api/v1/experiments/{id}/diagnostic-comparison` | 本轮与上一轮的阶段统计和产物 ID，每份诊断保留各自的实验 ID |
| GET | `/api/v1/experiments/{id}/trace` | 指定诊断运行的阶段统计和分页，支持 `run`、`role`、`stage`、`sort`、`order`、`min_union_ms`、`limit`、`offset`。`analysis=apply-to-confirm` 增加逐 offset 的应用后确认分析 |
| GET | `/api/v1/experiments/{id}/flamegraph` | 指定诊断运行的 CPU 热点和原图 ID，支持 `run`、`role`、`view`、`limit`。`symbol` 和可选的 `caller_depth` 按调用路径返回匹配样本 |
| GET | `/api/v1/experiments/{id}/run-comparison` | baseline、上一轮与本轮的运行记录 |
| GET | `/api/v1/experiments/{id}/run-logs?group=current` | 主负载三轮日志，`group` 可选 baseline、previous 或 current，省略时返回所有可用组 |
| GET | `/api/v1/experiments/{id}/runs/{run}/measurement` | 一轮的完整测量字段 |
| GET | `/api/v1/experiments/{id}/artifacts/{artifact}` | 日志、交互 SVG 或时间线 |

`POST` 可用 `{"id":"v11","title":"实验标题","targets":{"previous":"v10"}}`，
也可用 `cumulative_baseline` 和 `reference` 显式指定对照。页面的「复制实验摘要」调用同一个摘要接口。

`plan -config FILE` 查看测量步骤，`run -config FILE` 运行，`-resume` 核验输入与产物后继续。
默认只测本轮。选定的上一轮直接读取归档，显式提供 `baseline_build` 才会追加 A/B 测量。
`report -run RUN` 将已有采集转为报告，附 `-flamegraph-dir DIR` 生成交互 SVG。
`import -config FILE` 可为数据库中的待测实验补上本轮结果。配置提供实验 ID、benchmark 目录和可选诊断路径。
