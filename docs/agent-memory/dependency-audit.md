# Agent Memory 依赖源码核查附录

文档导航：[概览](README.md) · [待办清单](TODO.md)

> 本文保留调研与原始复现基线；当前实现和测试状态见[实施记录](implementation-progress.md)。

本附录补充[实施报告](implementation-report.md)，重点回答：**RedisVL 实际发送什么命令，Docket 究竟用了哪些数据结构，为什么有对应 Redis 命令仍不等于服务可直接运行。**

日期、两个仓库基线与主报告一致。第 1–6 节记录原 session 的锁定版本源码静态核查；第 7 节补充 2026-09-30 的 Dragonfly 最小运行复现。未安装或运行 Python 依赖包，未完成 AMS 端到端验证。

## 1. 来源与复核方法

按照 `AMS/V0/uv.lock` 获取公开 PyPI source distribution，只读取 Python/Lua 等源码，归档 SHA256 与锁文件一致。没有复制依赖源码进入 Dragonfly 仓库。

| 简称 | 包与版本 | sdist SHA256 |
| --- | --- | --- |
| RVL | redisvl 0.12.1 | `c4df3f7dd2d92c71a98e54ba32bcfb4f7bd526c749e4721de0fd1f08e0ecddec` |
| RPY | redis 6.4.0 | `b01bc7282b8444e28ec36b261df5375183bb47a07eb9c603f284e89cbc5ef010` |
| DKT | pydocket 0.18.2 | `cc2051d15557f83bb164a83b0743fa9c12c2bfe9a9145cff3a5922b4935ce4f5` |
| KVA | py-key-value-aio 0.3.0 | `858e852fcf6d696d231266da66042d3355a7f9871650415feef9fca7a6cd4155` |
| KVS | py-key-value-shared 0.3.0 | `8fdd786cf96c3e900102945f92aa1473138ebe960ef49da1c833790160c28a4b` |

引用路径相对于各包 sdist 解包根目录。`AMS-S/`、`DF/` 继续采用主报告定义。源码包下载地址：

- [redisvl 0.12.1](https://files.pythonhosted.org/packages/c0/ac/7c527765011d07652ff9d97fd16f563d625bd1887ad09bafe2626f77f225/redisvl-0.12.1.tar.gz)
- [redis 6.4.0](https://files.pythonhosted.org/packages/0d/d6/e8b92798a5bd67d659d51a18170e91c16ac3b59738d91894651ee255ed49/redis-6.4.0.tar.gz)
- [pydocket 0.18.2](https://files.pythonhosted.org/packages/b8/5f/82dde9fb6099b960a4203596d3b755d1bd2c0d0210fea104d015d6515d7f/pydocket-0.18.2.tar.gz)
- [py-key-value-aio 0.3.0](https://files.pythonhosted.org/packages/93/ce/3136b771dddf5ac905cc193b461eb67967cf3979688c6696e1f2cdcde7ea/py_key_value_aio-0.3.0.tar.gz)
- [py-key-value-shared 0.3.0](https://files.pythonhosted.org/packages/7b/e4/1971dfc4620a3a15b4579fe99e024f5edd6e0967a71154771a059daff4db/py_key_value_shared-0.3.0.tar.gz)

## 2. Docket：不是只有一个 Stream

### 2.1 实际键与数据结构

以下 `P` 为 Docket 前缀：AMS 默认单实例连接为 `memory-server`，cluster URL 路径为 `{memory-server}`。`T` 为 Docket task key，与 AMS 的 `task:<id>` 状态记录不是同一个命名空间。

| 数据结构 | 键模板 | 内容与读写 | 清理/用途 |
| --- | --- | --- | --- |
| **Stream** | `P:stream` | 消息字段有 key、when、function、args、kwargs、attempt、generation；XADD/XREADGROUP/XACK/XDEL/XAUTOCLAIM | 立即任务与到期任务的交付；consumer group/PEL 跟踪待确认消息 |
| **ZSet** | `P:queue` | member 为 T，score 为计划执行时间；ZADD/ZRANGEBYSCORE/ZREMRANGEBYSCORE | 延迟调度索引；不是消息正文，不是旧 sessions ZSet |
| **Hash** | `P:T` | 暂存待执行任务消息字段；HSET/HGETALL/DEL | 延迟任务载荷；到期转为 Stream 消息后删除 |
| **Hash** | `P:runs:T` | state、when、known、stream_id、function、args、kwargs、generation，以及运行/终态字段 | 调度去重、版本竞争、运行状态、结果引用；终态按 execution_ttl 保留或删除 |
| **Hash** | `P:progress:T` | 执行进度字段 | 与任务进度 Pub/Sub 通知配合；不能与 AMS `task:<id>` 状态混为一谈 |
| **ZSet** | `P:workers` | worker 名称 → 心跳时间 | 按 score 清理过期 worker，然后写入本次心跳 |
| **ZSet** | `P:task-workers:<function>` | 支持该函数的 worker → 心跳时间 | 按任务函数查找活跃 worker |
| **Set** | `P:worker-tasks:<worker>` | worker 支持的函数名集合 | SADD + EXPIRE，TTL 与心跳容忍窗口相关 |
| **String，JSON 封装** | 单实例 `P:results::T` | 任务返回值/异常序列化后的结果封装 | 默认结果后端 py-key-value；有结果且启用 execution_ttl 时保存 |
| **String，JSON 封装** | cluster 后端 `P:results:T` | 与单实例后端不同的键组合和封装路径 | ClusterKeyValueStore 的实现；不要假设切换后端直接读旧结果键 |
| **String，遗留兼容键** | `P:known:T`、`P:stream-id:T` | 新路径仍会读取/清理历史位置 | 新的 known/stream_id 主要写入 runs Hash；不是首选的新建结构 |
| **Stream** | `P:strikes` | strikelist 控制指令 | 管理阻止执行等控制行为；与工作消息 Stream 分开 |
| **Pub/Sub channel** | `P:state:T`、`P:progress:T`、`P:cancel:T` | 发布执行状态、进度和取消事件 | channel 不是持久化 Redis key，也不是可靠任务日志 |

关键证据：

- `DKT/src/docket/docket.py:610–638`：queue/stream/parked/runs/结果 collection 与遗留键名。
- `DKT/src/docket/execution.py:438–488`：HINCRBY generation、立即 XADD、延迟 HSET+ZADD、runs 字段。
- `DKT/src/docket/worker.py:455–480,552–554,628–702`：reclaim、消费/ACK、到期调度。
- `DKT/src/docket/_execution_progress.py:56,98–144,203`：进度 Hash 和 channel。
- `DKT/src/docket/_docket_snapshot.py:97–104`、`worker.py:1048–1065`：心跳 ZSet 和能力 Set。
- `DKT/src/docket/worker.py:924–935`：cloudpickle → Base64 → 结果存储；`docket.py:115` 默认 execution_ttl 为 15 分钟。
- `KVA/src/key_value/aio/stores/redis/store.py:133–150`、`KVS/src/key_value/shared/utils/compound.py:5–11,35–37`：默认结果是 String，collection 与 key 用 `::` 连接。
- `DKT/src/docket/_result_store.py:55–58,93–108`：cluster 结果 key 用 `:` 连接并调用 SET/SETEX。
- `DKT/src/docket/strikelist.py:218–220,305,584`：控制 Stream。

**生命周期不要混淆**：AMS 的 `task:<id>` 默认保留 7 天，Docket 的终态执行记录默认保留 15 分钟；这两层不是同一个状态存储。也不能把终态 TTL 解释成排队中任务会在 15 分钟后自动消失。

Docket 还提供可选 Concurrency/RateLimit 依赖，内部有 ZSet+Lua 实现，见 `DKT/src/docket/dependencies/_concurrency.py:218–259`、`_ratelimit.py:40–53`。本次未在 AMS 业务模块中找到直接使用这些两类依赖的入口，不能将它们算作所有请求必经的结构；若未来启用则追加验收。

### 2.2 一条任务如何流转

1. **立即执行**：调度脚本更新 generation，XADD 到 `P:stream`，同时 HSET `P:runs:T` 为 queued。
2. **延迟执行**：消息写入 `P:T` Hash，任务 ID 和时间写入 `P:queue` ZSet，runs 状态为 scheduled。
3. **到期搬运**：worker 从 ZSet 查询到期 ID，读取对应 Hash，XADD 到 Stream，删除临时 Hash，更新 runs 并发布状态通知。
4. **消费**：worker 通过 consumer group 读取并执行；未确认消息可以通过 XAUTOCLAIM 重投。
5. **结束或取消**：更新终态、处理 ACK/删除/重调度及 TTL；任务结果存在另一组 String 键中。

这里涉及跨多个键的原子操作。不能因为 Dragonfly 分别支持 ZADD、HSET、XADD，就推断调度脚本可以不改配置运行。

任务参数和结果涉及 Python 序列化，因此队列写权限应仅授予受信服务/worker；不应把结果反序列化接口或队列任意写权限开放给普通记忆调用方。

## 3. 明确的 Docket / Dragonfly 脚本兼容风险

### 3.1 到期调度脚本访问未声明键

`DKT/src/docket/worker.py:628–702` 的 `_scheduler_loop` 注册脚本，只传入两个 KEYS：

```text
KEYS[1] = P:queue
KEYS[2] = P:stream
ARGV[2] = P
```

但脚本实际通过 ZSet 内容拼接并访问：

```text
P:<task_key>       → HGETALL / DEL
P:runs:<task_key>  → HSET
P:state:<task_key> → PUBLISH channel
```

前两个是动态数据键，不在传入的 KEYS 列表中。

Dragonfly 默认 `default_lua_flags` 为空、自动 undeclared 修正关闭（`DF/src/server/script_mgr.cc:26–45`）；atomic 且未允许 undeclared 的脚本采用 LOCK_AHEAD 模式（`DF/src/server/main_service.cc:2263–2270`）。数据键不在声明锁集合时返回 undeclared-key 错误（同文件 `1307–1325`）。

从包内 AST 提取原始 Lua 字符串计算出的脚本 SHA1 为：

```text
8929ea55fc26d043e4a7d3f010a74baeff87a465
```

该 SHA 不在本次 Dragonfly 基线的内置 undeclared 脚本名单中。**源码表明这条到期搬运路径与默认声明键约束不兼容；2026-09-30 已通过原始脚本运行复现，见第 7 节。** 队列为空或仅测试立即任务时，未必触达动态 Hash 访问，不能以此判断调度兼容。

### 3.2 推荐处理顺序

- **P0 最小复现**：至少配置两个内部 shard，预置一个到期任务，运行与锁定包完全一致的脚本；记录错误及所有键的执行前后状态。另测试立即任务，明确区分两条路径。
- **可评估的最小部署适配**：对已审计脚本使用 `lua_undeclared_keys_shas` 定向允许未声明键。该设置在脚本加载时生效，不能假设热修改立即改变已缓存脚本的参数。
- **代价**：保留原子性并允许 undeclared 会转为 GLOBAL 模式，取得所有 shard 的相应锁；到期任务批量越大，越需要关注全局事务持有时间和前台尾延迟。见 `DF/docs/transaction.md:244`。
- **长期方案**：在队列适配层改为有界批次、显式声明本批任务的完整键集合，并在脚本内重新校验到期状态；或将后台队列与主记忆数据库分离部署。
- **不推荐的捷径**：为了通过脚本而全局启用 `disable-atomicity`。这会改变 ZSet→Hash→Stream 搬运的一致性，不是等价兼容修复。

本轮没有变更 Dragonfly 的配置或脚本白名单。该 SHA 仅对应锁定版本的原始脚本文本，依赖升级或文本变化后应重新审计，不能无限沿用。

## 4. RedisVL / redis-py 实际调用合同

### 4.1 连接、建索引和写入

| 操作 | 锁定版本源码行为 | 对兼容性判断的修正 |
| --- | --- | --- |
| 客户端初始化校验 | `CLIENT SETINFO LIB-NAME`；ResponseError 时尝试 ECHO | 0.12.1 的通用校验已移除模块版本强制检查，不能把 MODULE LIST 版本门槛当作此路径必然阻塞 |
| `AsyncSearchIndex.exists()` | `FT._LIST` 后检查索引名 | 不等价于每次启动都 FT.INFO |
| `create()` | HNSW 默认路径构造 IndexDefinition 后 FT.CREATE；SVS-VAMANA 另有能力检查 | V0 默认 HNSW 不触发 SVS 专用验证 |
| Hash `load()` | 非事务 pipeline 批量 HSET；只有传 ttl 时再 EXPIRE | 长期记忆默认写入仍不是逐条自动 TTL，也不是整批原子事务 |
| `query()` | redis-py FT.SEARCH，加 query.params | 距离名、PARAMS、返回字段与分页都属于合同 |
| `aggregate()` | 将参数直接传给 redis-py FT.AGGREGATE | 不会替应用自动从某个 VectorQuery 恢复丢失的参数 |
| `HSETEX` | redis-py 组装 FXX/KEEPTTL，追加 FIELDS 和字段数 | 应验证 Redis 形式而不是旧 Dragonfly ttl_sec 形式 |

证据：`RVL/redisvl/redis/connection.py:680–729`；`RVL/redisvl/index/index.py:1407–1438,1695–1707,1941–1955`；`RVL/redisvl/index/storage.py:484–493,591–625`；`RPY/redis/commands/core.py:5166–5246`。

这不意味着可以删掉全部能力检查：真实 FT.CREATE/FT.SEARCH、FT.INFO 重建 schema、RESP2/RESP3 解码和失败处理仍应测试。但“SDK 模块版本探测”不再是一个没有证据的总括阻塞项。

### 4.2 查询形态

以下是源码生成的查询形态示意，不是本轮抓取的运行命令；完整字段/参数需由端到端测试记录。

**普通 KNN**：

```text
FT.SEARCH memory_records
  '<scope-filter>=>[KNN <N> @vector $vector AS vector_distance]'
  PARAMS 2 vector <FLOAT32 bytes>
  ... RETURN / SORTBY / LIMIT / DIALECT ...
```

**范围查询**：

```text
FT.SEARCH memory_records
  '(@vector:[VECTOR_RANGE $distance_threshold $vector]=>{$YIELD_DISTANCE_AS: vector_distance} <scope-filter>)'
  PARAMS 4 vector <FLOAT32 bytes> distance_threshold <threshold>
  ...
```

`RVL/redisvl/query/query.py:422–423,597–641,1190–1227` 明确距离别名为 `vector_distance`。

**关键词与混合**：V0 的 TextQuery 路径使用配置 scorer；AggregateHybridQuery 构造 `SCORER + ADDSCORES`，通过 APPLY 混合文本分数与向量相似度，再 SORTBY。它不需要改用 Dragonfly 的 FT.HYBRID 才能工作，也不是默认 RRF。

混合评分的依赖实现为：

```text
vector_similarity = (2 - distance) / 2
text_score = __score
hybrid_score = (1 - alpha) * text_score + alpha * vector_similarity
```

证据：`RVL/redisvl/query/aggregate.py:165–177`。默认 COSINE 下应验证分数方向及范围；换距离度量后不能假设此归一化仍符合业务期望。

### 4.3 V0 recency 聚合存在应用侧合同问题

这组问题属于 **V0 应用与锁定依赖的接合处**，不能全部归因于 Dragonfly 缺功能。当前只完成源码级交叉核对，尚未逐项运行复现。

| 问题 | V0 代码与依赖证据 | 后果与实施要求 |
| --- | --- | --- |
| 查询对象不是字符串 | `AMS-S/utils/redis_query.py:52` 调用 `cls(vq.query)`；`RVL/redisvl/query/query.py:240–242` 的 query 返回对象自身；`AggregationQuery` 直接传给 redis-py，后者把 `_query` 放进参数列表 | 命令参数可能在客户端编码阶段就失败；必须明确提取实际查询字符串，而非传对象 |
| LOAD 传了一个 list 参数 | `AMS-S/utils/redis_query.py:58` 是 `load(self.DEFAULT_RETURN_FIELDS)`；`RPY/redis/commands/search/aggregation.py:119–132` 接受 `*fields`，直接 extend；`330–356` 原样构造参数 | 列表会成为单个 LOAD 参数，既不是正确字段数也不是平坦字符串参数；应按 SDK 合同展开并规范字段形式 |
| 向量参数丢失 | V0 `from_vector_query` 未保存 `vq.params`，`memory_vector_db.py:751` 仅调用 `aggregate(agg)`；`RPY/redis/commands/search/commands.py:968–995` 只从显式 query_params 追加 PARAMS | 查询中的 `$vector` / `$distance_threshold` 没有绑定；修复查询对象后仍需修复此项 |
| 距离别名不一致 | 依赖使用 `vector_distance`，V0 的 LOAD/APPLY/结果解析使用 `__vector_score`，见 `utils/redis_query.py:42,82` 和 `memory_vector_db.py:762` | 即使命令执行成功，也可能读错分数；必须统一显式 alias 与结果解析 |
| 分页候选数不足 | `AMS-S/memory_vector_db.py:724–748` 使用 num_results=limit 后再 paginate(offset, limit) | 对 KNN 路径，候选只有 limit 条时 offset>0 可能得到不足甚至空页；需明确候选预算与重排分页合同 |

此外，`from_vector_query(..., filter_expression=redis_filter)` 会把搜索过滤表达式传到聚合 FILTER；搜索查询语法与聚合表达式语法不应混为一谈，也应加入命令合同测试。

`AMS-S/memory_vector_db.py:874–889` 捕获 recency 聚合异常并退回普通搜索。因此 API 返回 200 或返回了结果，并不能证明数据库侧 recency 已生效。验收必须检查实际走的分支、分数和排序，而不仅是请求是否成功。

**修复顺序应为：先修复应用生成的查询合同，再验证 Dragonfly 的 max/pow 与排序语义。** 只新增两个数学函数无法使这条现有路径自动正确。

## 5. 更新后的实施优先级

| 优先级 | 必做项 | 首先改哪一层 |
| --- | --- | --- |
| P0-A | 完整两套 schema 的创建/往返；普通 KNN、范围、关键词/混合 | 合同测试，不先更改数据库实现 |
| P0-B | Docket 延迟任务动态 KEYS 最小复现；核对所有调度脚本 | 队列兼容/部署层，保留原子性 |
| P0-C | recency 查询对象、LOAD、PARAMS、别名、FILTER、分页 | AMS 应用适配层 |
| P1-A | max/pow、NULL、时间边界、排序与分页结果 | Dragonfly 通用表达式层，或明确采用客户端重排 |
| P1-B | immediate/delayed/retry/cancel/reclaim 全部队列路径 | Docket 与 Dragonfly 集成测试 |
| P1-C | 已删除记忆的晚到任务、跨租户任务键、摘要结果删除 | 业务一致性层 |

新增验收断言：

1. **不靠 fallback 掩盖错误**：测试必须能辨认数据库侧 recency 是否执行，异常回退次数可观察。
2. **不只测试立即任务**：ZSet 中的到期任务要最终转入 Stream，并清理对应 parked Hash。
3. **不以队列存在代表任务成功**：验证 runs 状态、AMS task 状态、记忆实际写入以及 ACK/reclaim 行为。
4. **避免只比较最终成功数**：脚本失败后检查 queue/parked/runs/stream 是否部分更新。
5. **确认恢复后的元数据**：任务状态、结果、generation、延迟队列和 Stream consumer group/PEL 均属于恢复范围。
6. **量化 GLOBAL 脚本代价**：若选择 SHA 定向兼容，后续测量到期批次大小、全局事务持有时间和前台尾延迟；本轮无性能数值。

## 6. 本次新增验证记录

- 已获取并核对五个锁定依赖包的源码归档 SHA256。
- 已静态提取 Docket scheduler Lua 的 SHA1，并确认不在当前 Dragonfly 内置名单中；没有执行该脚本。
- 已补充 Docket 的 ZSet/Hash/Set/String/Stream/PubSub 使用明细，以及 RedisVL/redis-py 的命令生成合同。
- 只增加/更新文档；未安装依赖，未修改服务代码、数据库配置或同级 AMS 仓库。
- 未运行 SDK 集成、队列故障注入、构建、单元测试或性能测试。后续实施应按本附录的失败场景逐项验证，不能把源码核查写成测试通过。

## 7. Session 接续：最小运行复现（2026-09-30）

### 7.1 接续范围与环境

原 session 最后的用户要求是继续分析并写清数据结构，交付停在两份报告；它没有落地 C++ 或 AMS 功能改动。本次接续首先验证两个已定位的数据库兼容问题，不把调研交付描述成完整 Agent Memory 实现。

- Dragonfly 分支仍为 `feat/agent-memory`，源码 HEAD 仍为 `ebcfe4c9b896d5e3d1c25512623859682cd89477`。
- 在已有 Debug 配置中执行 `ninja dragonfly filter_expr_test`，构建成功；搜索、collection/extension commands 均启用。
- 执行 `./filter_expr_test`，**79/79 通过**。这验证现有表达式实现，不能证明尚未注册的 max/pow 可用。
- 临时实例仅监听 `127.0.0.1`，使用随机可用端口、3 个 proactor threads、**2 个内部 shard**。每次启动独立进程/临时目录，无外部服务数据；结束后关闭进程。
- 实际启动参数为 `--force_epoll=true --proactor_threads=3 --num_shards=2 --dbfilename= --default_lua_flags= --lua_allow_undeclared_auto_correct=false`，另加 bind/port/log 参数。此基线不接受 `--proactor_type=epoll`，初次启动因此失败，改用源码定义的 force_epoll 后成功。
- 使用 Python 标准库 socket 直接发送 RESP2；没有安装 redis-py、RedisVL 或 Docket，也没有调用模型。

### 7.2 Docket 原始脚本：默认失败、SHA 适配后成功

从 pydocket 0.18.2 的 `worker.py` AST 中定位 `_scheduler_loop` 内的 `register_script`，以 `ast.literal_eval` 提取原始字符串，不改空白或代码。计算得到 SHA1 `8929ea55fc26d043e4a7d3f010a74baeff87a465`，与前述静态核查一致。

每个独立实例先验证空队列，然后准备完全相同的一条到期任务：

```text
HSET memory-server:probe key probe when 1 function probe args x kwargs x attempt 1 generation 0
HSET memory-server:runs:probe state scheduled
ZADD memory-server:queue 1 probe
EVAL <原始脚本字符串> 2 memory-server:queue memory-server:stream 100 memory-server
```

`args`/`kwargs` 是调度脚本搬运用占位字符串；没有 worker 消费，不代表可反序列化的完整 Docket 业务载荷。

| 观测项 | 默认脚本配置 | 仅增加 SHA 定向允许 |
| --- | --- | --- |
| 空队列返回 | `[0, 0]` | `[0, 0]` |
| 一条到期任务返回 | undeclared key 错误 | `[1, 1]` |
| queue ZSet | `probe`，score=1，仍保留 | 空 |
| parked Hash `memory-server:probe` | 7 个字段仍保留 | 已删除 |
| runs Hash | state=`scheduled` | state=`queued` |
| Stream | 空 | 1 条消息，7 个字段与原任务一致 |

默认错误原文：

```text
ERR Error running script (call to 8929ea55fc26d043e4a7d3f010a74baeff87a465): @user_script:11: -ERR script tried accessing undeclared key, key: memory-server:probe
```

对照实例仅增加：

```text
--lua_undeclared_keys_shas=8929ea55fc26d043e4a7d3f010a74baeff87a465
```

该选项在实例启动时设置，使用新进程避免缓存脚本配置干扰。实测表明它能修复**这一条到期搬运路径**；本次没有全局关闭原子性，也没有修改数据库内置白名单。默认失败在第一个动态 Hash 读取处发生，检查发现本用例没有部分写入；这不是对任意 Lua 失败都自动回滚的保证。

**结论**：仅空队列成功不足以证明 Docket 兼容；定向 SHA 是可复现的部署候选方案。仍须验证立即任务、真实序列化载荷、消费、ACK、reclaim、retry、cancel、Pub/Sub 接收、多任务批次和 GLOBAL 模式前台延迟。

### 7.3 FT.AGGREGATE：函数缺失不会直接报错

在上述两个实例均执行：

```text
FT.CREATE probe_idx ON HASH PREFIX 1 probe-doc: SCHEMA created_at NUMERIC last_accessed NUMERIC
HSET probe-doc:1 created_at 0 last_accessed 0
FT.AGGREGATE probe_idx * LOAD 2 @created_at @last_accessed APPLY "max(0, (86400 - @last_accessed)/86400)" AS days APPLY "pow(2, -1)" AS freshness
```

结果：索引创建返回 OK，聚合返回 1 行，两个原始时间字段都为字符串 `0`，但 `days` 和 `freshness` 均为 **RESP null**。预期的有效数值应分别为 1 和 0.5。两个实例结果一致，字段返回顺序无意义。

**结论**：原报告关于 max/pow 缺失的判断已得到运行验证；聚合命令成功不代表分数有效。后续函数实现必须同时测试中间数值和最终排序。本次只验证函数求值，不涉及向量 PARAMS、完整 recency 公式或 AMS 异常 fallback。

### 7.4 当前交付与剩余工作

- 已完成：Session 目标恢复、数据结构报告接续、两个最小失败场景及 SHA 对照验证、Debug 构建和 79 项单测。
- 报告中的两套完整 schema、RedisVL 返回解码及应用侧查询合同仍未做运行认证；P0 尚未整体完成。
- 尚未实现：max/pow 通用函数、AMS 查询合同修复、业务隔离/幂等/删除竞争及完整记忆服务；未改同级 AMS 仓库。
- pre-commit 命令及模块仍不可用；本次对两份 Markdown 直接检查尾随空白、末尾换行、代码块、链接，并执行 git diff --check。
- 当前交付仍为报告及实测证据，未提交或推送代码，未宣称完成端到端或性能验收。
