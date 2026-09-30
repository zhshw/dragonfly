# 基于 Dragonfly 的 Agent Memory 实施报告

文档导航：[概览](README.md) · [待办清单](TODO.md)

> 本文保留调研与原始复现基线；当前架构见[AMS 架构](ams-architecture.md)，当前实现和测试状态见[实施记录](implementation-progress.md)。

## 1. 结论与调研边界

**结论：Dragonfly 已具备实现 Agent Memory 所需的主要存储和检索基础设施。建议先适配独立的应用层记忆服务，而不是在数据库内新增一套记忆引擎。当前不能承诺只替换 Redis URL 就能完整运行 V0。**

关键判断：

1. 现有服务不是单纯的“向量数据库包装”：它组合了工作记忆、长期记忆、结构抽取、去重、摘要、遗忘策略、后台任务和认证。
2. 数据主路径是 **工作记忆 JSON + 长期记忆 Hash + 二级搜索索引**；辅助使用 String、Set、Stream，Docket 延迟调度和心跳还使用 **ZSet + Hash**。源码另保留 List/Hash 摘要路径和旧 ZSet 会话目录迁移逻辑；旧会话 ZSet 弃用不等于系统不再使用 ZSet。
3. 当前 Dragonfly 源码已有 HASH/JSON 索引、TEXT/TAG/NUMERIC、FLAT/HNSW、COSINE、KNN、VECTOR_RANGE、聚合 APPLY、BM25 等能力；不能以过往版本印象判断它“不支持向量”或“不支持聚合”。
4. 已确认的数据库功能缺口是 recency 表达式需要 `max()`、`pow()`，当前函数表未注册。此外，依赖源码核对发现 **Docket 调度 Lua 的动态键访问与默认脚本规则冲突**，以及 **V0 recency 与锁定版 SDK 的查询对象/参数/别名合同不匹配**；不能只补数学函数就宣布适配完成。
5. 第一阶段选择**单个 Dragonfly 实例，允许实例内部多 shard**。这不等于承诺 Redis Cluster 或跨实例搜索兼容。

### 1.1 基线与交付

| 项目 | 本次基线 |
| --- | --- |
| 调研日期 | 2026-09-29 |
| Dragonfly | `ebcfe4c9b896d5e3d1c25512623859682cd89477` |
| 同级 agent-memory-server | `94192c39e2a4a154f441a5411e3d73c4f54974a6` |
| 分支 | `feat/agent-memory`，由 `chore/debug-dev-build` 调整而来 |
| 本轮交付 | 实施报告；不修改服务功能，不提交或推送代码 |
| 证据级别 | 源码静态核查；2026-09-30 补充两 shard 最小运行复现和 79 项表达式单测。不是 AMS 端到端认证或性能实测 |

路径约定：`AMS/` 表示同级 `../agent-memory-server/` 仓库；`DF/` 表示本仓库。
下文 `AMS-S/` 专指 `AMS/V0/agent_memory_server/`。行号对应上述基线。

`AMS/AGENTS.md:3–20`、`AMS/CLAUDE.md:14–23` 明确区分：

- `V0/` 是保留的开源研究实现，本报告分析的服务器源码位于这里。
- `agent-memory-benchmark/` 是独立的 LongMemEval 评估工具，不是下一版服务器。
- Redis Iris 中的 Redis Agent Memory 是官方托管产品路径，不能从 V0 推定其内部实现或服务等级。

V0 项目要求 Redis 8（`AMS/V0/AGENTS.md:3–5`）。锁文件实际版本为 RedisVL **0.12.1**、redis-py **6.4.0**、pydocket **0.18.2**，见 `AMS/V0/uv.lock:2092–2094,2328–2339`。应以锁文件而不是宽泛的依赖下限做兼容测试。

后续已获取这些版本及结果存储依赖的公开源码，核对归档 hash；具体数据结构、命令、脚本和应用侧问题见[依赖源码核查附录](dependency-audit.md)。附录同样是静态分析，不是端到端验证。

## 2. 现有服务如何实现记忆

### 2.1 分层职责

| 层 | 现有实现及职责 | 对 Dragonfly 的意义 |
| --- | --- | --- |
| 接入 | REST API 与 MCP 共用记忆逻辑 | 保留在应用进程，不放进数据库命令处理器 |
| 工作记忆 | 会话消息、context、结构化 memories、token 计数、会话配置 | JSON 文档存储与会话查询 |
| 长期记忆 | 记忆记录、embedding、标签、时间、访问次数、来源 | Hash 存储与多类型二级索引 |
| 模型编排 | embedding、抽取、摘要、语义合并 | 外部模型调用；不应阻塞数据库 shard |
| 后台任务 | Docket worker、任务状态、延迟抽取、周期整理与遗忘 | Stream 等队列基础能力，另需验证 Docket 的完整命令合同 |
| 生命周期 | 会话 TTL、内容去重、按年龄/访问情况遗忘、删除 | TTL 只是其中一部分，不等于完整业务遗忘 |

接口分层依据：`AMS/V0/AGENTS.md:76–105`；任务集合见 `AMS-S/docket_tasks.py:24–48`。

### 2.2 写入与召回过程

1. **会话写入**：将消息、记忆、上下文等组装成一个 JSON 文档，调用 `JSON.SET`；配置会话 TTL 时再调用 `EXPIRE`。当前是两个调用，不应描述为天然原子写入。
2. **长期记忆生成**：工作记忆中的内容经提升或抽取成为记忆记录；应用生成 embedding，转换成 FLOAT32 二进制，再由 RedisVL 写入 Hash。
3. **自动索引**：搜索索引读取 Hash 的正文、标签、数值字段和向量字段；会话索引读取工作记忆 JSON 的作用域与时间字段。
4. **召回**：语义模式使用 `VectorQuery`，传入距离阈值时使用 `RangeQuery`；关键词模式使用 `TextQuery`；混合模式使用 `AggregateHybridQuery` 的派生类。服务端时间重排另走聚合查询。
5. **整理**：按 ID、内容 hash、语义相似度去重；可调用模型合并内容、补充主题和实体，并更新访问时间或执行遗忘。

关键代码：

- `AMS-S/working_memory.py:595–664`：JSON 写入与 TTL。
- `AMS-S/long_term_memory.py:1217,1556,1635,1786,1912`：索引、三类去重及工作记忆提升入口。
- `AMS-S/memory_vector_db.py:779–815`：embedding 转 `np.float32.tobytes()`，通过 `index.load(..., id_field="id_")` 写入。
- `AMS-S/memory_vector_db.py:700–747,900–994`：向量、范围、关键词与混合检索路径。
- `AMS-S/long_term_memory.py:2301–2489`：遗忘选择、访问时间更新及周期任务。

## 3. 实际涉及哪些数据结构

**必须区分三层：Redis 对外数据类型、搜索二级索引结构、Python 内存对象。`list[MemoryMessage]` 不是 Redis List；`topics` 不是 Redis Set；`memory_hash` 也不是另一张 Redis Hash 表。**

### 3.1 主路径与辅助路径的数据结构总表

下表中的键名采用默认配置；`[...]` 表示可选部分，不是键中的字面字符。

| 数据结构 | 实际键或索引名 | 存储内容 | 已见调用 / 操作 | 生命周期与用途 |
| --- | --- | --- | --- | --- |
| **JSON** | `working_memory:[namespace:][user_id:]session_id` | messages 数组、memories 数组、context、data、tokens、作用域、时间、策略 | `JSON.SET`、`JSON.GET`、`EXPIRE`、`DEL` | 当前工作记忆主存储；可选整键 TTL |
| **Hash** | `memory_idx:<memory_id>` | text、vector、标签、时间戳、访问统计、来源、metadata 等字段 | RedisVL `load`，直接 `HSET`、`HSETEX`，删除与读回接口 | 当前长期记忆主存储；写入路径没有给每条记录统一设置整键 TTL |
| **JSON 上的搜索索引** | `working_memory_idx`，前缀 `working_memory:` | 作用域 TAG 与创建/更新时间 NUMERIC | RedisVL 创建索引、`FilterQuery` 查询 | 当前会话枚举目录；不是额外 Redis Set/ZSet |
| **Hash 上的搜索索引** | `memory_records`，前缀配置 `memory_idx` | TEXT、TAG、NUMERIC、VECTOR | RedisVL 向量/范围/关键词查询与聚合 | 过滤、全文召回、向量召回、重排；索引不是业务记录本身 |
| **String，值为 JSON 文本** | `task:<task_id>` | 任务状态与结果等 Task 序列化数据 | `SET ... EX`、`GET` | 7 天 TTL；更新状态时重新设置该 TTL；不是 RedisJSON |
| **Stream + consumer group** | 单实例 `memory-server:stream`；cluster URL 路径为 `{memory-server}:stream` | Docket 工作队列 | XADD/XREADGROUP/XACK/XDEL/XAUTOCLAIM；AMS 显式 `XGROUP CREATE ... $ MKSTREAM` | 队列源码已核查；端到端行为待验证，详见附录 |
| **ZSet + Hash** | `memory-server:queue` + `memory-server:<task_key>` | 计划执行时间与延迟任务载荷 | ZADD/ZRANGEBYSCORE + HSET/HGETALL；Lua 到期搬运 | 当前 Docket 延迟调度必需，与已弃用 sessions ZSet 无关 |
| **Hash** | `memory-server:runs:<task_key>` / `memory-server:progress:<task_key>` | 执行状态、generation、结果引用、进度 | HSET/HINCRBY 等；终态 TTL | Docket 元数据，不是 AMS `task:<id>` String |
| **ZSet + Set** | `memory-server:workers` / `task-workers:<function>` / `worker-tasks:<worker>`（均带 Docket 前缀） | 心跳与 worker 能力目录 | ZADD/ZREMRANGEBYSCORE、SADD/EXPIRE | 运行时队列调度元数据；完整键见附录 |
| **String + Pub/Sub** | 结果键 `memory-server:results::<task_key>`；state/progress/cancel channels | 持久化结果封装与非持久化事件 | SET/SETEX、PUBLISH/SUBSCRIBE | 两类机制须分开理解：channel 不是结果存储或可靠队列 |
| **String** | `extraction_debounce:<session_id>` | `extracted` 标记 | `EXISTS`、`TTL`、`SETEX` | 按配置秒数防抖；不是队列 |
| **String** | `extraction_pending:<session_id>` | 计划抽取的时间戳 | `SETEX`、`GET`、`DEL` | TTL 为本次 debounce 秒数的两倍；识别过时的延迟任务 |
| **String，值为 JSON 文本** | `summary_view:<view_id>:config` | 摘要视图配置 | `SET`、`GET`、`DEL` | 写入未设置 TTL；不是 RedisJSON |
| **Set** | `summary_view:index` | 摘要视图 ID 集合 | `SADD`、`SMEMBERS`、`SREM` | 应用维护的视图目录；元素不会因其他键删除而自动移除 |
| **String，值为 JSON 文本** | `summary_view:<view_id>:summary:<partition_key>` | 分组后的物化摘要结果 | `SET`、`SCAN`、`GET` | 写入未设置 TTL；删除视图配置时当前保留这些结果 |
| **String，值为 JSON 文本** | `auth_token:<token_hash>` | 令牌描述、创建和过期时间、hash | `SET`、`GET`、`EXPIRE`、`SCAN` | 可选整键 TTL；注意 token 内容与访问日志脱敏 |
| **Set** | `auth_tokens:list` | token hash 集合 | `SADD` 等集合操作 | 虽然名字带 `list`，实际是 Set，不是 List |
| **String** | `working_memory:migration:complete` / `working_memory:migration:remaining` | 迁移完成标志、剩余旧键数量 | `SET`、`GET`、计数调整 | 运维元数据，不是用户记忆 |

证据索引：

- 键名：`AMS-S/utils/keys.py:12–106`；默认索引配置：`AMS-S/config.py:377–418`。
- 工作记忆与长期记忆：`AMS-S/working_memory.py:623–652`、`AMS-S/memory_vector_db.py:474–505,803–815`。
- 两类索引：`AMS-S/working_memory_index.py:20–65`、`AMS-S/memory_vector_db_factory.py:129–175`。
- 任务状态：`AMS-S/tasks.py:11–13,47–75,155`；Stream 键：`AMS-S/utils/redis.py:83–92`；consumer group：`AMS-S/cli.py:500–512`。
- 防抖与延迟标记：`AMS-S/long_term_memory.py:163–246`。
- 摘要视图：`AMS-S/summary_views.py:45–57,102–183`；认证存储：`AMS-S/cli.py:954–977`。
- 迁移标记：`AMS-S/working_memory.py:26–27,55–100`。

### 3.2 残留、历史路径：List、Hash、ZSet

| 结构 | 源码中的用途 | 本报告如何界定 |
| --- | --- | --- |
| **List**：`messages:<session_id>` | `summarize_session` 使用 `LLEN`、`LRANGE`、`LTRIM` 处理历史消息 | 摘要兼容路径仍在且注册为任务；不是当前 JSON 工作记忆的 messages 数组 |
| **Hash**：`metadata:<session_id>` | 同一摘要路径使用 `HGETALL`、`HMSET` 保存 context/tokens | 与 List 配套的旧式表示；不应与 `memory_idx:*` 长期记忆 Hash 混淆 |
| **ZSet**：`sessions` / `sessions:<namespace>` | 旧会话目录，当前提供扫描并删除的迁移清理逻辑 | 已弃用；当前会话查询改走 JSON 搜索索引，不应新建这套 ZSet 双写目录 |
| **String 中的 JSON**：旧 `working_memory:*` | 迁移时读旧 String、删除旧键、写成 RedisJSON，并恢复 TTL | 历史存储格式，不是当前推荐表示 |

证据：`AMS-S/summarization.py:88–102,213–227`；`AMS-S/docket_tasks.py:24–38`；`AMS-S/utils/keys.py:41–54`；`AMS-S/working_memory.py:175–250`。

`context_key()`、`token_count_key()` 等键名辅助函数的存在，本身不能证明现行请求路径仍在写这些键。Docket 内部的延迟任务 ZSet、任务 Hash、心跳 ZSet 等已从锁定依赖源码确认，见附录第 2 节；它们不属于本节所说的旧会话目录。

### 3.3 工作记忆 JSON：字段级说明

| 字段组 | 文档内类型 | 作用 |
| --- | --- | --- |
| `messages` | JSON 数组，每个元素为消息对象 | 对话历史；不是 Redis List |
| `memories` | JSON 数组，每个元素为记忆记录 | 当前会话持有的结构化记忆；不是独立 Hash 列表 |
| `context` | 上下文内容 | 压缩或累计的上下文 |
| `data` | JSON 对象 | 应用扩展状态 |
| `tokens` | 数值 | 上下文预算统计；不是独立计数键 |
| `session_id` / `user_id` / `namespace` | 标识值，部分可空 | 查询作用域；key 组件也由这些标识构造 |
| `created_at` / `updated_at` / `last_accessed` | 数值时间戳 | 会话排序与访问时间 |
| `ttl_seconds` | 数值或空 | 应用配置字段；实际过期由额外 `EXPIRE` 生效 |
| `long_term_memory_strategy` | JSON 对象 | 抽取/提升策略配置 |

`working_memory_idx` 只索引 `session_id`、`namespace`、`user_id` 三个 TAG，以及 `created_at`、`updated_at` 两个 NUMERIC，均配置 sortable。它不是对整个会话文档自动全文索引。

### 3.4 长期记忆 Hash：字段、编码与索引

| 字段 | Hash 中的表示 | 搜索类型 / 作用 |
| --- | --- | --- |
| `text` | 文本 | TEXT：关键词检索 |
| `vector` | FLOAT32 数组的原始二进制 bytes | VECTOR：默认 HNSW + COSINE；不是 JSON 向量数组或 Base64 字符串 |
| `id_` | ID 字符串 | TAG；用于生成 `memory_idx:<id>` |
| `namespace`、`user_id`、`session_id` | 字符串；空值写成空字符串 | TAG：作用域过滤 |
| `memory_type`、`extraction_strategy` | 字符串 | TAG：类型和抽取方式 |
| `topics`、`entities`、`extracted_from` | 经编码的逗号分隔标签字符串 | TAG，分隔符 `,`；不是三个 Redis Set，也不是图数据库边 |
| `memory_hash` | 应用生成的内容指纹字符串 | TAG：精确内容去重；不是单独的去重 Hash/Set |
| `discrete_memory_extracted`、`pinned` | 标记值 | TAG：抽取状态、遗忘保护 |
| `metadata`、`extraction_strategy_config` | JSON 序列化后的字符串 | 配置为 TEXT；不是可按任意嵌套 JSONPath 查询的对象 |
| `access_count` | 数值字段值 | NUMERIC：访问计数 |
| `created_at`、`last_accessed`、`updated_at`、`persisted_at`、`event_date` | Unix 时间戳，缺失时间字段可不写入 | NUMERIC：范围过滤、排序和时间衰减 |

完整 schema 在 `AMS-S/memory_vector_db_factory.py:137–175`；编码在 `AMS-S/memory_vector_db.py:446–505,803–815`。

默认维度配置是 **1536**，但 `_get_embedding_dimensions()` 会结合模型配置确定实际维度，不能将 1536 写成所有模型的固定约束。维度、模型、距离算法变化应视为需要重建索引的 schema 变化。

容量估算应拆分正文、字段/键开销、倒排索引、数值索引、向量载荷和 HNSW 图。仅原始 FLOAT32 向量就是 `4 × D` 字节；1536 维为 6144 字节/条。这个数不是总内存，也不是本轮性能测量；是否存在向量副本要按索引实现和配置评估，不能机械按“双份向量”计算。

### 3.5 搜索索引究竟是什么

- **TEXT 倒排索引**：词项 → 包含词项的文档 ID，并保存评分所需信息。用于关键词召回。
- **TAG 倒排索引**：标签值 → 文档 ID 集合。用于 namespace/user/session/topics 等精确过滤。
- **NUMERIC 范围索引**：数值和文档 ID 的有序组织。用于时间范围、访问统计等过滤。
- **HNSW 向量索引**：多层近邻图，用于近似向量召回；Hash 中仍保留原始记忆与向量。
- **FLAT 向量索引**：存储向量并计算候选距离，适合小规模或正确性基线，不是现有服务的默认算法。

因此，`memory_records` 不是与 `memory_idx:<id>` 并列的另一份业务文档；它是从业务数据构建的检索结构。索引可重建，但 schema、模型版本和正文必须可恢复。

## 4. Dragonfly 有没有对应基础设施

### 4.1 Redis 类型到 Dragonfly 内部结构

| 对外结构 | Dragonfly 当前内部承载 | 是否需要重新实现 |
| --- | --- | --- |
| 键空间及值 | 每 shard 的 `PrimeTable = DashTable<PrimeKey, PrimeValue, ...>`；值使用紧凑对象表示 | 不需要新建全局记忆表 |
| String | 紧凑字符串值，可存 JSON 文本、状态标记和二进制数据 | 不需要 |
| Hash | 小对象使用 listpack，其他路径使用 `StringMap`；`StringMap` 基于 `DenseSet` | 不需要；支持长期记忆字段存储 |
| JSON | `jsoncons` 派生的 `JsonType` 及 JSON 命令/路径访问基础设施 | 不需要；文档内数组仍属于 JSON 类型 |
| Set | 整数集合路径使用 intset；字符串集合当前默认 OAHSet，可配置使用 StringSet | 不需要；可承载视图/令牌目录 |
| List | `QList` 节点式列表基础设施 | 不需要；旧摘要路径或新的有界消息日志可复用 |
| ZSet | listpack 或 `SortedMap`；后者声明为 `ScoreMap + BPTree` | 不需要；不是当前 V0 会话目录的必需结构 |
| Stream | stream 的 radix tree/listpack、consumer group、PEL 等结构 | 不需要另造基本队列类型；Docket 协议兼容性仍须验证 |

代码依据：

- `DF/src/server/table.h:28,127`：DashTable。
- `DF/src/server/hset_family.cc:114–145`、`DF/src/core/string_map.h:46`：Hash 两种表示。
- `DF/src/core/json/json_object.h:17–37`：JSON 类型。
- `DF/src/server/set_family.cc:33,72–106`：Set 实现选择。
- `DF/src/server/list_family.cc:101–130`：QList 使用路径。
- `DF/src/server/zset_family.cc:119–206`、`DF/src/core/sorted_map.h:142–148`：有序集合实现；不要沿用该头文件旧注释把当前实现直接称为 skiplist。
- `DF/src/redis/stream.h:22–29,61–79`：Stream 与 consumer group/PEL。

### 4.2 搜索索引到 Dragonfly 内部结构

| 索引类型 | 当前实现 | 对 Agent Memory 的用途 |
| --- | --- | --- |
| 文档 ID 映射 | `ShardDocIndex::DocKeyIndex` 维护 key 与 shard-local DocId 映射 | 把检索结果映射回记忆 Hash/JSON |
| TEXT | `RaxTreeMap` 词项目录 + `BlockList<CompressedSortedSet>`，另有词频、位置、文档长度信息 | 关键词、短语与 BM25 类评分 |
| TAG | `RaxTreeMap` + `BlockList<SortedVector<DocId>>` | namespace/user/session 与主题实体过滤 |
| NUMERIC | `RangeTree` 或可选 `absl::btree_set` 实现 | 时间范围与数值筛选 |
| FLAT | 连续 `vector<byte>` 向量区 + presence 位标记 | 小规模正确性对照、精确候选距离计算 |
| HNSW | hnswlib 适配；每 `(index, field)` 一个实例内跨 shard 的全局图 | 长期记忆近似召回；不是每个 shard 独立的一张完整 HNSW 图 |

证据：`DF/src/core/search/indices.h:50–85,88–94,197–208,217–253,294–314`；`DF/src/core/search/indices.cc:310–425`；`DF/src/server/search/doc_index.h:284–359`；`DF/src/core/search/hnsw_index.cc:8–10,63–67`。

HNSW 图使用带 shard 信息的 GlobalDocId，正文仍分片存储；复制要同时恢复图和文档映射，见 `DF/docs/hnsw-index-replication.md:5–10,27–50`。实现扩展时应沿用现有 fiber 和索引同步机制，不能另加全局 `std::mutex` 或同步模型调用。

### 4.3 命令与语义兼容矩阵

“已有”表示在此基线找到实现/测试，不代表整个 RedisVL/Docket 调用链已经运行通过。

| 服务依赖 | Dragonfly 结论 | 证据 / 验收重点 |
| --- | --- | --- |
| `FT.CREATE ON HASH/JSON`，TEXT/TAG/NUMERIC、sortable 字段 | 已有基础设施 | `search_family_test.cc:776–825,3505`；需用 RedisVL 实际 schema 创建并读回 |
| HNSW/FLAT、FLOAT32、COSINE | 已有 | `search_family.cc:109–148`；`search_family_test.cc:5225` 有 HNSW COSINE 距离测试 |
| KNN + 过滤、VECTOR_RANGE + 过滤 | 已有主要路径 | `search_family_test.cc:5809,5975,6040,6242,7154`；不能把 KNN 截断当成范围召回的等价替代 |
| `FT.AGGREGATE`、LOAD、APPLY、SORTBY、PARAMS | 已有主要路径 | `search_family.cc:842–913`；`search_family_test.cc:7058–7190` |
| recency 表达式的 `max()` / `pow()` | **明确缺口** | `filter_functions.cc:507–546` 未注册，`filter_eval.cc:157–160` 对未知函数返回 null |
| 关键词 BM25 / WITHSCORES / SCORER | 单实例已有；跨实例 cluster search 有明确限制 | `search_family_test.cc:8011–8254`；`search_family.cc:3086–3089` 拒绝该 cluster 路径的评分参数 |
| `FT.INFO`、`FT._LIST`、`FT.ALTER`、`FT.DROPINDEX` | 已注册并有相关测试 | `search_family.cc:4377–4390`；`search_family_test.cc:7470–7585,7756`；仍需匹配 SDK 解码字段 |
| `MODULE LIST` | 有兼容返回，不是未实现 | `server_family.cc:3998–4019` 返回 ReJSON/search 名称与版本；不可仅以此证明能力完整 |
| `HSETEX ... FXX KEEPTTL FIELDS ...` | 已支持 Redis 形式与集合条件 | `hset_family.cc:903–1043`；`hset_family_test.cc:607` 起的 Redis 格式测试 |
| JSON 读写、整键 TTL、DEL | 已有 | 需验收 `JSON.SET` 后 TTL 行为、更新与查询索引一致性 |
| Lua 内 `TYPE/TTL/GET/DEL/JSON.SET/EXPIRE` | 有相关基础能力，组合脚本待验收 | V0 迁移脚本见 `working_memory.py:234–250`；不能只验证独立 JSON.SET |
| Stream consumer group / ACK / reclaim | 命令基础已有 | `stream_family.cc:4323–4348` 注册 XADD/XGROUP/XREADGROUP/XACK/XAUTOCLAIM；不据此推断 Docket 全兼容 |
| 文档删除与搜索索引清理 | 有删除回调和过期结果处理 | `doc_index.cc:988–1031,1308–1309`；`db_slice.cc:949–952`；应用自建 Set 和摘要结果不在此自动清理范围 |

本节未带 `DF/` 的 Dragonfly 文件分别位于 `src/server/search/` 或 `src/server/`，以文件名所对应模块为准。

## 5. 需要实施的适配与风险处理

### 5.1 明确功能缺口：服务端时间衰减排序

`AMS-S/utils/redis_query.py:61–83` 构造：

- `days_since_access = max(0, (now - last_accessed) / 86400)`；
- `freshness = pow(2, -days_since_access / half_life_access)`；
- 类似方式计算创建时间衰减；
- 再与 `1 - __vector_score / 2` 混合，按 boosted_score 排序。

当前 Dragonfly 未注册 `max`、`pow`，未知函数在求值时返回 null。2026-09-30 的实际 FT.AGGREGATE 调用已确认两个函数均返回 RESP null，详见附录第 7 节。因此不能以“APPLY 命令没报错”判断排序正确，必须校验中间字段和最终顺序。

**建议**：在通用表达式层补齐必要函数及参数/空值/非有限数处理；新增表达式单测和 V0 原式集成测试。若先做不改内核的 MVP，可关闭 server-side recency，使用已有客户端重排，但要固定候选集合与分页语义。客户端只重排有限候选，不能宣称与全量服务端排序完全等价。

### 5.2 SDK 和后台任务：已定位问题与剩余验证

- RedisVL 0.12.1 的常规初始化已取消模块版本强制校验；`exists()` 走 `FT._LIST`。应测试实际命令和解码，不把猜测的 MODULE LIST 门槛当作既定阻塞。
- V0 recency 路径传递 query 对象而非字符串、LOAD list 未展开、未传向量 PARAMS，并使用与依赖不同的距离别名；另需核对 FILTER 和分页候选预算。详见附录第 4.3 节。这些是应用/SDK 合同问题，不能只修 Dragonfly。
- Docket 延迟调度 Lua 根据 ZSet 内容访问动态 Hash 键，但只声明 queue/stream 两个 KEYS；与默认 Dragonfly 声明键规则存在冲突，详见附录第 3 节。2026-09-30 已复现默认错误和 SHA 定向允许后的成功搬运；后续仍需队列端到端验证和 GLOBAL 模式延迟评估，见附录第 7 节。
- 定向允许 undeclared 并保持原子性会采用 GLOBAL 事务模式，可能影响前台延迟；不可用全局 disable-atomicity 作为无代价替代。
- 仍须运行两套完整 schema、RedisVL CRUD/检索/聚合、Docket 发布/消费/延迟/取消/重投，以及 RESP、TAG 转义、TTL 和恢复测试。本报告没有将静态问题定位等同于端到端认证。

**实施次序补充**：先修复 V0 的查询合同，再验收第 5.1 节的数学函数和排序；聚合异常会 fallback 到普通搜索，返回成功不能证明 server-side recency 生效。

### 5.3 隔离不能照搬 namespace 软过滤

`AMS-S/api.py:751–780` 在语义搜索无结果时会移除包括 `namespace` 在内的部分过滤条件，再进行 fallback。这个行为可以作为业务召回策略，但**不能直接把 namespace 当作不可跨越的租户权限边界**。

实施要求：

1. 从认证上下文得到 tenant/user 权限；强制作用域必须保留在所有检索、计数、读 ID、更新、删除和后台任务中。
2. 业务标签允许放宽，权限过滤永远不能放宽；不允许在无权限约束的 Top-K 之后才过滤。
3. 当前防抖键只含 `session_id`，新服务应编码 tenant/user/session，避免跨作用域互相抑制任务。
4. 对拼接键组件采用无歧义编码或内部不透明 ID；仅用冒号拼接可选组件不是完整的命名空间设计。
5. 对数据库凭据使用最小权限；API key 的存在不自动赋予逐条记忆授权语义。

### 5.4 TTL、业务遗忘和删除是三件不同的事

- **工作记忆 TTL**：当前为 JSON 整键 EXPIRE；索引清理机制不能顺便清理应用自建的其他键。
- **长期记忆遗忘**：当前写入没有通用逐条 EXPIRE，主要按年龄、未访问时长、预算等选择 ID 后删除；`pinned` 是业务保护逻辑。不能写成“长期记忆默认自动 TTL”。
- **HSETEX 的 KEEPTTL**：此处用于保留 Hash 字段过期语义，FXX 防止后台任务复活已删除记录。不能用普通 HSET 无条件替代。
- **摘要物化结果**：`summary_views.py:147–166` 明确保留被删视图的分区摘要；新服务若承诺彻底删除，必须补清理。
- **异步任务竞争**：对已删除或已迁移版本的记忆，worker 应检查 tombstone/version，再决定是否写入；只删正文不足以保证永不重建。

推荐将写入状态更新与任务投递设计为可恢复流程：使用受支持的原子操作或 outbox/补偿扫描，不能把“写入成功但任务未投递”永久留在 pending。

### 5.5 持久化和复制要分别验收

`DF/src/server/rdb_save.cc:1795–1805` 明确：传统 RDB 保存模式不写搜索索引定义；其他保存/复制模式存在 `search-index` AUX 信息路径。因此，导出 RDB 后不能默认搜索索引自动恢复，应保存 schema 并安排重建。

HNSW 图已有专门序列化与恢复设计；`DF/src/server/snapshot.cc:33` 的 `serialize_hnsw_index` 默认关闭。图不随该优化路径传输时，复制端可走重建流程，但仍须验证查询就绪时间和结果。参见 `DF/docs/hnsw-index-replication.md:103–158`。

验收必须区分：单机重启、所选快照格式、主从全量同步、增量写入/删除、不同 shard 数恢复和故障切换。不能把“正文存在”当作“检索已就绪”。

## 6. 推荐实施方案

### 6.1 架构与改动边界

推荐顺序：**保留应用层编排 → 验证当前 RedisVL 路径 → 修复明确的通用缺口 → 仅在确有需要时增加 Dragonfly 专用适配器。**

- API/MCP：认证、参数校验、作用域、幂等 ID、记忆业务协议。
- Memory service：工作记忆读写、抽取/摘要、长期记忆管理、检索融合和遗忘策略。
- Worker：模型调用、异步索引准备、整合、重试和清理。
- Dragonfly：JSON/Hash/String/Set/Stream、搜索向量索引、TTL、事务基础能力与恢复。

`AMS-S/memory_vector_db.py:138–245` 已有 `MemoryVectorDatabase` 抽象；`AMS-S/memory_vector_db_factory.py:204–218` 支持配置工厂。若确需适配，可在该边界新增实现，不需要把业务代码复制到 Dragonfly C++ 核心。

但此抽象主要覆盖长期记忆：**换一个 vector DB factory 并不会自动适配工作记忆 JSON、认证、摘要视图和 Docket**，这些也要纳入合同测试。

### 6.2 第一版到底保留哪些数据结构

| 场景 | 推荐结构 | 理由 |
| --- | --- | --- |
| 会话工作记忆 | 保留 JSON + TAG/NUMERIC 会话索引 | 最小化 V0 改动；规定文档大小上限和并发写入版本控制 |
| 长期记忆 | 保留 Hash + TEXT/TAG/NUMERIC/HNSW 索引 | 与现有字段编码、RedisVL 模式一致 |
| 任务状态 | String(JSON 文本) + 有限 TTL | 简单可恢复；与队列消息区分 |
| 任务传递 | 优先验证 Docket 原路径的 Stream | 不在兼容性尚未定位时先重写队列 |
| 摘要目录 | Set；配置和物化结果用 String | 复用已有表示，但补授权、清理和结果版本 |
| 防抖/幂等/删除墓碑 | 带作用域的 String 或小 Hash，按语义设置 TTL | 明确区分各类状态，不混用一个锁标记 |
| 旧会话 ZSet | 不作为新主路径 | 避免与 JSON TTL/索引再维护一套不一致目录 |
| 独立消息 List/Stream | 首版不新增；大消息量时再评估 | 当前 JSON 是整文档写入；拆分可以减少重写，但同时引入序列、摘要和 TTL 一致性成本 |

第一版不引入图数据库：topics/entities/extracted_from 目前是标签和来源 ID，不是图查询需求。也不为了“实现记忆”新建通用数据结构；真正新增的是应用协议、一致性约束和适配测试。

### 6.3 分阶段交付与退出条件

| 阶段 | 工作内容 | 交付与退出条件 |
| --- | --- | --- |
| P0：命令合同 | 锁定依赖；提取两套完整 schema；记录 RedisVL/Docket 调用；用固定小向量绕过真实模型依赖 | 明确每项支持/失败/未验证；两个索引可创建、写入、查询和删除；失败有最小复现 |
| P1：最小业务闭环 | 工作记忆读写/TTL，长期记忆写入/读 ID/作用域检索/删除；选定 recency 路径；状态与幂等 | 不跨租户；无重复副作用；删除后后台任务不复活数据；分页与排序可解释 |
| P2：完整记忆能力 | 补齐必要表达式函数；关键词与混合检索；抽取、去重、摘要、遗忘；Docket 重投 | V0 对应功能用例在 Dragonfly 后端通过；输出分数、数量和副作用符合约定 |
| P3：可运维交付 | schema 版本化、重建、备份恢复、主从、观测、容量控制、删除清理 | 所选恢复方式检索可用；恢复时间可观测；故障注入无永久丢任务或隔离破坏 |
| P4：扩展评估 | 数据量增长、模型升级、多实例/cluster、性能与内存专项 | 单独出兼容和测量报告，不能沿用单实例结论 |

### 6.4 预计改动位置（本轮不实施）

| 仓库 / 位置 | 后续工作 |
| --- | --- |
| Dragonfly `src/server/search/filter_functions.cc` | 通用 `max/pow` 能力，若选择数据库侧补齐方案 |
| Dragonfly `src/server/search/filter_expr_test.cc`、`search_family_test.cc` | 数学函数边界和真实 recency 聚合测试 |
| Dragonfly `tests/dragonfly/` 下新增 agent-memory 兼容测试 | 两套 schema、RedisVL 返回值、TTL/删除、恢复用例；避免把外部模型 API 作为基础测试前置条件 |
| AMS `memory_vector_db_factory.py` / `memory_vector_db.py` | 仅在原路径不能满足合同时增加适配器；保留字段和评分合同 |
| AMS `api.py` / `working_memory.py` | 强制作用域、fallback 限制、会话更新原子性或版本控制 |
| AMS `long_term_memory.py` / `summary_views.py` / 任务层 | 作用域键、删除墓碑、摘要清理、幂等与任务补偿 |
| 部署配置和启动检查 | 固定搜索构建选项、依赖版本、embedding 维度、索引 schema 和就绪检查 |

这些是后续实施候选，不表示本报告已批准或修改同级仓库。

## 7. 验收计划

### 7.1 按数据结构验证

| 对象 | 必须覆盖的测试 |
| --- | --- |
| JSON 工作记忆 | messages/memories/data 往返不丢失；可空字段；并发更新；TTL；按作用域分页；旧 String 迁移脚本 |
| Hash 长期记忆 | 二进制向量完整性；字段编码；维度错误；局部更新不破坏向量；FXX 在键或字段缺失时不重建孤儿 Hash |
| TAG/TEXT/NUMERIC | 同名用户跨租户；逗号/转义；空值；时间边界；关键词与短语；排序稳定性 |
| HNSW/FLAT | 固定向量 COSINE 距离；KNN 与范围；多过滤条件；零向量和非法向量行为；更新/删除/过期后结果 |
| APPLY/排序 | V0 原始 max/pow 衰减公式；空字段；未来时间；零/非法 half-life；offset/limit；分数不是 null |
| String/Set 辅助数据 | 任务状态 TTL；token 过期后的目录残留处理；摘要视图删除覆盖分区结果；防抖作用域不冲突 |
| Stream/Docket | 重复投递、worker 崩溃、ACK、pending reclaim、延迟任务、重试上限；明确至少一次投递下的幂等 |
| 生命周期 | JSON/Hash 删除后索引无残留可见结果；晚到任务不复活记录；pinned 遗忘策略；租户清理 |
| 恢复 | 快照加载、索引重建、复制、不同 shard 数、故障切换、查询就绪门槛 |

### 7.2 可复用测试与运行方式

V0 已有相关测试入口，如 `AMS/V0/tests/test_recency_aggregation.py`、`test_memory_compaction.py`、`test_summary_views.py`、`test_tasks.py`、`test_no_worker_mode.py`。应确认 fixture 对 Redis 8 的启动/连接方式后再接入 Dragonfly，不要只替换某一个 URL 就声称全套测试已迁移。

Dragonfly 已有 `search_family_test`、`filter_expr_test`、`hset_family_test`、`json_family_test`、`stream_family_test` 等目标，见 `DF/src/server/CMakeLists.txt:142–188`。后续实现的基本验证命令：

```bash
# 在 Dragonfly 仓库根目录配置开发构建，保留搜索和 JSON 所需选项。
./helio/blaze.sh -DWITH_AWS=OFF -DWITH_GCP=OFF \
  -DWITH_SEARCH=ON -DWITH_COLLECTION_CMDS=ON -DWITH_EXTENSION_CMDS=ON

# 在 build-dbg 目录构建并执行相关测试。
cd build-dbg
ninja dragonfly search_family_test filter_expr_test hset_family_test json_family_test stream_family_test
./search_family_test
./filter_expr_test
./hset_family_test
./json_family_test
./stream_family_test
```

以上为完整实施阶段命令。2026-09-30 已执行其中的 `ninja dragonfly filter_expr_test` 和 `./filter_expr_test`，79 项单测全部通过；其余目标未执行。后续代码变更还应运行对应 Python 集成测试，以及适用的 Debug/Release、ASAN/UBSAN 验证；不运行 codeql_checker。

### 7.3 本轮完成与未完成项

- 已完成两仓库基线核对、主要实现/数据结构/搜索能力的静态分析。
- 已将分支调整为 `feat/agent-memory`，新增本报告及依赖核查附录；依赖源码仅下载到临时目录，没有安装或执行。
- 未改动 C++、Python 服务功能或同级 AMS 仓库。
- 2026-09-30 已构建 dragonfly/filter_expr_test，79 项表达式单测通过；用两个内部 shard 实测 Docket 原始调度 Lua 和 FT.AGGREGATE 数学函数。过程和剩余项见附录第 7 节。
- 未运行模型调用、RedisVL/Docket 端到端、恢复测试或性能测试；最小复现不等同于完整 Agent Memory 功能已实现。
- 执行 `pre-commit run --files docs/agent-memory-implementation-report.md` 失败：命令未安装；`python3 -m pre_commit --version` 也返回模块不存在，未自动安装依赖。
- 两份文档自检通过：末尾换行、无尾随空白、代码块闭合、文档链接有效；主报告 36 个、附录 23 个带前缀源码路径已检查存在及起始行号范围；五个依赖归档 SHA256 与锁文件一致。`git diff --check` 通过；新文件另做了上述直接检查。两个仓库状态核对确认仅新增两份文档。

## 8. 最终建议

**优先做基于现有数据结构的服务适配，而不是重新实现数据结构。**

工作记忆使用 JSON，长期记忆使用 Hash，搜索层使用 TEXT/TAG/NUMERIC 与 HNSW，任务及辅助状态使用 Stream/ZSet/Hash/String/Set 和 Pub/Sub；Dragonfly 对应设施已经存在。第一轮开发资源应集中在应用/SDK 查询合同、Docket 动态键 Lua 兼容、recency 表达式、租户隔离、异步删除/幂等及恢复，而不是新增数据库专用记忆命令。

完成 P0/P1 后再决定是否需要专用 adapter 或更深层内核扩展。只有真实失败用例和测量结果，才能证明下一步该改哪一层。
