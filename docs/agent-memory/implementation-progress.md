# Agent Memory 实施记录

[概览](README.md) · [待办清单](TODO.md) · [运行方式](compatibility.md)

## 2026-09-30：兼容阻塞与工作记忆写入

本轮基于 Dragonfly `ebcfe4c9b896d5e3d1c25512623859682cd89477` 和 AMS `94192c39e2a4a154f441a5411e3d73c4f54974a6` 的工作区修改，尚未提交。Dragonfly 分支为 `feat/agent-memory`，AMS 分支为 `feat/dragonfly-agent-memory`。

| 项目 | 实现与验收证据 |
| --- | --- |
| P0-01 | `tests/agent_memory/run.sh`、`conftest.py`：锁定 SDK、自检版本、独立临时实例、两个内部 shard、固定向量与退出清理 |
| P0-02 | `test_search.py`：使用实际 V0 schema，通过 RedisVL 创建 JSON/Hash 索引，写入/读回、更新/删除、过滤与 SDK 解码；长期记忆分别使用 FLAT/HNSW |
| P0-03 | `test_search.py`：KNN、VECTOR_RANGE、关键词/混合检索、TAG 空格/连字符转义、逗号拒绝、空结果、错误向量维度与距离数值 |
| P0-04 | AMS `utils/redis_query.py`、`memory_vector_db.py`：查询字符串、LOAD 展开、PARAMS、别名、FILTER 与排序、固定候选池分页及 lookahead；`test_recency_aggregation.py` 与真实 SDK 测试 |
| P0-05 | Dragonfly `filter_functions.cc`：max/pow/coalesce；`filter_expr_test.cc` 验证数值字符串、参数数、null、NaN/Inf、定义域与溢出 |
| P0-06 | `test_search.py`：实际聚合分数、客户端排序对照、第二页、缺失时间、未来时间、同分 ID 顺序及范围查询；直接执行聚合，禁止用 fallback 当通过 |
| P0-07 | `tools/agent_memory/audit_docket.py` 和九段 SHA 清单；选定 scheduler SHA 允许策略；`test_docket.py` 验证默认失败且状态未变、定向允许后的多任务搬运 |
| P0-08 | `test_docket.py`：真实 Docket 序列化与 worker，立即/延迟、结果 Pub/Sub、AMS Task 状态 API、progress 清理、ACK/PEL、retry/cancel；使用测试任务，不代表业务抽取闭环 |
| P1-01 | AMS `working_memory.py`、models/API/MCP/client：原子 JSON+TTL、版本冲突、文档大小限制；`test_working_memory.py` 验证读写/枚举/TTL、并发冲突、409/413 以及删除重建拒绝旧 revision |

P1-02 已增加实际业务按 ID 读取、hash 去重、访问统计和删除测试；访问更新改为原子存在性/时间间隔检查，删除后不再重建 Hash。语义去重、完整业务副作用尚需继续验证，所以没有勾选该项。

P1-04 已用跨删除/过期保留的单调 revision 防止工作记忆旧版本覆盖重建会话，但长期记忆墓碑、抽取任务的写入竞争和任务作用域/幂等仍未完成。

P1-05 新增 `test_crashed_worker_reclaims_pending_message`：真实子进程 worker 接收任务后被 SIGKILL，恢复 worker 重投 pending message；验证结果、执行副作用和 PEL 清空。单独运行 **1/1** 通过。这不能替代业务任务的幂等和完整故障状态收敛验收。

P1-06 新增 `test_summary_cleanup.py`：确定性模型桩验证摘要结果和删除清理，并验证含 glob 字符的 view ID 不误删其他摘要。

P1-04 摘要竞争修复：每次保存配置由服务器分配新 generation，计算结果携带读取时的 generation；原子 Lua 在发布前检查配置仍属同一代。结果键按 generation 分区，替换/删除配置的事务捕获旧配置，只清理旧代，避免清理过程中误删重建后的结果。覆盖配置替换、同 ID 删除重建、旧格式配置清理与重建竞争、缺失 revision 拒绝、模型调用暂停期间重建，以及同步分区 API 的 HTTP 409。后台旧 refresh 记录 FAILED，不覆盖新结果。长期记忆墓碑与抽取幂等仍未完成，P1-04 保持未勾选。

P1-03 部分实现：已验证 JWT 的 namespace/sub 绑定工作记忆 API 的读/写/删除/枚举与 memory prompt 的会话访问。结构化记录也必须位于同一 namespace/user/session。检索、compact、forget 注入可信作用域；检索过滤只能使用作用域的精确相等条件，无结果软回退保留授权 namespace/user。admin role 不绕过租户绑定；未绑定 namespace 的旧 token 对这些接口返回 403。`test_tenant_scope.py` 使用本地签名 RSA JWT，经真实认证和 HTTP 路由验证；其中检索回退使用核心查询桩检查两次调用的实际参数。

工作记忆键采用 v2 固定 namespace/user/session 位置及 URL 编码，区分 None/空字符串并消除冒号拼接碰撞。已索引的旧 JSON 文档在读/写/删除时按文档身份原子迁移，保留 TTL 与 revision，文档 revision 高于旧计数器时取较大值；目标 revision 已存在时拒绝迁入旧数据。验证旧格式 read/write/delete、更新冲突与删除后旧键复活拒绝。未完成旧 String 批量迁移、混合版本部署与完整 schema 迁移验收。

P1-03 摘要/任务入口已补所有权：SummaryView 与 Task 保存服务器分配的 owner_namespace/owner_user_id，认证开启时枚举只返回本人资源，跨租户按 ID 读取/运行/删除返回 404。无 owner 的历史资源不自动归属于访问者。配置创建注入可信过滤，分区 group 不能越过 owner；后台查询也以持久化 owner 强制覆盖作用域，防止旧 filters 或 group 放宽范围。

P1-04 摘要任务幂等：客户端自定义 ID 按 namespace/user/ID 固定位置编码为 URL-safe 的返回 task ID；Redis SET NX 原子保留任务，不覆盖已有记录。同 owner、view、generation 和任务类型的重复请求返回已有任务，不重复入队；不匹配则 HTTP 409。队列载荷固定配置 generation，执行前配置已变化时将 AMS task 标为 FAILED，不查询新配置。

P1-04/P1-05 Task 一致性：每次成功创建任务分配独立 task generation，HTTP 摘要队列载荷携带该值。状态回写采用声明单键的原子 CAS；竞争时重新读取并重新校验状态转换，删除/过期返回 False，不创建任务。同 ID 重建时旧 worker 的 generation 无效，不能回写新任务。摘要发布脚本同时校验配置 generation、任务 generation 与 RUNNING 状态；任务到期或替换后旧模型结果不能发布。已完成任务的重投直接跳过，并发重投保留单个分区结果且不将 SUCCESS 改为 FAILED。

`test_task_consistency.py` 以读取屏障复现终态竞争、读后删除/真实 TTL 过期/同 ID 重建；在真实模型桩等待期间替换任务，验证新任务和结果未被旧 worker 修改。并发重复摘要可仍调用模型两次，但不会生成多个分区记录；没有宣称模型调用 exactly-once。入队失败后的持久化恢复、所有业务任务的 generation 协议、长期记忆墓碑与抽取幂等仍未完成。

P1-07 已验证摘要子链路：修复 FastAPI 为子类注解注入基础 BackgroundTasks 的行为，HTTP 调度显式选择 HybridBackgroundTasks，进入实际 Docket。`test_tenant_scope.py` 的真实 HTTP → Docket → worker → RedisVL → 模型桩 → 摘要结果 → Task API 测试，使用真实索引中的三个记录；模型提示词只含本租户两条记录，结果 memory_count=2，Task SUCCESS，PEL=0。重复请求只保留一条 Stream 消息；配置变更后的排队任务 AMS 状态 FAILED，模型不被再次调用。该拒绝属于已处理的业务条件，Docket 可以正常结束执行，不等于 AMS 业务成功。

P1-03 仍未完成：MCP 身份绑定、抽取防抖键、旧 Hash 迁移和所有内部删除路径还需统一。长期记忆 API 与新键的后续进展见下节。P1-07 仍需工作记忆提升/抽取、长期召回和删除的完整链路。当前不能将部分入口的隔离等同于整个服务已强制租户隔离。

## 验证记录

- Debug：`ninja dragonfly filter_expr_test` 构建成功，`./filter_expr_test` **83/83** 通过。
- Release：`ninja filter_expr_test` 构建成功，`./filter_expr_test` **83/83** 通过。
- Debug 搜索回归：`ninja search_family_test`；`./search_family_test --gtest_filter='*Aggregate*'` **65/65** 通过。
- 真实 SDK、工作记忆、摘要清理、租户入口与 Docket 合同：`UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q`，**58/58** 通过，包括 worker 崩溃重投、摘要/Task generation 竞争、工作记忆键迁移及真实 HTTP 摘要队列链路。`UV_BIN` 是本机工具路径，其他环境使用已安装的 uv。
- AMS 与 Python client 相关回归：auth、recency、memory_vector_db、working_memory_strategies、models、client/tool schemas，**220/220** 通过。
- AMS 摘要函数回归：`test_summary_views.py` 的 TestBuildLongTermFiltersForView、TestPartitionMemoriesByGroup、TestDecodePartitionKey、TestSummarizePartitionPlaceholder，**10/10** 通过；未运行其全部 Docker/API/model 集成用例。
- AMS ruff 格式与 lint 通过。Dragonfly 原 pre-commit 下载 GitHub hooks 时超时；使用已安装的同版本 pyflakes 3.4.0、pre-commit-hooks 4.3.0、clang-format 14.0.6、black 25.1.0，以临时 local-hook 配置执行对应文件检查。

上述 58 项与 220+10 项是较早验证记录，不能覆盖后续全部修改。集成测试有一条现有 Pydantic class-based config 弃用警告。

未完成全套 C++/AMS 测试、ASAN/UBSAN、真实生成模型接入、完整业务抽取/摘要/遗忘、租户隔离、业务任务故障状态收敛、快照/复制与性能验收。整体目标仍在实施，不能把本轮 P0 通过写成端到端或上线完成。

## 后续工作区进展：长期记忆作用域

已加入长期记忆 v2 作用域键、保持逻辑 id_、API 创建及按 ID 访问的 JWT 绑定、精确身份过滤、身份 TAG 的 CASESENSITIVE 就绪检查。`test_http_long_term_same_id_is_independent_per_owner` 覆盖五组 owner（包括大小写差异）共用同一逻辑 ID 的 HTTP → Docket → RedisVL 写入及各自读/改/删；模型抽取子任务被桩替代，不代表完整抽取链路。FLAT/HNSW 测试覆盖不删除文档的身份索引重建。

接续摘要记录最近一次集成测试为 **61 passed**、AMS/client 与摘要函数回归为 **230 passed**。本次文档整理没有重新运行这些测试；230 项运行早于最新精确身份过滤，61 项也不能覆盖其后修改的 exact_scope/遗忘策略。遗忘选择已改用作用域物理身份区分同名记录，但跨 owner pin/预算回归仍需补充并重跑。代码静态核对确认 compact 和语义合并删除尚未传入 exact_scope=True。

以上均为未提交工作区修改。下一步、命令和迁移限制见[接续工作](handoff.md)；任务仍为 9/20 完成，没有增加完成勾选。

## 接续验证：精确删除与同名记忆遗忘

compact 和语义合并的内部删除已传入 exact_scope=True，与 ID 去重、无效抽取清理及遗忘一致。新增 `test_forgetting_same_id_preserves_other_owners`，分别在 FLAT/HNSW、年龄/预算策略下验证：三组身份共用逻辑 ID，只删除选中记录，另一 owner 的 pin 和无 owner 的新记录保留，dry-run 不改数据，实际 Hash 内容及索引结果一致。

最新真实 Dragonfly 集成测试 **65 passed**（一条既有 Pydantic 弃用警告），AMS/client 与摘要函数回归 **230 passed**。本轮应用文件 Ruff 格式/lint 通过，两个 Dragonfly Python 测试通过同版本 local pre-commit hooks，diff 空白检查通过。未运行完整模型/API、C++ 全套及运维验收，P1-02/P1-03/P1-06 总任务保持未完成。

## 接续验证：跨批次精确删除

发现 exact_scope=True 且目标为无 owner 时，首批 500 条可能全部属于其他 owner，原实现因零删除提前结束。删除查询现在用 RedisVL paging 跳过保留记录，扣除本批已删除行后推进 offset；保留每键 Lua 身份检查。

`test_exact_unowned_delete_scans_past_other_owners` 在 FLAT/HNSW 各写入 502 条同 ID 记录，明确选择首批之外的文档改为无 owner，验证精确删除只删除这一条、其他 501 条保留、再次精确删除为零。后续全局删除验证剩余记录跨批次清空。没有承诺并发新增/重排期间的搜索快照语义。

完整真实数据库集成测试 **67 passed**；新增全局清理断言后的定向测试 **2 passed**；AMS memory_vector_db/recency 回归 **39 passed**。Ruff 和同版本 local pre-commit 检查通过。

MCP 核查发现共享 `_current_request` 请求字段及工具直接调用 FastAPI 依赖函数的问题；尚未修改 MCP。后续需一并实现请求隔离、传输认证、可信身份注入和 SSE/HTTP 会话所有权检查，不能只把 URL namespace 作为授权来源。

## 接续实现：MCP 请求上下文隔离

AMS `mcp.py` 移除服务实例共享的 `_current_request`，改用实例内 ContextVar；SSE 和 streamable HTTP 在处理期间 set，并在 finally 中 reset。工具 namespace 从任务上下文获取，工具参数复制后再注入，避免修改调用者持有的字典。

新增 `test_mcp_context.py`：并发两个 namespace 验证各自的工具参数、调用后默认 namespace 不受污染、原参数不变；streamable HTTP handler 抛异常后上下文恢复为 None。测试使用传输/工具执行桩，尚未覆盖真实 MCP 客户端会话或传输认证。

修改后完整兼容测试 **69 passed**，保留一条既有 Pydantic 弃用警告。AMS MCP Ruff 和新测试的同版本 local pre-commit 检查通过。P1-03 保持未完成：下一步需传输层 bearer 验证、工具调用的可信身份注入、SSE/HTTP 会话所有权、抽取防抖键及真实客户端跨租户测试。ContextVar 只解决并发上下文共享，不将 URL namespace 提升为授权来源。

## 接续实现：MCP 传输 bearer 校验

SSE 连接、SSE message POST 和 streamable HTTP handler 调用与 REST 相同的 get_current_user/authorize_scope，成功后把身份保存在任务独立请求的 state.mcp_user。认证失败返回 JSON 错误及原状态/headers，不进入 MCP transport；URL namespace 必须匹配认证绑定，显式禁用认证仍为开发模式。

新增 SSE/HTTP ASGI 拒绝测试，以及真实本地 RSA 签名 JWT 的 HTTP 测试：有效 owner 成功、跨 namespace/无 namespace 绑定返回 403、无认证或无效 token 返回 401。MCP session manager 使用桩，尚未跑真实 MCP 客户端握手、SSE 交互和完整工具链。

完整集成测试 **72 passed**（一条既有 Pydantic 弃用警告），MCP Ruff 与修改测试的同版本 local pre-commit 检查通过。工具函数的可信身份注入仍未完成；有效凭证不能代替 SSE/HTTP session 所有权检查。当前不能宣布 MCP 租户隔离完成，也不能用此结果覆盖 STDIO 的身份协议。

## 接续实现：MCP 工具身份注入

新增 _tool_user 从已认证请求上下文获取 UserInfo；认证开启且没有上下文返回 401，开发禁用认证使用显式本地身份。创建、搜索、prompt、长期记忆按 ID 读/改/删显式向 API 传 current_user；工作记忆读写与 compact 调用 authorize_scope，prompt 的过滤调用 authorize_filter_scope。工作记忆结构记录校验原始 owner 和 session，搜索不再将 HTTP 授权错误转换为空结果。

上下文/作用域测试验证按 ID 调用传入 owner、跨 namespace 和 any 过滤拒绝、上下文外访问拒绝。真实 RSA JWT → MCP helper → 工具 → 核心 API → RedisVL → 两 shard Dragonfly 测试验证同 ID 仅读取/删除本租户记录，另一租户 Hash 保留；没有使用真实 MCP 客户端握手。

应用修改后完整集成测试 **73 passed**；随后增加真实读删测试，MCP 定向测试 **7 passed**。Ruff、同版本 local pre-commit 和 diff 检查通过。仍需 SSE/HTTP session 所有权、真实客户端、默认 namespace/user 与认证模式的完整合同、STDIO 身份协议，以及抽取防抖键等 P1-03 验收。

## 接续实现：MCP 默认作用域合同

开发 namespace/user 默认值只在 disable_auth 或 auth_mode=disabled 时应用，认证模式使用请求中已验证身份。set_working_memory 的参数默认改为 None，并在调用时读取开发配置，避免模块导入时固化身份。工具 dispatcher、创建、检索和 prompt 的默认注入遵守同一规则。

回归配置 foreign-default/other-user，验证认证检索绑定 orchard/alice，认证 dispatcher 不注入 foreign-default。完整集成测试 **75 passed**；MCP Ruff、新测试同版本 local pre-commit 和 diff 检查通过。

认证开启的 STDIO 当前没有 bearer 请求上下文，记忆工具会 fail closed 返回 401；尚未实现可用的 STDIO 身份协议。SSE/HTTP session 所有权与真实客户端全链路仍待验证，P1-03 保持未完成。

## 接续实现：SSE session 所有权

锁定 SDK 在 endpoint SSE event 发布 UUID，POST 使用 query session_id 查找 writer。现在在转发 endpoint event 前把 UUID 绑定连接的已认证 namespace/sub，消息 POST 验证该绑定；其他身份与已关闭/未知会话返回 404，无效 UUID 返回 400。连接 finally 清理绑定，保留请求 ContextVar 恢复。

传输桩回归覆盖同 namespace 不同用户：其他有效身份不能提交，本人 POST 为 202，断开后本人也不能重用 session。所有权不是从首次 POST 认领，身份仍须每次提交验证。SDK endpoint event 格式发生变化时需要重新核查绑定协议。

完整集成测试 **76 passed**，MCP 定向 **9 passed**；Ruff、同版本 local pre-commit 及 diff 检查通过。该回归未使用真实 SSE 客户端。streamable HTTP 的 stateful session 所有权、STDIO 协议及真实 MCP 客户端验证仍待实现，P1-03 保持未完成。

## 接续实现：stateful HTTP session 所有权

streamable HTTP 包装器在成功响应的 Mcp-Session-Id header 对客户端可见前绑定 namespace/sub。后续携带该 ID 的请求必须匹配 owner；未知或其他 owner 返回 404，不能通过有效 bearer 认领已有会话。绑定按 SDK _server_instances 与 is_terminated 状态清理，覆盖正常终止和 SDK 移除会话；stateless 模式不生成 owner 元数据。

新增传输桩测试验证创建、本 owner 继续调用、同 namespace 的另一用户被拒绝且不进入 manager、DELETE 后 session 被拒绝。完整集成 **77 passed**，Ruff、同版本 local pre-commit 与 diff 检查通过。SDK 内部会话字段依赖必须随锁定版本升级重新核查；真实 stateful HTTP/SSE 客户端测试仍待补，P1-03 不勾选。

## 接续验收：真实 MCP HTTP/SSE 客户端

`test_real_mcp_client_owner_and_database` 用实际 Uvicorn 本地随机端口运行全局 MCP app，锁定 SDK 的 ClientSession 分别通过 stateful streamable HTTP 和 SSE initialize 后调用工具。身份使用真实 RSA JWT，记忆通过 RedisVL 写入两 shard Dragonfly；两个用户同 namespace/ID，仅读回本人内容，删除本人记录后另一用户 Hash 保留。

HTTP 分支另验证有效 bob JWT 复用 alice session ID 返回 404；SDK 退出自动 DELETE 后 alice 重用旧 ID 也返回 404。SSE 分支跑实际握手/消息 POST/工具调用/关闭，跨用户 session POST 和断开清理仍由已有传输桩回归验证，不宣称真实 SSE 攻击场景全覆盖。

两个真实客户端用例 **2 passed**，完整集成测试 **79 passed**（一条既有 Pydantic 弃用警告），同版本 local pre-commit 与 diff 检查通过。当前仍未完成创建/更新/搜索/prompt/工作记忆所有 MCP 工具的真实客户端组合、并发重连、STDIO 身份协议、抽取防抖键及全局 P1-03 验收。

## 接续实现：抽取调度作用域

抽取 pending/debounce 键及 Docket task key 改用完整 working_memory v2 身份编码，包含 namespace/user/session 并防止冒号拼接碰撞。should_extract_session_thread 与 set_extraction_debounce 接收关键字作用域，调度、延迟执行和消息提升调用传入实际 owner。旧的仅 session 键不再作为授权或调度状态来源；已有短 TTL 键自然过期，升级需停止旧 worker，避免混用载荷/键协议。

新增真实数据库回归验证同 session 不同 namespace/user、无 owner 和冒号身份的 debounce 互不干扰且 TTL 正确；pending/task 测试检查三个 owner 的独立键及载荷，队列使用桩，不替代真实抽取 worker。旧 AMS debounce 测试清理路径同步更新。

debounce 修改后完整集成 **80 passed**；随后增加作用域 task key 与 pending 测试，定向 **2 passed**。Ruff、同版本 local pre-commit 与 diff 检查通过。原 AMS test_thread_aware_grounding 的 Docker/模型套件未运行。抽取与删除竞争、pending 条件清理、防抖期间新消息处理、summarization metadata 键及完整 P1-03/P1-04 仍待验收。

## 接续实现：抽取 pending 条件清理

run_delayed_extraction 的缺失会话、无待抽取消息及成功路径现在通过声明单键 Lua 比较读取时的 pending 值后删除；读取/模型期间出现新调度时保留新 pending，替代无条件 DEL。真实数据库回归在读取会话时更新 pending，验证新值及 TTL 保留；调度未变化时正常清理。

完整集成 **82 passed**；随后将回归扩展为变更/不变两种 pending，定向 **2 passed**。Ruff、同版本 local pre-commit 和 diff 检查通过。尚未解决模型结果发布、消息已抽取标记与新消息/删除竞争，也未验证 pending 与 post-extraction debounce 对新消息的完整调度语义。

摘要 metadata 核查：summarization.py 的 summarize_session 是仍注册于 Docket 的旧 List+Hash 任务，当前 API 在 JSON 工作记忆路径实施摘要；没有直接改旧键协议。历史任务的作用域、迁移/停用与 worker 兼容需明确实现，不能把当前 API 不调用它当作删除旧入口或隔离已完成。

## 接续实现：旧 List 摘要的显式作用域

summarize_session 增加关键字 namespace/user_id；显式传任一身份时使用新的 scoped_messages_key/scoped_metadata_key（完整 working-memory v2 身份编码），不回退历史数据。无身份历史任务仍使用原 messages/metadata 键，原 helpers 不变；当前 JSON API 摘要路径不受影响。

真实数据库配合确定性摘要桩验证三组 owner 共用 session ID 时，只有目标 owner 的 metadata 更新和消息裁剪，其他 owner 及历史无 owner metadata 不变；未知作用域不会触发历史数据摘要。完整集成 **84 passed**，有既有 Pydantic 和旧 hmset 弃用警告；Ruff、同版本 local pre-commit 及 diff 检查通过。

没有自动认领或迁移历史 namespace-only List/Hash；新任务的生产者需使用 scoped helpers 并携带身份，旧 worker 不能混合部署。模型调用期间的 WATCH 重试、删除/重建竞争、旧任务迁移与真实 Docket 该任务验证尚未完成。P1-03/P1-04/P1-06 保持未勾选。

## 接续修复：旧摘要 WATCH 重试

旧 summarize_session 在首次读取后进入 while，WatchError 后复用旧消息和已修改 metadata。现在每次尝试重新 WATCH/读取，模型完成后才 MULTI，冲突时 reset 并重新计算；会话已删除或消息不足时退出，不从旧结果重建 Hash。metadata 写入改为 hset，移除该路径 hmset 弃用警告。

真实数据库回归在确定性模型调用中删除或重建 List/Hash，触发 WATCH 冲突。删除场景确认两个键仍不存在且只计算一次；重建场景确认第二次 prompt 使用新消息/新 context，最终结果与保留消息均来自新会话。

完整集成 **86 passed**（一条既有 Pydantic 弃用警告），Ruff、同版本 local pre-commit 与 diff 检查通过。该保证限于当前被 WATCH 的旧摘要键，不替代长期记忆墓碑、抽取 generation、所有业务任务幂等或完整迁移验收。

## 接续验收：抽取计算期间的工作记忆冲突

新增 `test_delayed_extraction_rejects_changed_session_before_publish`，在抽取模型桩的等待屏障期间执行真实工作记忆更新、删除或同 ID 重建，再继续旧任务。现有 revision CAS 均拒绝旧提交，任务返回零，索引调用 spy 未被执行，新消息保持未抽取，新 pending 保留，未设置旧任务 debounce。重建遵守首次写入 version=0 合同，由服务端分配高于保留计数器的 revision。

三种场景 **3 passed**，同版本 local pre-commit 与 diff 检查通过；本轮未改应用代码，也未重复全套集成测试。工作记忆读写使用实际 Dragonfly，抽取和长期索引为模型桩/调用 spy，不据此声称长期索引原子写入已验证。

剩余关键窗口：工作记忆 CAS 成功后到长期记忆 embedding/Hash 写入之间，删除仍可发生。需在最终长期写入处原子检查源会话 revision/存在性，并覆盖去重/合并及长期记忆 tombstone；仅在 index_long_term_memories 入口再读一次不够。P1-04 保持未完成。

## 接续实现：抽取源会话的最终写入检查

MemoryRecord 增加服务器私有 _source_session（源 JSON key、revision key、版本），不进入 API 字段或模型序列化。延迟抽取工作记忆 CAS 后为结果绑定该版本，index_long_term_memories 在去重转换后保留标记。RedisVL add_memories 对带标记记录用实际 SDK 写索引前缀之外的临时 Hash（300 秒 TTL），再由声明四键的原子 Lua 校验源存在性与 revision，RENAME 到正式 Hash 并清除临时 TTL；拒绝时不发布，finally 清理临时键。普通未绑定记录保持原 SDK 写入路径。

FLAT/HNSW 在 embedding 等待屏障期间执行源更新、删除、重建，以及无变更发布，共 **8 passed**。检查拒绝后正式 Hash/检索为空、临时键清理；成功后可检索且正式记录无临时 TTL。完整集成 **97 passed**，AMS memory_vector_db/models/recency **75 passed**；Ruff、同版本 local pre-commit 和 diff 检查通过。

这不是 P1-04 完成：去重前的访问统计/删除副作用仍未受源版本原子保护，私有标记不会穿过 Docket 序列化，其他提升/子任务未接入；长期记忆自身删除墓碑尚未实现。当前抽取先标消息已抽取再索引，索引失败后的业务恢复也须解决。分批发布不是整个批次原子提交。SDK staging TTL 在进程中断时的完整清理仍需故障注入，不能仅凭正常 finally 验证作保证。

## 接续实现：抽取索引失败后的调度状态

延迟抽取的成功 debounce 和 pending 条件清理移到索引成功之后。索引异常时，若本任务已提交消息标记，则以该 revision CAS 恢复原工作记忆抽取资格；并发更新/删除/重建会拒绝恢复，防止旧快照覆盖新会话。该恢复只在版本仍属本任务时执行，不无条件回滚用户更新。

新增索引故障桩验证无并发更新时消息恢复为未抽取，存在并发更新时新 context/消息保留，均不设置成功 debounce。与模型期间会话冲突共 **5 passed**；完整集成 **99 passed**（一条既有 Pydantic 弃用警告）。Ruff、同版本 local pre-commit 与 diff 检查通过。

仍需业务恢复协议：并发更新导致恢复 CAS 失败时，旧消息资格可能保持已抽取；需要逐消息任务标记/持久化结果或统一原子提交，而不是覆盖整份快照。部分索引成功后失败的幂等恢复、Docket retry/状态收敛仍未实现。当前异常返回零而非持久化失败任务，不能勾选 P1-04/P1-05/P1-06。

## 接续实现：逐消息抽取恢复

MemoryMessage 增加可持久化 extraction_generation，旧记录缺失时为 None。延迟抽取只标记原本未抽取的消息，并为这些标记分配本任务 ULID；以前已处理消息的标记保持原样。索引失败时重新读取最新会话，匹配 generation、消息 ID、role/content/created_at，只恢复本任务的消息资格；保留最新 context、新消息及其他任务的标记，CAS 冲突最多重新读取五次。会话不存在或 created_at 不同则不恢复，避免修改正常重建会话。

更新真实工作记忆/索引故障桩测试，验证无并发及追加新消息两种情况都恢复旧消息为未抽取，其他已处理消息 generation 保留。完整集成 **99 passed**；AMS models/working_memory_strategies/client **96 passed**，Ruff、同版本 local pre-commit 与 diff 检查通过。

该字段是任务标记，不是授权凭证；readOnly schema 描述本身不强制校验客户端输入。仍需 API/MCP 输入的标记归一化、成功任务清理/进程崩溃恢复、持续 CAS 冲突的持久化重试及会话 incarnation 协议，不能用 created_at 对比替代完整 generation。部分索引成功后的幂等及长期记忆墓碑仍待实现。

## 接续实现：成功抽取标记清理

成功发布后重读最新工作记忆，只对本任务 generation 且内容一致的消息清除 generation，保留已抽取状态；失败恢复与成功清理共用逐消息匹配和 revision CAS。publication_completed 标识防止索引已成功后的 cleanup/debounce 异常将消息恢复为未抽取。此前其他任务的消息和并发新增内容不被修改。

新增无并发/并发追加两种成功场景，检查索引期间存在任务 marker，返回后自己的 marker 清理、其他 marker 保留、新 context/消息保留。成功/失败相关定向 **4 passed**；完整集成 **101 passed**（一条既有 Pydantic 弃用警告），Ruff、同版本 local pre-commit 与 diff 检查通过。成功索引为调用桩，最终数据库源版本保护由已有真实 RedisVL 测试覆盖。

标记清理 CAS 最多五次，尚无持久化补偿；进程在发布后清理前崩溃、清理失败及输入 marker 归一化仍需验收。成功后设置 debounce 对并发新消息的调度语义也待完善。部分写入幂等、长期记忆墓碑与完整任务状态恢复仍未完成。

## 接续实现：并发新消息的后续调度

成功抽取清理 marker 后再次读取当前会话；仍有未抽取消息则安排 trailing extraction，不设置成功 debounce。无待处理消息时设置 debounce 的 Lua 原子验证源存在性及 revision；读取后出现更新则拒绝 debounce，并安排后续任务。当前会话已删除/正常重建时不设置旧任务 debounce。

消息提升入口在 cooldown 内看到新未抽取消息时仍安排延迟任务，不再只打印 skip 而丢弃唯一触发。成功场景测试检查并发新消息导致安排任务且没有 debounce，无新消息则设置 debounce；另用真实会话 revision 验证旧版拒绝、当前版接受，并验证 cooldown 的后续调度。

完整集成 **102 passed**（一条既有 Pydantic 弃用警告），Ruff、同版本 local pre-commit 和 diff 检查通过。调度次数/参数用调用桩检查，实际持续新消息 → Docket → worker 的完整闭环仍待验收；入队失败恢复、任务元数据 TTL 与幂等依然未完成。

## 接续验收：冷却期消息的真实 Docket 入队

新增 `test_cooldown_followup_uses_real_docket_scoped_payloads`：实际 Dragonfly 工作记忆与 debounce 中，三组 owner 共用 session ID，调用真实 promotion/schedule → Docket Lua 入队。读取实际 ZSet 延迟队列，再由 Docket.get_execution 解码持久化载荷，核对 owner、作用域 task key 和 pending 时间戳，三条任务各自独立。

定向 **1 passed**，同版本 local pre-commit 与 diff 检查通过；本轮未重复完整集成。该测试替代对应入队调用桩，但没有启动消费 worker、执行抽取模型或长期索引。下一步需要经审计 SHA 的真实 worker 验证 cooldown 到期、抽取结果、标记、pending/PEL 与新消息持续到达的完整状态收敛。P1-05/P1-06/P1-07 保持未完成。

## 接续验收：真实抽取 worker 与长期写入

新增 `test_real_extraction_worker_scoped_sessions_converge`，启动经审计 scheduler SHA 的两 shard Dragonfly，实际 Docket 延迟调度、载荷消费、run_delayed_extraction、ACK 与执行结果。三个 owner 共用 session ID，分别验证空结果和非空确定性模型桩；非空分支走实际 index_long_term_memories、RedisVL embedding 桩、作用域 Hash 和源版本原子发布，同逻辑结果 ID 各自可检索。

任务结束检查消息已抽取且本 generation 清理、pending 不存在、延迟队列空、PEL=0；非空分支检验所有 owner 的实际 Hash 和检索身份。定向 **2 passed**，同版本 local pre-commit 与 diff 检查通过；本轮未改应用代码，未重复全套集成。

话题/实体结构子任务用空 scheduler 关闭，semantic merge 与后续 discrete 子抽取关闭，不调用真实模型 API。仍需持续追加新消息、worker 中断/重投、部分写入失败、完整 API/MCP → 抽取 → 召回 → 删除，以及其他子任务的 scope/generation 协议；不把此子链路当作 P1-06/P1-07 完成。

## 接续验收：抽取期间追加消息的真实两轮 worker

新增 `test_real_worker_extracts_messages_arriving_after_first_publication`，经真实 Docket worker、实际 RedisVL/源版本原子发布完成第一轮后，在成功清理前插入真实工作记忆追加。任务自动安排第二轮；模型桩分别看到 first/second 消息，两个不同长期结果均可检索。

记录实际入队 key，并由 Docket.get_execution 验证两条 execution COMPLETED、结果各为 1。最终所有消息已抽取、generation 清理，pending/延迟队列为空，Stream 与 PEL 清空。Docket ACK 合同会删除 Stream entry，因此不以留存长度推断执行次数。

定向 **1 passed**，同版本 local pre-commit 与 diff 检查通过。本轮只新增测试与证据，未重复全套集成。模型/embedding 使用确定性桩，话题实体和 semantic merge 关闭；尚未验证长时间连续流量、旧任务重投、入队失败及 worker 崩溃恢复，不能据此勾选 P1-05/P1-06/P1-07。

## 接续验证：派发异常与并发索引初始化

schedule_trailing_extraction 不再吞掉 Docket 派发异常。真实队列回归确认首次派发失败后 pending 存在但队列为空；显式重试确实创建一条作用域正确的持久化任务。尚无 durable outbox，不能宣称自动恢复。

完整测试曾出现实际非空抽取 worker 因并发初始建索引失败。RedisVLMemoryVectorDatabase 现使用实例内锁，并在其他实例赢得创建时重新检查实际索引及 CASESENSITIVE 身份合同；不会只凭捕获异常标记就绪。八个独立实例同步观察缺失后并发初始化的真实回归通过。

本次取回此前运行结果：完整兼容套件 **107 passed**；该运行收集后新增的并发索引测试单独 **1 passed，107 deselected**；AMS memory_vector_db/recency **39 passed**。完整套件用时 23.56 秒，有一条既有 Pydantic 弃用警告。这些结果不代表完整 P1 验收，任务勾选仍为 9/20。

文档整理新增 current-status 作为当前状态入口；TODO 保留整项验收，handoff 聚焦下一步，历史报告及附录继续保留基线。此次整理未修改应用代码。

## 接续实现：尚未发布的抽取结果删除保护

Keys.memory_generation_key 使用完整长期记忆身份，位于搜索前缀之外。exact_scope 删除在查询前递增每个请求 ID 的 generation，包含尚不存在的记录；计数不设置 TTL，避免旧发布在计数过期后复活。

带 _source_session 的写入在 embedding 前读取目标 generation，最终五键 Lua 同时检查源会话 revision 和目标 generation，再 RENAME 发布。删除期间尚未生成 Hash 的抽取结果被明确拒绝，临时 Hash 清理。新增 FLAT/HNSW 实际数据库回归，连同原源会话竞争回归 **10 passed**。AMS Ruff、固定版本 local pre-commit 及两个仓库 diff 检查通过。

这是 P1-04 的部分实现：只保护本次调用进入 embedding 前已捕获版本的源绑定发布。普通写入、读取旧记录后更新、队列序列化前后的长期 generation、全局管理删除、去重副作用、批次部分成功及 generation 容量回收仍未完成。删除计数与检索删除不是一个整体原子事务，尚不能据此证明所有并发创建/删除语义。任务总数仍为 9/20。

新增保护后完整兼容套件 **110 passed，24.19 秒**（含并发索引与新增删除回归，一条既有 Pydantic 警告）；AMS memory_vector_db/recency **39 passed**。未运行真实模型或完整运维验收。

## 接续实现：普通写入的目标 generation 检查

所有 add_memories/update_memories 调用现在都在 embedding 前读取目标删除 generation，通过 RedisVL 编码写临时 Hash，再以声明键 Lua 检查并发布；未绑定工作会话的写入使用三键 Lua，绑定源会话的写入继续同时检查源 revision。正式记录不保留临时 TTL，拒绝时清理临时键。

新增 FLAT/HNSW 实际回归覆盖同 owner 删除拒绝旧发布、不同 owner 删除不影响发布、删除后的新创建成功。完整兼容套件 **114 passed，25.29 秒**，一条既有 Pydantic 警告。AMS 原 add/update mock 假设一次批量直接 load，已更新为暂存路径的客户端与逐记录 load；实际数据库回归作为原子发布行为证据。

仍未完成 P1-04：当前 generation 在 add_memories 入口捕获，不能保护进入此入口之前已排队或已读取的旧业务任务；读取后晚到更新、全局管理删除、去重副作用和部分批次幂等仍需更早绑定版本。每条记录增加临时写入和 Lua，吞吐成本需在 P2-04 测量；generation 不过期的容量治理也仍待完成。

AMS memory_vector_db/recency 更新 mock 后 **39 passed**；AMS Ruff、固定版本 local pre-commit 和两个仓库 diff 检查通过。未修改 C++、未提交代码。

## 接续实现：管理删除的通配作用域 generation

管理删除（exact_scope=False）现在在查询前为每个逻辑 ID 递增匹配选择器的 generation：全局、namespace、user、namespace+user。精确无 owner 与全局通配使用不同键，避免混淆。发布原子检查精确 owner 及适用的三个管理选择器，覆盖 Hash 尚未生成时的管理删除。

FLAT/HNSW 回归确认全局、同 namespace、同 user 删除均拒绝旧发布；不同 namespace/user 删除不影响目标 owner。完整兼容套件 **120 passed，26.50 秒**；随后扩展不匹配选择器场景，定向 **10 passed，114 deselected**，未把新增场景计入此前全套。AMS memory_vector_db/recency **39 passed**，Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过。

P1-04 仍未完成：generation 捕获仍在写入入口，必须延伸到队列生产、旧记录读取及业务副作用；删除计数与索引查询删除不是整体原子操作，部分批次提交/重投仍需协议。永久计数的容量治理及写入额外 Redis 往返的性能测量保留在 P2。

## 接续实现：读回记录保留删除版本

长期 Hash 写入时保存 _publication_generation 四项快照，普通检索和 recency LOAD 返回此字段；转为 MemoryRecordResult 后保存在 PrivateAttr，不进入 API model_dump。add/update 对带快照记录继续使用读到的版本，不重新认领删除后的 generation。存储字段类型不合法时明确报错。

新增实际 FLAT/HNSW 回归：创建 → 检索旧副本 → 精确删除 → 修改旧副本并 update，旧更新被拒绝且实际 Hash/检索保持为空、临时键清理；同时验证私有版本不在公开模型序列化中。

完整兼容套件 **126 passed，27.64 秒**（一条既有 Pydantic 警告）；AMS memory_vector_db/recency/models **75 passed**。Ruff、固定版本 local pre-commit 与 diff 检查通过。

仍有缺口：旧 Hash 无此字段需迁移；API 转换与业务 model_dump/model_validate 会丢私有属性，Docket 序列化也不保留，必须显式传可信版本。多记录共用选择器的批次快照目前按键缓存，仍需逐记录绑定防止同批次版本覆盖。公开按 ID 更新、队列子任务、去重副作用、部分发布幂等及持久化恢复尚未验收；P1-04 保持未勾选。

## 接续修复：逐记录批次 generation

add_memories 改用与输入一一对应的 generation 快照，不再用跨记录共享的键字典。暂存 Hash 的私有版本与最终 Lua 参数都来自同一条记录，避免同 ID 新记录覆盖旧副本的版本。embedding 返回数量现在以 strict zip 校验，数量不匹配会在发布前报错。

新增实际 FLAT/HNSW 回归：读出旧副本、删除、将旧副本和同 ID 新创建组成批次，旧副本必须被拒绝，数据库为空；单独新创建仍成功。完整兼容套件 **128 passed，28.16 秒**，一条既有 Pydantic 警告；AMS memory_vector_db/recency/models **75 passed**。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过。

这是版本覆盖修复，不是批次原子性保证：先发布成功的记录可能在后续记录失败时留存。API 按 ID 更新、队列版本传递、旧 Hash 迁移、业务副作用及部分发布重投仍未完成，P1-04 保持未勾选。

## 接续验收：认证 HTTP 更新与删除竞争

新增 test_http_update_cannot_revive_deleted_memory，经真实 bearer 验证与 ASGI HTTP 路由读取/更新实际 RedisVL 数据。在 PATCH 的 embedding 屏障期间发起 HTTP DELETE，确认删掉一条；继续 PATCH 返回当前约定的 400（deleted before publication），之后 GET 404、实际 scoped Hash 不存在、暂存 Hash 清空。

定向 **1 passed，128 deselected**，固定版本 local pre-commit 与 diff 检查通过。本轮只新增回归，未重复全套或修改应用实现。现有 get_long_term_memory_by_id 返回 MemoryRecordResult，update_long_term_memory 的 model_copy 保留 PrivateAttr，因此这条按 ID 更新路径保持了读取时 generation。

该证据验证认证与路由到实际存储，HTTP 通过 ASGITransport 执行而非独立网络服务器；embedding 使用确定性屏障桩。队列版本、其他重构模型的业务入口、历史数据、重投幂等和完整 P1-04/P1-07 仍待完成。

## 接续实现：API 入队前绑定删除版本

create_long_term_memory 在身份授权后从实际 Redis 捕获四项 generation，将 publication_generations 作为服务器生成的独立任务参数传入 index_long_term_memories。任务入口校验数量/类型，并在任何去重前比较当前版本；过期任务明确失败。恢复后的版本绑定 MemoryRecord PrivateAttr，去重转换后继续保留，最终发布仍进行原子复核。

完整兼容套件 **131 passed，28.62 秒**。随后将版本序列化回归改为实际 Docket.add/get_execution 的持久化载荷读回，再删除目标并调用恢复载荷验证入口拒绝；FLAT/HNSW 定向 **2 passed，129 deselected**。这次定向没有启动 worker，不以它证明 ACK/任务失败状态；此前完整套件的认证创建/真实 worker 验证了新参数成功路径。AMS memory_vector_db/recency/models **75 passed**；Ruff、固定版本 local pre-commit 和 diff 检查通过。

P1-04 仍未完成：仅 API/MCP 共享创建入口接入生产版本，其他提升/结构子任务和历史队列还需绑定；任务开始后的去重副作用没有原子版本保护，payload 仍需涵盖源会话版本。派发 outbox、旧 Hash 迁移、部分批次幂等及崩溃恢复尚待完成。

## 接续实现：结构子任务的目标删除版本

成功 add_memories 在内存对象绑定实际发布的 generation；index_long_term_memories 安排 extract_memory_structure 时显式传递该版本。结构任务对带版本载荷使用声明五键 Lua，原子检查目标存在、四项删除计数以及 topics/entities 字段存在后才 HSET，保留 TTL。历史无版本载荷暂保留原 HSETEX FXX 路径，不能声称旧任务完整防重建。

实际 FLAT/HNSW 回归在模型屏障期间删除或删除后同 ID 重建，旧结构结果均不能写入新 incarnation；无删除时正常写 topics/entities。完整兼容套件 **135 passed，29.43 秒**；随后添加无删除成功场景，定向 **6 passed，131 deselected**。AMS memory_vector_db/recency/models **75 passed**。结构单测还发现沿用旧物理键字符串的断言，改为 Keys.memory_key 以匹配现有 v2 协议。Ruff、固定版本 local pre-commit 与 diff 检查通过。

这些结构回归直接执行任务函数和实际数据库，未验证该新载荷的独立 worker 崩溃/重投。discrete 子抽取、源工作会话版本、其他业务副作用及历史载荷迁移仍未完成，P1-04/P1-06 保持未勾选。

AMS 结构抽取单测更新旧键断言后 **2 passed，32 deselected**。未提交代码。

## 接续修复：无版本结构载荷拒绝执行

结构抽取移除无版本 HSETEX fallback。缺少 publication_generation/服务器私有版本时，明确抛出需从当前记忆重排任务的异常；非法空列表也不能回退私有版本。版本验证前移到模型调用前，有效载荷继续使用最终原子检查。

实际 FLAT/HNSW 回归将旧记录序列化成无版本载荷，删除并同 ID 重建后执行旧任务，确认模型未调用且新 topics 保留。完整兼容套件 **139 passed，30.16 秒**，一条既有 Pydantic 警告；更新为 Lua 合同的 AMS 结构单测 **2 passed，32 deselected**。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过。

此变更故意不允许无可信版本历史任务认领当前 incarnation；历史任务自动重排、重排幂等和失败状态收敛仍须实现，不能把拒绝执行当作迁移完成。其他子抽取、旧 Hash、业务副作用及完整 P1 验收仍未完成。

## 接续修复：discrete 子抽取继承源身份

extract_memories_with_strategy 原先把所有模型结果合并为无来源字典，创建 MemoryRecord 时缺失 namespace/user/session，可能把租户记忆写成无 owner。现在每个结果绑定实际源 MemoryRecord，继承三项作用域并设置 extracted_from=[源逻辑 ID]，不接受模型生成的身份覆盖服务器来源。

新增实际 FLAT/HNSW 回归：两个 owner 共用 source ID/session，各抽取一条确定性结果，调用实际数据库更新源并写入子结果；检查四条记录的 owner/namespace、来源 ID 及实际 scoped Hash。模型策略使用桩，index 调用包装为实际 db.add_memories，未验证去重/子任务调度。

定向 **2 passed，139 deselected**；AMS extraction 的两个策略 event_date 回归 **2 passed，12 deselected**。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过。本轮未重复完整套件。

discrete 子任务仍需显式传入源版本，并在最终子结果发布原子检查源长期 Hash/generation。模型异常后仍标已处理的历史行为也待修复；部分结果成功后重试幂等、旧任务迁移和完整 P1-03/P1-04/P1-06 验收未完成。

## 接续修复：discrete 模型失败保留重试资格

extract_memories_with_strategy 不再将模型异常当作已处理：记录错误后重新抛出，源记录的 discrete_memory_extracted 保持 f。模型阶段发生失败时，已收集的批次更新/子结果也尚未进入数据库，不以异常空结果假装成功。

新增 FLAT/HNSW 实际数据库回归，第一次策略抛 ConnectionError，检查源 Hash 标记 f、数据库只有源记录；显式第二次调用成功后标记 t，并产生一条作用域正确的子记忆。结合身份回归定向 **4 passed，139 deselected**；AMS 两个 extraction event_date 策略回归 **2 passed，12 deselected**。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过。本轮未重跑完整套件。

不代表故障恢复已完成：源标记目前仍先于子结果索引，索引失败/部分成功需要持久化幂等与补偿；本回归为显式重试，未验证真实 Docket retry/失败状态。不得直接交换两个写入顺序而忽略源删除竞争，需先在子结果最终发布加入源长期记忆版本检查。

## 接续实现：子结果发布检查源长期记忆

MemoryRecord 增加服务器私有 _source_memory（源 Hash、四个删除计数键及快照）。discrete 结果从源记录绑定该标记，index 去重转换继续保留；RedisVL 暂存发布的声明键 Lua 同时检查源 Hash 存在及源删除 generation，删除/同 ID 重建后拒绝子结果发布。

实际 FLAT/HNSW 回归覆盖正常发布、embedding 期间删除、删除并完成重建后才释放 embedding，确认拒绝后子 Hash 不存在且暂存清空。最初定向 **8 passed，141 deselected**；重建回归随后调整屏障，完整套件用于复核最终版本。AMS memory_vector_db/recency/models/extraction **87 passed，2 skipped**；Ruff、固定版本 local pre-commit 和两个仓库 diff 检查通过。

源标记当前只防删除/重建，不是内容更新 revision；未穿过所有队列序列化边界，旧源无 generation 时也未绑定。去重前副作用、先标已处理后子结果发布失败、部分批次幂等与持久化恢复仍未解决，P1-04/P1-05/P1-06 保持未完成。

最终屏障版本的完整兼容套件 **149 passed，31.73 秒**，一条既有 Pydantic 警告。未提交代码。

## 接续实现：discrete 队列源版本与空批次语义

index_long_term_memories 调度 discrete 子任务显式携带每条已发布源记录的 generation。extract_memories_with_strategy 恢复后校验数量/类型，与实际 Redis 删除计数比较，在策略/model 调用前拒绝已删除源；版本重新绑定 PrivateAttr，供源更新和最终子结果发布检查使用。

真实 Docket.add/get_execution 回归验证版本参数持久化，删除源后直接调用恢复载荷，在模型前失败；未在该回归启动 worker。修复 memories=[] 意外扫描全库：只有 memories=None 才使用管理扫描，显式空列表不处理其他 owner。FLAT/HNSW 回归检查空批次不抽取数据库中待处理源。

完整兼容套件 **153 passed，32.38 秒**，一条既有 Pydantic 警告；AMS extraction/vector_db/recency/models **87 passed，2 skipped**。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过。

仍未完成：无版本历史 discrete 载荷、源内容 revision、真实 worker 失败/重投状态、先标源已处理后子索引失败、部分发布幂等，以及去重副作用的原子保护。任务总数保持 9/20，未提交代码。

## 接续修复：无版本 discrete 源拒绝认领新记录

extract_memories_with_strategy 在分组/策略调用前要求每个源具备服务器私有 publication generation。新队列显式参数会恢复它，管理扫描读取已迁移字段；无版本历史载荷和旧 Hash 不会在执行时认领当前删除计数，而是明确要求从当前记忆重排。

FLAT/HNSW 回归将源模型序列化成旧载荷，删除并同 ID 重建后执行，确认策略未调用、新内容和待抽取标记保留。相关定向 **8 passed，147 deselected**；AMS extraction 模块 **12 passed，2 skipped**。两个 event_date 单测为服务器源显式设置版本，以符合当前合同。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过；本轮未重复完整套件。

拒绝旧载荷只是关闭不安全路径，并未完成历史迁移：仍需扫描迁移旧 Hash 的版本、按当前 incarnation 重排、任务失败状态和幂等恢复。完整 P1-04/P1-05/P1-06 保持未完成。

## 接续实现：旧 Hash 作用域迁移入口

新增 migrate_scoped_memory_keys_5 与 CLI migrate-scoped-memories --dry-run。按 SCAN 读取身份，单 Hash 的声明六键 Lua 校验身份未变、目标无冲突，绑定当前 generation 并 RENAME；内容/二进制 vector/TTL 保留。v2 无版本 Hash 也可原地补字段，有字段的 v2 跳过。目标已存在时明确失败且不覆盖/删除任一 Hash；部分扫描成功后可重跑，不把整批称为原子。

FLAT/HNSW 实际回归 **4 passed，155 deselected**：dry-run 不变、迁移后全部旧字段逐字节一致、TTL 保留、RedisVL 检索恢复版本、二次运行零改动，以及 dry-run/实迁移冲突保留双方。向量为二进制，回归使用禁用解码的 HGETALL 比较。CLI --help 确认命令注册，Ruff、固定版本 local pre-commit 与 diff 检查通过；未重复完整套件、未执行用户数据迁移。

仍需完成：重复记录的显式冲突处理策略/回滚、失败注入、TTL 到期竞争、存量非法版本验证、历史任务重排，以及 schema/model/dimension 迁移。迁移要求停写停 worker，当前无停机升级未验收；P2-01 与 P1-03 保持未完成。

## 接续完善：迁移版本校验、过期和响应丢失

迁移 Lua 在跳过已迁移 v2 Hash 前先校验存量版本：必须为四个非负十进制字符串且匹配实际删除计数。非法 JSON/长度/数字类型/负值明确报错；过期版本不允许被重新绑定认领当前 incarnation。legacy Hash 已有合法版本时也检查后再迁移。

FLAT/HNSW 新回归覆盖损坏版本在 dry-run/实迁移均保留原字段、stale 版本不重置、读取身份后 TTL 到期不复活。迁移相关定向 **19 passed，152 deselected**（包含此前其他名称匹配的迁移测试）。随后补实际 Lua 成功后模拟 ConnectionError 丢响应，重试观察目标已迁移且零改动，回归 **2 passed，171 deselected**。Ruff、固定版本 local pre-commit 和两个仓库 diff 检查通过；未重复完整套件。

迁移仍须停写；重试保证限于逐记录成功状态，不代表全批事务。冲突内容选择、回滚、历史任务重排、schema/model/dimension 及生产迁移验证仍未完成，任务勾选保持 9/20。

## 接续完善：迁移 CLI 失败合同

migrate-scoped-memories 将数据校验/冲突、Redis 错误及连接错误转换为 ClickException，向终端输出明确 Error 并返回非零退出码；成功才输出迁移数量。新增命令级 dry-run/实迁移计数、冲突、非法版本和连接错误回归 **5 passed，77 deselected**。这些为 CLI mock 回归，数据迁移行为由此前实际数据库测试验证。

AMS Ruff 与两个仓库 diff 检查通过。此次未运行用户数据迁移；版本迁移的冲突解决、回滚、历史任务重排及完整上线交付保持未完成。

积累的迁移/历史载荷改动完成全套复核：完整兼容套件 **173 passed，36.01 秒**，一条既有 Pydantic 警告。未跑真实模型、运维完整验收或提交代码。

## 接续实现：显式合并完全相同的迁移冲突

migrate_scoped_memory_keys_5 新增 resolve_identical，CLI --resolve-identical 显式启用，默认仍拒绝冲突。Lua 校验源版本后，原子比较两 Hash 全部字段（包括二进制 vector 和已有版本）及 PTTL，完全一致才保留目标、删除旧键；缺版本的双方相同记录为目标绑定当前版本。差异内容/字段或 TTL 不同仍保留双方并报错，不任意选择最新或扩大过期时间。

FLAT/HNSW 实际回归连同默认冲突场景 **6 passed，171 deselected**，确认 dry-run 双键保留、显式合并后目标逐字节不变、检索唯一、重跑零改动及 TTL 差异拒绝。迁移 CLI 默认合同和显式选项回归 **6 passed**。Ruff、固定版本 local pre-commit 与 diff 检查通过；未重复全套、未迁移用户数据。

此选项只解决严格相同的重复记录；legacy 缺版本而 v2 有版本不视为逐字段相同。不同内容的冲突仍需审计决策/备份保留，回滚和历史任务重排未完成，P1-03/P2-01 保持未勾选。

## 接续验收：迁移部分成功后冲突与恢复

新增 FLAT/HNSW 实际数据库回归，注入固定 SCAN 顺序，让第一条 legacy Hash 已迁移、第二条遭遇内容冲突。确认前一条成功留存，冲突双方逐字节不变；恢复真实 SCAN 重试仍明确报冲突，不修改已完成记录。测试中显式将冲突 legacy 归档到搜索前缀外，保留原始字段，之后迁移零改动，正式索引只剩两个有效记录。

定向 **2 passed，177 deselected**，固定版本 local pre-commit 和 diff 检查通过。本轮只新增回归证据，未改应用代码或重跑全套。归档由测试显式操作，不代表已有自动冲突归档工具或生产回滚方案；此证据验证可接续性，不能声称整批迁移原子。

下一步仍需可审计的冲突解决/回滚工具和历史任务重排。P2-01 保留完整 schema/model/dimension 验收条件，不能仅凭键迁移回归勾选。

## 接续实现：冲突归档与恢复凭据

新增 archive_scoped_memory_conflict/restore_scoped_memory_archive，CLI archive-memory-conflict LEGACY_KEY 与 restore-memory-archive RECEIPT。停写期间显式选择保留 v2 当前版本，将冲突 legacy Hash RENAME 到搜索前缀外；声明键 Lua 核对双方实际身份，并记录源/目标/archived 状态。归档保留所有字段和原 TTL；恢复核对凭据、状态和原键空闲，不覆盖占用键。成功恢复更新状态，重复恢复返回已恢复。

FLAT/HNSW 实际数据库 **4 passed，179 deselected**，覆盖内容/TTL/目标不变、恢复及重复恢复、占用原键拒绝覆盖。CLI 归档/恢复及此前迁移合同 **9 passed，77 deselected**。Ruff、固定版本 local pre-commit 和两个仓库 diff 检查通过。本轮未跑全套或操作用户数据。

这不是整个迁移回滚：恢复 legacy 后可能重新出现重复索引记录；必须停写并重新决定迁移冲突。归档沿用 TTL，过期数据不能恢复；凭据日志目前不过期，容量治理、响应丢失时凭据查找、全批迁移撤销及历史任务重排仍待实现。

## 接续实现：归档凭据查找与响应丢失恢复

新增 find_memory_archive_receipts 和 CLI find-memory-archives LEGACY_KEY --limit，只读扫描凭据返回 receipt/source/target/state/archive_pttl。数量限制为 1–1000，超过明确报错，不静默截断。状态为查询时观察值，恢复仍以原子 Lua 最终校验为准。

FLAT/HNSW **4 passed，183 deselected**：真实归档 Lua 成功后模拟连接错误，凭据查找得到已提交归档并能恢复；实际 TTL 过期后凭据留存、archive_pttl=-2，恢复拒绝且不复活原键。CLI 归档/查询/迁移合同 **10 passed，77 deselected**。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过；本轮未跑全套。

凭据仍永久留存，查找扫描所有 receipt，尚无索引分页/容量回收。归档恢复只处理指定旧键，不是全批迁移撤销；历史任务重排与完整 schema/model/dimension 迁移仍待完成。任务勾选保持 9/20。

## 接续验收：恢复提交后响应丢失与完整回归

新增实际 FLAT/HNSW restore_response_loss_is_idempotent：恢复 Lua 完成后模拟连接错误，实际原 Hash 已恢复、凭据状态已更新；重试返回 False（已恢复）且全部字段不变。配合先前归档响应丢失测试，验证指定凭据的恢复收敛，未模拟进程/主从故障。

包含近期迁移、冲突处理、归档、凭据查询与恢复回归的完整兼容套件 **189 passed，38.64 秒**，一条既有 Pydantic 警告。AMS CLI 迁移/归档合同 **10 passed，77 deselected**；固定版本 local pre-commit 与 diff 检查通过。

本轮未改应用实现，未操作用户数据或提交代码。P2-01 仍缺完整 schema/model/dimension 协议和全批回滚；下一步继续业务任务持久化派发/重排恢复，不能把这些迁移验证替代 P1-05/P1-07 的业务验收。

## 接续实现：抽取派发持久化 outbox

Docket 抽取调度用声明两键 Lua 原子设置 pending 与 extraction_outbox:<完整作用域身份>；outbox 不设 TTL，存储最新载荷/原 task key/时间戳。派发异常仍抛出，载荷保留。派发前比较当前 outbox 并恢复 pending，Docket 成功后比较载荷条件清理，避免清掉新意图。reconcile_extraction_outbox 扫描并以原 ID 重派；无 Docket 分支维持原 pending TTL 与 asyncio 调度。

新增真实数据库/Docket 回归：首次构造队列失败，pending 到期后 outbox 仍在，显式 reconciliation 确实创建一条原 ID/时间戳载荷；成功清理 outbox，再次 reconciliation 零操作。此前 dispatch/cooldown/真实 worker 相关定向 **4 passed，185 deselected**。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过。

这是持久化派发基础，不是 P1-05 完成：reconciliation 尚未接自动 worker/运维入口，缺源会话 incarnation/版本、积压期间 pending TTL 保留、派发成功但清理前崩溃的重复交付验收。outbox 只保留各 scope 最新意图；业务执行幂等、失败状态收敛与容量治理仍需完成。

新增 outbox 恢复回归后的完整兼容套件 **190 passed，38.73 秒**，一条既有 Pydantic 警告。未提交代码。

## 接续实现：outbox 运维入口与入队后清理故障

新增 CLI reconcile-extractions，用当前配置连接 Redis/Docket，显式重派持久化抽取意图并输出数量，错误返回非零。没有后台自动循环，部署还需接启动/周期恢复。

实际数据库/Docket 回归在已成功入队后、条件清理前注入连接错误，outbox 保留；调用 reconciliation 后队列保留相同唯一 task key、载荷身份正确、outbox 清理。定向 **1 passed，190 deselected**；CLI mock 回归 **1 passed，87 deselected**。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过；本轮未重跑全套。

回归没有运行消费 worker，只证明待执行延迟队列的原 ID 重派不会新增第二条；消费/完成期间的重派幂等与结果收敛仍待验收。源工作会话 incarnation、积压 pending TTL、坏载荷隔离、恢复循环及容量指标仍是 P1-05/P2 待办。

## 接续修复与验收：outbox 并发替换与派发计数

_dispatch_extraction_outbox 返回实际是否派发，reconciliation 不再把比较失败的旧快照计入成功数量。实际数据库回归注入读取旧快照，原子比较观察到新载荷后不派发，计数零且新 pending/outbox 不变。

另一回归在真实 Docket 上下文进入期间写入新 intent/pending，确认旧入队后的条件清理保留新意图；随后 reconciliation 派发新 ID 并清理，仅保留最新 pending。两个版本任务都可在队列，旧任务应由执行入口判断已被新调度取代；此次未启动 worker 验证该执行分支。

相关定向 **3 passed，190 deselected**（包含此前入队后清理故障重试），AMS Ruff、固定版本 local pre-commit 和 diff 检查通过；本轮未跑全套。源会话 incarnation、自动恢复、积压 TTL、消费/完成期间重派幂等及容量仍未完成，P1-05 保持未勾选。

## 接续实现：worker 启动前恢复抽取 outbox

CLI task-worker 创建 Stream/group 后，使用同一 Redis 客户端调用 reconciliation，记录恢复数量，然后进入 Worker.run。恢复报数据/连接错误时通过 ClickException 明确返回失败，不继续消费；持久化意图仍可在之后恢复。

新增 mock 启动顺序/失败回归确认 recover 在 consume 前执行，失败没有 consume。既有 TaskWorker 单测为 mock settings.redis_url 补缺省本地 URL，避免无环境时 fixture 返回 None；这些测试不连接该 URL。此验证覆盖 CLI 接线，不声称已运行真实 CLI 进程的重启恢复，实际派发协议由既有真实数据库/Docket 回归验证。

自动恢复目前仅启动一次；长期运行期间的周期恢复、坏载荷隔离、源会话版本及积压 TTL/消费期间重派仍未完成。P1-05 保持未勾选。

CLI worker/reconciliation 相关回归 **8 passed，82 deselected**；AMS Ruff 与两个仓库 diff 检查通过。本轮未重跑完整套件或提交代码。

## 接续实现：worker 周期 outbox 恢复

task-worker 启动恢复成功后创建受生命周期管理的周期任务，默认每 30 秒调用 reconciliation，可用 --outbox-recovery-interval 配置正间隔。运行期间恢复异常记录日志并保留意图供下次重试；Worker.run 结束/异常时 finally 取消并等待恢复任务，取消不吞掉。

新增 CLI mock 回归用实际事件循环/短周期验证启动调用、首次周期连接故障、下一周期成功、worker 结束后恢复任务 done。worker/reconciliation 相关 **10 passed，81 deselected**；AMS Ruff 与两个仓库 diff 检查通过。未跑独立真实 worker 进程或重启验收，未重跑完整兼容套件。

仍需坏载荷按记录隔离，避免一个损坏 outbox 阻碍其他作用域；源会话 generation、积压 TTL、消费/完成期间重派幂等和容量告警仍待补齐，P1-05 保持未勾选。

## 文档整理与最新 outbox 定向验证

取回 `UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x -k outbox`：**8 passed，189 deselected**，一条既有 Pydantic 弃用警告。最新实现按记录校验 outbox 字段、作用域身份、task key 和带时区时间戳；坏载荷保留并记录错误，其他作用域继续恢复。

文档统一以 current-status 为当前快照、TODO 为任务与验收入口、handoff 为接续操作、本文为历史证据。补充源 incarnation、真实 worker 重启、部分发布恢复、历史任务重排和容量治理的具体待办，仍为 9/20。检查发现旧快照回归使用无效 `old` 时间戳，新校验下提前拒绝；需改为合法旧载荷才能再次证明原子比较分支，本次文档整理未修改测试代码。

整理后重跑完整兼容套件 `UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x`：**197 passed，38.95 秒**，一条既有 Pydantic 弃用警告。此结果不消除上述旧快照测试覆盖缺口，也不代表完整业务或运维验收。

## 接续修复：旧 outbox 快照实际原子比较覆盖

旧快照测试改为合法带时区时间戳和作用域 task key，新存量也使用完整合法载荷。包装真实 Redis eval，断言恰好一次声明两键的比较返回 0；禁止打开 Docket，并核对新 pending/outbox 内容不变。这消除了此前无效时间戳提前退出造成的假覆盖。

`UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x -k outbox`：**8 passed，189 deselected**，既有 Pydantic 警告。固定版本本地 pre-commit 配置 `/tmp/agent-memory-format-hooks.yaml` 通过，未运行原始网络 hooks。本轮仅修改测试与状态文档，无 C++/应用代码改动，未重复完整套件。P1-05 尚缺源 incarnation、积压 TTL、真实进程恢复和消费后重派，任务仍为 9/20。

## 接续修复：Docket 积压不再使有效调度过期

Docket 调度的声明两键 Lua 和 outbox 派发比较 Lua 将 pending 改为无 TTL SET。任务被新调度替换时仍通过时间戳比较跳过；完成/空源时条件删除，asyncio 分支保持原 TTL。避免已成功入队却因 worker 延后而永久跳过有效抽取。

现有真实 worker 两轮抽取回归增加消费前等待（防抖 1 秒，等待 2.1 秒）和 pending TTL=-1 断言；数据库实际发布、消息标记、两轮任务结果、Stream ACK/PEL 均检查。定向 **1 passed，196 deselected**；outbox **8 passed，189 deselected**。AMS Ruff 和固定版本本地 pre-commit 通过。源 incarnation、取消/失败标记清理、消费后重派与真实进程重启仍未完成，P1-05 不勾选。

此修复后的完整兼容套件 **197 passed，40.05 秒**，一条既有 Pydantic 警告。两个仓库 diff 检查通过；未提交代码，未重跑无改动的 C++。

## 接续修复：抽取发布失败不再被记录为成功

run_delayed_extraction 的模型/写入/发布异常现在在回滚本次消息标记后重新抛出，保留当前 pending 供队列重投或运维重试。原先清 pending 并返回 0 会让 Docket 错记成功，同时让同一 timestamp 重投直接跳过。成功路径仍条件清理 pending；部分发布幂等和回滚自身异常补偿尚未解决。

实际数据库回归覆盖有/无并发更新两种情况：注入索引失败，断言 RuntimeError、pending 保留、仅本次标记恢复且并发上下文/新消息留存；替换成功发布桩后使用相同 timestamp 再执行，断言发布一次、标记完成及 pending 清理。定向 **2 passed，195 deselected**。此回归调用真实工作记忆存储，发布为桩，不代表真实 Docket worker 自动重试或部分发布恰好一次。AMS Ruff、固定版本本地 pre-commit 和两个仓库 diff 检查通过。

完整回归首次在旧会话变更测试的旧返回值断言失败（176 passed 后停止）；将 update/delete/recreate 三种测试改为断言 WorkingMemoryConflictError，保留实际未发布、新 pending 和消息状态断言，并在 finally 收集任务异常。重跑完整兼容套件 **197 passed，39.96 秒**，既有 Pydantic 警告；格式和 diff 检查通过。未提交代码。

## 接续验收：真实 worker 失败状态与原 ID 显式重投

新增真实 Dragonfly/Docket worker 回归，模型桩抛 RuntimeError。实际任务状态为 FAILED，get_result 抛原异常，pending 保留且无 TTL，消息仍为未抽取，无残余 extraction_generation；延迟队列、Stream 和 consumer group PEL 均清空。随后恢复模型桩（返回空结果），用原 task ID/原 timestamp 显式 Docket.add 重投，再次运行 worker：任务为 COMPLETED、结果 0、消息标记完成、pending 条件清理，Stream/PEL 仍空。

定向 `-k records_failure` **1 passed，197 deselected，2.91 秒**，既有 Pydantic 警告。固定版本本地 pre-commit 与 diff 检查通过。本轮只新增测试，未重复全套；此前全套 197 passed 不包含此新测试。此证据覆盖失败后显式重投的空抽取，不代表自动 retry、进程重启、部分发布恰好一次或源 incarnation 完成，P1-05 保持未勾选。

## 接续实现：抽取准备阶段有界自动重试

新增 ExtractionPreparationError 与 Docket ExtractionPreparationRetry：仅消息标记/发布前的抽取准备异常最多执行 3 次、间隔 1 秒。run_delayed_extraction 将该阶段异常包装后传播；写入冲突、索引和清理异常不匹配此自动重试，部分发布幂等协议仍未完成。直接调用同样会抛 RuntimeError 子类。实际 extract_memories_from_session_thread 原先吞策略异常返回空列表，现改为抛出，避免把失败当成成功空抽取。

真实 worker 回归把故障注入到 memory strategy，保留真实线程抽取调用链：持续故障实际调用策略 3 次后 FAILED；显式原 ID 重投后首次瞬时故障、自动第二次执行成功，消息标记完成、pending 清理、结果及 Stream/PEL 正确。最终策略层定向 **1 passed，197 deselected，5.89 秒**。此前配置重试后的全套 **198 passed，44.48 秒**，但那轮尚未包含最终策略层注入/内部异常传播改动，不能当作最终全套证据。Ruff、本地固定版本 pre-commit 通过。源 incarnation、进程重启及部分发布自动恢复仍待实现，保持 9/20。

最终改动完整兼容套件 **198 passed，44.35 秒**，一条既有 Pydantic 弃用警告。两个仓库 diff 检查通过；未提交代码或重跑无改动 C++。此结果包含实际策略异常传播及最终 worker 自动重试回归。

## 接续实现：服务器保存稳定会话 incarnation

工作记忆两键 CAS Lua 写入内部 JSON 字段 `_session_incarnation`：新 JSON 用保留 revision 计数本次新值作为身份，普通更新沿用已有身份；缺字段历史文档更新时沿用更新前 version（或旧格式 0）。客户端序列化字段不能覆盖服务器选取的身份。删除/过期后重建因 revision 不重置而取得更大的新身份。此字段暂未加入队列载荷，不能宣称旧队列任务已被 incarnation 拦截。

实际数据库新回归检查普通更新保留、伪造字段不能替换、删除重建及实际 TTL 到期重建均改变身份。初始定向 `-k "incarnation or session_version"` **12 passed，187 deselected**（加入伪造字段断言前），既有 Pydantic 警告。AMS Ruff 和固定版本本地 pre-commit 通过；下一步把 incarnation 原子绑定到 outbox 并验证 worker 入口与历史任务协议。仍保持 9/20。

最终完整兼容套件 **199 passed，44.94 秒**，包含伪造字段断言，既有 Pydantic 警告。再次格式检查及两个仓库 diff 检查通过；未提交代码。

## 接续实现：调度载荷绑定源会话 incarnation

Docket 调度改为声明三键 Lua，原子读取工作 JSON 的服务器 incarnation（历史文档 fallback 为当前 version），并写入 pending/outbox；outbox dispatcher 校验可选十进制 source_incarnation，传给真实 Docket task，恢复保持原身份。asyncio 调度也读取该身份传入任务，但读取与 pending 写入暂非同一原子操作。无源调度载荷为 null，不能认领后来创建的会话。

执行入口对有 scheduled_timestamp 但无 incarnation 的旧载荷报 ValueError，明确 reschedule required，不自动绑定当前源；有身份时核对源 JSON incarnation 及读取对象 version，替换会话跳过并条件清理旧 pending。读后变化仍由原有 CAS/最终源 revision 保护。新增真实队列载荷往返回归覆盖普通更新仍可抽取、删除重建不调用模型且新消息不变；该新回归直接调用执行入口，未运行其消费 worker。已有真实 worker 正常/失败回归使用新载荷。

初始相关定向 **14 passed，185 deselected**（新增两项源绑定测试前），Ruff、固定版本 local pre-commit 通过。历史任务重排工具、全部任务生产者、源版本跨全生命周期及部分发布幂等仍待完成，不勾选 P1-04/P1-05。

全套首次在旧 pending 清理测试的无版本载荷失败（44 passed 后停止）；为该测试显式提供源身份，保持旧任务不能清新 pending 的断言。新增历史无版本调度拒绝回归，确认 reschedule required 且 pending 不被清理。

最终完整兼容套件 **202 passed，45.13 秒**，一条既有 Pydantic 警告，包含队列源身份往返/更新重建及历史无版本拒绝。Ruff、固定版本本地 pre-commit 和两个仓库 diff 检查通过。未提交代码；不能据此勾选整个租户隔离、幂等或故障恢复任务。

## 接续实现：asyncio 调度原子绑定源身份

asyncio 分支改为声明两键 Lua，原子读取 JSON incarnation 并 SETEX pending，返回调度源身份；bytes/text 返回均归一化。保持原两倍防抖 TTL 与进程内任务跟踪协议，未引入持久队列。

新增真实数据库/实际 asyncio task 回归：观察真正两键 Lua 返回服务器 incarnation，pending TTL 正确；等待执行前删除重建，模型不被调用、新消息保持未抽取、旧 pending 条件清理、任务从跟踪集合移除。定向 `-k asyncio_schedule` **1 passed，202 deselected，2.63 秒**，既有 Pydantic 警告。AMS Ruff、固定版本 local pre-commit 及两个仓库 diff 检查通过；本轮未重跑全套，此前 202 passed 不包含新测试。进程内任务依旧不提供进程崩溃恢复；全部入口和部分发布幂等仍未完成。

## 接续实现：删除会话原子清理抽取调度元数据

delete_working_memory 使用声明四键 Lua，同时 DEL 工作 JSON 与相同 scoped identity 的 pending/debounce/outbox，保留 revision counter；已排入 Docket 的任务仍由 pending/incarnation 校验拒绝，不直接修改共享队列。源已不存在也清除残余 metadata；索引 fallback/migration 路径保持原授权范围。

新增有源/无源真实数据库回归，删除后 metadata 全无、revision 不变，另一 owner 同名会话和三类 metadata 原样保留，重复删除安全。连同源绑定/会话变更/asyncio 调度定向 **8 passed，197 deselected，3.91 秒**。AMS Ruff、固定版本 local pre-commit 通过。此变更只治理显式会话删除，不提供 TTL 自然过期、取消、失败任务的统一容量回收或队列撤销；P1-05/P2-03 仍未完成。

最新完整兼容套件 **205 passed，46.58 秒**，包含前轮 asyncio 原子绑定和本轮删除清理，既有 Pydantic 警告。两个仓库 diff 检查通过；未提交代码，P1/P2 剩余验收不变，仍为 9/20。

## 接续实现：outbox 恢复清理自然过期或被替换的源意图

_dispatch_extraction_outbox 比较 Lua 扩展为声明三键，在 outbox 仍匹配读取快照后，核对载荷 source_incarnation 与源 JSON。源不存在或身份不同则删除匹配的旧 outbox；仅 pending 时间戳仍匹配才删除它，返回未派发，Docket 不打开。历史无身份载荷仍保留原派发/执行拒绝协议，未自动认领当前源。

新增实际 TTL 到期/过期后重建两种回归：先注入入队失败以保留真实绑定 outbox，待源实际到期，reconciliation 返回 0 并清理旧意图；无源时清旧 pending，重建并有新 pending 时保留新值/会话；重复恢复零操作。相关 `-k outbox` **10 passed，197 deselected，2.48 秒**，既有 Pydantic 警告。旧快照回归更新为实际三键比较断言，保持合法载荷与 Docket 不打开验证。Ruff、本地固定版本 pre-commit 通过。

这只清理尚未派发的失效 outbox，已入队任务的残余 pending/debounce、永久 revision/generation/receipt 容量治理及历史重排仍待完成；P1-05/P2-03 不勾选。

相关真实 worker 回归 `-k "real_extraction_worker or real_worker_extracts_messages"` **4 passed，203 deselected，11.89 秒**，验证多作用域发布/空结果、连续两轮及准备阶段失败重试仍正常。两个仓库 diff 检查通过。本轮未重跑完整套件，前轮全套 205 passed 不包含新增两项；未提交代码。

## 接续验收：实际抽取 worker 进程崩溃与新进程接管

新增两子进程真实 Dragonfly/Docket 回归，执行真实 run_delayed_extraction/线程抽取和工作 JSON 存储，模型策略为确定性桩。首 worker 策略进入后阻塞，数据库观察任务 RUNNING 与 PEL=1，再 SIGKILL 并 wait；启动不同 worker 新进程，通过 Docket pending reclaim 接管，空抽取完成。断言两个策略调用、COMPLETED/结果 0、消息标记完成且无 extraction_generation、pending 消失、Stream/PEL 清空。子进程始终 finally 回收并保留测试日志。

首次使用 100ms redelivery_timeout 导致仍在运行的阻塞任务被同进程提前重复接管，策略调用数 3 而非 2；日志确认并行重复执行。首进程改为 30 秒，恢复进程仍用 100ms 加速 reclaim，定向 `-k process_crash` **1 passed，207 deselected，4.71 秒**，既有 Pydantic 警告。过短 redelivery_timeout 可重复执行活跃任务，部署须结合执行时长/心跳配置；此回归不证明部分发布副作用幂等。

固定版本 local pre-commit 和两个仓库 diff 检查通过。本轮只新增测试，未重跑全套；前轮全套 205 passed 未包含之后三个新测试。此证据覆盖模型准备阶段进程崩溃和空结果恢复，不覆盖标记后/部分发布中/完成清理前崩溃、CLI outbox 启动恢复或真实模型，P1-05 仍未完成。

## 接续修复：标记后崩溃不再误报抽取完成

执行入口在无未抽取消息但仍有 extraction_generation 时抛 RuntimeError（unfinished publication/recovery required），不清 pending、不伪装成功空抽取。此为可诊断的失败保护，尚无自动恢复发布协议；有新未抽取消息时的混合残留仍需处理。

实际子进程回归扩展 prepare/marked 两阶段：marked 策略返回记忆，真实工作 JSON 标记后、索引前阻塞并 SIGKILL；新进程 reclaim 后 FAILED，get_result 提示恢复，策略调用仅一次，原 generation/pending 留存，PEL/Stream ACK 清理。prepare 仍可恢复完成。恢复进程 100ms 阈值再次发生活跃重复接管，改为 1 秒避免测例自身引入短阈值竞争；首进程仍 30 秒。定向 **2 passed，207 deselected，10.00 秒**，既有 Pydantic 警告。

需要下一步持久化抽取结果/发布进度与幂等协议，才能恢复标记后或部分发布后的任务，不能把明确失败当作 P1-05 完成。

最新完整兼容套件 **209 passed，55.36 秒**，包含近期失效 outbox 和两阶段进程崩溃回归，既有 Pydantic 警告。Ruff、固定版本 local pre-commit 和两个仓库 diff 检查通过；未提交代码。P1-05 标记后自动恢复及部分发布幂等仍未完成，保持 9/20。

## 接续实现：客户端 PUT 抽取状态归一化

API/MCP 共用 put_working_memory_core 在摘要前读取精确作用域 JSON，仅输入 revision 与服务器相同且消息 id/role/content/created_at 全部相同才保留服务器 discrete_memory_extracted/extraction_generation/persisted_at。新或修改消息归一化为 f/None/None，忽略客户端伪造状态；重复消息 ID 返回 HTTP400，CAS 仍保护读后并发变化。服务器内部 set_working_memory 继续保留任务写入状态。

新增实际存储/API core 回归确认伪造新消息标记被清除，未变消息客户端 f/伪造 generation 不覆盖服务器 t/generation/persisted_at，编辑后重新具备抽取资格；重复 ID 拒绝且数据库 revision 不变。定向 **1 passed，209 deselected，1.65 秒**。初次因缺 Keys 导入失败，补导入后 Ruff 与定向回归通过；固定版本 local pre-commit 通过。此证据为共享 core，不是认证 HTTP/MCP 新端到端测试；旧键迁移状态保留、摘要生成消息及所有其他写入入口仍需验证，P1-06 不勾选。

最终完整兼容套件 **210 passed，55.76 秒**，一条既有 Pydantic 警告。AMS Ruff、固定版本 local pre-commit 和两个仓库 diff 检查通过；未提交代码，整体保持 9/20。

## 接续验收：认证 HTTP PUT 的抽取状态与真实存储

新增 RS256 认证 HTTP ASGI 请求到真实两 shard 数据库回归，按 content/role/created_at 三种修改参数化。检查创建时伪造 t/generation/persisted_at 归一化，服务器已有标记在未变 PUT 中保留，修改后实际 JSON 重置 f/None/None；重复旧 revision 为 409，另一 subject 指定 alice 为 403，实际存量 JSON 与 revision 不变。摘要为 identity 桩，长记忆后台任务关闭，认证/路由/CAS/数据库真实运行。

定向 `-k authenticated_put_normalizes` **3 passed，210 deselected，2.25 秒**，既有 Pydantic 警告。固定版本 local pre-commit 与两个仓库 diff 检查通过。本轮仅新增测试，未重跑全套；前轮 210 passed 不包含此三项。不代表实际网络监听服务、MCP 工具或摘要/抽取后台完整端到端已验收，P1-03/P1-06/P1-07 保持未勾选。

## 接续验收：真实 MCP SDK HTTP/SSE 写入标记与版本隔离

扩展既有真实 uvicorn 监听服务、RS256 身份、MCP ClientSession 的 HTTP/SSE 双传输回归。在原长期记忆同 ID 读删/会话 owner 断言后，调用实际 set_working_memory 工具：伪造新消息 t/generation 在数据库归一化，返回身份为认证 owner；修改输入成功，原 version 重投返回 isError，指定其他 user 返回 isError，实际 JSON/version 不变。摘要为 identity 桩，工作记忆写入时关闭长期后台派发，其他协议/认证/网络/存储真实运行。

定向 `-k real_mcp_client_owner` **2 passed，211 deselected，2.77 秒**，既有 Pydantic 警告。固定版本 local pre-commit 与两个仓库 diff 检查通过。本轮只扩展既有两个测试，未新增任务勾选或重复全套。MCP 所有工具、并发重连、STDIO 与工作记忆到实际发布完整链路仍未完成。

## 接续修复：摘要裁剪的信息进入线程抽取

_build_thread_extraction_context 将非空 working context 作为 conversation summary 与 recent messages 一并传给策略，context 字典包含 working_memory_context；源指纹纳入压缩 context，防止只更新摘要但近期消息未变时被 unchanged-summary 检查跳过。无 context 时原 message 指纹保持不变；source_message_ids/时间范围仍只指近期真实消息，不虚构被裁剪消息时间。

新增共享 PUT core→真实摘要裁剪算法→真实 JSON→实际线程抽取函数的回归：固定 token 预算触发两条旧消息摘要、一条近期消息保留；模型摘要和抽取策略为确定性桩。验证归一化状态、持久 context、策略同时收到摘要/近期内容、来源 ID 正确、仅 context 变化的指纹不同。定向 **1 passed，213 deselected，1.74 秒**，既有 Pydantic 警告。Ruff、固定版本 local pre-commit 通过。

此修复保留压缩后的上下文，不能保证摘要模型无损保留所有事实；摘要版本、原始消息持久化与真实模型验收仍待完成。P1-06 不勾选。

最新完整兼容套件 **214 passed，56.98 秒**，包含近期认证 HTTP/MCP 与摘要 context 抽取链路，既有 Pydantic 警告。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过；未提交代码，整体保持 9/20。

## 接续修复：未完成抽取消息不被自动摘要裁剪

_summarize_working_memory 在超过摘要软阈值后检查服务器 extraction_generation，存在则记录延后并保留原 context/消息，不调用模型或裁剪。共享 PUT 的客户端归一化在摘要前执行，伪造新 generation 已清除，只有未变消息的真实服务器状态会延后摘要。硬 JSON 大小限制和 CAS 继续生效。

新增实际数据库 PUT 回归，存量带 server-owned generation 和一条新未抽取消息，强制超过 token 阈值；摘要模型禁止调用，响应与实际 JSON 保留两消息、原 context/ownership 和新消息资格，revision 正常增加。与此前正常摘要裁剪回归合跑 **2 passed，213 deselected，1.84 秒**，既有 Pydantic 警告。Ruff、本地固定版本 pre-commit 和两个仓库 diff 检查通过；未重复全套，前轮 214 passed 不包含新测试。

延后不是发布恢复，可能临时超过 token 软预算；客户端显式编辑/删除未完成消息的协议、混合旧残留与新输入、持久发布恢复仍待完善。P1-06/P1-05 不勾选。

## 接续实现：未完成服务器消息的客户端修改协议

共享 PUT core 在当前 revision 存量消息存在 extraction_generation 时，要求该消息继续存在且 id/role/content/created_at 不变，否则 HTTP409 提示 resolve publication before editing/removing。客户端不能靠编辑/省略消息抹除恢复证据；保留旧消息并追加新输入仍允许，CAS 保证校验后并发状态变化不被覆盖。显式整个会话删除维持既有清理语义。

新增 remove/content/role/created_at 四种实际数据库 core 回归，拒绝后完整 JSON/version 原样不变，随后追加新消息成功，旧 generation 保留、新输入为 f。已有 core/认证 HTTP 编辑测试改为先通过服务器存储清理已完成 generation，再验证正常修改归一化；未变消息状态保留断言仍在。相关 **8 passed，211 deselected，3.33 秒**。Ruff、固定版本 local pre-commit 通过。

这保护恢复线索，不提供未完成发布的自动完成；混合旧 generation 与新输入的执行规则、持久抽取结果/发布进度及历史迁移仍未完成。

最终完整兼容套件 **219 passed，57.91 秒**，一条既有 Pydantic 警告。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过；未提交代码，整体仍为 9/20。

## 接续实现：抽取结果与消息标记原子持久化

服务器 set_working_memory 新增内部 extraction_batch 参数（generation、MemoryRecord 列表）。调用前校验 ULID/序列化及批次大小上限；CAS Lua 可声明第三键，在版本匹配后将 JSON 标记与 extraction_batch:<源 scoped key>:<generation> 一并写入。批次 schema_version=1/state=prepared，保存结果完整模型字段、generation/source key/消息 ID，Lua 绑定实际 source_revision 与 incarnation。批次不设 TTL，重复键报错且不修改数据；普通写入仍两键。run_delayed_extraction 在发布前使用该原子写入。

实际 marked 阶段 SIGKILL/新 worker reclaim 回归现在额外核对残留批次中的模型结果、源 revision、generation、消息 ID 和无 TTL；reclaim 仍明确 recovery required，尚未自动重放。新真实 CAS 回归覆盖完整批次往返、重复 generation 不覆盖、stale writer 不生成批次且源 JSON 不变。相关定向 **3 passed，217 deselected，10.35 秒**；之前失败恢复定向 **4 passed，215 deselected**。Ruff（修 import 顺序）、固定版本 local pre-commit 通过。

批次仅为 durable prepared intent：尚无逐记录 publication receipt/去重副作用协议、published/finalized 状态和恢复读取/重放。包括成功任务的批次目前永久保留，容量/回收及源删除档案治理仍待实现；不能凭有批次自动认领当前 deletion generation 重放。P1-05/P2-03 不勾选。

最终完整兼容套件 **220 passed，58.28 秒**，一条既有 Pydantic 警告。Ruff、固定版本 local pre-commit 及两个仓库 diff 检查通过；未提交代码，整体保持 9/20。

## 接续实现：prepared batch 保存服务器删除版本

线程抽取在模型结果返回后、标记前捕获每结果四项 publication generation（已有服务器私有版本优先保持），批次 schema 升至 2，显式 publication_generations 与结果顺序对应；内部 batch 写入拒绝缺失/非四项/非十进制字符串版本。发布传同一快照到 index_long_term_memories，避免 prepared intent 后删除目标仍重新认领当前 generation。

新增真实数据库回归在批次标记已落盘后，通过实际 db.delete_memories 对尚不存在目标发精确删除，再进入实际 index 版本校验：拒绝且 Hash 不存在。批次版本保持原零计数；从持久模型字段恢复 MemoryRecord 并传原快照再次拒绝，不复活目标。定向 **1 passed，220 deselected，1.64 秒**；进程/CAS/索引失败相关 **5 passed，215 deselected**（新增删除回归前）。marked 崩溃回归核对 schema2 和版本快照。Ruff、固定版本 local pre-commit 通过。

当前捕获发生在模型返回之后，固定摘要 ID 的模型运行期间删除仍须更早绑定；去重副作用、逐记录发布收据和自动恢复协议未完成。旧 schema1 批次不能补当前版本自动重放。P1-04/P1-05 保持未勾选。

最终完整兼容套件 **221 passed，58.43 秒**，一条既有 Pydantic 警告。Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过；未提交代码，仍为 9/20。

## 接续修复：固定摘要 ID 在模型调用前绑定删除版本

summary 策略在固定 thread_summary ID 确定后、既有摘要检查/模型前捕获其四项服务器删除版本。转换结果时绑定私有 publication generation，线程发布准备阶段优先保留该快照，再写 schema2 batch 和进入索引校验，不能在模型返回后认领更新的删除计数。其他未知输出 ID 仍在结果返回后捕获，不能推广为所有生产者已完成。

新增模型 barrier→实际 db.delete_memories→释放模型→实际 prepared batch/index 校验回归，精确作用域/全局 absent-ID 删除两种均拒绝晚到固定摘要，实际 Hash 不存在，batch 保存旧零快照与固定 ID，源消息回滚为 f。定向 **2 passed，221 deselected，1.87 秒**，既有 Pydantic 警告。模型和既有摘要匹配检查为桩；真实数据库删除/存储/版本校验执行。Ruff、固定版本 local pre-commit 通过。

非固定结果 ID 的早期身份协议、旧摘要/其他任务生产者、去重副作用、发布收据/自动重放/回收仍待完成，P1-04/P1-05 不勾选。

相关 summary 回归 **20 passed，203 deselected，3.72 秒**，既有 Pydantic 警告。两个仓库 diff 检查通过；本轮未重跑全套，前轮 221 passed 不包含新两项；未提交代码。

## 接续实现：索引成功后的批次 published 收据

新增 mark_extraction_batch_published：声明单键 Lua 核对 schema2/source key/generation，将 prepared 原子改为 published 并记录时间；已 published 返回成功且不重写时间/结果/版本。线程抽取在 index 成功返回后、消息标记清理前调用。先置 publication_completed=True，因此收据连接失败不会回滚为可重新抽取，异常仍传播并保留恢复线索。无记录/旧 schema/不符身份明确拒绝。

新真实存储回归核对 prepared→published 仅新增状态/时间、模型和版本完整保持、重复记录幂等、不存在批次拒绝且不影响现有；注入收据连接失败，原消息 t/generation 和 prepared 批次保留，不错误恢复抽取资格。与正常标记清理合跑 **4 passed，221 deselected，2.12 秒**，既有 Pydantic 警告。Ruff、固定版本 local pre-commit 通过。

published 表示业务 index 成功返回，不等于逐结果原子 receipt；部分发布/索引响应丢失仍为 prepared 歧义，不能直接自动重放。published 后 cleanup 崩溃恢复、finalized 状态及回收仍待实现。P1-05 不勾选。

最新完整兼容套件 **225 passed，59.20 秒**，一条既有 Pydantic 警告。Ruff、固定版本 local pre-commit 和两个仓库 diff 检查通过；未提交代码，整体仍为 9/20。

## 接续实现：published 后清理崩溃的自动恢复

执行入口在无新未抽取消息但有残余 generation 时，尝试 finalize_published_extraction_batch。声明三键 Lua 校验源 revision/incarnation、schema2 published receipt/source key/generation 及所拥有消息 ID/t 标记，原子清 generation、递增 revision 并记录 finalized/时间；不调用模型或 index。源变化报 CAS 冲突，重建或 prepared/无收据继续 recovery required。恢复成功条件清理原 pending，多个 generation 逐批处理；尚非全部批次事务。

真实子进程回归新增 published 阶段：空抽取写 published 后、清理标记前阻塞 SIGKILL，新 worker reclaim 后 COMPLETED/结果0、模型仅调用一次、标记清理/批次finalized/pending删除/ACK。与 prepare 正常恢复、marked 明确失败合跑。新 CAS 测试确认旧 revision 拒绝并完整保留数据、重建当前revision仍拒绝旧receipt，普通上下文更新使用新 revision清理成功且上下文保留。

首次 TTL 断言失败，发现根 JSON.SET 重写清除 TTL；Lua 现读取剩余 PTTL 并在同一脚本恢复，不重新使用配置时长。最终相关 **5 passed，223 deselected，13.47 秒**，既有 Pydantic 警告。Ruff/local pre-commit 通过。

只恢复有明确 published 收据且无新 f 消息的清理；mixed 新输入、prepared 部分发布、每记录幂等/收据丢失和正常路径finalized/回收仍待实现。P1-05 不勾选。

最终完整兼容套件 **228 passed，62.10 秒**，一条既有 Pydantic 警告。Ruff、固定版本 local pre-commit 和两个仓库 diff 检查通过；未提交代码，整体仍为 9/20。

## 接续验证与文档整理：正常成功路径 finalized

正常抽取成功后，finalize_flags 校验原消息内容及 ownership，再调用 finalize_published_extraction_batch；消息标记清理与 finalized 收据使用同一 CAS Lua 完成。失败回滚仍走普通工作记忆写入。published 崩溃 fixture 改为在该 helper 阻塞；新增正常空抽取回归检查 finalized/时间、消息 t/无 generation、revision 与剩余 TTL。

取回已运行的定向命令 `UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x -k 'normal_extraction_records or process_crash or successful_extraction'`：**6 passed，223 deselected，13.85 秒**，一条既有 Pydantic 弃用警告。此前完整结果仍是 228 passed，不代表本改动后全套已通过；代码格式检查及完整回归仍待接续。

本轮按文档整理请求，将 handoff 的历轮追加记录归并为当前恢复基础、执行顺序与验证入口，历史证据保留在本文件；同步 TODO、当前状态与协议说明。mixed 新输入、prepared 部分发布、finalized 响应丢失幂等及容量回收仍未完成，整体保持 9/20；没有提交或推送。

## 接续实现：finalized 重试与 mixed 新输入

finalize_published_extraction_batch 收据新增 finalized_from_revision/finalized_revision。已 finalized 且源 incarnation 匹配、消息无该 generation 时，原清理 revision 或当前 revision 的重复调用只读返回，不改写 JSON、收据或 TTL；重建源拒绝。非 finalized 路径维持 stale CAS 冲突优先，定向回归曾发现并修复错误类型变化。

执行入口现先处理全部残余 generation，再准备新 f 消息。published 批次 CAS 清理后同步本地消息/version，随后抽取新输入；prepared/歧义批次明确拒绝且不调用模型。新增真实存储 mixed published/prepared 两种回归核对模型调用前旧批次状态、新输入资格、最终两批 finalized 或 prepared 原样保留；重复清理回归核对数据/收据/TTL 不变和重建拒绝。

定向 `-k 'mixed_pending_generation or cleanup_response_loss or process_crash'`：**6 passed，226 deselected，13.84 秒**；此前 CAS/正常路径定向 **4 passed，226 deselected，1.91 秒**。AMS Ruff、固定版本 local pre-commit 通过。完整回归正在运行；prepared 部分发布、逐结果收据、清理故障最终收敛及容量治理仍待完成，P1-05 保持未勾选。

完整回归首次在 182 passed 后发现旧失败 fixture 的 previous-task/other-task 无持久收据，与新的 mixed 拒绝协议冲突；将两个回归改为真实 ULID prepared→published 批次，断言旧批次 finalized 及新发布失败仅回滚新消息，保留并发更新。缺收据拒绝仍由执行入口保护，未放宽恢复校验。

最终完整兼容回归 **232 passed，62.97 秒**，一条既有 Pydantic 弃用警告。AMS Ruff、固定版本 local pre-commit 与两个仓库 diff 检查通过；无 C++ 变更，未重复 C++ 测试。未提交代码，整体仍 9/20。下一步处理成功发布后清理异常被吞/5 次 CAS 耗尽的收敛及实际响应丢失注入，再推进逐结果发布协议。

## 接续实现：发布后清理失败传播与实际 Lua 响应丢失

run_delayed_extraction 不再吞掉成功发布后的 finalize_flags 异常；五次 CAS 重试耗尽显式抛 WorkingMemoryConflictError。publication_completed 已置位，异常不回滚抽取资格，pending 保留且不设置 debounce，任务调用方能够观察失败。下一次执行根据 published receipt 清理，无模型/索引重放；若 Lua 成功但响应丢失，finalized 标记已清，下一次只清 pending。

新增真实存储三种故障注入：清理调用前连接失败；实际三键 Lua 执行后 ConnectionError 模拟传输响应丢失；每次清理前真实并发 context CAS 更新造成连续五次冲突。均断言首次异常/pending 保留/无 debounce、批次 published 或 finalized，以及恢复无第二次模型、消息最终 t/无 generation、并发 context 保留。

定向 `-k publication_cleanup_failure`：**3 passed，232 deselected，2.05 秒**，一条既有 Pydantic 弃用警告。AMS Ruff、固定版本 local pre-commit 通过。完整兼容套件正在运行；这仍不是逐结果部分发布恢复或所有队列错误的自动重试协议，P1-05 不勾选。

最终完整兼容套件 **235 passed，63.67 秒**，一条既有 Pydantic 弃用警告。Ruff、本地固定版本 pre-commit、两个仓库 diff 检查通过。未提交代码，整体仍 9/20；下一步逐记录原子发布收据及 prepared 部分成功恢复，随后自动恢复调度/终态回收。

## 接续实现：逐记录 Hash 发布原子收据基础

MemoryRecord 新增服务器私有 _extraction_batch（批次键、零基位置）；线程发布时绑定，dedup 的处理结果保留该绑定。RedisVL staging 发布 Lua 声明额外批次键，在 RENAME 前校验 schema2 prepared/source key/revision、结果位置/id 和原四项删除版本；在同一脚本 RENAME/PERSIST 后写 record_receipts[位置]={target_key,state:published}。未绑定的普通写入不要求批次。错误位置明确拒绝且不修改已有收据。

真实 RedisVL/Dragonfly 回归在原子 Lua 成功后注入 ConnectionError，核对实际目标 Hash 和 prepared 批次逐记录收据同时存在、stage 已清理；越界位置拒绝且收据不变。首次 fixture 使用不在固定向量表中的 result 导致 KeyError，已改为确定性三维 embedding 桩。最终定向 **1 passed，235 deselected，1.67 秒**，既有 Pydantic 弃用警告。Ruff、本地固定版本 pre-commit 通过；完整回归运行中。

这只完成实际 Hash 写入的原子收据基础，不等于整批业务已完成：hash 去重跳过、语义合并/删除及访问统计副作用尚无收据；不支持自动重放或 receipt 命中跳过，也未调整 index 异常的消息回滚协议。下一步必须保留部分成功批次 ownership，再实现逐结果恢复和去重副作用协议。P1-04/P1-05 仍不勾选。

最终完整兼容套件 **236 passed，63.62 秒**，一条既有 Pydantic 弃用警告。Ruff、本地固定版本 pre-commit 和两个仓库 diff 检查通过；未提交代码，整体 9/20。接续优先处理已存在逐记录收据的 index 异常不得回滚为 f，并用真实部分成功 Hash 回归，再补安全重放/去重副作用。

## 接续实现：部分 Hash 发布禁止消息回滚

set_working_memory 新增服务器内部 unpublished_batch_generation。回滚使用声明第三批次键的同一 CAS Lua，在写 JSON/revision 前验证 schema2 prepared、源/generation/incarnation，并拒绝存在任何 record_receipts 的批次；批次缺失/状态歧义亦拒绝。准备与回滚参数不可同时使用。只有确认未发布的批次允许恢复 f；与发布 Lua 的源 revision 校验共同避免读取收据后并发发布的竞争。

finalize_flags(False) 将该 generation 传入原子写；拒绝回滚时保留原 index 异常、消息 ownership、prepared 批次和 pending，不再退回新抽取资格。重投目前明确 recovery required，自动重放仍待实现。缺收据的去重/语义副作用协议仍未覆盖，不能把无 record_receipts 等同所有业务副作用未发生。

新增真实两条 Hash 发布回归，分别在第二条 Lua 前失败、第一条 Lua 后响应丢失；第一条 Hash 与 receipt 已存在，第二条不存在，源消息 t/generation 保留，重投无第二次模型。与完全未发布/并发更新回滚回归合跑 `-k 'partial_hash_publication or index_failure_restores'`：**4 passed，234 deselected，2.23 秒**，既有 Pydantic 弃用警告。Ruff、本地固定 pre-commit 和两个仓库 diff 检查通过；完整兼容回归运行中。P1-04/P1-05 保持未勾选。

最终完整兼容套件 **238 passed，64.22 秒**，一条既有 Pydantic 弃用警告。Ruff、本地固定 pre-commit、两个仓库 diff 与文档链接检查通过；未提交代码，整体仍 9/20。下一步同批次重复发布跳过/逐结果恢复、源更新后的恢复身份与去重副作用收据。

## 接续实现：同批次 Hash 重投跳过已发布结果

发布 Lua 在原 source/删除版本检查后查逐结果收据，已有 published/同目标键且目标仍存在则返回 2，不执行 RENAME/PERSIST 或改写批次；错误形状/目标及缺失目标拒绝。仍先执行原删除版本保护，删除后的 receipt 不能复活数据。调用方视 1/2 为已发布，stage 在 finally 清理。

扩展真实响应丢失回归：手动更新目标 text/access_count，再重投原结果，二进制完整 Hash 与收据不变；实际精确删除后重投被拒绝。扩展两个 partial 回归，从持久模型字段/原四项删除版本/source_revision/位置重建记录，直接数据库发布跳过第一条后写入第二条，第一条后续更新保留、两项收据齐全，无第二次模型。此为数据库发布原语，不自动跳过原 index 的 dedup/后台副作用。

首次完整 Hash 比较因向量二进制触发 decode_responses UnicodeDecodeError，改为 HGETALL 的 NEVER_DECODE 校验，覆盖向量与所有字段。定向 `-k 'record_publication_receipt or partial_hash_publication'`：**3 passed，235 deselected，2.05 秒**，既有 Pydantic 弃用警告。Ruff/local pre-commit 通过；完整回归运行中。自动 prepared 恢复、源更新后身份绑定、去重跳过/副作用及后台结构任务仍待协议，P1-04/P1-05 不勾选。

## 2026-10-01 接续修复：同 ID 更新不预先删除旧 Hash

恢复上一轮被中断的完整回归时，session 25278 已不存在，进程检查亦无在跑的兼容 pytest；未把缺失结果记为通过。本轮继续从实际工作区验证。

deduplicate_by_id 保留旧记录并继承 persisted_at，不再调用 delete_memories；最终发布通过已有 RENAME/PERSIST 与原删除版本校验原子替换。避免去重自身推进删除 generation 使新摘要被拒绝，也避免 embedding/发布前失败丢失旧数据。更新 AMS 原 ID 去重单测，检查不调用删除且保留持久时间。

新增真实 index→ID 去重→RedisVL 发布→批次 finalized 的同 ID 摘要更新回归，以及 embedding 失败时完整二进制旧 Hash 保留回归；四项删除版本均不增加。首次 persisted_at 断言用 ISO，实际 Hash 合同为 epoch，改为核对对应 timestamp。

定向 `-k 'same_id_summary or record_publication_receipt or partial_hash_publication'`：**5 passed，235 deselected，2.51 秒**，既有 Pydantic 警告。AMS `.venv` 下 `python -m pytest tests/test_long_term_memory.py -q -x -k deduplicate_by_id`：**1 passed，1 skipped，32 deselected**，跳过项不算通过。Ruff/local pre-commit 通过；完整兼容回归运行中。语义合并删除、hash 去重访问统计、prepared 自动恢复仍待实现，整体仍 9/20。

最终完整兼容套件 **240 passed，64.65 秒**，一条既有 Pydantic 弃用警告。Ruff/local pre-commit 与两个仓库 diff 检查通过；包含前轮 Hash 收据重复发布跳过和本轮同 ID 更新。未提交代码，整体 9/20；继续源更新恢复身份、去重跳过/语义删除副作用收据及离散子记忆发布完成协议。

## 2026-10-01 接续修复：离散子记忆发布后才标记源完成

extract_memories_with_strategy 将 all_updated_memories 的更新移到子记忆 index 成功之后；子发布异常直接传播，源未被提前标 t。无子结果时仍在策略成功后完成源更新。新增真实数据库成功/失败两种回归，断言实际 index 前源为 f，成功后子 Hash 内容及源 t，注入发布前失败后源完整二进制 Hash 原样保留。

定向 `-k discrete_source_is_marked`：**2 passed，240 deselected，1.82 秒**，一条既有 Pydantic 警告。AMS `.venv` 下 `python -m pytest tests/test_extraction.py -q -x`：**12 passed，2 skipped**，跳过不计为通过。Ruff/local pre-commit 通过。

这只修复顺序，尚未完成离散抽取幂等协议：子记忆 ID 仍在每次抽取时生成，部分子发布/源更新失败仍需持久批次与收据；源完成 update_memories 仍为全记录重写，需要防止覆盖并发源更新。P1-04/P1-06 保持未勾选；完整兼容套件基线仍为此前 240 passed，不能声称本改动后 242 项全套已通过。

相关兼容回归 `-k "discrete or real_extraction_worker or source_memory"`：**13 passed，229 deselected，10.28 秒**，既有 Pydantic 警告。两个仓库 diff 检查通过；未重复完整套件，未提交，整体仍 9/20。

## 2026-10-01 接续实现：离散源完成只写标记并 CAS 校验

MemoryVectorDatabase 增加 mark_extraction_completed 合同，未实现的 provider 明确 NotImplementedError，不退回全记录重写。RedisVL 实现声明源 Hash 与四项删除 generation 的五键 Lua，核对服务器快照及源正文/配置/metadata/时间等持久字段，成功仅 HSET discrete_memory_extracted=t；不重写向量、不调用 embedding，不覆盖 access_count/last_accessed。每源顺序完成，尚非多源整体事务。

离散抽取发布子结果后调用该合同。新增真实数据库 content/delete/stats 三种并发变更回归：正文/metadata 修改保留新数据与 f 并拒绝完成，删除不复活；统计更新可完成且17次访问与新last_accessed保留。与上一轮顺序回归合跑 **5 passed，240 deselected，2.39 秒**，既有 Pydantic 警告。AMS 事件日期单测的 mock 原断言 update_memories 失败，改为 mark_extraction_completed 且断言不调用全记录更新；最终 **12 passed，2 skipped**。

Ruff/本地固定 pre-commit 已覆盖本轮生产代码与 Dragonfly 测试。对 AMS tests/test_extraction.py 的本地 pyflakes hook 报既有第199行 bertopic availability import 未使用（已有 noqa:F401，但该替代 hook 不识别）；其 Black/AST/空白检查通过，使用 Ruff F401 验证该文件。未为此修改无关测试。完整兼容回归运行中。

稳定子 ID、持久抽取批次、部分发布/源完成响应丢失及子任务恢复仍未完成；源 CAS 失败时已发布子数据仍需明确治理，P1-04/P1-06 保持未勾选。

最终完整兼容套件 **245 passed，65.67 秒**，一条既有 Pydantic 弃用警告。两个仓库 diff 检查通过，未提交；整体仍 9/20。下一步离散稳定子 ID/持久批次和源 CAS 拒绝后的恢复，与线程 prepared 恢复及去重副作用协议共同推进。

## 2026-10-01 接续实现：离散结果持久化与稳定子 ID

provider 新增 get/prepare_discrete_extraction 合同。RedisVL 用源持久字段（排除 processed/访问统计）与四项原删除版本的 SHA256 派生 scoped discrete_batch 键，声明源/四项 generation/批次六键 Lua 核对源仍匹配，首次 SET 持久 prepared 结果，已有批次返回原载荷不覆盖。记录 raw 策略结果、服务器生成子 ID、创建时间及逐子四项删除版本；大小受 working_memory_max_bytes 限制，无 TTL。读取亦校验当前源，替换/删除拒绝。

离散抽取先读批次，缺失才调用模型并准备；恢复输出使用原子 ID、创建/更新/访问初始时间和原删除版本，继续传源版本保护到 index。扩展子发布前失败回归：源 f 保留，实际批次输出/版本存在，释放故障后同 ID 发布完成且模型调用仅一次。新增首次输出不被第二次prepare覆盖、无 TTL、absent 子 ID 删除后恢复保留原版本并拒绝发布，以及源正文变化拒绝旧批次读取。

定向 `-k 'discrete_source or discrete_prepared'`：**6 passed，240 deselected，2.59 秒**；AMS 抽取单测 **12 passed，2 skipped**，既有 Pydantic 警告。随后补子创建时间持久化并进入完整回归。Ruff/local pre-commit 与 AMS 测试 F401 Ruff 检查通过。

这是 durable prepared 输出与稳定身份，不等于逐子发布完成协议：目前 discrete_batch 未有逐子原子 receipt/finalized、去重副作用与部分成功不覆盖后续更新的重放、源CAS拒绝后档案治理及容量回收仍待完成。尚无实际离散worker进程SIGKILL验收。P1-04/P1-06 保留未勾选，整体 9/20。

最终完整兼容套件 **246 passed，65.64 秒**，一条既有 Pydantic 弃用警告。Ruff/local pre-commit、测试F401及两个仓库 diff 检查通过，未提交；整体 9/20。接续为离散批次绑定逐子原子发布收据，收据命中时跳过已发布项并保留后续更新，源完成与 finalized 收据原子收敛。

## 2026-10-01 接续实现：离散子 Hash 的原子发布收据

MemoryRecord 新增服务器私有 _discrete_batch（批次键/位置）。provider 恢复结果附加内部绑定，离散转换及 index 的处理结果保留该绑定。Hash 发布 Lua 声明批次键，核对 schema1 prepared、源 Hash/key/字段/原删除版本、子 ID 和子原四项删除版本后，在 RENAME/PERSIST 同一脚本写 record_receipts[位置]={target_key,state:published}。已有匹配 receipt 且目标仍存在则返回2，保留后续修改；删除版本仍先校验。

新增实际两子 index/RedisVL 发布回归：第二子 Lua 前失败或第一子成功后响应丢失；首次源 f、第一子 receipt 留存/第二子缺失；修改第一子 text/access_count 后恢复，模型仅一次、第一子完整二进制 Hash 不变、第二子内容正确，两收据齐全且源完成 t。测试使用 deduplicate=False 隔离实际发布原语，不能推广为 dedup=True 全副作用幂等。

定向 `-k discrete_partial_publication`：**2 passed，246 deselected，1.93 秒**；原 source/prepared 相关 **6 passed，240 deselected，2.66 秒**（新增两项前）。AMS 单测 fixture 缺少上轮新增的服务器创建时间导致 datetime 验证失败，补固定 _created_at 与批次位置后重跑。Ruff/local pre-commit 通过；AMS 测试使用 Ruff F401（既有 bertopic noqa 的替代 pyflakes 问题见前轮），完整回归运行中。

逐子收据已接入，但 source processed 与 batch finalized 尚非同一操作；dedup 跳过收据、语义删除/访问统计、结构后台任务、实际离散进程崩溃和终态容量回收仍待完善，P1-04/P1-06 不勾选。

最终完整兼容套件 **248 passed，66.25 秒**，一条既有 Pydantic 弃用警告；AMS 抽取单测 **12 passed，2 skipped**。Ruff/local pre-commit、AMS 测试F401与两个仓库 diff 检查通过，未提交；整体 9/20。接续源完成与 finalized 原子收据，之后收据命中在 dedup 前跳过及去重副作用协议。

## 2026-10-01 接续实现：离散源完成与 finalized 原子状态

mark_extraction_completed 由五键扩展为六键，派生原源字段/删除版本批次键，校验源、schema1/source_snapshot/source_generations 与 prepared receipt；逐结果缺失 published 收据就 recovery required，源保持 f。所有收据齐全后同 Lua HSET 源t并 SET batch finalized/finalized_at。空输出可完成；已 finalized 且源t/身份匹配重复完成只读返回。多源仍逐条，不是整组事务。

新增真实缺收据拒绝/空输出 finalized/重复完成回归。cjson 重编码空 records 可能变为空对象，恢复读取只对空对象归一化为空数组。统计并发 fixture 改为实际发布子记录后更新源统计，维持真实收据约束，不用 stub 成功证明发布。

定向 `-k discrete`：**17 passed，232 deselected，4.47 秒**，既有 Pydantic 警告。随后补源快照/版本及 finalized 源t校验，完整回归运行中；Ruff/local pre-commit 通过。一次编辑路径使用错误 cwd 未写入，已在正确 AMS V0 路径执行改动并校验。

finalized 后从执行入口重投需避免再次进入仅 prepared 的子发布，完成响应丢失/进程重启验收仍待实现；dedup 跳过收据、去重副作用和后台子任务尚未完成，P1-04/P1-06 继续未勾选。

最终完整兼容套件 **249 passed，66.13 秒**，一条既有 Pydantic 弃用警告。Ruff/local pre-commit 与两个仓库 diff 检查通过，未提交；整体 9/20。优先接续 finalized 执行入口重投与实际完成响应丢失，避免已完成批次重新进入 prepared-only 子发布。

## 2026-10-01 接续：终态重投恢复与认证 HTTP 主链路

按用户要求调整执行节奏：复用已验证 Dragonfly/SDK 基础合同，相关改动只跑必要定向回归，阶段收尾再跑完整兼容套件；不减少原20项范围及验收要求。

离散批次读取 Lua 对 finalized 校验源仍t，恢复结果返回空待发布集合；执行入口不再把 finalized 批次带回 prepared-only 子发布。扩展两个真实部分发布回归，在恢复完成的六键 Lua 成功后注入连接错误，随后重投禁止调用 index/模型，收据内容保持不变且子后续更新保留。相关 `-k 'discrete_partial_publication or discrete_completion'`：**3 passed，246 deselected，2.10 秒**。

新增 RS256 认证 HTTP → working PUT → 真实 Docket promote/run_delayed_extraction → RedisVL Hash/批次finalized → keyword recall → DELETE → 再召回空结果链路，核对实际数据库、源消息t、跨用户404及PEL0。仅模型抽取/embedding为确定性桩，结构后台任务关闭；实际调用API/授权/调度/worker/存储/搜索/删除。首次固定日期误用了未来UTC时间导致422，改为固定历史时间。

定向 `-k authenticated_http_working_memory_worker`：**1 passed，249 deselected，2.93 秒**，既有 Pydantic 弃用警告。Ruff/local pre-commit 与两个仓库 diff 检查通过。未重复全套，最新全套仍是此前249 passed，不声称当前250项全套已通过。HTTP主链路已有证据；MCP完整业务链路、真实模型/全部副作用仍待验收，P1-07保持未勾选。未提交。

## 2026-10-01 接续：真实 MCP HTTP/SSE 主链路

复用认证 HTTP 主链路配置，通过真实本地 Uvicorn MCP 服务与 MCP ClientSession SDK，HTTP/SSE 两种传输分别运行 set_working_memory→实际Docket promotion/extraction→get/search→delete→search空结果。核对模型调用一次、实际 scoped Hash 删除、batch finalized、PEL0，并拒绝指定其他用户搜索。模型/embedding为确定性桩，结构后台任务关闭。

定向 `-k authenticated_mcp_working_memory_worker`：**2 passed，250 deselected，5.18 秒**，既有 Pydantic 弃用警告。本地固定 pre-commit、diff检查通过，不重复全套。API 与两种网络 MCP 的基本业务主链路均已有证据；STDIO/全工具、并发重连、全部副作用和真实模型仍不在此验收范围。P1-07原定义要求P0/P1对应场景，不因基本主链路通过而提前勾选。

## 2026-10-01 接续：hash 去重排除同 ID 更新

deduplicate_by_hash 现在排除同 ID，候选从1扩到2以保留同ID之外的真正重复记录。同ID原子更新不再被当成重复跳过，也不因重投触发自身访问统计副作用。离散真实部分发布/响应丢失/完成重投回归扩为 deduplicate=False/True；开启 ID/hash 去重且关闭可选语义合并，四种组合均通过，完整子Hash后续更新保留、模型一次。

定向 `-k discrete_partial_publication`：**4 passed，250 deselected，2.32 秒**，既有 Pydantic 警告。AMS hash去重单测原HSET mock 与之前已改为原子pipeline的统计合同不符，改为pipeline.eval/execute并补同ID不触发统计断言。Ruff/local pre-commit及diff检查；不重复全套。不同ID重复项的批次skip receipt、语义合并删除/后台结构副作用仍待实现。

## 2026-10-01 接续：hash 去重候选实际状态复核

update_last_accessed 新增可选 expected_memory_hash，在同一统计Lua中核对当前Hash hash；deduplicate_by_hash 只在实际匹配并完成统计后丢弃输入。候选被删除或替换时返回原输入，不用过期搜索快照判定重复，也不复活/修改过期目标。真实回归覆盖deleted/changed/current三种状态。

current回归发现整数now与刚写入小数last_accessed导致min_interval=0仍被限流；Lua零间隔现明确关闭时间门槛，正间隔继续限流。最终定向 **3 passed，254 deselected，1.98秒**，相关AMS hash去重单测 **1 passed，33 deselected**。Ruff/local pre-commit与diff检查通过，不跑全套；不同ID批次去重skip收据及语义合并仍待完成。

## 2026-10-01 接续：不同 ID hash 去重原子收据

绑定 thread/discrete 抽取批次的 hash 去重不再提前更新统计或直接跳过输入。候选身份与四个删除版本经服务器私有属性传入现有发布 Lua；源身份/快照、输入删除版本、候选当前 hash 与原删除版本均先校验，访问统计与 `outcome=hash_duplicate` 收据同事务写入，不创建重复 Hash。重试从持久收据恢复原候选和版本，已有收据直接返回，不重复计数；未绑定批次的常规去重保持当前 hash 原子复核路径。该分支跳过语义合并，防止把纯 hash 重复再交给可选合并逻辑。

复用现有部分发布恢复测试，增加不同 ID 重复场景，覆盖后续子失败、首子响应丢失、保留候选后续更新、模型仅一次以及源 finalized/终态重投。定向 **6 passed，253 deselected，2.49 秒**；相关 HTTP/MCP/hash/部分恢复批次在新增参数前 **10 passed，247 deselected，7.42 秒**。首次新增 Lua 分支的裸 return 后仍拼接语句，触发 Lua 语法错误；改为 do/end 块后通过。

本轮精简 current-status，只展示当前证据；历史逐轮结果保留在本文。thread 分支复用同一发布校验，但本轮不同 ID 新场景直接验证离散路径；不据此勾选整项。语义合并、结构副作用、thread prepared 自动重放和运维仍待完成。阶段全量 **259 passed，72.10 秒**，一条既有 Pydantic 弃用警告。AMS 去重单测 **2 passed，1 skipped，31 deselected**，跳过项不计为通过。Ruff、本地固定 pre-commit 与两个仓库 diff 检查通过；未提交，整体仍为 9/20。

## 2026-10-01 接续：thread prepared 自动重放与追加恢复

run_delayed_extraction 在新模型调用前恢复已有 prepared 批次，重建原 MemoryRecord、稳定 ID/时间、原四个删除版本与逐记录收据绑定，经真实 index 重投补缺失项；完成后 published/finalized 清理。新批次保存 context 与原有消息 id/content/role/time 快照。三键 Lua 原子核对当前 source/incarnation、消息 ownership 和输入快照；仅原输入不变、末尾追加的更新可重绑源 CAS revision，保留 original_source_revision，目标删除版本始终不重绑。旧无快照批次只允许原 revision 不变的重放，输入改变仍需要显式处理。

实际 worker SIGKILL marked 阶段现在由新进程完成真实 Hash 发布与 finalized，模型仅一次，pending/PEL/stream 收敛。原部分 Hash 发布/响应丢失测试改为走实际执行入口和真实 ID/hash 去重，保留已有 Hash 后续 text/access_count/向量更新并补第二项；mixed 场景扩为仅追加可恢复/context 改变拒绝。初轮旧测试仍断言 recovery-required，更新为新行为；快照初版 isoformat 与 JSON 日期编码/省略 null 不一致导致拒绝，统一 model_dump JSON 编码与 Lua null 归一化后通过。

定向 `-k 'process_crash or mixed_pending_generation or partial_hash_publication or prepared_batch_preserves'`：**10 passed，251 deselected，12.72 秒**。Ruff、本地固定 pre-commit 与 diff 检查通过。未重跑全量，最新全量为本轮改动前259项；更广关联 `-k 'extraction or generation or publication or source_memory or discrete'`：**88 passed，173 deselected，35.43 秒**，一条既有 Pydantic 弃用警告。未提交，整体仍9/20。重复活跃执行、失败任务自动重派、语义合并与结构副作用仍未验收。

## 2026-10-01 接续：语义合并原子发布、持久计划与统计保留

索引时语义去重和后台语义压缩均移除提前删除。合并结果保留输入 ID/作用域与原删除版本；候选身份/hash/四个删除版本及非统计字段快照在模型前捕获，发布 Lua 在源与目标保护后核对候选，再在同事务替换目标、推进候选精确删除版本并删除旧 Hash。候选正文/元数据变化或删除、输入删除、embedding 失败均不损坏当前数据。发布时合计实际 access_count 和最大 last_accessed，保留模型期间的访问统计变化。

thread/discrete 绑定的合并计划（模型正文/候选快照/目标版本/anchor hash）作为批次 semantic_plans 原子 first-write-wins 保存，并受工作记忆大小限制。index 重试先恢复计划，跳过重复去重与模型，即使配置已关闭语义合并也继续原计划；Hash/删除效果与原逐结果收据同事务，响应丢失后已有收据跳过，不覆盖目标后续更新。候选排除同批次其他计划输出和离散源，避免破坏其完成收据。

新增六种真实发布场景及两个 thread 计划恢复场景；离散部分发布回归扩为语义开启/关闭。批量 `-k 'semantic or discrete_partial_publication or partial_hash_publication'`：**24 passed，251 deselected，5.76 秒**。AMS 相关语义/ID/hash 单测 **5 passed，1 skipped，28 deselected**；跳过项不计为通过。Ruff、本地固定 pre-commit 与两个仓库 diff 检查通过。本轮未重跑全量，最后全量仍为之前259项。

计划恢复测试初版未传 deduplicate=True，触发的是普通发布而非语义分支，修正后实际计划/模型次数断言通过。尚未验收完整合并属性（如 anchor 元数据/pinned 保留和竞争）、后台 hash 压缩的 generation/事务保护、离散实际语义计划（本轮直接测 thread，离散测试验证保护源/兄弟输出）、结构后台任务、重复活跃执行与终态回收。P1-02/04/06 保持未勾选，整体9/20，未提交。

## 2026-10-01 接续：后台 hash 压缩原子化与合并属性保留

compact_long_term_memories 的 hash 分支移除手写 FT.AGGREGATE/带错误引号的 FT.SEARCH 和裸 pipeline DEL，统一经 RedisVL provider scoped list 获取每轮最多 limit 条候选，按 namespace/user/session/hash 分组。provider compact_hash_group 在单个声明键 Lua 中核对所有参与者的非统计字段与四个 deletion generations，再合并实际统计/标签/来源/metadata，推进被移除记录的精确 generation 并删除。保留 pinned 记录（多个 pinned 均保留）；包含未完成 discrete f 的组暂缓处理。metadata 冲突由 updated_at/ID 较新的值胜出，最老 created_at、最新 last_accessed 和 access_count 合计保留，向量不重建。数字字段按数值比较，兼容 Lua 与 SDK 时间字符串编码不同。网络错误向任务传播，响应丢失后重复压缩不重复计数。

真实测试覆盖普通压缩、pinned survivor、候选元数据并发修改拒绝、候选删除与响应丢失重投；固定 scope 的其他用户同 hash Hash 完整保持不变。旧 AMS 压缩单测原来依赖错误 RESP mock 和吞异常，更新为 provider 空结果及无裸命令断言。

merge_memories_with_llm 补 pinned、metadata（较新覆盖冲突）、access_count、extracted_from、最早 persisted_at/event_date 和 anchor 抽取策略/配置。后台语义压缩不再手动挑少量字段转换，从 MemoryRecordResult 完整 model_dump 转换，防止属性在进入模型前丢失；相关既有模型单测增加实际属性断言。

批量 `-k 'compaction or semantic or discrete_partial_publication or partial_hash_publication'`：**29 passed，251 deselected，6.58 秒**。AMS 相关单测 **7 passed，1 skipped，26 deselected，0.08 秒**，跳过项不计为通过。Ruff、本地固定 pre-commit 与两个仓库 diff 检查通过；未重复全量，最新全量仍为早前259项。未提交，整体9/20。

尚待 anchor 元数据并发保护、全量/分页压缩调度与容量界限（当前每轮 limit 候选，不等于全数据压缩验收）、实际离散语义计划、结构子任务和全部任务生产者版本保护、重复活跃执行与终态回收；不将本轮结果视为整项P1完成。

## 2026-10-01 接续：结构抽取同 ID 更新保护与并发标签 CAS

extract_memory_structure 的最终写入除了原四个删除 generation 和 Hash 存在检查，还原子核对任务原正文、memory_hash 与 updated_at，避免同 ID 更新但未删除时旧任务覆盖新标签。模型结果只与当前 Redis 标签合并，不从任务旧 topics/entities 覆盖当前值；HMGET 后的标签变化由 Lua CAS 检出并最多五次重试，模型在循环外仅调用一次。标签清洗后按原顺序去重，重复执行不累积重复 TAG；仅写两个结构字段，保留统计、向量及其他属性。CAS 耗尽抛错；源删除/修改则明确跳过。纯 hash 去重的输入没有独立 Hash，不再为其派发结构/离散子任务。

FLAT/HNSW 真实结构回归扩为删除/重建、同 ID 正文更新、并发标签变化、实际 Lua 标签 CAS 竞争、统计变化，以及成功路径重复执行后整个二进制 Hash 内容不变。`-k structure_task`：**16 passed，272 deselected，4.12 秒**。标签去重补充前，关联 `-k 'structure_task or compaction or semantic or discrete_partial_publication or partial_hash_publication'`：**45 passed，243 deselected，9.37 秒**。

AMS 定向 **5 passed，29 deselected，0.07 秒**；单测 HMGET mock 与新增 Lua 参数断言同步。一次较宽 -k index 选入需要 localhost:6379 的既有 fixture，因无该实例在 setup 失败；缩小为本次相关单测，并以隔离 Dragonfly 的45项真实集成验证关联路径，不声称该宽 AMS 模块通过。Ruff、本地固定 pre-commit 与 diff 检查通过。最新全量仍为早前259项，未重跑全量、未提交，整体9/20。

结构模型结果持久缓存/逐任务收据、事务内派发 outbox、派发失败/worker 崩溃的自动恢复仍待完成；本轮只关闭结构写入竞争，不将整个后台任务链路或P1-04/P1-06标为完成。

## 2026-10-01 接续：结构模型结果缓存、原子 outbox 与真实 worker 恢复

结构抽取按 scoped Hash、原四个 generation、正文/hash/updated_at 生成稳定 identity 和 structure_batch 键。模型输出经过大小限制后 SET NX 持久 prepared（first-write-wins），后续重投从缓存取原输出；标签 CAS 和 finalized 收据在同一六键 Lua 完成，删除/变更源在该事务标记 superseded。终态入口重投直接返回，不调用模型、不改写目标。缓存与收据目前无 TTL，容量/回收仍待 P2 处理。

RedisVL 正常 Hash 发布在同一声明键 Lua 写 structure_outbox（开启 topic/NER 时），因此 Hash 写入后响应丢失或调用方进程在派发前中断不会丢失结构工作。intent 保存原 payload/generation，无 TTL、受大小限制；纯 hash duplicate 不创建独立任务。reconcile_structure_outbox 校验 payload/identity/版本，使用稳定 Docket key 派发，已 scheduled/queued/running 的任务不重复入队；FAILED/CANCELLED 可原键重派。intent 保留至恢复观察到 Docket COMPLETED 且结构业务收据 finalized/superseded 后 CAS 删除；初版直接在结构 Lua 删除 intent，改为确认队列完成后删除，以覆盖写入响应丢失但 Docket FAILED 的情况。非 Docket 恢复路径执行后再 CAS 删除。结构恢复并入原手动/启动/周期 reconciliation，坏 intent 逐条隔离。

新增四条真实两 shard Docket worker 路径：Hash publication 执行后响应丢失、派发失败、结构写入前失败、结构写入后响应丢失。首轮失败保留 intent，重派后任务 COMPLETED、原模型只调用一次、标签正确、批次 finalized、intent 确认完成后清理及 PEL0；活跃任务的第二次 reconciliation 不重复派发。定向 **4 passed，288 deselected，2.79 秒**。现有结构/outbox 回归 **26 passed，262 deselected，5.12 秒**。AMS 定向 **5 passed，29 deselected，0.08 秒**。

阶段完整兼容套件 **292 passed，76.76 秒**，一条既有 Pydantic 弃用警告。Ruff、本地固定 pre-commit 与两个仓库 diff 检查通过。未提交，整体9/20。仍待结构实际 SIGKILL/跨进程恢复、并发模型/派发治理、同版本配置重排和终态容量回收；普通即时结构派发仍与 outbox 恢复共存，统一稳定任务键及并发重复模型需要继续处理，不据此宣称整项P1-04/P1-05/P1-06完成。

## 2026-10-01 接续：统一即时结构任务键与真实 SIGKILL 接管

HybridBackgroundTasks.add_task 增加服务器内部 _docket_key 参数，仅传给 Docket.add 的 key，不转发至任务函数；其他任务仍使用原默认行为。index_long_term_memories 的结构任务用与 structure_outbox 完全一致的作用域/版本/正文/updated_at identity 派发。真实即时索引→Docket 路径验证，reconciliation 识别已排队的同一任务，避免再创建随机 ID 的结构任务；初版的即时/恢复双任务键问题已关闭。

新增两个实际子进程 SIGKILL 场景：模型结果 prepared 后、标签写入前；标签/收据 finalized 后、worker ACK 前。首 worker 保持 RUNNING/PEL，父进程 kill 后由第二 worker XAUTOCLAIM 接管。两阶段均模型调用一次、标签正确、任务 COMPLETED、intent 在队列完成确认后清理、PEL0/Stream0；finalized 场景收据原字节内容保持不变。首 worker redelivery 30秒、恢复 worker 1秒，沿用已验证的接管配置。

`-k 'structure_worker_sigkill or structure_outbox_real_worker'`：**7 passed，288 deselected，9.68 秒**。关联结构/outbox/认证 HTTP/MCP 主链路：**36 passed，259 deselected，18.34 秒**。AMS dependencies 模块 **11 passed，0.86 秒**；结构及索引分支定向 **4 passed，30 deselected，0.07 秒**。Ruff、本地固定 pre-commit 和两个仓库 diff 检查通过。最新全量仍是本轮修改前292 passed，76.76秒；未重复全量、未提交，整体9/20。

稳定键解决 Docket 两条派发路径的重复入队；asyncio 模式和直接并发模型调用尚未建立跨进程租约，取消/容量回收、anchor 元数据竞争也未验收。下一批优先推进 schema/model/dimension 就绪与重建，随后再审计P1整项证据，不以本轮子场景替代全部任务定义。


## 2026-10-01 接续：索引合同与保留数据的重建

AMS 新增持久 schema/model/version 合同及实际 FT.INFO 校验；模型版本用 embedding_model_version（默认1）显式声明。覆盖维度、datatype、算法、距离、字段类型、prefix、模型、模型版本和非空无合同旧数据，均在 embedding/写入前拒绝不兼容配置。API lifespan 初始化长期索引，使不兼容配置在启动阶段失败。空索引采用 SET NX 注册合同并重读确认。

provider rebuild_index 保留 Hash/向量（overwrite=True, drop=False），仅允许沿用原模型/版本/向量布局/前缀的定义重建，合同用原值 CAS 更新；CLI 改为调用 provider。实际 FLAT→HNSW 重建核对原 Hash 二进制内容完全相同并继续召回；模型/版本/维度改变则拒绝。并发重建合同冲突会明确报 recovery required，尚未实现物理索引回滚。最初模型版本拒绝回归发现序列化漏写版本字段，已修复并由全量覆盖。

命令：`UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x`。**308 passed，86.02秒**，一条既有 Pydantic 弃用警告；AMS CLI rebuild 定向 **1 passed，90 deselected，0.06秒**。本地固定 pre-commit 替代配置（远程原 hooks 下载此前失败）与两仓库 git diff --check 通过。未修改 C++、未提交。

P2-01 保留未完成：重新 embedding 迁移、旧无合同数据显式迁移、worker/MCP 启动就绪、重建失败回滚仍待完成。总进度保持9/20。按用户要求批量相关验证，阶段边界才跑全量，不重复此前已通过的 C++ 构建。


## 2026-10-01 接续：API/worker/MCP 强制启动检查

新增共享 readiness gate。worker 在 stream/group 创建、outbox 恢复及消费前检查；MCP SSE 的 ASGI lifespan、HTTP session-manager lifespan 和 STDIO 启动均先检查，API 同步使用共享入口。长期记忆禁用时跳过 provider。

真实 FLAT/HNSW 持久合同测试揭示此前 API 使用 count_memories 的证据不足：该接口吞掉异常返回0。改用 limit=1 的 filter-only list_memories，schema/contract 与连接查询错误均传播，不做全量计数。此前“API 不兼容启动即失败”的结论由本轮实现纠正。新增三个传输的真实索引模型版本不匹配测试；相关正常 MCP worker 链路注入同一个隔离 provider，避免参数化用例沿用已关闭 event loop 的全局 provider。

AMS `pytest -q tests/test_storage_readiness.py tests/test_cli.py -k 'readiness or worker or rebuild_index'`：**17 passed，80 deselected，0.14秒**，含失败前无 Redis/恢复/消费副作用与禁用分支。本轮未重复 C++ 构建，全量308仍为上一阶段证据。P2-01 仍缺重新 embedding/旧数据迁移、重建失败回滚；总进度9/20，未提交。

关联真实两 shard 回归：`-k 'mcp_startup or index_contract or index_rebuild or authenticated_mcp_working_memory_worker or authenticated_http_working_memory_worker'`，**22 passed，292 deselected，9.59秒**。本地固定 pre-commit 替代配置和两仓库 diff 检查通过。STDIO 本轮仅启动失败验收，完整身份/业务链路仍待完成。


## 2026-10-01 接续：索引重建异常回滚与并发互斥

rebuild_index 增加无 TTL owner 锁，以 SET NX 获得所有权并用比较 owner 的 Lua 释放。已有合同的重建异常时，若合同仍是原值或本次目标值，使用原合同 schema 重建原索引（drop=False），CAS 恢复原合同并核对实际定义。新合同或回滚失败会报错，不吞异常。

真实 FLAT/HNSW 覆盖 after_drop、after_create、合同 CAS 执行后响应丢失、就绪检查失败：回滚后原合同原字节、Hash/向量原字节保持，重新初始化原 provider 并召回原 ID。并发重建测试暂停首执行者，第二执行者在索引修改前拒绝，首执行者完成后 owner 锁消失。测试还揭示 RedisVL schema round-trip 将 text weight 的1归一化成1.0；合同校验改为比较解码 JSON 值，CAS 仍使用原始字节。

`-k index_rebuild`：**14 passed，310 deselected，3.83秒**。AMS readiness/worker/rebuild CLI 定向：**17 passed，80 deselected，0.14秒**。未修改 C++，未提交。

范围限制：无合同空索引尚无原定义回滚；进程 SIGKILL 会留下无 TTL owner 锁，尚未实现接管/恢复命令；回滚过程再失败、正常服务与重建间的维护窗口治理也未完成。P2-01 仍未完成，总进度9/20；继续重新 embedding、历史无合同迁移及跨进程恢复，不能将本轮异常恢复代替整个迁移验收。

相关索引合同/重建/MCP启动/认证入口主链路合并回归 **32 passed，292 deselected，11.23秒**。本地固定 pre-commit 替代配置和两仓库 diff 检查通过；上一阶段全量308并非本轮324全量通过证据。


## 2026-10-01 接续：持久重建记录与显式中断恢复

重建 owner 锁、原合同和目标合同 journal 在同一三键 Lua 中创建。修改索引前已具有跨进程恢复输入；无法成功回滚的异常保留 owner/journal。显式 recover_index_rebuild 比较 owner、journal 原值与当前原/目标合同后接管为新 owner，恢复原索引/原合同、校验实际定义，再比较状态原子清理记录。任何恢复失败保留新 owner 以便重试。CLI `agent-memory recover-index-rebuild --owner TOKEN --writers-stopped` 要求操作者先停止所有索引服务、worker 和其他重建/恢复进程；未声明停写时不创建 provider。

真实子进程分别在 drop、create、合同 CAS 后等待并被 SIGKILL。FLAT/HNSW 共 **6 passed，324 deselected，4.82秒**；保留 journal/owner，错误 owner 不改状态，新的 provider 从原合同恢复，原 Hash/向量字节、召回和合同保持，记录清理。另覆盖重建失败→回滚失败→首次恢复失败→第二次恢复成功，期间新 owner/原合同持续保留；外部合同改变时恢复在修改索引前拒绝。

重建定向 **22 passed，310 deselected，7.71秒**；CLI 原重建及恢复入口 **4 passed，90 deselected，0.10秒**；最终相关索引合同/重建/MCP启动/认证入口合并回归 **40 passed，292 deselected，14.89秒**。本地固定 pre-commit 替代配置和两仓库 diff 检查通过。本轮未重复全量或 C++ 构建，未提交，总进度9/20。

仍待：模型/维度重新 embedding、旧无合同迁移、空索引无原合同恢复、自动维护窗口治理。原合同恢复是本轮范围，不能视为完整 P2-01。此前“SIGKILL 后无恢复命令”已由本轮关闭；历史记录保持原时间结论。


## 2026-10-01 接续：缓存 provider 的维护入口检查

_ensure_index 在缓存判断前读重建 owner，活动/中断重建拒绝新请求，防止同一进程已有 _index_created=True 绕过重建状态。重建内部校验使用 ContextVar 的(index_name,owner)，不在 provider 实例上开放共享 owner 豁免。同实例独立 asyncio 任务不能使用该授权。压缩、离散批次读取/准备及源完成增加索引入口检查；原 add/search/list/delete 已经过该入口。

真实 FLAT/HNSW 测试暂停重建，同实例并发7类业务操作均在 embedding 或 Hash 修改前拒绝；已有目标保持、新目标不存在，重建完成后可正常召回。相关 `-k 'index_rebuild or discrete or compaction'`：**56 passed，278 deselected，13.61秒**；AMS CLI/readiness/worker 定向：**17 passed，83 deselected，0.14秒**。本地固定 pre-commit 替代配置通过，未修改 C++、未提交。

此检查只拒绝进入 provider 后的新请求，不提供全服务请求排空或原子请求/维护锁；已经通过检查的请求、直接使用 Redis 的入口和模型/任务调度仍须通过停写窗口治理。模型重新 embedding/旧无合同迁移继续未完成，整体9/20。

阶段完整命令 `UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x --tb=short`：**334 passed，92.34秒**，一条既有 Pydantic 弃用警告。首轮全量在219 passed后发现另一条真实 MCP owner 测试启动检查未注入隔离 provider、误用localhost6379；已将 readiness 与业务入口绑定同一真实测试数据库，新全量通过。两仓库 diff 检查及本地固定 pre-commit 替代配置通过。仍未提交，整体9/20。


## 2026-10-01 接续：重建 CLI 停写约束与 P2-05 交付说明

重建 CLI 新增 --writers-stopped，无声明时不创建 provider；不支持的后端和存储/合同错误返回非零退出码。AMS `pytest -q tests/test_cli.py -k rebuild_index`：**4 passed，93 deselected，0.10秒**。恢复 CLI 原有停写要求保持。

新增 release-compatibility.md，逐项核对 P2-05 文档合同：版本（DF基线ebcfe4c9、AMS基线94192c3/包0.15.2、Python3.12、锁定三个SDK）、当前二进制hash和未提交候选限制、构建功能选项、两shard启动参数和scheduler SHA、API/MCP/worker入口、真实验收/缺失门槛、配对制品回滚、原索引恢复及多实例/Cluster独立slot/故障/性能验证计划。实际读取pyproject入口并核对二进制--version与SHA256，文档内本地链接校验通过。P2-05 是说明交付，未宣称生产可用或其他P2验收完成。整体更新为10/20，未提交。


## 2026-10-01 接续：embedding 向量布局校验与正确编码

原 provider 写入硬编码 float32，查询也由SDK按默认float32转换；当实际schema声明float64时与合同不一致。新增统一 _encode_embedding：按实际vector字段datatype编码（float32/float64），要求一维、维度严格匹配、实数数组、转换后全部有限。NaN/Inf、float32溢出、嵌套数组、长度错误、bool和字符串数组均拒绝。写入先构建并校验全部data_list再发布，避免后续向量错误导致早期记录部分成功。查询的语义/range/混合/数据库recency都用校验后的bytes，避免SDK默认datatype转换。

真实FLAT/HNSW错误批次验证原Hash/向量原字节未变、第二目标与stage键不存在；错误查询在query/aggregate前拒绝，recency直接调用数据库分支，不以fallback算成功。独立实际schema float32/float64验证vector长度12/24字节，语义/混合/range/recency召回原ID。`-k embedding_contract`：**30 passed，328 deselected，6.55秒**（包含此前6条MCP模型版本合同启动拒绝）；关联recency/普通检索/同ID摘要更新：**37 passed，319 deselected，7.64秒**（datatype新增前）。

本轮是重新embedding迁移所需的编码/校验基础，尚无重新embedding迁移命令、持久原向量备份或模型切换回滚，P2-01仍未完成。整体10/20，未提交，未修改C++。最新全量334为本轮之前阶段，不能当作新增358项全量证据。

最终关联命令 `-k 'embedding_contract or recency or semantic or extraction or publication'`：**119 passed，239 deselected，40.72秒**。一条既有Pydantic警告。本地固定pre-commit替代配置和两仓库git diff检查通过；未重复全量或C++构建。


## 2026-10-01 并行推进：业务、持久化、迁移及性能

认证STDIO/HTTP/SSE全部10个MCP工具、双owner并发、重连及7类越权；真实worker抽取、创建、编辑、三种召回、prompt、compact、删除，核对finalized、PEL0和外租户Hash原字节。业务FLAT/HNSW发现并修复semantic merge计数重复累计。定向37 passed，21.59秒；原merge单测1 passed。P1-02完成，P1-03/07仍按原整项验收条件推进。

DF原生SAVE→SIGKILL恢复、full+增量复制→同步offset→杀主→显式单副本提升，逐字节核对JSON/Hash/vector、TTL、合同、任务/结果、延迟队列、Stream/group/consumer/PEL；恢复后实际检索及任务SUCCESS/ACK/队列清空。3 passed，17.12秒；关联业务5 passed，17.58秒。P2-02完成，边界见persistence.md。

新增运维聚合端点、显式operations角色和持久计数；批次在模型前硬拒绝。故障/权限/容量9 passed，2.50秒，关联业务/outbox38 passed，7.60秒。P2-03尚缺全producer准入/GC等。

65项迁移/合同/重建测试通过18.83秒：模型/版本/维度重新embedding、持久原字节与全覆盖CAS、三阶段SIGKILL显式resume/rollback、旧无合同显式认领、旧CASE=false精确schema、空无合同重建三阶段SIGKILL、旧缓存provider迁移后拒绝写。维护CLI和迁移文档集成后按P2-01原条件复核。

性能正式等工作量测量完成；原始JSON与工具、数据规模和QPS/P95/P99见performance.md。P2-04完成。当前13/20；全量334为上一阶段证据，新增场景尚需阶段全量。未修改C++、未提交。

## 2026-10-01 并行收口：原任务18/20

按 TODO 原20项审计，完成P1-03/04/05、P2-01/03后，合计18项；剩P1-06真实生成/embedding服务与依赖它的P1-07最终门槛。安全GC、无限自动重试不新增为原任务门槛；明确边界保留。

历史租户迁移采用原raw SHA256审核、明确owner和精确历史filters，原config/task/result/TTL归档后创建scoped新ID/generation与持久重派intent；未知owner拒绝。生产旧unowned摘要、无generation任务及旧List/Hash入口fail-closed，旧载荷FAILED/ACK但原数据不动。摘要view与原generation/config/task/lease同Lua fence；真实retry三次，模型异常与empty不假装成功，full refresh清理消失分组，删除config清结果。65项关联26.58秒；最新20项11.79秒。MCP三传输全部10工具、双owner并发重连及STDIO显式bearer身份已通过，P1-03完成。

重新embedding迁移持久原Hash/vector字节、四级generation CAS、全旧索引覆盖与维护owner，支持prepare/apply/resume/rollback/adopt；未知旧模型不猜测。旧CASE=false精确schema认领、迁移与原字节回滚，空无合同drop/create/contract三阶段SIGKILL恢复已验。每批1..100、总量不超过10000、维护CAS payload不超过64MiB，CLI源schema文件不超过1MiB。65项18.83秒；CLI16项0.06秒，P2-01完成。

模型执行guard使用SET NX PX owner、默认30秒租约与10秒续租，丢owner取消并fence，子Task不继承owner权限。结构/thread/离散模型与持久结果、prepared CAS、Hash发布/semantic计划均有owner保护。API访问统计/提升沿用原查询快照，不在查询后读取live版本重新绑定；没有publication_generations的真实旧队列index拒绝。源快照副作用13场景、guard12场景；相关39 passed36.67秒，最后16 passed6.22秒。

遗忘与Hash compact采用完整23字段原快照及四级generation原子CAS，仅实际删除计数；access/pinned/body/metadata/clone ABA变化拒绝。六个NUMERIC字段按数值等值比较以兼容FT RETURN归一化。相关60 passed11.77秒；AMS遗忘8 passed。上述原快照与发布收据证据满足P1-04。

实际BoundedDocketWorker接入真正run生命周期；scheduler每轮最多64，仅ZREM选中IDs，默认250ms，调度上限256 due tasks/s。全部AMS生产者采用原子ZCARD+XLEN<=64准入，稳定key去重、ACK/cancel释放，retry/replace不新增容量；满队列启动延后Perpetual注册以免死锁。claim保留stream_id；cancel原子XACK+XDEL且真实active pubsub取消PEL0。新19 passed3.71秒，旧worker关联55 passed72.74秒，dependencies11 passed。生产bounded scheduler SHA为46dec5a67c168033d38f45742aba3572a9d73be9，manifest/audit已更新；无授权负例保留。

运维端点显式operations角色，聚合无正文/tenant ID；四类outbox扫描有界，计数与last-event使用数据库TIME，最近300秒无新错误解除告警但保留累计counter。记录queue拒绝/Lua失败等故障，memory/queue/PEL/readiness指标、JSON与向量批次硬限制均已测试。运维11 passed2.82秒，相关19 passed3.45秒；P2-03完成。generation/receipt永久保留，是明确容量边界，安全GC留待后续优化。

AMS task新增CANCELLED/docket_key；active无TTL、terminal七天。create_task与summary_outbox同Lua原子创建；summary派发容量满返回503且保留intent。取消先AMS generation CAS fence再Docket signal，signal失败持久重试，恢复先cancel再dispatch；启动/周期reconciliation包含summary任务intent。相关5 passed2.26秒。源TTL/删除/incarnation替换退休、重复活跃执行、取消/retry终态、prepared部分成功/响应丢失及真实SIGKILL接管证据满足P1-05。

恢复边界：线程模型准备最多自动三次，publication失败保存prepared/收据/pending；Docket终态FAILED后由运维以原身份/载荷显式重投，reconciliation不自动扫描此类failed thread pending。SIGKILL实际测试，SIGTERM/INT仅关闭代码路径；asyncio模式不承诺跨进程恢复。原生SAVE及同步单副本显式提升已验证，不扩展为自动failover/Cluster/未同步零丢失。维护必须停API/MCP/worker并排空，owner gate不自动排空。

AMS配套命令（V0先激活.venv）：`pytest -q tests/test_cli.py tests/test_embedding_migration_cli.py tests/test_storage_readiness.py tests/test_dependencies.py tests/test_forgetting.py`，136 passed1.10秒。双仓固定本地pre-commit此前全量通过，原远程hooks下载失败，替代配置为/tmp/agent-memory-format-hooks.yaml。未新增C++改动，不重复先前Debug/Release表达式各83及聚合65。

新增opt-in真实模型验收脚本，只注入存储路由而不替换模型输出；覆盖合成绿茶偏好→认证HTTP→实际BoundedWorker→finalized→语义召回/作用域→foreign404→统计消费→删除/Hash消失→Stream0/PEL0。默认跳过，尚未执行；实际settings为gpt-5/text-embedding-3-small，OpenAI/Anthropic凭据均未配置。操作入口见real-model-acceptance.md，P1-06/07保持未勾。

最终全量过程中修正两处既有fixture：502条分页seed改为100条批次（服务硬限制不放宽）；old extraction cleanup新增lease后误连localhost6379，补应用get_redis_conn隔离注入，生产lease不改。分页定向2 passed1 skipped3.31秒；test_search关联201 passed39.48秒。最终完整结果待下一条记录。未提交、未推送。


最终完整命令：`UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x --tb=short`。**544 passed、1 skipped、1 warning，164.65秒（2分44秒）**。跳过项仅为未配置凭据的opt-in真实模型场景；warning为既有Pydantic class-based config弃用。原任务20项明确勾选18项（P0八项/P1-01..05/P2五项）；P1-06/07保持未完成。15份文档本地链接、固定本地pre-commit文本检查、两仓库diffcheck通过，未提交、未推送。


## 2026-10-01 用户要求提交：正式检查与验证边界

用户要求本地git提交，包含两仓库适配、测试及文档。提交前成功下载并运行两个仓库正式pre-commit：Dragonfly的pyflakes/whitespace/EOF/AST/clang-format/Black，AMS的Ruff/format/whitespace/EOF/large-file/typos均通过。修正AMS import排序、CLI维护命令注册移至主入口执行前、readiness测试context组合；CLI模块入口实际help包含新增迁移命令。配套136测试重跑通过1.10秒。

按AMS指南执行 `make test-api`：740 passed、215 failed、40 skipped、24 errors，34.62秒。已见localhost6379不可用、未配置真实模型凭据，以及原mock索引缺少当前schema合同导致storage/prefix拒绝；全部失败尚未逐项归因。不能声称原仓库全量通过，本次为开发候选保存，不代表发布验收。

适配全量重跑发现持久化fixture使用10ms redelivery timeout，繁忙主机上会把刚写SUCCESS但尚未ACK的健康任务再次交付，导致测试probe尝试SUCCESS→RUNNING。改为1秒接管窗口，保留恢复值、SUCCESS/result、PEL/ACK及字节级断言；未放宽生产状态机。最终合并结果随后记录。

AMS本地提交：`7dfc08ff47119727f2bf1461fef75e0f85849458`，`feat: add Dragonfly-compatible memory storage and task recovery`；47文件，8826行新增、843行删除。沿用Dragonfly仓库现有作者身份，仅对本次AMS提交指定，未改全局配置。未推送。

提交前最终兼容全量：`UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x --tb=short`，544 passed、1 skipped、1 warning，164.52秒。真实模型入口仍跳过，既有Pydantic弃用警告保留。此前持久化测试10ms接管竞争已按1秒窗口修复，新全量通过。两个仓库正式pre-commit、文档链接及git diff检查通过；原AMS make test-api未通过的结果与限制仍保留，不外推为上游全测试通过。
