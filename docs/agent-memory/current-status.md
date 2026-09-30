# Agent Memory 当前状态

[概览](README.md) · [待办](TODO.md) · [接续工作](handoff.md) · [历史证据](implementation-progress.md)

更新：2026-10-01。结论：Dragonfly 单实例多 shard 能支持本项目，已完成独立 AMS 应用适配的原定 18/20 项。当前是配对开发候选；真实模型与最终端到端验收未完成。

## 任务状态

已完成 P0-01..08、P1-01..05、P2-01..05；未完成 P1-06、P1-07。确定性业务证据不能替代真实服务验收。

| 范围 | 已验证 | 边界 |
| --- | --- | --- |
| 命令与检索 | max/pow/coalesce、RedisVL FLAT/HNSW、recency评分及分页 | 适配版本，非任意官方版本组合 |
| 工作/长期记忆 | 原子JSON+TTL、revision/incarnation、scoped键、四级generation、原查询快照CAS、去重与访问统计 | generation/receipt永久保留 |
| API/MCP身份 | 全部10工具、HTTP/SSE/STDIO、双owner并发重连、真实worker链路、历史摘要与任务fail-closed/显式迁移 | STDIO要求显式服务凭据 |
| 抽取/恢复 | 持久prepared、逐Hash收据、semantic计划、模型执行lease/fencing、多阶段SIGKILL接管、取消/retry/PEL/ACK | terminal FAILED thread需显式原载荷重投；asyncio不承诺跨进程恢复 |
| schema/模型迁移 | 全覆盖重新embedding、原字节CAS、resume/rollback、旧无合同显式认领、空索引中断恢复 | 维护须停写并排空；真实模型质量待验 |
| 持久化 | 原生SAVE恢复、同步单副本显式提升，恢复检索与任务处理 | 未声明自动failover、未同步零丢失或Cluster |
| 性能/运维 | 9600前台操作、批次测量、故障窗口告警、JSON/向量限制、所有AMS Docket生产者原子准入64、scheduler每轮64 | Debug共享主机测量；producer→Stream非worker执行吞吐 |

## 最新验证

| 验证 | 结果 |
| --- | --- |
| 完整兼容套件 | 544 passed、1 skipped、1 warning，164.52秒（2分44秒） |
| AMS CLI/迁移/启动/依赖/遗忘 | 提交前重跑136 passed，1.10秒 |
| AMS原仓库make test-api | 740 passed、215 failed、40 skipped、24 errors，34.62秒；未通过 |
| 历史租户/摘要隔离与恢复 | 65 passed，26.58秒；后续20 passed，11.79秒 |
| 迁移/合同/重建 | 65 passed，18.83秒；迁移CLI16 passed，0.06秒 |
| 模型lease及源快照副作用 | 39 passed，36.67秒；后续16 passed，6.22秒 |
| bounded队列与worker | 新19 passed，3.71秒；旧worker关联55 passed，72.74秒 |
| 原快照遗忘/压缩 | 60 passed，11.77秒；AMS遗忘8 passed |
| 运维与任务派发/取消 | 运维11 passed，2.82秒；派发/取消5 passed，2.26秒 |
| 原生快照与复制提升 | 3 passed，17.12秒 |
| C++ Debug/Release历史证据 | 表达式各83、聚合65 passed；本轮无C++修改 |

专项结果存在重叠，不相加作为场景总数。最终全量前修正两处旧fixture：502条分页seed分成100条批次；新增执行lease的旧测试补齐隔离Redis注入。服务限制和生产租约均未放宽。完整套件现已通过；跳过项仅为真实模型入口，warning为既有Pydantic配置弃用。

提交前执行了原仓库 `make test-api`，没有全量通过。已确认部分失败为localhost6379不可用、无真实模型凭据，以及旧mock索引缺少当前schema合同；例如 `test_add_memories_with_mock_index` 被storage/prefix校验拒绝。未逐项归因全部215失败/24错误，不能全部视为环境问题。适配兼容套件通过不代表上游全量回归通过；正式发布仍需收口这些结果。

当前配置为 `gpt-5` / `text-embedding-3-small`，已确认 OpenAI/Anthropic settings 凭据均未配置。真实服务入口见[真实模型验收](real-model-acceptance.md)，默认套件跳过。需要实际生成和embedding服务配置，才能完成最后两项。

## 恢复与保留规则

线程抽取模型最多自动尝试三次；发布失败保留prepared/收据/pending，Docket终态FAILED后以原身份/载荷显式重投，reconciliation不自动扫描此类failed thread pending。pending与active AMS task无TTL，terminal task七天TTL；generation/receipt永久保留以拒绝晚到任务复活。安全GC是未来容量优化。

SIGKILL为实际进程验收；SIGTERM/INT只有关闭代码路径，未独立做信号验收。运维容量与告警合同见[运维说明](operations.md)，维护及回滚见对应指南。

## 工作区

Dragonfly分支 `feat/agent-memory`；同级AMS分支 `feat/dragonfly-agent-memory`。本次源码、测试与文档按配对开发候选本地提交，未推送；配对AMS提交为 `7dfc08ff47119727f2bf1461fef75e0f85849458`；Dragonfly为包含本文的开发候选提交。
