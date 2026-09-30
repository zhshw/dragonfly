# Dragonfly Agent Memory

目标是以 Dragonfly 为存储与检索底座，适配同级 `agent-memory-server/V0` 的独立记忆服务。模型调用、抽取、摘要和遗忘策略保留在应用层。首版范围为单个 Dragonfly 实例，允许多个内部 shard。

## 文档入口

| 文档 | 用途 |
| --- | --- |
| [AMS 架构](ams-architecture.md) | 当前模块分工、进程拓扑、数据流、存储与恢复机制；区分原设计和本次适配 |
| [当前状态](current-status.md) | 已实现范围、最新验证与剩余限制 |
| [接续工作](handoff.md) | 当前工作区、下一步操作与待重跑验证 |
| [待办清单](TODO.md) | 开发顺序、改动归属和验收条件；接续工作从这里开始 |
| [历史租户迁移](tenant-migration.md) | 旧摘要与任务归档、owner证明与版本化重算 |
| [模型与索引迁移](embedding-migration.md) | schema/model/version、重新embedding、中断恢复与回滚 |
| [持久化验收](persistence.md) | 原生快照、主从提升、实际任务继续执行及边界 |
| [性能评估](performance.md) | 固定规模 QPS/P95/P99、GLOBAL 影响与候选容量 |
| [运维说明](operations.md) | 指标、故障告警、容量配置与保留规则 |
| [发布与回滚](release-compatibility.md) | 版本范围、构建/启动合同、发布门槛、回滚和 Cluster 验证计划 |
| [运行方式](compatibility.md) | 开发环境、SDK 合同、Docket 配置与工作记忆版本协议 |
| [真实模型验收](real-model-acceptance.md) | 现有服务配置与最终验收命令 |
| [实施记录](implementation-progress.md) | 已实现改动、测试证据与剩余限制 |
| [实施报告](implementation-report.md) | 架构、数据结构、能力矩阵和实施方案 |
| [依赖核查附录](dependency-audit.md) | 锁定 SDK 的命令合同、Docket Lua 风险和最小运行复现 |

## 当前状态

截至 2026-10-01，原定 20 项已完成 18 项：P0 八项、P1-01..05、P2 五项。剩余 P1-06 的真实生成/embedding 服务验收，以及依赖它的 P1-07 最终端到端验收。当前是配对开发候选；原仓库全量回归尚未通过，验证边界见当前状态。

- Dragonfly 新增 max/pow/coalesce；先前 Debug/Release 各 83 项表达式单测通过，本轮未重复构建。
- AMS recency、FLAT/HNSW、工作/长期记忆及 API/MCP 全工具隔离已验证。
- 持久发布收据、原快照 CAS、模型执行租约和真实 worker 中断/重投已验证。
- schema/model 迁移与回滚、快照/复制恢复、容量硬限制、运维指标和性能测量已交付。

最新验证结果见[当前状态](current-status.md)。当前改动与验证命令见[实施记录](implementation-progress.md)和[运行方式](compatibility.md)。原始调研基线与失败复现保留在实施报告/依赖附录中，不能当作当前实现状态。

## 工作约定

以 [TODO](TODO.md) 作为后续进度入口。完成任务时记录版本、复现命令、结果和限制，再勾选对应项；实施报告保留总体设计，依赖附录保留核查证据。已有最小复现通过不能替代完整业务验收。

`AMS/` 表示仓库根目录同级的 `../agent-memory-server/`，`AMS-S/` 表示其 `V0/agent_memory_server/`，`DF/` 表示 Dragonfly 仓库根目录；这些源码路径与行号按报告基线解释，不相对于本文档目录。
