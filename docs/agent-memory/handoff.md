# Agent Memory 接续工作

[概览](README.md) · [任务定义](TODO.md) · [当前状态](current-status.md) · [运行方式](compatibility.md)

截至 2026-10-01，原 20 项完成 18 项。Dragonfly 分支 `feat/agent-memory`；同级 `../agent-memory-server/V0` 分支 `feat/dragonfly-agent-memory`。本次实现、测试和文档按配对开发候选本地提交，未推送；配对版本见当前状态。

剩余 P1-06 真实生成/embedding 服务验收与 P1-07 最终端到端验收。确定性 API/MCP 全工具、三传输、真实 worker 业务及隔离已通过；当前没有可用模型凭据，真实服务场景默认跳过。配置现有服务后按[真实模型验收](real-model-acceptance.md)执行，再复核 P0/P1 原验收条件；不要重开已完成项或以模型桩代替真实验收。

恢复协议包括工作记忆 revision/incarnation、四级删除 generation、原查询快照 CAS、持久 prepared 模型输出和逐记录收据、稳定 Docket key、执行 lease/owner fencing、结构/摘要/取消 outbox。启动/周期 reconciliation 恢复可重派 intent；真实 SIGKILL 接管覆盖多个发布阶段，恢复后核对结果、AMS task、Stream/PEL/ACK。SIGTERM/INT 有关闭代码路径，尚无独立进程信号验收。asyncio 进程内模式不承诺跨进程恢复。

线程抽取模型最多自动尝试三次；发布失败保存 prepared/逐记录收据/pending，Docket 终态 FAILED 后需以原身份和载荷显式重投。reconciliation 不自动扫描此类失败 thread pending，不能承诺无限自动重试。generation/receipt 永久保留；pending 与 active AMS task 无 TTL，terminal task 七天 TTL。删除、过期及 incarnation 替换使旧源退休。

索引/模型维护须先停 API/MCP/worker 并排空已开始请求；owner gate 只拒绝新入口。schema/model/version 合同、重新 embedding prepare/apply/resume/rollback/adopt 和原字节恢复见[迁移指南](embedding-migration.md)。未知旧模型不得猜测认领；历史无版本任务不得补当前 generation 直接重投，按[租户迁移](tenant-migration.md)归档审核后重算。

Dragonfly 根目录复用已构建二进制执行兼容套件：

```bash
UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x --tb=short
```

`UV_BIN` 为本机路径，其他环境使用已安装 uv。AMS 命令先在 V0 激活 `.venv`。最新完整套件544 passed、1 skipped、1 warning，164.52秒；跳过项为真实模型验收，详见 current-status。先前 C++ Debug/Release 表达式各 83、聚合 65 已通过，本轮没有 C++ 修改，不重复构建。

此前本地格式检查使用 `/tmp/agent-memory-format-hooks.yaml` 的固定等效 hooks，原远程 hooks 曾下载失败；提交前已成功运行两个仓库的正式 pre-commit 配置。后续变更按代码归属运行相关单测/格式检查；更新 current-status 与 implementation-progress，满足整项验收后再勾 TODO。未经用户要求不提交或推送。

原仓库 `make test-api` 本次740 passed、215 failed、40 skipped、24 errors，未全量通过。数据库/凭据缺失与旧mock合同不匹配均有证据，完整失败归因与上游回归仍须收口；不能把适配套件结果外推为全部原测试通过。
