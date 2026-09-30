# 历史摘要与任务的租户迁移

本流程面向单实例、多 shard 的维护窗口。生产认证模式下，无 owner 的旧 SummaryView 不再被周期任务执行，无 generation 的旧摘要任务也不能绑定当前配置；API 的摘要与任务查询继续拒绝没有明确所有者的历史记录。未知所有者的数据保留在原位置，等待人工确认，不自动归属给当前登录用户。

## 操作合同

迁移入口在同级 AMS：`agent_memory_server/tenant_migration.py`，维护命令在 `tenant_migration_cli.py`。先停止写入与 worker，核对实际 Redis 连接，并准备以下已审核信息：

- 原 `view_id`、明确的 namespace/user_id，以及原配置 GET 返回字节的 SHA-256。
- 若需要接续历史 task：原 task_id，以及 `task:<task_id>` GET 返回字节的 SHA-256。
- 配置必须已有匹配的 owner 字段，或历史 filters 必须分别包含精确匹配的 namespace/user_id；缺失、`any`、模糊筛选和矛盾 owner 都拒绝迁移。任务必须指向该配置且没有相反的 owner。

不要用 `redis-cli --raw GET ... | sha256sum` 直接计算批准指纹；该命令添加的换行不属于原值。可在 AMS `.venv` 中通过 `source_digest(await redis.get(key))` 计算原值指纹。指纹必须对应维护人员审核过的原数据。

```bash
agent-memory migrate-legacy-summary VIEW_ID \
  --namespace NAMESPACE --user-id USER_ID \
  --expected-view-sha256 VIEW_SHA256 \
  --legacy-task-id TASK_ID --expected-task-sha256 TASK_SHA256 \
  --maintenance-window
```

历史 task 参数可以成对省略；此时仍会创建新的摘要重算任务。命令返回迁移 receipt key、状态，以及新 view/task ID。

## 原状态、发布与重算

迁移先持久保存 `prepared` receipt，包含原配置、task、所有被迁移摘要结果的完整 JSON、观察到的 TTL，以及固定的新配置/任务 generation。每个归档限制为配置的 `working_memory_max_bytes`，最多包含 1000 个摘要分区；超过限制会拒绝，原数据不变。

随后同一 Lua 原子操作检查全部原值和目标冲突，发布新的 scoped view/task，移除旧配置、任务、摘要与旧索引项，并将 receipt 改为 `pending`。源改变或过期、目标已经存在，都不会部分发布或覆盖已有数据。原状态仍保留在 receipt 中，供有 Redis 管理权限的维护人员查看；普通租户 API 不会提供归档读取接口。

旧摘要文本不会被直接声明为新所有者的结果。新的版本化 task 从该 owner 的实际长期记忆重新计算，输出进入新 view/generation 的结果键。旧队列载荷不添加当前 generation，也不改写其 owner。新 task 在待消费期间不设置元数据 TTL。

派发使用固定 receipt key 作为 Docket key；重复命令、启动/周期 reconciliation 和人工恢复共享该身份。已在 scheduled/queued/running 的任务不会重复排队。提交后响应丢失或派发失败，可以使用相同批准指纹重复执行命令，或运行：

```bash
agent-memory reconcile-legacy-summary-migrations
```

业务 task 成功后 receipt 为 `finalized`，失败后为 `failed`，显式取消后为 `cancelled`；目标被删除、配置或 task incarnation 被替换后为 `superseded`。这些终态不会因重复迁移请求复活源或目标。坏 receipt 保留诊断并跳过，不阻塞其他 owner。原配置和结果的恢复不得直接通过修改 receipt 状态来完成；如需人工恢复，应另行审核数据、TTL 和当前目标状态。

## 验收与范围

真实数据库回归位于 `tests/agent_memory/test_tenant_migration.py`，覆盖无 owner/无版本生产拒绝、未知/宽泛/相反 owner、源指纹/源内容/过期校验、目标冲突、提交响应丢失、入队失败恢复、真实 worker scoped 重算、归档原字节、终态重复请求与删除不复活。与全部 10 个 MCP 工具在 HTTP/SSE/STDIO 的双 owner、并发和重连测试共同验收租户隔离。

这份迁移针对 SummaryView 配置和关联 AMS 摘要 task。历史结构、离散抽取与延迟抽取载荷没有源版本时，现有业务入口拒绝执行；重新提交必须从审核后的当前源创建新的工作，不能把旧模型输出或旧任务直接补成当前版本。generation 墓碑、业务收据和迁移归档的保留与回收属于独立运维任务；本流程不自动删除这些保护记录。

遗留 `summarize_session` 使用的独立 List/Hash 协议没有版本化任务生产者，在生产认证模式下无条件拒绝执行，即使旧载荷附带 namespace/user_id 也不能认领当前源。维护人员应审核并迁移为当前 WorkingMemory JSON 后创建新任务；保留的旧注册名仅使旧队列载荷明确失败、ACK 并留下原数据，不能执行旧模型/写入路径。开发认证关闭时的旧协议仅用于显式兼容测试。
