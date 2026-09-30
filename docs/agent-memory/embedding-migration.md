# Embedding 合同与迁移

[当前状态](current-status.md) · [发布与回滚](release-compatibility.md)

P2-01 定义覆盖 schema/model/version 启动检查、保留数据的索引重建、模型/维度重新 embedding 及显式中断恢复。AMS 自有迁移实现位于 `agent_memory_server/utils/embedding_migration.py`。

## 维护前提

先停止 API、MCP、worker、直接 Redis 写入及其他维护进程，并排空已进入的请求。所有迁移命令要求 `--writers-stopped`；该选项是操作者停写声明，不是自动流量排空。无 TTL 的 `memory_index_rebuild_lock:<index>` 阻止新 provider 入口；已缓存 provider 也检查持久合同，迁移后不能继续使用旧模型/版本写向量，必须重启服务。

备份配对制品、配置和已验收快照；保留 migration receipt/manifest 直到确认回滚窗口结束。保存内容包括原正文和向量字节，不能放入公共日志或未经授权导出。CLI 仅输出状态/数量，不输出正文或向量。

## 已有合同数据切换

1. 目标配置使用新模型/版本/schema，index name、prefix、storage type保持。每批最多100个唯一 scoped v2 Hash，使用不同 preparation ID；全迁移最多10000条、维护CAS载荷最多64MiB。prepare保存全Hash原字节、四项删除generation、源/目标合同和新向量。模型错误、非有限值、维度错误和超限在发布前拒绝。
2. 批次必须覆盖旧索引全部Hash。apply校验全字段/generation及源合同，原子替换向量，重建目标索引，发布目标合同并核对实际定义。不能将部分准备静默视为全索引迁移。
3. 相同批次prepare重试沿用保存向量；apply中断重试不再次调用模型。各阶段保留durable manifest/owner；进程SIGKILL后必须显式指定预期owner接管，不能删除锁绕过检查。

```bash
# 在 AMS V0 激活 .venv，设置目标模型/schema配置
agent-memory prepare-embedding-migration --migration-id prep-001 \
  --key EXACT_SCOPED_V2_HASH --writers-stopped
# 多批次：--preparation-id 可重复，每批prep ID不同
agent-memory apply-embedding-migration --migration-id switch-001 \
  --preparation-id prep-001 --writers-stopped
# 中断后，从embedding_migration_manifest:<index>:sha256(migration_id)读取owner
agent-memory apply-embedding-migration --migration-id switch-001 \
  --preparation-id prep-001 --expected-owner OWNER --writers-stopped
agent-memory rollback-embedding-migration --migration-id switch-001 \
  --expected-owner OWNER --writers-stopped
```

rollback恢复原向量、原索引定义和原合同；源Hash变化/删除、外部合同变化或owner不匹配均拒绝盲目覆盖。恢复失败保留新的owner与manifest，读取后显式重试。模型同维度只改名称或版本同样属于迁移，不允许以布局相同绕过合同。

## 无合同旧数据

先完成 scoped v2 Hash 迁移。历史模型来源必须由操作者明确核对：不知道原模型/版本时保留拒绝状态，不从向量推测、也不直接登记成目标模型。

`adopt-embedding-contract`要求显式源模型/版本；如果物理旧schema与当前配置不同，提供完整真实 RedisVL schema JSON。核对旧维度/datatype、TAG CASE敏感性、算法、距离、prefix等，错误schema在claim前拒绝。

```bash
agent-memory adopt-embedding-contract --migration-id known-source-001 \
  --source-model VERIFIED_OLD_MODEL --source-model-version VERIFIED_OLD_VERSION \
  --source-schema-file VERIFIED_OLD_SCHEMA.json --writers-stopped
```

认领逐条检查精确scoped键、向量布局/finite、持久TTL与全字段CAS。历史CASE=false只允许在停止业务的维护窗口精确读Hash；新目标schema必须CASE=true，再按上述prepare/apply迁移。原定义rollback可精确恢复，恢复后仍需迁移到可对外服务的schema才开放流量。

无合同空索引重建中断使用 `recover-index-rebuild --owner OWNER --writers-stopped`；只有prefix下确无Hash且索引num_docs=0才可完成新合同。有旧Hash时拒绝，不将其解释为目标模型。

## 验证与边界

真实单实例2shard定向65项通过18.83秒，见 `tests/agent_memory/test_embedding_migration.py`、索引合同及重建测试。覆盖维度/模型/版本迁移、全覆盖拒绝、超限、全字段竞争/删除、三阶段SIGKILL各resume/rollback、旧CASE=false认领/迁移/回滚、缓存旧provider拒绝写、无合同空索引drop/create/contract三阶段SIGKILL恢复。CLI另验停写前不创建provider、错误非零退出和不泄露保存内容。

这些是确定性embedding的存储/恢复协议验收；真实模型质量由P1-06单独验收。维护窗口由操作部署负责；无自动排空、自动failover或Cluster迁移保证。
