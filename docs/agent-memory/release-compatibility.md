# Agent Memory 发布兼容性与回滚

[当前状态](current-status.md) · [运行合同](compatibility.md) · [待办](TODO.md)

更新：2026-10-01。本文件完成 P2-05 的交付说明；当前是开发候选，20 项完成 18 项，P1-06/07 的真实模型与端到端验收仍待完成。数据库与 AMS 必须作为同一适配版本部署。

## 已验证的版本与范围

| 组件 | 当前验证对象 |
| --- | --- |
| Dragonfly | 本仓库包含本文的开发候选提交，基线 `ebcfe4c9`；二进制版本为 `dev-0000000`，不能当作官方发行版 |
| AMS | 同级仓库 `7dfc08ff47119727f2bf1461fef75e0f85849458`，V0 package version `0.15.2` |
| Python | 3.12；AMS 要求 `>=3.12,<3.13` |
| RedisVL / redis-py / Docket | 锁定 `0.12.1` / `6.4.0` / `0.18.2`，以 V0 `uv.lock` 为依赖合同 |
| 数据库拓扑 | Linux，单实例、2 个内部 shard、3 个 proactor threads；FLAT/HNSW |
| 模型 | 确定性三维 embedding/模型桩；真实模型接入尚未验收 |

本机 Debug 二进制 SHA256：`13492d9728b3836d06509b7ab07388b49973ac57a7549e93f640fd4989ff4d4f`。该值仅标识当前测试二进制。两个仓库已按开发候选本地版本化，复现使用配对AMS提交与包含本文的Dragonfly提交，不能只用原基线；正式交付必须记录两个仓库的完整提交、依赖锁文件 hash、二进制 hash 和构建参数，重新跑验收后生成不可变制品。没有已声明支持的官方 Dragonfly/原版 AMS 组合。

## 构建和启动合同

在 Dragonfly 根目录执行：

```bash
./helio/blaze.sh -DWITH_AWS=OFF -DWITH_GCP=OFF \
  -DWITH_SEARCH=ON -DWITH_COLLECTION_CMDS=ON -DWITH_EXTENSION_CMDS=ON
cd build-dbg
ninja dragonfly filter_expr_test search_family_test
./filter_expr_test
./search_family_test --gtest_filter='*Aggregate*'
```

Release 使用 `./helio/blaze.sh -release` 加相同功能选项，并在 `build-opt` 使用 Ninja；Debug 的结果不能替代目标 Release 制品验收。JSON、Hash、搜索扩展和新增 max/pow/coalesce 都属于本适配合同。

测试拓扑启动示例（临时无持久化实例）：

```bash
build-dbg/dragonfly --bind=127.0.0.1 --port=6380 \
  --force_epoll=true --proactor_threads=3 --num_shards=2 \
  --dbfilename= --default_lua_flags= \
  --lua_allow_undeclared_auto_correct=false \
  --lua_undeclared_keys_shas=46dec5a67c168033d38f45742aba3572a9d73be9
```

`--dbfilename=` 是测试清理配置；P2-02 已验收原生 SAVE 恢复与单副本同步后显式提升，正式部署需按[持久化指南](persistence.md)另配。不得把临时拓扑当作持久化部署模板。Docket 仅定向允许已审计 bounded scheduler SHA，保留 GLOBAL Lua 原子性。所有 AMS producer 的 queue+Stream 原子上限默认 64；scheduler 每批最多 64、默认每 250ms 一次。上游无 LIMIT scheduler SHA `8929ea55fc26d043e4a7d3f010a74baeff87a465` 只保留为原依赖合同和历史基准说明；升级 Docket 后重新运行 `tools/agent_memory/audit_docket.py` 并完成 worker 回归。

AMS 在 V0 激活 `.venv`，设置 `REDIS_URL`、认证和模型配置；默认 auth_mode 为 disabled，正式部署必须显式配置已验收的认证模式。启动入口：

```bash
agent-memory api --task-backend docket
agent-memory mcp --mode streamable-http --task-backend docket
agent-memory task-worker
```

API/MCP/worker 必须使用相同数据库、Docket 名、schema/model/version 配置。启动检查失败时修复或恢复，不得把旧向量登记成新模型以绕过检查。生产密钥通过部署环境提供，不写入本文或制品。

## 验收证据与发布门槛

- 最新阶段 SDK 全量及对应命令：见[当前状态](current-status.md)，不以先前批次结果证明最新代码。
- 先前 Debug/Release 表达式各 83 passed，聚合 65 passed；本轮未重编译 C++。
- API/MCP HTTP/SSE→工作记忆→真实 Docket worker→召回→删除已有真实数据证据；STDIO全部工具及双owner业务链已验收；历史任务隔离、删除竞争、派发/接管/取消恢复与 bounded queue 已验收；真实模型及其完整端到端链路仍待验收。
- 已验证重建保留 Hash/向量、失败回滚、三个阶段 SIGKILL 恢复；重新embedding、已知旧模型无合同认领、空无合同索引SIGKILL恢复与回滚已验收，见embedding-migration.md。

发布前必须完成剩余 P1-06/07 的真实模型和完整入口链路，收口原AMS `make test-api` 的失败，并验证正式配对 Release 制品。P2-01..05 已完成选定范围，容量及观测见[运维指南](operations.md)。四类 outbox（extraction、structure、summary、task_cancel）与永久 revision/generation/业务收据都属于配对版本协议；保护记录没有自动安全 GC，不能按会话 TTL 删除。本文完成不代表生产上线门槛已通过。

## 维护与回滚

1. 停 API、MCP、worker 和其他写入/维护进程，确认在执行请求已结束。当前 provider 锁只拒绝新入口，不能排空已开始的请求。
2. 保存制品/config/合同/journal 信息；数据备份与恢复必须采用 P2-02 已验证路径。选定原生SAVE与单副本显式提升已验收；不承诺自动failover/未同步零丢失。
3. 同模型/版本/布局的索引定义重建使用 `agent-memory rebuild-index --writers-stopped`。中断后读取 `memory_index_rebuild_journal:<index>` 的 owner，执行 `agent-memory recover-index-rebuild --owner TOKEN --writers-stopped`，恢复原定义/合同并核对召回。
4. 恢复再次失败时 journal 保留且 owner 更新；读取新 owner 重试。合同/owner 与预期不一致时停止维护并调查，不删除锁或改写合同。
5. 应用回滚只允许切回理解当前 scoped v2、revision/incarnation、generation、批次收据和 outbox 协议的已验收配对制品。切回原版 AMS 不具备该保证；禁止直接改 Redis URL 让原版消费现有任务。
6. 模型/维度变化不能使用 rebuild-index；使用已验收的重新embedding迁移/回滚命令及停止排空窗口，见embedding-migration.md。历史无版本任务不得补当前 generation 直接重投。
7. 用原模型/schema 配置先做启动检查和定向召回/读删，再启 worker、观察 outbox/任务/PEL 收敛，最后开放业务流量。

回滚后的核对包含作用域授权、Hash/JSON 内容、TTL、向量召回、任务状态、延迟队列、结果和 consumer group/PEL。仅检查进程存活或 HTTP 200 不算验收。

## 多实例与 Redis Cluster 独立验证计划

不沿用单实例结论。开始前固定拓扑、版本与故障模型，逐项提供真实数据证据：

- 主从复制/切换和索引重建：覆盖 JSON/Hash、TTL、索引、任务结果及待执行任务。
- 对全部实际 Lua KEYS、动态 scheduler keys、scope key 编码做 slot 审计；多键 GLOBAL 脚本不能直接当作 Cluster 事务能力。
- 跨 slot 的工作记忆 CAS、发布/去重/结构 outbox、generation、批次收据与恢复必须定义分片共置或替代协议后再测试。
- 模拟网络分区、重复消费、leader 切换、worker 接管与取消；核对 Stream/PEL、结果和业务数据收敛。
- 在固定数据规模下比较吞吐与前台 P95/P99，并制定每批容量、积压和故障告警阈值。

上述拓扑目前未声明支持；单实例 P2-02/03/04 证据不能外推为 Cluster 的恢复、监控或性能验收。
