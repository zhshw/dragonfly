# Agent Memory 开发与兼容性合同

[概览](README.md) · [当前状态](current-status.md) · [待办](TODO.md) · [历史证据](implementation-progress.md)

本说明对应当前两个仓库的配对开发候选，需要 Dragonfly 表达式扩展和同级 AMS 适配同时部署。20 项完成 18 项，P1-06/07 的真实模型及端到端验收仍待完成；已完成 P2-03 不代表生产制品或任意数据库拓扑已验收。最新结果以[当前状态](current-status.md)为准。

## 可重复测试环境

Linux、Python 3.12、uv、CMake/Ninja，以及同级 `../agent-memory-server/V0`。依赖按 AMS `uv.lock` 固定为 RedisVL 0.12.1、redis-py 6.4.0、pydocket 0.18.2。审计同时核对九段上游 Docket Lua 和四段 AMS 适配脚本的 SHA；升级依赖先审计再回归。

在 Dragonfly 仓库根目录执行：

```bash
./helio/blaze.sh -DWITH_AWS=OFF -DWITH_GCP=OFF \
  -DWITH_SEARCH=ON -DWITH_COLLECTION_CMDS=ON -DWITH_EXTENSION_CMDS=ON
cd build-dbg
ninja dragonfly filter_expr_test search_family_test
./filter_expr_test
./search_family_test --gtest_filter='*Aggregate*'
cd ..
bash tests/agent_memory/run.sh -q
```

runner 同步锁定依赖、激活 AMS 环境、审计脚本并执行 SDK 合同测试，不自动构建服务器。`AMS_ROOT`、`UV_BIN`、`DRAGONFLY_PATH` 可覆盖路径。每个用例使用隔离随机端口、临时目录、3 proactor 和 2 shard；FLAT/HNSW 确定性测试不调用外部模型。真实模型用例需要显式配置并启用，不以跳过替代验收。

## 表达式与查询

| 函数 | 语义 |
| --- | --- |
| max(a, ...) | 至少一参；数值或数值字符串；null/非数值返回 null，NaN 传播、Inf 保留 |
| pow(base, exponent) | 恰好两参；数值字符串可用；null/非数值返回 null，IEEE 定义域/溢出语义 |
| coalesce(a, ...) | 首个非 null，保留 0 和空字符串；全 null 或无参返回 null |

这些是 Dragonfly 扩展，不能承诺原版 Redis 8 支持同名 max/pow。AMS 已修复 query string、LOAD、PARAMS、距离 alias、SORTBY 和结果解码；SDK 测试直接验数据库分支，异常 fallback 不算通过。recency 时间显式 to_number，缺失按无限年龄、未来年龄为 0，semantic 使用 `1-distance`，half-life 至少 0.001 天且拒绝非有限参数。同分按 ID 升序。

KNN recency 默认固定候选池 1000，可用 `recency_params.candidate_limit` 配置；`offset+limit` 不得超池。只承诺候选池内排序，不是全库最优或快照分页；多读一行判断 next_offset，total 为已观察数量下界。FT.NUMERIC 返回值会归一化数字字符串，条件业务副作用按该字段数值比较，其他完整字段与删除代数保持原查询快照校验。

## Docket 生产部署

AMS 使用应用级 `BoundedDocket` 和 `BoundedDocketWorker`，不修改 site-packages。生产定向允许最终 bounded scheduler：

```text
--lua_undeclared_keys_shas=46dec5a67c168033d38f45742aba3572a9d73be9
```

只对审计过的动态 parked/runs 到期脚本启用 GLOBAL 原子模式，不全局 disable-atomicity。schedule/claim/terminal/cancel 声明完整键集合；到期调度每批最多 64，默认间隔 250ms。所有 AMS producer 在 schedule 原子事务内实施 `ZCARD(queue)+XLEN(stream)<=64`，可将两个上限配置为 1–64。重复稳定键、替换/重排和历史超限不增长行为见[运维](operations.md)。

```bash
source ../agent-memory-server/V0/.venv/bin/activate
python tools/agent_memory/audit_docket.py
```

清单在 `tools/agent_memory/docket-scripts.json`。上游 scheduler SHA `8929ea55fc26d043e4a7d3f010a74baeff87a465` 仅用于原依赖合同与历史基准；其无 LIMIT 测量不能称为最终 bounded worker 吞吐。可选 Concurrency/RateLimit/依赖 Debounce 脚本仍单独列审计清单，没有声明为 AMS 功能验收范围。

## 工作记忆写入与身份

首次写入 version=0，后续 API/MCP PUT 必须携带 GET/PUT 返回 revision，冲突为 409。JSON、revision、incarnation 和 TTL 通过声明键 Lua 原子维护；TTL 为 1–2147483647 整数或 None，None 清除旧 TTL。编码后 JSON 默认上限 1MiB，超限 413 且不改旧内容。错误输入在修改前校验。

工作键为 `working_memory:v2:<namespace>:<user>:<session>`，None 编码 `~`，字符串为 `+` 加 URL 编码，空字符串保留。永久 revision 位于索引前缀外，删除/过期重建创建新 incarnation，客户端不能指定内部身份。PUT 只保留同 revision 下 id/role/content/created_at 均未变的存量抽取标记；新/修改消息重置资格，重复 ID 拒绝，未完成服务器 generation 的消息不能被修改或移除。工作 context 纳入抽取输入和源指纹，未完成发布时自动摘要延后。

认证开启时 namespace/sub 必须非空且不含逗号/首尾空白，入口绑定该 owner；跨 owner 读写/删/任务/摘要被拒绝。MCP HTTP/SSE session 绑定 owner，全部 10 工具及双 owner 并发/重连已有确定性业务验收；STDIO 使用显式服务身份，不使用开发默认值认领租户。历史无 owner/版本数据 fail closed，显式迁移见[租户迁移](tenant-migration.md)。开发 disabled 认证不用于生产。

## 长期记忆与业务任务

长期 Hash 为 `<prefix>:v2:<payload>`，payload 是 `[namespace,user_id,logical_id]` JSON 的 URL-safe Base64，无末尾 `=`；逻辑 ID 保留在 id_，不同 owner 可用同 ID。None/空 namespace/user 在 Hash 中归一化。身份 TAG 必须 CASESENSITIVE，启动检查实际 schema/合同；检索再校验精确 owner。

发布携带服务器原四级删除 generation，并恢复为私有 `_publication_generation`，客户端不能指定。删除墓碑与 Hash 发布、去重/统计、语义合并、结构更新、离散完成及后台 hash 压缩的条件副作用均核对原身份/字段。forget 与后台 hash 压缩使用原 SDK 全业务快照和四级 generation；访问、固定、正文、metadata 或 ABA 替换后安全跳过，无快照不重新认领当前对象。只有实际删除才推进精确 owner 墓碑并计入 deleted_ids。

摘要配置每次保存新 view_generation，结果键带 generation；替换/删除只清旧代。任务另有 task generation 和持久 owner，状态 CAS 不复活过期/替换对象，发布原子检查仍 RUNNING。自定义逻辑 task_id 返回 scoped 实际 ID，客户端用返回 ID 轮询；同身份重复不重新排队，配置不符 409。待执行/运行 task 无 TTL，终态才有界保留。

## 持久恢复与保留

线程/离散/结构批次持久保存模型输出、稳定结果 ID、原源 incarnation/字段和删除代数；逐结果发布收据与 Hash/去重/语义副作用在同事务记录。prepared 只重放原结果，部分成功/响应丢失不重新生成结果；published/finalized 清理核对源身份，保留后续数据。模型执行 lease、所有权续期与发布 fencing 约束活跃重复执行；不能据此承诺外部模型调用 exactly-once。

四类 durable outbox 为 extraction、structure、summary、task_cancel。启动及默认 30 秒周期 reconciliation 复用稳定 task key、原 generation，坏记录诊断保留并隔离；`agent-memory reconcile-extractions` 可手动恢复相关派发。取消持久 intent，原子更新 metadata 并清 Stream/PEL；失败、接管、终态重投与源过期/重建拒绝见历史测试证据。legacy summary migration receipt 另走显式迁移恢复协议。

revision、删除 generation、业务收据和迁移 receipt 当前永久保留，不按会话 TTL 清除，也没有自动安全 GC。Docket 硬队列上限不约束这些元数据或 durable outbox 总量；容量告警与预算见[运维](operations.md)。

## 迁移、维护与恢复

停 API/MCP/worker 及其他写入，确认执行请求排空后统一升级，不能混用旧无版本载荷与新 worker。旧任务不得添加当前 generation 冒充原版本；从审核后的当前源创建新工作。scoped 迁移、冲突归档及历史摘要任务重算见[租户迁移](tenant-migration.md)和[接续工作](handoff.md)；schema/model/version 迁移、重新 embedding、原字节 CAS 和 resume/rollback 见[模型迁移](embedding-migration.md)。

重建入口需要停写声明：

```bash
agent-memory rebuild-index --writers-stopped
agent-memory recover-index-rebuild --owner OWNER_TOKEN --writers-stopped
```

锁/journal 永久保存原/目标合同与 owner，中断恢复保留 Hash/向量；未知 owner/合同不删锁或补当前身份。provider 拒绝维护中的新入口，但不能排空已执行请求。模型/维度变化不能用普通 rebuild 代替重新 embedding。原生 SAVE 恢复与单副本同步后显式提升的已验收范围见[持久化](persistence.md)；发布和配对回滚见[发布兼容性](release-compatibility.md)。
