# 持久化与复制验收

[当前状态](current-status.md) · [发布与回滚](release-compatibility.md) · [测试](../../tests/agent_memory/test_persistence.py)

2026-10-01，P2-02 选定以下真实恢复路径完成验收：

| 路径 | 实际过程 |
| --- | --- |
| Dragonfly 原生 DF 快照 | SAVE → SIGKILL → 同数据目录启动自动载入 |
| 主从全量与增量同步 | 先完整业务数据，再建立复制；修改 JSON/Hash/TTL → 等待 offset 同步 → 杀主 → REPLICAOF NO ONE |
| 全部业务命令增量同步 | 先建立复制，再写所有 AMS/SDK/Docket 数据 → 等待 offset 同步 → 杀主并提升 |

每项均检查非空原始数据及恢复值：完整 Hash 字段含向量字节、JSON 文档、索引合同、TTL 绝对截止、AMS task 元数据、已完成任务的实际 result、延迟任务的 parked 载荷与 ZSET score、Stream entries、consumer group last ID/pending、consumer 名/pending，以及 PEL 的 ID/owner/delivery count。

恢复后使用实际 RedisVL JSON 过滤和向量查询核对索引；读取原完成结果，启动真实 Docket worker，通过 XAUTOCLAIM 接管原 PEL、处理延迟任务。核对三条任务 SUCCESS/result，以及 ACK 后 PEL/Stream/ZSET/parked 清理。短 TTL 到期、长 TTL 剩余，TTL 不因恢复重新起算。

独立定向：3 passed，17.12 秒。主 agent 合并长期记忆业务验收重跑：5 passed，394 deselected，17.58 秒。命令：

```bash
UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv bash tests/agent_memory/run.sh -q -x \
  -k 'restores_ams_and_docket'
```

版本/构建对应[发布说明](release-compatibility.md)。这只证明本机单实例两 shard、单副本、显式手动提升和 Dragonfly 原生 DF 快照。杀主前确认复制 offset 同步，不承诺尚未同步写入零丢失；未验收自动故障转移、Redis Cluster、云快照或 RDB。部署应选择已验收路径，其他拓扑另测。
