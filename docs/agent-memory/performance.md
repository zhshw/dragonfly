# Agent Memory 性能评估

[当前状态](current-status.md) · [运维](operations.md) · [原始结果](../../tools/agent_memory/benchmark-results.json)

2026-10-01：P2-04 固定配置后端评估完成。测试工具为 `tools/agent_memory/benchmark.py`，原始结果保留每轮样本、启动参数、二进制 SHA、版本、CPU/load 和内存。

## 测量合同

本机 Debug 二进制，单实例、2 shard、3 proactor；RedisVL 0.12.1 / redis-py 6.4.0 / Docket 0.18.2。1000×1536d FLAT cosine 向量，实际 AMS 1KiB JSON revision CAS；向量和 JSON 各4并发，每轮各400次、3轮、4场景，共9600次前台操作。不包含 HTTP、真实模型、生产 worker 或生产容量。共享主机负载会影响尾延迟。

baseline、声明键原子脚本、GLOBAL 原子同脚本、真实 Docket scheduler 交替运行。非 baseline 固定64次后台调用；声明键和 GLOBAL 使用相同64 keys×8循环工作量，原子性保持。声明键每轮前台期间完成2/7/4次，GLOBAL为64/64/64，完整记录调度重叠而不假装干扰强度相同。

## 前台结果

QPS 为三轮算术平均，P95/P99 为三轮区间，单位毫秒。

| 场景 | 向量 QPS | 向量 P95 | 向量 P99 | JSON QPS | JSON P95 | JSON P99 |
| --- | ---: | --- | --- | ---: | --- | --- |
| baseline | 166.17 | 35.31–36.26 | 41.99–46.86 | 159.78 | 64.43–79.56 | 71.89–185.14 |
| declared_atomic | 166.44 | 37.01–46.13 | 41.13–54.84 | 156.06 | 91.70–134.09 | 149.56–171.26 |
| global_atomic | 132.62 | 38.97–50.72 | 45.56–82.14 | 127.80 | 71.45–102.82 | 110.16–141.95 |
| docket_global_scheduler | 157.04 | 23.20–53.39 | 42.76–226.18 | 133.05 | 66.37–107.51 | 95.13–156.90 |

GLOBAL 相对 baseline 向量与 JSON 吞吐均下降约20%。共享主机噪声和锁调度差异不支持“每种尾延迟均更差”的结论；Docket 向量 P99 最高226ms也需要复测确认。

## Docket 批次与初始容量

使用真实 SDK producer 序列化、到期 scheduler→Stream；每批仅3轮，调度最大耗时作为观察值，不把3点插值 P99 当稳健尾延迟。

| 每批任务 | producer tasks/s | producer→Stream tasks/s | scheduler 最大 ms |
| ---: | ---: | ---: | ---: |
| 1 | 1159 | 864 | 0.324 |
| 16 | 1955 | 1600 | 1.896 |
| 64 | 2583 | 2070 | 6.713 |
| 256 | 2558 | 2075 | 23.866 |

64→256 的 producer→Stream 吞吐仅增加 0.26%，调度最大耗时却增加约 3.56 倍。这里测量的是原 pydocket 0.18.2 无 LIMIT scheduler，SHA `8929ea55fc26d043e4a7d3f010a74baeff87a465`；producer tasks/s 为生产者提交吞吐，producer→Stream 也不包含 worker 执行、模型调用或业务完成，不能称为最终 worker 吞吐。

P2-03 已按此依据完成最终适配：生产 bounded scheduler SHA `46dec5a67c168033d38f45742aba3572a9d73be9`，每次最多搬运 64、默认每 250ms 调度；所有 AMS producer 的 queue+Stream 原子准入上限默认 64。默认调度频率带来的等待及最终 adapter 的端到端吞吐没有被本历史基准测量覆盖，需在目标 Release 环境复测。不能将表中 256 任务/批当作当前生产支持值。

候选首轮容量：≤1000 vectors、1536维、每session data≤1KiB、RSS预算≥256MiB、Docket queue+Stream 原子容量默认≤64（同时含待消费及活跃/PEL）。观察 used_memory 20.49MiB、RSS131.74MiB；这些是本次固定配置的预算，不是大规模容量保证。四类 outbox 与永久 generation/业务收据在此短时测试中未模拟长期累积，需另纳入存储预算，见[运维](operations.md)。候选复测门槛：vector P99>250ms、JSON P99>200ms 或单次 scheduler>10ms时触发复测。

## 复现

```bash
source ../agent-memory-server/V0/.venv/bin/activate
LITELLM_LOCAL_MODEL_COST_MAP=True HF_HUB_OFFLINE=1 python tools/agent_memory/benchmark.py \
  --output tools/agent_memory/benchmark-results.json \
  --vectors 1000 --dimensions 1536 --samples 100 --repeats 3
```

该命令在 Dragonfly 根目录运行，使用隔离实例并清理；实测数据已经保存，无需每轮重复运行。
