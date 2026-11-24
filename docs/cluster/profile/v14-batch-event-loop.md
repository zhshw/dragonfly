# v14 Folly io_uring 批次事件循环首版

**保留新架构继续修正，首版吞吐下降。** 普通 SET 改为有界准入、固定编码执行器、独立 ApplyLane 和共享任期回复水位，移除逐笔确认 fiber 与 applied 回查。节点 RPC 使用 Folly `AsyncIoUringSocket`，测试 RESP 也切到 io_uring。

## 实验结果

Raft memory、1 连接、pipeline 64、256 MiB、256 KiB/value，SET + 原连接 `WAIT 1`，正式三轮。上一轮复用 v13；测试网络后端同时变化，不能将差异单独归因于确认合批。 [平台](http://127.0.0.1:8787/#/v14/overview)

| 指标 | v13 | v14 | 变化 |
| --- | ---: | ---: | ---: |
| 吞吐中位数（MiB/s） | 486.54 | 365.10 | −25.0% |
| 吞吐算术均值（MiB/s） | 470.84 | 378.77 | −19.6% |
| 批次 P95 中位数（ms） | 45.25 | 49.84 | +10.1% |
| 批次 P95 算术均值（ms） | 48.68 | 46.92 | −3.6% |

三轮吞吐 **427.00 / 344.20 / 365.10 MiB/s**，P95 **49.84 / 56.55 / 34.36 ms**。SET 阶段吞吐中位数 **471.27 MiB/s**，末尾 WAIT 中位数 **200.42 ms**。WAIT 单独计时，不属于批次 P95。

## 诊断与后续

- Leader worker 排队区间并集 **263.28 → 20.32 ms**，CPU **506 → 506 ms**；Follower CPU **200 → 264 ms**。排队明显缩短，CPU 和最终吞吐没有同步获益。
- 本轮 Leader 编码 **81.97 ms / 259 批**，ApplyLane 执行 **96.83 ms / 380 批**；Follower Fetch RPC **579.40 ms**。独立诊断与正式轮分开，不能用于解释某一正式轮的慢因。
- 代码核查发现两个 100 ms 等待窗口：Follower 应用在途时可能挂起携带旧 applied 的长轮询，Leader WAIT 又按 100 ms 轮询副本位置。[v14b](v14b-batch-progress.md) 延续同一 Folly io_uring 架构，仅修复这两个进度通知 bug 并归档结果；正式上一轮选择 v13。

首版仍未实现相邻 SET 的 shard 内合批；同步 RPC handler 使用固定、有界的引擎 fiber 槽，网络收发已是 Folly io_uring。控制、恢复与快照冷路径保留同步屏障。 [完整设计及实现边界](../raft-batch-event-loop.md)

Debug、Release、ASAN/UBSAN 各 **42 项相关单测**通过；Debug **14 项**、Release **2 项**集成回归通过，覆盖 flush/memory、反压、断连、关闭、条件写、恢复、快照和换主。三轮正式测试均通过副本确认与全量读回；独立诊断的 **2241 条观测**已在 Langfuse 核验。

源码快照、二进制和额外验证日志在执行机 `~/.local/state/dragonfly-raft-workflow/runs/v14/v14-run-001/`，原始数据不提交 Git。
