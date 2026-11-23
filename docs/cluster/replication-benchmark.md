# Raft 复制优化与 benchmark

**原始 `raft.memory` 基线为 169.56 MiB/s，当前为 224.69 MiB/s，累计提高 32.5%。**
两者为相同负载和 SET + WAIT 1 口径的不同轮次三轮中位数；只测 1 连接、pipeline 64。
**206.56 MiB/s 是已完成共享缓冲和直接编码的上一版，本轮复测值仅用于计算增量 +8.8%**；本轮批次 P95 降低 17.4%。
历史优化对比使用 `flush` 语义；memory 优化与确认模式对比单独列出。吞吐取三轮中位数，
各项使用不同阶段的基线，提升比例不能相加。

<a id="本轮同连接-set-已能提前提交"></a>

## 做了哪些优化、提升多少

- **有界批量应用 + 借用 SET 参数**：相对已优化上一版，本轮增量 **206.56 → 224.69 MiB/s（+8.8%）**，批次 P95 **90.91 → 75.08 ms（−17.4%）**；相对原始 memory 基线累计 **+32.5%**。原 cluster 为 607.34 MiB/s，仍快约 **2.70 倍**。
- **共享 WAL 缓冲 + 直接编码**：前一组确认吞吐 **164.59 → 203.44 MiB/s（+23.6%）**；去掉中间编码字符串，日志格式及确认条件不变。
- **SET 提交与确认解耦**：普通 SET 可按序提前入队，仍等多数落盘及应用后回复。单连接 pipeline 64 的确认吞吐 **81.48 → 163.75 MiB/s（+101.0%）**，批次 P95 降低 **49.4%**；逐笔等回复的负载反而下降 **9.5%**。
- **异步 WAL 与 Fetch 重叠**：leader 刷盘期间允许 follower 提前取日志。已实现并通过故障回归，性能收益未单独量化。
- **并发 drain 合批**：收集在途 I/O 期间已就绪的写，共享下一轮刷盘。历史 Helio 插桩、8 连接下，相对原合批的 SET 吞吐 **170.67 → 180.89 MiB/s（+6.0%）**；每千笔的段同步、标记同步各减少约 **9.1%**。
- **Raft 文件 I/O 接入 Folly**：提交、完成及重试使用 Folly EventBase。相同 liburing 2.15 下，1 / 8 连接 SET 吞吐分别 **+8.2% / −2.2%**，未显示稳定整体优势。
- **自适应 1/4 MiB drain + 4 MiB 数据 Fetch**：相对固定 1/1 MiB，1 / 8 连接 SET 吞吐分别 **+3.3% / −4.4%**，有回退，不能认为大批次普遍更快。
- **按当前段容量截取前缀、绑定入队任期**：保持 FIFO，拒绝旧任期排队写。最新 1 / 2 / 8 连接确认吞吐分别 **+4.2% / −0.3% / +2.4%**；同时默认开启异步 I/O 和 drain，但两版测试均显式开启，不把默认值变化计作收益。
- **CRC 指令优化（独立试验，未合入）**：1 / 8 连接 SET 吞吐分别 **+8.5% / +4.3%**，收益有限，也没有解决提交阻塞。

历史 SET 吞吐只计到 SET 回复；“确认吞吐”还计入批末 WAIT。Folly 对照为 64 MiB、
自适应和 CRC 对照为 256 MiB，均每连接一笔在途；不能与 pipeline 结果直接混比。
实现、故障边界和历史测试细节见 [io_uring 文档](io_uring.md)。

<a id="latest-pipeline"></a>

## flush 段边界优化对比：1、2、8 连接

2026-11-23，`172.31.23.2`，Ubuntu 26.04 / i7-13700KF / NVMe ext4，Folly Release。
每轮 256 MiB、256 KiB/value、不同 key、64 笔预热、5 秒静置；每连接发送 64 笔再收齐回复。
三副本停掉一台，**从首笔 SET 发出计到所有原写连接的 `WAIT 1` 成功**，单位 MiB/s。
改前版 `71080870a731…` 已支持提前提交；当前版 `5978ec01ba18…` 包含段边界与任期改动。
原 cluster 使用当前同一二进制。

| 连接数 | 改前 Raft | 当前 Raft | 变化 | 原 cluster |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 162.22 | 169.08 | +4.2% | 623.66 |
| 2 | 180.47 | 179.86 | −0.3% | 634.36 |
| 8 | 191.11 | 195.75 | +2.4% | 652.86 |

<a id="当前实现与后续试验"></a>
<a id="fetch应用与复制的剩余成本"></a>

## 结论与剩余问题

- **该阶段改动收益有限**：2 / 8 连接的三轮范围重叠；8 连接 SET 吞吐仅提高 0.2%，批次 P95 略升。WAIT 检查时机也影响确认吞吐。
- **原 cluster 仍明显更快**：其 WAIT 确认副本应用；Raft SET 还要求多数落盘，两者持久性保证不同。
- **正确性通过**：27 轮均通过各写连接 WAIT 1、离线副本重启后的 WAIT 2 和全部 1088 个值读回。
- **仍需定位等待**：票据容量、WAL 排空和 worker 等待应用尚未消除，下一步用 trace / 火焰图量化。最新测试未刻意跨越 1 GiB 段边界，也没有新刷盘计数，不能单独量化段截取收益。

## 复现与原始数据

保留改动前的 `dragonfly`、`raft_storage`，在真实磁盘目录运行；[Folly 构建方法](io_uring.md#folly-编译变体)。

```sh
ninja -C build-opt dragonfly raft_storage
for clients in 1 2 8; do
  python3 tests/integration/raft_commit_benchmark/dragonfly/pipeline_compare.py \
    --build-dir build-opt --baseline-build /path/to/previous-pipeline-build \
    --include-cluster --clients "$clients" --pipeline-depth 64 \
    --output-dir "$PWD/docs/cluster/benchmarks/replication-pipeline-c$clients"
done
```

原始数据仅保存在本地 `docs/cluster/benchmarks/` 及测试机，已由 `.gitignore` 排除；
64 连接样本和同步 Raft 性能数据已删除。最新日志位于测试机隔离仓库
`/home/zhangshenwang/workspace/db/dragonfly-io-uring-20261123/results-segment-drain-pipeline-20261123-c{1,2,8}/`。

## 确认模式：raft.flush / raft.memory / 原 cluster

同日、同机、同一未插桩 Release 二进制 `5f185e62b82d…`，只测 **1 连接、pipeline 64**；
负载和 WAIT 口径同上，9 轮均通过恢复与全部 1088 个值读回。

| 模式 | 确认吞吐 MiB/s | 批次 P95 ms |
| --- | ---: | ---: |
| raft.flush | 162.38 | 114.15 |
| raft.memory | 169.56 | 113.75 |
| 原 cluster | 628.87 | 22.89 |

- **memory 仅提高 4.4%**，原 cluster 仍为其 **3.71 倍**；逐轮刷盘不是主要差距来源。
- **CPU 与等待仍有明显成本**：独立诊断中，raft.memory leader 的 `memcpy/memset` 占 CPU 样本 **68.8%**，worker 排队区间合并后 **588 ms**，编码 **475 ms**、应用 **237 ms**。
- **后台刷盘仍会阻塞**：leader FSYNC 区间约 **138 ms**；不能把 memory 解释为完全没有磁盘等待。以上阶段存在重叠，不能相加。
- **下一步优先减少 worker 内数据复制和应用往返**。暂停扩展连接数；详见 [火焰图与 trace 结论](replication-observability.md)。

`--raft_data_ack_mode=flush` 仍为默认；`memory` 是多数 page cache 接收确认并后台刷盘，改变持久性保证。
复现单连接对照：

```sh
python3 tests/integration/raft_commit_benchmark/dragonfly/pipeline_compare.py \
  --build-dir build-opt --ack-modes flush memory --include-cluster \
  --clients 1 --pipeline-depth 64 \
  --output-dir "$PWD/docs/cluster/benchmarks/ack-modes-c1"
```

原始结果在测试机隔离仓库 `docs/cluster/benchmarks/ack-modes-20261123-c1/`，不提交 Git。

## memory 优化：共享 WAL 缓冲与直接编码

2026-11-23，同机 Folly Release，**1 连接、pipeline 64**，沿用 256 MiB / 256 KiB/value 和
SET + WAIT 1 口径，新旧版本显式使用 `memory`，各三轮交错运行。

| 版本 | 确认吞吐 MiB/s（范围） | 批次 P95 ms |
| --- | ---: | ---: |
| 原始版，该轮复测 `5f185e62b82d…` | 164.59（162.18–168.49） | 129.00 |
| 共享缓冲 + 直接编码 `b8362f34fc6e…` | **203.44（196.54–223.93）** | **83.38** |
| 原 cluster，同一新版二进制 | 607.46（605.95–626.61） | 35.51 |

- **该轮对照确认吞吐 +23.6%，批次 P95 −35.4%**；该轮新旧吞吐范围不重叠，原 cluster 仍为当时新版的 **2.99 倍**。
  原始版首次测量为 169.56 MiB/s，此处 164.59 是同一原始版本的复测值，保留真实样本。
- **共享缓冲单独收益有限**：前一组三轮对照为 170.20 → 172.36 MiB/s（+1.3%），范围重叠；不能与本组结果直接相加。
- **实现**：聚合 WAL 通过 move 和不可变共享所有权交给 I/O；SET 的 DFKV 头、key、value 直接写入最终 batch，
  去掉中间 payload/body/records。日志格式、CRC、提交与应用确认条件保持不变。
- **正确性**：本轮 9 轮均完成 WAIT 1、离线副本恢复、WAIT 2 和 1088 个值读回；
  Debug/Release 单元回归、57 项 ASAN/UBSAN 检查、11 项集群回归通过。
- 本轮只测 1 连接；effect 缓存副本、drain 聚合复制和逐笔应用仍保留。代码依据和后续顺序见 [profile](profile.md)。

```sh
python3 tests/integration/raft_commit_benchmark/dragonfly/pipeline_compare.py \
  --build-dir build-opt --baseline-build /path/to/before-build \
  --ack-mode memory --include-cluster --clients 1 --pipeline-depth 64 \
  --output-dir "$PWD/docs/cluster/benchmarks/profile-buffer-c1"
```

原始数据和两阶段二进制保存在测试机 `docs/cluster/benchmarks/profile-buffer-20261123/`，由 Git 忽略。

## memory 优化：批量应用与借用参数

同机、同负载、同一 SET + WAIT 1 口径，未插桩 Folly Release 各三轮交错运行，**1 连接 / pipeline 64**。
本轮增量对照使用已完成共享缓冲与直接编码的上一版；两版均为 `memory`。
原始 memory 基线仍为 **169.56 MiB/s**，当前 **224.69 MiB/s** 相对它累计 **+32.5%**（跨轮比较）。

| 版本 | 确认吞吐 MiB/s（范围） | 批次 P95 ms |
| --- | ---: | ---: |
| 已优化上一版，本轮复测 `b8362f34fc6e…` | 206.56（192.83–211.18） | 90.91 |
| 批量应用 + 借用参数 `f039be66100f…` | **224.69（223.36–228.81）** | **75.08** |
| 原 cluster，同一新版二进制 | 607.34（444.33–619.21） | 33.71 |

- **本轮增量：确认吞吐 +8.8%，批次 P95 −17.4%**；三轮新版均快于对应上一版，范围不重叠。原 cluster 首轮偏低，保留全部样本。
- **实现**：连续已提交替换按 32 笔 / 1 MiB 合批；owner 每次执行约 1 ms 后可返回已完成前缀。
  SET 同步借用 key/value，省去临时参数副本，仍走原有 SET 事务；日志位置、slot generation 由组 worker 推进。
- **正确性**：Debug、Release、ASAN/UBSAN 各 19 项单测、12 项集群回归通过；9 轮 benchmark 均通过原连接 WAIT 1、
  离线副本恢复、WAIT 2 和全部 1088 个值读回。本轮没有扩大连接数。
- 普通 SET 的命令分发、follower WAL 读回/解码和确认票据回查仍存在；本轮不是整个应用路径的零复制。

复现使用上面的 `--ack-mode memory` 命令，基线指定 `profile-buffer-20261123/encoded-bin`。
本轮二进制、源码差异和原始结果在 `docs/cluster/benchmarks/profile-apply-20261123/`，不提交 Git。
火焰图和 trace 结论见 [profile](profile.md)。
