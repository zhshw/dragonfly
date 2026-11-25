# Raft / 原 cluster 性能分析

- Langfuse：<http://172.31.23.2:3007>，监听 `0.0.0.0:3007`。
- 火焰图：<http://172.31.23.2:3137>，监听 `0.0.0.0:3137`；SVG 支持点击缩放和搜索。
- 登录账号：`admin@local.test`。密码为测试机
  `/data/dragonfly-observability/config/langfuse.env` 中的 `LANGFUSE_ADMIN_PASSWORD`；文件权限 `0600`。
- 数据库、对象存储和报告均在 `/data/dragonfly-observability/state/`。
  原 `/dev/sda1` NTFS 和已有文件保留；新建 `dragonfly-observability.ext4` 提供 ext4 语义。
  容量 64 GiB，当前 NTFS `prealloc` 挂载使该文件实际占用 64 GiB。
- `/etc/fstab` 和 `dragonfly-observability.service` 保证先挂载再启动。
  Docker 容器使用 `on-failure:3`，由 systemd 负责开机启动，避免盘未挂载就先启动数据库。
  镜像和 Docker 限额日志仍使用 Docker 自身目录。

## 部署与启动

在 `172.31.23.2` 首次准备数据盘时运行。

```sh
sudo bash tools/raft-observability/setup_host.sh
```

Compose 固定 Langfuse `4.83.0`，数据库不发布宿主机端口。禁用公开注册和遥测。
`LANGFUSE_PUBLIC_URL` 是浏览器回调地址，默认为 `http://172.31.23.2:3007`；
`OBSERVABILITY_BIND` 默认为 `0.0.0.0`。 [Compose 配置](../profile/compose.yaml)

```sh
make -C tools/profile langfuse
```

同时启动实验 Web 用 `make profile`。 [profile 使用说明](../profile/README.md#启动)

## 采集

生产源码和原 Release 二进制不加入 trace。`prepare.py` 复制参与采集的源文件，在副本插桩并重新链接。
`group.cc` 分别标记数据组和元数据组，避免把元数据的空闲 Fetch 长轮询误当成数据复制等待。
所有 span 在线程内存中缓冲，正常退出时写 CSV；达到容量会计数，报告生成器拒绝有丢失的采集。
采集只记录阶段、时间和数字位置，不记录 key 或 value。
应用路径另记录 WAL 读取、batch/effect 解码、owner 排队及批次执行；owner span 的 `arg0` 为待执行笔数。
`raft.set.cache_bytes` 在追加登记后记录缓存预算占用（`arg0`）与本轮 WAL 缓冲 capacity（`arg1`），单位为字节。
follower 对应 `raft.fetch.cache_bytes`，字段口径相同；`raft.fetch.cache_prepare` 记录已验证记录入缓存的处理时间，
`raft.apply.cache_hit` 记录每条从缓存取出的 effect，其 `arg0` 为记录起始 offset。失败后重试可能再次从 WAL 读回，命中数不是成功应用数。
共享缓存按唯一 capacity 计数；这些是瞬时值，不能累加，也不代表含输入、应用和 I/O 引用的总在途内存。
`raft.set.input_take` / `input_copy` 区分普通 SET 的参数接管与复制，`arg0` 为 key/value 逻辑字节数。
`raft.set.input_lifetime` 从请求创建持续到 drain 不再引用输入，`arg0` 为准入字节，`arg1` 为接管参数的
`HeapMemory()`（含参数偏移存储，不含对象自身、KV 或 WAL）；复制回退的 string 容量不计入这个字段。
`raft.set.input_release` 的 `arg0` 记录释放后剩余参数堆容量，`arg1` 为仍保留的准入字节；
`raft.set.ticket_lifetime` 持续到票据销毁释放配额，字段与 input lifetime 一致。上述诊断只存在于独立插桩二进制。
已提交普通 SET 的 `raft.apply.shard_run` 每个连续同 shard 段记录一次，含本线程直接执行；
`arg0` 为该段笔数，`arg1` 为 key 和 value 的总字节数。段执行时间包含事务锁等待。
`raft.apply.shard_queue` 只记录跨线程投递，从提交 L2 到回调开始，含队列反压；
次数即投递次数，`arg0` 为笔数，`arg1` 为目标 shard ID。
恢复路径达到时间预算后可能只完成前缀，剩余项的下一次投递会重复计入 `arg0`；完成笔数应核对 `journal.apply` 次数。

```sh
python3 tools/raft-observability/prepare.py --build-dir build-opt \
  --output "$PWD/docs/cluster/benchmarks/observe/probe"

python3 tools/raft-observability/collect.py \
  --binary "$PWD/docs/cluster/benchmarks/observe/probe/dragonfly" \
  --output "$PWD/docs/cluster/benchmarks/observe/memory-c1-p64" --mode memory \
  --profiler "$HOME/.local/opt/dragonfly-observability/profiler-build/libprofiler.so"
```

原 cluster 使用同一命令，改为 `--mode cluster` 和新的输出目录。固定为 **1 连接、pipeline 64**，
默认 256 MiB / 256 KiB/value、64 笔预热、5 秒静置；主指标包含原写连接的 `WAIT 1`。
继续执行离线副本恢复、`WAIT 2` 和全部值读回。测试数据在输出目录下的真实磁盘，不能放 `/tmp` tmpfs。
Ubuntu 的 Python Redis 和解压工具依赖若未安装到系统，需要沿用集成测试的 `PYTHONPATH`、`PATH` 和 `LD_LIBRARY_PATH`。
当前隔离仓库的 Redis Python 依赖在 `.python-packages`；运行前设置 `PYTHONPATH="$PWD/.python-packages"`。

CPU 使用 gperftools `2.19.3` 的独立 `libprofiler.so`，通过 `LD_PRELOAD` 注入。
`SIGUSR2` 在预热后开始、最后 WAIT 后停止，500 Hz；因此 CPU 窗口比精确客户端窗口略宽。
server span 按客户端起止时间严格裁剪。该方式不修改系统 `perf_event_paranoid`。
构建 profiler 使用 CMake + Ninja，开启 `GPERFTOOLS_BUILD_CPU_PROFILER`，关闭 heap/debug allocator。
Go pprof 读取 CPU profile，GNU `addr2line` 解析 ELF 映射，Brendan Gregg FlameGraph 生成 SVG。
依赖脚本保存在测试机 `$HOME/.local/opt/dragonfly-observability/downloads/`。

采集器另在计时前后读取 leader/follower 的 RSS、匿名 RSS 和峰值 RSS，写入 metadata 并显示在报告中。
峰值覆盖进程启动、预热和测量，包含实际 KV 数据；不代表临时缓冲分配字节，也不包含内核 page cache。
正式吞吐仍使用不带这些诊断的 benchmark。

## 生成报告并导入 Langfuse

```sh
run="$PWD/docs/cluster/benchmarks/observe/memory-c1-p64"
python3 tools/raft-observability/report.py --run "$run" \
  --go "$HOME/.local/go/bin/go" \
  --flamegraph-dir "$HOME/.local/opt/dragonfly-observability/downloads"
python3 tools/raft-observability/import_langfuse.py --run "$run"
python3 tools/raft-observability/publish.py --run "$run"
```

HTTP 上报只在测量完成后进行，使用 Langfuse v4 OTLP/HTTP JSON 和 v4 ingestion header；
导入后通过 v2 Observations API 分页检查每个 observation ID，避免把仅接收成功当作数据已可读。
Langfuse 每个节点、阶段展示最长的 50 个 span，并附总数；完整事件在报告的 `timeline.json` 中，
可用 Perfetto 打开。CPU profile、CSV 和 JSON 原始文件由 `.gitignore` 排除。

统计中的 `sum_ms` 会重复计算并发等待，`union_ms` 合并同一阶段的重叠区间；不同阶段仍有重叠，不能相加。
火焰图表示 CPU 采样，span 表示经过时间，两者一起分析。采样短的原 cluster follower 只适合判断大类热点。
插桩吞吐不能替代未插桩 benchmark；观察到排队不等于已经确定全部因果，优化后还需单独对照。
