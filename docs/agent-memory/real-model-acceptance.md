# 真实模型验收

[当前状态](current-status.md) · [接续工作](handoff.md) · [任务定义](TODO.md)

这是 P1-06 尚未执行的真实服务验收入口；通过后才能收口依赖它的 P1-07。默认兼容套件使用确定性模型，不能证明真实生成和 embedding 服务可用。

先在同级 AMS V0 激活环境，通过现有部署环境配置生成和 embedding 服务。当前默认 `gpt-5` / `text-embedding-3-small`；配置项包括 `GENERATION_MODEL`、`EMBEDDING_MODEL`、`OPENAI_API_KEY`、`OPENAI_API_BASE`，其他已支持 provider 使用对应配置。不要把凭据写入验收记录。若通过 `REDIS_MEMORY_CONFIG` 文件配置，环境变量覆盖规则见 AMS 配置实现。

在 Dragonfly 根目录执行：

```bash
source ../agent-memory-server/V0/.venv/bin/activate
AMS_REAL_MODEL_ACCEPTANCE=1 UV_BIN=/tmp/uv-x86_64-unknown-linux-gnu/uv \
  bash tests/agent_memory/run.sh -q -x --tb=short -k real_provider_http_worker
```

其他机器使用已安装 uv；需要 search/JSON 与本适配 C++ 函数的 Dragonfly 二进制。测试创建隔离临时双 shard 实例，仅注入存储路由；实际 embedding 与生成调用使用已配置 provider，不替换模型输出。会产生真实服务调用与费用。

用合成的绿茶偏好验证认证 HTTP 工作记忆写入、实际 BoundedDocketWorker 抽取、durable finalized 收据、实际语义召回内容/作用域、foreign owner 404、访问统计消费、删除后 Hash 消失，以及 Stream0/PEL0。请求返回 200 单独不算通过。

记录真实模型名/版本、schema 维度、配对代码/依赖版本、命令、结果和耗时；失败记录实际调用错误，不能改成固定输出或关闭隔离。此场景是最终验收入口；还须依据 P1-06 原定义补齐真实摘要等业务内容的验收证据，再复核 P1-07 全部 P0/P1 门槛。当前未执行，默认套件跳过这一场景。
