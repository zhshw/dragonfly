# Badcases

记录已复现的问题、触发条件、原始证据与修复验收要求。编号在问题修复后保留，状态与修复提交随处理结果更新。

| 编号 | 分类 | 问题 | 状态 |
| --- | --- | --- | --- |
| [RAFT-001](RAFT-001-follower-readonly-get-moved.md) | Raft / Follower 读取 | 已同步的 Follower 接受 READONLY 后，GET 仍返回 MOVED | 已修复，回归通过 |
