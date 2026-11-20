// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/status/statusor.h>

#include <chrono>
#include <filesystem>
#include <memory>

#include "server/cluster/raft/bootstrap/raft_directory.h"
#include "server/cluster/raft/config/quorum_config.h"
#include "server/cluster/raft/log/snapshot_file.h"
#include "server/cluster/raft/protocol/rpc_frame.h"

namespace util::fb2 {
class ProactorBase;
}

namespace dfly::cluster::raft {
class ElectionCampaign;
class FetchReplication;
class PartitionLog;

// All access to a partition's log, including application callbacks, runs on its disk worker.
class RaftGroup {
 public:
  enum class Kind { kMetadata, kData };
  struct State {
    const ElectionCampaign& election;
    FetchReplication& replication;
    PartitionLog& log;
  };
  using Operation = absl::FunctionRef<absl::Status(State)>;
  using ApplyCommitted = std::function<absl::Status(const PartitionLog&, int64_t)>;

  // A metadata observer uses the controllers' voter set for discovery and never votes.
  // validate checks a fully downloaded snapshot before it can replace the durable log.
  static absl::StatusOr<std::unique_ptr<RaftGroup>> Open(
      const std::filesystem::path& dir, const RaftDirectoryIdentity& identity, Kind kind,
      int32_t partition, const VoterConnections& voters, bool observer = false,
      ApplyCommitted apply = {}, SnapshotValidator validate = {});
  ~RaftGroup();
  void Start(util::fb2::ProactorBase* owner);
  void RequestStop();
  void Join();
  protocol::RpcHandlers Handlers();
  absl::Status Visit(Operation operation);

  // Run on the disk worker and recheck after a commit or leadership change until done.
  // Only the calling fiber parks. The operation may use a ticket to track an earlier append.
  absl::Status VisitUntil(std::chrono::steady_clock::time_point deadline,
                          absl::FunctionRef<absl::Status(State, bool* done)> operation);

 private:
  class Impl;
  explicit RaftGroup(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

inline constexpr size_t kMaxRaftFrame = 16U << 20;

}  // namespace dfly::cluster::raft
