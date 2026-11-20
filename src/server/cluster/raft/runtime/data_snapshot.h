// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <chrono>
#include <string>

#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster/raft/runtime/group.h"

namespace dfly {
class Service;

namespace cluster::raft {

// One checkpoint stream per data partition. Validation and restore run on the group worker.
// Call MaybeSave from a proactor fiber so the group can keep applying during RDB traversal.
class DataSnapshotter {
 public:
  DataSnapshotter(std::string cluster_id, int32_t group_id, Service& service,
                  util::fb2::ProactorBase* owner);

  absl::Status Validate(const SnapshotWriter& writer);
  absl::Status Restore(const PartitionLog& log, int64_t hw, CommittedEffects* effects);
  absl::Status MaybeSave(RaftGroup& group, const CommittedEffects& effects);

 private:
  std::string cluster_id_;
  int32_t group_id_;
  Service& service_;
  util::fb2::ProactorBase* owner_;

  uint64_t last_bytes_ = 0;
  std::chrono::steady_clock::time_point last_snapshot_ = std::chrono::steady_clock::now();
};

}  // namespace cluster::raft
}  // namespace dfly
