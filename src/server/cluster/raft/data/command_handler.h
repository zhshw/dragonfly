// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "server/cluster/cluster_defs.h"
#include "server/cluster/raft/data/write_position.h"
#include "server/common_types.h"

namespace dfly::cluster::raft {

enum class SetCondition { kIfMissing, kIfExists };

struct DataTopology {
  // Published only after every slot has an owner. Shards list only groups with a routable leader.
  ClusterShardInfos shards;
  std::string local_node_id;
  size_t known_nodes;
  size_t assigned_groups;
};

struct DataReplicaStatus {
  ClusterNodeInfo node;
  // Current leader-confirmed KV apply report from this replica. Missing means no fresh report.
  std::optional<int64_t> applied_offset;
};

struct DataReplicationState {
  int32_t group_id;
  int32_t epoch;
  bool is_leader;
  int64_t local_applied_offset;
  ClusterNodeInfo leader;
  std::vector<DataReplicaStatus> replicas;
  bool connected_to_leader = false;
};

// Installed before Service::Init and kept alive until Service::Shutdown completes.
// Methods run on client fibers. SET returns only after the group's committed effect is applied.
class DataCommandHandler {
 public:
  virtual ~DataCommandHandler() = default;
  virtual absl::Status Set(DbIndex db, std::string_view key, std::string_view value,
                           DataWritePosition* position) = 0;
  virtual absl::StatusOr<bool> SetIf(DbIndex db, std::string_view key, std::string_view value,
                                     SetCondition condition, DataWritePosition* position) = 0;
  virtual absl::StatusOr<std::optional<std::string>> Get(DbIndex db, std::string_view key) = 0;

  virtual absl::StatusOr<DataReplicationState> ReplicationState() {
    return absl::UnavailableError("-CLUSTERDOWN Data group has no known leader");
  }

  // Return a consistent projection of the committed metadata image for Redis cluster discovery.
  virtual absl::StatusOr<DataTopology> Topology() {
    return absl::UnavailableError("-CLUSTERDOWN Cluster metadata is unavailable");
  }
};

}  // namespace dfly::cluster::raft
