// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <string>
#include <string_view>

#include "server/cluster/cluster_defs.h"
#include "server/common_types.h"

namespace dfly::cluster::raft {

struct DataTopology {
  ClusterShardInfos shards;
  std::string local_node_id;
};

// Installed before Service::Init and kept alive until Service::Shutdown completes.
// Methods run on client fibers. SET returns only after the group's committed effect is applied.
class DataCommandHandler {
 public:
  virtual ~DataCommandHandler() = default;
  virtual absl::Status Set(DbIndex db, std::string_view key, std::string_view value) = 0;
  virtual absl::Status CheckRead(DbIndex db, std::string_view key) = 0;

  // Return a consistent projection of the committed metadata image for Redis cluster discovery.
  virtual absl::StatusOr<DataTopology> Topology() {
    return absl::UnavailableError("-CLUSTERDOWN Cluster metadata is unavailable");
  }
};

}  // namespace dfly::cluster::raft
