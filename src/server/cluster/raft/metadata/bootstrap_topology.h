// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/raft/metadata/metadata_image.h"

namespace dfly::cluster::raft {

// Read the legacy shard array with group_id added to each shard and broker_id,
// raft_port, optional raft_host added to every node. Stage the returned records as
// one metadata transaction. The placeholder brokers remain fenced until they join.
absl::StatusOr<std::vector<MetadataRecord>> ParseBootstrapTopology(std::string_view json);

// Validate an observation from the reporting broker's registered RAFT endpoint.
// Duplicate and older reports have no record to append. Publish only after commit.
absl::StatusOr<std::optional<MetadataRecord>> PrepareLeaderReport(
    const MetadataImage& image, int32_t source_broker_id,
    const protocol::GroupLeaderRecord& report);

}  // namespace dfly::cluster::raft
