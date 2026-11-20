// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/raft/metadata/metadata_image.h"

namespace dfly::cluster::raft {

// Build the initial Raft metadata transaction from the configured data groups and slot ranges.
// Each group declares its ID, members and slot ranges. Each member supplies its Redis identity,
// broker_id and RESP and Raft endpoints. Brokers remain fenced until they join.
absl::StatusOr<std::vector<MetadataRecord>> ParseBootstrapTopology(std::string_view json);

// Validate an observation from the reporting broker's registered RAFT endpoint.
// Duplicate and older reports have no record to append. Publish only after commit.
absl::StatusOr<std::optional<MetadataRecord>> PrepareLeaderReport(
    const MetadataImage& image, int32_t source_broker_id,
    const protocol::GroupLeaderRecord& report);

}  // namespace dfly::cluster::raft
