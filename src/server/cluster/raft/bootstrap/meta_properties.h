// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>

#include "server/cluster/raft/bootstrap/raft_directory.h"

namespace dfly::cluster::raft {

// Kafka v1 requires cluster.id and node.id. Formatter also populates directory.id.
absl::StatusOr<RaftDirectoryConfig> DecodeMetaProperties(std::string_view bytes,
                                                         std::string* bootstrap_id = nullptr);
absl::StatusOr<std::string> EncodeMetaProperties(const RaftDirectoryConfig& identity);

}  // namespace dfly::cluster::raft
