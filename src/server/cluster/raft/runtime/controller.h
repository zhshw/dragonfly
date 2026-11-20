// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/status.h>

#include <filesystem>
#include <string>

namespace util {
class AcceptServer;
class ProactorPool;
}  // namespace util

namespace dfly::cluster::raft {

// Run a controller-only static metadata quorum until the internal listener shuts down.
absl::Status RunController(const std::filesystem::path& dir, const std::string& cluster_id,
                           int32_t node_id, const std::string& voters, util::ProactorPool* pool,
                           util::AcceptServer* acceptor);

}  // namespace dfly::cluster::raft
