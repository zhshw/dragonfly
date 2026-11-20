// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <filesystem>
#include <string>

#include "server/cluster/raft/runtime/admin_listener.h"

namespace dfly::cluster::raft {

// Run the metadata quorum and its optional RESP management listener until shutdown.
absl::Status RunController(const std::filesystem::path& dir, const std::string& cluster_id,
                           int32_t node_id, const std::string& voters, util::ProactorPool* pool,
                           util::AcceptServer* acceptor, const std::string& bootstrap_file,
                           const ControllerAdminConfig& admin);

}  // namespace dfly::cluster::raft
