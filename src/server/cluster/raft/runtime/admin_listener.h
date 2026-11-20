// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/status.h>

#include <cstdint>
#include <functional>
#include <string>

#include "server/cluster/raft/runtime/admin_command.h"

namespace util {
class AcceptServer;
class ProactorPool;
}  // namespace util

namespace dfly::cluster::raft {

struct ControllerAdminConfig {
  uint16_t port = 0;
  std::string bind_address;
  std::string password;
};

using AdminHandler = std::function<AdminReply(std::span<const std::string_view>)>;

// Add the controller RESP listener to the same acceptor as the internal quorum port.
// A disabled admin port leaves the controller with its internal listener only.
absl::Status AddControllerAdminListener(const ControllerAdminConfig& config, AdminHandler handler,
                                        util::ProactorPool* pool, util::AcceptServer* acceptor);

}  // namespace dfly::cluster::raft
