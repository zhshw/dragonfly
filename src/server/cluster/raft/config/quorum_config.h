// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace dfly::cluster::raft {

struct QuorumEndpoint {
  std::string host;
  uint16_t port;

  bool operator==(const QuorumEndpoint&) const = default;
};

using VoterConnections = std::map<int32_t, QuorumEndpoint>;

// Kafka controller.quorum.voters. Leave DNS resolution to the transport.
absl::StatusOr<VoterConnections> ParseVoterConnections(std::string_view value);

// Discovery endpoints for a dynamic quorum, in the supplied order.
absl::StatusOr<std::vector<QuorumEndpoint>> ParseBootstrapServers(std::string_view value);

}  // namespace dfly::cluster::raft
