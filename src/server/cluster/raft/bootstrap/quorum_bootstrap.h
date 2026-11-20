// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <filesystem>

#include "server/cluster/raft/protocol/voter_records.h"

namespace dfly::cluster::raft {

// A null data_group selects the metadata partition. No VotersRecord starts a joining
// replica as an observer until its first Fetch learns the current configuration.
struct QuorumBootstrap {
  std::optional<int32_t> data_group;
  protocol::VotersRecord voters;

  std::string Partition() const;
  absl::StatusOr<std::string> Identity() const;
  absl::Status Install(const std::filesystem::path& dir) const;
};

absl::StatusOr<QuorumBootstrap> ParseQuorumBootstrap(std::optional<std::string_view> initial,
                                                     std::optional<int32_t> data_group);

}  // namespace dfly::cluster::raft
