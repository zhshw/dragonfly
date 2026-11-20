// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "server/cluster/raft/metadata/record_serde.h"

namespace dfly::cluster::raft {

struct MetadataImage;

struct AdminReply {
  enum class Kind { kSimple, kError, kBulk, kNull, kArray, kMap };
  Kind kind = Kind::kNull;
  std::string text{};
  std::vector<std::optional<std::string>> elements{};
};

// Plan on the controller's current stable image. A nonempty result must commit and
// apply in the same leadership term before its reply can be sent to the client.
std::vector<MetadataRecord> PlanAdminCommand(const MetadataImage& image,
                                             std::string_view cluster_id,
                                             std::span<const std::string_view> args,
                                             AdminReply* reply);

}  // namespace dfly::cluster::raft
