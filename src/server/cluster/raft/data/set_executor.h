// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <optional>
#include <string>
#include <string_view>

#include "server/common_types.h"

namespace dfly {
class JournalExecutor;

namespace cluster::raft {
struct SetEffectView;

// Apply on a server proactor through the same synchronous transaction used by journal replay.
absl::Status ExecuteSetEffect(JournalExecutor& executor, const SetEffectView& effect);

// Inspect a committed key through the normal shard transaction while the group's proposals
// are serialized. EXISTS accepts every Redis value type.
absl::StatusOr<bool> ExistsForSet(JournalExecutor& executor, DbIndex db, std::string_view key);

// Read through a shard transaction before releasing the group's read barrier.
absl::StatusOr<std::optional<std::string>> ReadCommittedValue(JournalExecutor& executor, DbIndex db,
                                                              std::string_view key);

}  // namespace cluster::raft
}  // namespace dfly
