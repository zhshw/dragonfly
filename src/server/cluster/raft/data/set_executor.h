// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <string_view>

#include "server/common_types.h"

namespace dfly {
class JournalExecutor;

namespace cluster::raft {
struct SetEffect;

// Apply on a server proactor through the same synchronous transaction used by journal replay.
absl::Status ExecuteSetEffect(JournalExecutor& executor, const SetEffect& effect);

// Inspect a committed key through the normal shard transaction while the group's proposals
// are serialized. EXISTS accepts every Redis value type.
absl::StatusOr<bool> ExistsForSet(JournalExecutor& executor, DbIndex db, std::string_view key);

}  // namespace cluster::raft
}  // namespace dfly
