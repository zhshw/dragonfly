// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <optional>
#include <string>
#include <string_view>

#include "server/cluster/raft/data/set_effect.h"

namespace dfly {
class JournalExecutor;

namespace cluster::raft {
namespace trace {
class Batch;
}

// Apply on a server proactor through the same synchronous transaction used by journal replay.
absl::Status ExecuteSetEffect(JournalExecutor& executor, const SetEffectView& effect);

// Apply contiguous same-shard runs in log order. On error, completed excludes the failing
// record and no later record has executed. The batch owns all borrowed key/value bytes.
absl::Status ExecuteSetBatch(JournalExecutor& executor, const CommittedEffects::ApplyBatch& batch,
                             size_t* completed);
absl::Status ExecuteSetBatch(JournalExecutor& executor, const CommittedEffects::ApplyBatch& batch,
                             trace::Batch* trace, size_t* completed);

// Inspect a committed key through the normal shard transaction while the group's proposals
// are serialized. EXISTS accepts every Redis value type.
absl::StatusOr<bool> ExistsForSet(JournalExecutor& executor, DbIndex db, std::string_view key);

// Read through a shard transaction before releasing the group's read barrier.
absl::StatusOr<std::optional<std::string>> ReadCommittedValue(JournalExecutor& executor, DbIndex db,
                                                              std::string_view key);

}  // namespace cluster::raft
}  // namespace dfly
