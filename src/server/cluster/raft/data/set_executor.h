// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/status.h>

namespace dfly {
class JournalExecutor;

namespace cluster::raft {
struct SetEffect;

// Apply on a server proactor through the same synchronous transaction used by journal replay.
absl::Status ExecuteSetEffect(JournalExecutor& executor, const SetEffect& effect);

}  // namespace cluster::raft
}  // namespace dfly
