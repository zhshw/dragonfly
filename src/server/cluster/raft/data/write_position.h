// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly::cluster::raft {

// The exclusive position of a write submitted on a Redis connection.
struct DataWritePosition {
  int32_t group_id = -1;
  int32_t epoch = -1;
  int64_t end_offset = -1;
};

}  // namespace dfly::cluster::raft
