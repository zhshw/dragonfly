// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>

namespace dfly::cluster::raft {

// The exact bytes of the transferred RDB are bound into the group log by their 32-byte SHA256.
std::string KvRdbDigest(std::string_view bytes);

}  // namespace dfly::cluster::raft
