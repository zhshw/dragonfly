// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>
#include <map>
#include <string>

namespace dfly::cluster::raft::protocol {

using Uuid = std::array<uint8_t, 16>;
using TaggedFields = std::map<uint32_t, std::string>;

enum class CodecError {
  kOk,
  kUnsupportedVersion,
  kUnsupportedField,
  kInvalidData,
};

}  // namespace dfly::cluster::raft::protocol
