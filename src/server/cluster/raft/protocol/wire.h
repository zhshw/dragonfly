// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>

namespace dfly::cluster::raft::protocol {

using Uuid = std::array<uint8_t, 16>;
using TaggedFields = std::map<uint32_t, std::string>;

// Opaque Kafka record batches as carried by Fetch, with distinct null and empty encodings.
struct RecordBytes {
  std::optional<std::string> data;
  bool operator==(const RecordBytes&) const = default;
};

enum class CodecError {
  kOk,
  kUnsupportedVersion,
  kUnsupportedField,
  kInvalidData,
};

}  // namespace dfly::cluster::raft::protocol
