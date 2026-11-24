// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace dfly::cluster::raft::protocol {

using Uuid = std::array<uint8_t, 16>;
using TaggedFields = std::map<uint32_t, std::string>;

// Opaque Kafka record batches as carried by Fetch, with distinct null and empty encodings.
struct RecordBytes {
  std::optional<std::string> data;

  // Shared decoding retains the complete immutable receive allocation, not just this slice.
  struct Borrowed {
    std::shared_ptr<const void> owner;
    std::string_view bytes;
    size_t capacity;
  };
  std::optional<Borrowed> borrowed;

  std::optional<std::string_view> View() const {
    if (data)
      return *data;
    if (borrowed)
      return borrowed->bytes;
    return std::nullopt;
  }

  bool operator==(const RecordBytes& other) const {
    return View() == other.View();
  }
};

enum class CodecError {
  kOk,
  kUnsupportedVersion,
  kUnsupportedField,
  kInvalidData,
};

}  // namespace dfly::cluster::raft::protocol
