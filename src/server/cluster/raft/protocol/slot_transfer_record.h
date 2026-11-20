// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Dragonfly cluster extension schemas by generate.py.

#pragma once

#include <optional>
#include <string_view>
#include <vector>

#include "server/cluster/raft/protocol/wire.h"

namespace dfly::cluster::raft::protocol {

struct SlotTransferRecord {
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int16_t version{};
  int8_t kind{};
  Uuid migration_id{};
  uint16_t slot{};
  int32_t source_group{};
  int32_t target_group{};
  int64_t generation{};
  int64_t position{};
  int32_t source_epoch = -1;
  int64_t source_end_offset = -1;
  std::vector<int8_t> digest{};
  RecordBytes bytes{};
  TaggedFields unknown_tags;
  bool operator==(const SlotTransferRecord&) const = default;
};

// Encode or decode one control record value, including its embedded version.
// On failure the destination keeps its previous value.
CodecError Encode(const SlotTransferRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, SlotTransferRecord* out);

}  // namespace dfly::cluster::raft::protocol
