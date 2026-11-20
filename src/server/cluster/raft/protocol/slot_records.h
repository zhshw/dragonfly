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

struct RedisNodeRecord {
  static constexpr int16_t kApiKey = 16384;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int32_t broker_id{};
  std::string redis_node_id{};
  TaggedFields unknown_tags;
  bool operator==(const RedisNodeRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const RedisNodeRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, RedisNodeRecord* out);

struct DataGroupRecord {
  static constexpr int16_t kApiKey = 16385;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int32_t group_id{};
  std::vector<int32_t> members{};
  TaggedFields unknown_tags;
  bool operator==(const DataGroupRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const DataGroupRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, DataGroupRecord* out);

struct GroupLeaderRecord {
  static constexpr int16_t kApiKey = 16386;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int32_t group_id{};
  int32_t leader_id = -1;
  int32_t leader_epoch{};
  int64_t voter_config_offset = -1;
  int32_t reporter_id{};
  int64_t reporter_epoch{};
  TaggedFields unknown_tags;
  bool operator==(const GroupLeaderRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const GroupLeaderRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, GroupLeaderRecord* out);

struct SlotAssignmentRecord {
  static constexpr int16_t kApiKey = 16387;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  uint16_t first_slot{};
  uint16_t last_slot{};
  int32_t expected_group = -1;
  int64_t expected_generation{};
  int32_t group_id{};
  int64_t generation{};
  TaggedFields unknown_tags;
  bool operator==(const SlotAssignmentRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const SlotAssignmentRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, SlotAssignmentRecord* out);

}  // namespace dfly::cluster::raft::protocol
