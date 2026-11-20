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

struct SlotMigrationRequest {
  static constexpr int16_t kApiKey = 20000;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 2;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  std::string cluster_id{};
  int8_t action{};
  Uuid migration_id{};
  uint16_t slot{};
  int32_t source_group{};
  int32_t target_group{};
  int64_t generation{};
  int32_t group_id = -1;
  int32_t reporter_id = -1;
  int64_t broker_epoch = -1;
  int32_t leader_epoch = -1;
  int32_t record_epoch = -1;
  int64_t record_end_offset = -1;
  int64_t high_watermark = -1;
  int64_t applied_end_offset = -1;
  int8_t expected_phase{};
  int32_t source_epoch = -1;
  int64_t source_end_offset = -1;
  std::vector<int8_t> digest{};
  TaggedFields unknown_tags;
  bool operator==(const SlotMigrationRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const SlotMigrationRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, SlotMigrationRequest* out);

struct SlotMigrationResponse {
  static constexpr int16_t kApiKey = 20000;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int16_t error_code{};
  std::string error_message{};
  int8_t phase{};
  bool source_finished{};
  bool target_finished{};
  TaggedFields unknown_tags;
  bool operator==(const SlotMigrationResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const SlotMigrationResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, SlotMigrationResponse* out);

}  // namespace dfly::cluster::raft::protocol
