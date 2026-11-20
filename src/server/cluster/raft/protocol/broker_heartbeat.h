// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#pragma once

#include <optional>
#include <string_view>
#include <vector>

#include "server/cluster/raft/protocol/wire.h"

namespace dfly::cluster::raft::protocol {

struct BrokerHeartbeatRequest {
  static constexpr int16_t kApiKey = 63;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 2;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 2;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int32_t broker_id{};
  int64_t broker_epoch = -1;
  int64_t current_metadata_offset{};
  bool want_fence{};
  bool want_shut_down{};
  std::vector<Uuid> offline_log_dirs{};
  std::optional<std::vector<Uuid>> cordoned_log_dirs{};
  TaggedFields unknown_tags;
  bool operator==(const BrokerHeartbeatRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const BrokerHeartbeatRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, BrokerHeartbeatRequest* out);

struct BrokerHeartbeatResponse {
  static constexpr int16_t kApiKey = 63;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 2;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int32_t throttle_time_ms{};
  int16_t error_code{};
  bool is_caught_up{};
  bool is_fenced = true;
  bool should_shut_down{};
  TaggedFields unknown_tags;
  bool operator==(const BrokerHeartbeatResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const BrokerHeartbeatResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, BrokerHeartbeatResponse* out);

}  // namespace dfly::cluster::raft::protocol
