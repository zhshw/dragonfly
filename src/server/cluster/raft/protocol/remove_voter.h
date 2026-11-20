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

struct RemoveRaftVoterRequest {
  static constexpr int16_t kApiKey = 81;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 2;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  std::optional<std::string> cluster_id{std::in_place};
  int32_t voter_id{};
  Uuid voter_directory_id{};
  TaggedFields unknown_tags;
  bool operator==(const RemoveRaftVoterRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const RemoveRaftVoterRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, RemoveRaftVoterRequest* out);

struct RemoveRaftVoterResponse {
  static constexpr int16_t kApiKey = 81;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int32_t throttle_time_ms{};
  int16_t error_code{};
  std::optional<std::string> error_message{std::in_place};
  TaggedFields unknown_tags;
  bool operator==(const RemoveRaftVoterResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const RemoveRaftVoterResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, RemoveRaftVoterResponse* out);

}  // namespace dfly::cluster::raft::protocol
