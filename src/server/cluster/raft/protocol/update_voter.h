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

struct UpdateRaftVoterRequest {
  static constexpr int16_t kApiKey = 82;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 2;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  std::optional<std::string> cluster_id{std::in_place};
  int32_t current_leader_epoch{};
  int32_t voter_id{};
  Uuid voter_directory_id{};
  struct Listener {
    std::string name{};
    std::string host{};
    uint16_t port{};
    TaggedFields unknown_tags;
    bool operator==(const Listener&) const = default;
  };

  std::vector<Listener> listeners{};
  struct KRaftVersionFeature {
    int16_t min_supported_version{};
    int16_t max_supported_version{};
    TaggedFields unknown_tags;
    bool operator==(const KRaftVersionFeature&) const = default;
  };

  KRaftVersionFeature k_raft_version_feature{};
  TaggedFields unknown_tags;
  bool operator==(const UpdateRaftVoterRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const UpdateRaftVoterRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, UpdateRaftVoterRequest* out);

struct UpdateRaftVoterResponse {
  static constexpr int16_t kApiKey = 82;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int32_t throttle_time_ms{};
  int16_t error_code{};
  struct CurrentLeader {
    int32_t leader_id = -1;
    int32_t leader_epoch = -1;
    std::string host{};
    int32_t port{};
    TaggedFields unknown_tags;
    bool operator==(const CurrentLeader&) const = default;
  };

  CurrentLeader current_leader{};
  TaggedFields unknown_tags;
  bool operator==(const UpdateRaftVoterResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const UpdateRaftVoterResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, UpdateRaftVoterResponse* out);

}  // namespace dfly::cluster::raft::protocol
