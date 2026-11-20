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

struct AddRaftVoterRequest {
  static constexpr int16_t kApiKey = 80;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 1;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 2;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  std::optional<std::string> cluster_id{std::in_place};
  int32_t timeout_ms{};
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
  bool ack_when_committed = true;
  TaggedFields unknown_tags;
  bool operator==(const AddRaftVoterRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const AddRaftVoterRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, AddRaftVoterRequest* out);

struct AddRaftVoterResponse {
  static constexpr int16_t kApiKey = 80;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 1;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int32_t throttle_time_ms{};
  int16_t error_code{};
  std::optional<std::string> error_message{std::in_place};
  TaggedFields unknown_tags;
  bool operator==(const AddRaftVoterResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const AddRaftVoterResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, AddRaftVoterResponse* out);

}  // namespace dfly::cluster::raft::protocol
