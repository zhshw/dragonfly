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

struct BrokerRegistrationRequest {
  static constexpr int16_t kApiKey = 62;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 4;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 2;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int32_t broker_id{};
  std::string cluster_id{};
  Uuid incarnation_id{};
  struct Listener {
    std::string name{};
    std::string host{};
    uint16_t port{};
    int16_t security_protocol{};
    TaggedFields unknown_tags;
    bool operator==(const Listener&) const = default;
  };

  std::vector<Listener> listeners{};
  struct Feature {
    std::string name{};
    int16_t min_supported_version{};
    int16_t max_supported_version{};
    TaggedFields unknown_tags;
    bool operator==(const Feature&) const = default;
  };

  std::vector<Feature> features{};
  std::optional<std::string> rack{std::in_place};
  bool is_migrating_zk_broker{};
  std::vector<Uuid> log_dirs{};
  int64_t previous_broker_epoch = -1;
  TaggedFields unknown_tags;
  bool operator==(const BrokerRegistrationRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const BrokerRegistrationRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, BrokerRegistrationRequest* out);

struct BrokerRegistrationResponse {
  static constexpr int16_t kApiKey = 62;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 4;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int32_t throttle_time_ms{};
  int16_t error_code{};
  int64_t broker_epoch = -1;
  TaggedFields unknown_tags;
  bool operator==(const BrokerRegistrationResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const BrokerRegistrationResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, BrokerRegistrationResponse* out);

}  // namespace dfly::cluster::raft::protocol
