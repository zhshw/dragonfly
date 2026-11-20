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

struct RegisterBrokerRecord {
  static constexpr int16_t kApiKey = 0;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 4;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int32_t broker_id{};
  bool is_migrating_zk_broker{};
  Uuid incarnation_id{};
  int64_t broker_epoch{};
  struct BrokerEndpoint {
    std::string name{};
    std::string host{};
    uint16_t port{};
    int16_t security_protocol{};
    TaggedFields unknown_tags;
    bool operator==(const BrokerEndpoint&) const = default;
  };

  std::vector<BrokerEndpoint> end_points{};
  struct BrokerFeature {
    std::string name{};
    int16_t min_supported_version{};
    int16_t max_supported_version{};
    TaggedFields unknown_tags;
    bool operator==(const BrokerFeature&) const = default;
  };

  std::vector<BrokerFeature> features{};
  std::optional<std::string> rack{std::in_place};
  bool fenced = true;
  bool in_controlled_shutdown{};
  std::vector<Uuid> log_dirs{};
  std::optional<std::vector<Uuid>> cordoned_log_dirs{};
  TaggedFields unknown_tags;
  bool operator==(const RegisterBrokerRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const RegisterBrokerRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, RegisterBrokerRecord* out);

struct UnregisterBrokerRecord {
  static constexpr int16_t kApiKey = 1;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int32_t broker_id{};
  int64_t broker_epoch{};
  TaggedFields unknown_tags;
  bool operator==(const UnregisterBrokerRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const UnregisterBrokerRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, UnregisterBrokerRecord* out);

struct FenceBrokerRecord {
  static constexpr int16_t kApiKey = 7;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int32_t id{};
  int64_t epoch{};
  TaggedFields unknown_tags;
  bool operator==(const FenceBrokerRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const FenceBrokerRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, FenceBrokerRecord* out);

struct UnfenceBrokerRecord {
  static constexpr int16_t kApiKey = 8;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int32_t id{};
  int64_t epoch{};
  TaggedFields unknown_tags;
  bool operator==(const UnfenceBrokerRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const UnfenceBrokerRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, UnfenceBrokerRecord* out);

struct FeatureLevelRecord {
  static constexpr int16_t kApiKey = 12;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  std::string name{};
  int16_t feature_level{};
  TaggedFields unknown_tags;
  bool operator==(const FeatureLevelRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const FeatureLevelRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, FeatureLevelRecord* out);

struct BrokerRegistrationChangeRecord {
  static constexpr int16_t kApiKey = 17;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 3;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int32_t broker_id{};
  int64_t broker_epoch{};
  int8_t fenced{};
  int8_t in_controlled_shutdown{};
  std::vector<Uuid> log_dirs{};
  std::optional<std::vector<Uuid>> cordoned_log_dirs{};
  TaggedFields unknown_tags;
  bool operator==(const BrokerRegistrationChangeRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const BrokerRegistrationChangeRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, BrokerRegistrationChangeRecord* out);

struct NoOpRecord {
  static constexpr int16_t kApiKey = 20;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  TaggedFields unknown_tags;
  bool operator==(const NoOpRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const NoOpRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, NoOpRecord* out);

struct BeginTransactionRecord {
  static constexpr int16_t kApiKey = 23;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  std::optional<std::string> name{};
  TaggedFields unknown_tags;
  bool operator==(const BeginTransactionRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const BeginTransactionRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, BeginTransactionRecord* out);

struct EndTransactionRecord {
  static constexpr int16_t kApiKey = 24;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  TaggedFields unknown_tags;
  bool operator==(const EndTransactionRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const EndTransactionRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, EndTransactionRecord* out);

struct AbortTransactionRecord {
  static constexpr int16_t kApiKey = 25;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  std::optional<std::string> reason{};
  TaggedFields unknown_tags;
  bool operator==(const AbortTransactionRecord&) const = default;
};

// Encode or decode one metadata record body without its serde header.
// On failure the destination keeps its previous value.
CodecError Encode(const AbortTransactionRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, AbortTransactionRecord* out);

}  // namespace dfly::cluster::raft::protocol
