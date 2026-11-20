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

struct ApiVersionsRequest {
  static constexpr int16_t kApiKey = 18;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 5;
  static constexpr int16_t kFirstFlexibleVersion = 3;
  static constexpr int16_t HeaderVersion(int16_t version) {
    return version < kFirstFlexibleVersion ? 1 : 2;
  }

  std::string client_software_name{};
  std::string client_software_version{};
  std::optional<std::string> cluster_id{};
  int32_t node_id = -1;
  TaggedFields unknown_tags;
  bool operator==(const ApiVersionsRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const ApiVersionsRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, ApiVersionsRequest* out);

struct ApiVersionsResponse {
  static constexpr int16_t kApiKey = 18;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 5;
  static constexpr int16_t kFirstFlexibleVersion = 3;
  static constexpr int16_t kHeaderVersion = 0;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int16_t error_code{};
  struct ApiVersion {
    int16_t api_key{};
    int16_t min_version{};
    int16_t max_version{};
    TaggedFields unknown_tags;
    bool operator==(const ApiVersion&) const = default;
  };

  std::vector<ApiVersion> api_keys{};
  int32_t throttle_time_ms{};
  struct SupportedFeatureKey {
    std::string name{};
    int16_t min_version{};
    int16_t max_version{};
    TaggedFields unknown_tags;
    bool operator==(const SupportedFeatureKey&) const = default;
  };

  std::vector<SupportedFeatureKey> supported_features{};
  int64_t finalized_features_epoch = -1;
  struct FinalizedFeatureKey {
    std::string name{};
    int16_t max_version_level{};
    int16_t min_version_level{};
    TaggedFields unknown_tags;
    bool operator==(const FinalizedFeatureKey&) const = default;
  };

  std::vector<FinalizedFeatureKey> finalized_features{};
  bool zk_migration_ready{};
  TaggedFields unknown_tags;
  bool operator==(const ApiVersionsResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const ApiVersionsResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, ApiVersionsResponse* out);

}  // namespace dfly::cluster::raft::protocol
