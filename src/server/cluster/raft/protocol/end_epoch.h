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

struct EndQuorumEpochRequest {
  static constexpr int16_t kApiKey = 54;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 1;
  static constexpr int16_t kFirstFlexibleVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t version) {
    return version < kFirstFlexibleVersion ? 1 : 2;
  }

  std::optional<std::string> cluster_id{};
  struct TopicData {
    std::string topic_name{};
    struct PartitionData {
      int32_t partition_index{};
      int32_t leader_id{};
      int32_t leader_epoch{};
      std::vector<int32_t> preferred_successors{};
      struct ReplicaInfo {
        int32_t candidate_id{};
        Uuid candidate_directory_id{};
        TaggedFields unknown_tags;
        bool operator==(const ReplicaInfo&) const = default;
      };

      std::vector<ReplicaInfo> preferred_candidates{};
      TaggedFields unknown_tags;
      bool operator==(const PartitionData&) const = default;
    };

    std::vector<PartitionData> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const TopicData&) const = default;
  };

  std::vector<TopicData> topics{};
  struct LeaderEndpoint {
    std::string name{};
    std::string host{};
    uint16_t port{};
    TaggedFields unknown_tags;
    bool operator==(const LeaderEndpoint&) const = default;
  };

  std::vector<LeaderEndpoint> leader_endpoints{};
  TaggedFields unknown_tags;
  bool operator==(const EndQuorumEpochRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const EndQuorumEpochRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, EndQuorumEpochRequest* out);

struct EndQuorumEpochResponse {
  static constexpr int16_t kApiKey = 54;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 1;
  static constexpr int16_t kFirstFlexibleVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t version) {
    return version < kFirstFlexibleVersion ? 0 : 1;
  }

  int16_t error_code{};
  struct TopicData {
    std::string topic_name{};
    struct PartitionData {
      int32_t partition_index{};
      int16_t error_code{};
      int32_t leader_id{};
      int32_t leader_epoch{};
      TaggedFields unknown_tags;
      bool operator==(const PartitionData&) const = default;
    };

    std::vector<PartitionData> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const TopicData&) const = default;
  };

  std::vector<TopicData> topics{};
  struct NodeEndpoint {
    int32_t node_id{};
    std::string host{};
    uint16_t port{};
    TaggedFields unknown_tags;
    bool operator==(const NodeEndpoint&) const = default;
  };

  std::vector<NodeEndpoint> node_endpoints{};
  TaggedFields unknown_tags;
  bool operator==(const EndQuorumEpochResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const EndQuorumEpochResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, EndQuorumEpochResponse* out);

}  // namespace dfly::cluster::raft::protocol
