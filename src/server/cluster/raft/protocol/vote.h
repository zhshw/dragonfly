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

struct VoteRequest {
  static constexpr int16_t kApiKey = 52;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 2;
  static constexpr int16_t kHeaderVersion = 2;

  std::optional<std::string> cluster_id{};
  int32_t voter_id = -1;
  struct TopicData {
    std::string topic_name{};
    struct PartitionData {
      int32_t partition_index{};
      int32_t replica_epoch{};
      int32_t replica_id{};
      Uuid replica_directory_id{};
      Uuid voter_directory_id{};
      int32_t last_offset_epoch{};
      int64_t last_offset{};
      bool pre_vote{};
      TaggedFields unknown_tags;
      bool operator==(const PartitionData&) const = default;
    };

    std::vector<PartitionData> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const TopicData&) const = default;
  };

  std::vector<TopicData> topics{};
  TaggedFields unknown_tags;
  bool operator==(const VoteRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const VoteRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, VoteRequest* out);

struct VoteResponse {
  static constexpr int16_t kApiKey = 52;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 2;
  static constexpr int16_t kHeaderVersion = 1;

  int16_t error_code{};
  struct TopicData {
    std::string topic_name{};
    struct PartitionData {
      int32_t partition_index{};
      int16_t error_code{};
      int32_t leader_id{};
      int32_t leader_epoch{};
      bool vote_granted{};
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
  bool operator==(const VoteResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const VoteResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, VoteResponse* out);

}  // namespace dfly::cluster::raft::protocol
