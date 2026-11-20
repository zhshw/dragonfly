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

struct DescribeQuorumRequest {
  static constexpr int16_t kApiKey = 55;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 2;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 2;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  struct TopicData {
    std::string topic_name{};
    struct PartitionData {
      int32_t partition_index{};
      TaggedFields unknown_tags;
      bool operator==(const PartitionData&) const = default;
    };

    std::vector<PartitionData> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const TopicData&) const = default;
  };

  std::vector<TopicData> topics{};
  TaggedFields unknown_tags;
  bool operator==(const DescribeQuorumRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const DescribeQuorumRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, DescribeQuorumRequest* out);

struct DescribeQuorumResponse {
  static constexpr int16_t kApiKey = 55;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 2;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  struct ReplicaState {
    int32_t replica_id{};
    Uuid replica_directory_id{};
    int64_t log_end_offset{};
    int64_t last_fetch_timestamp = -1;
    int64_t last_caught_up_timestamp = -1;
    TaggedFields unknown_tags;
    bool operator==(const ReplicaState&) const = default;
  };

  int16_t error_code{};
  std::optional<std::string> error_message{std::in_place};
  struct TopicData {
    std::string topic_name{};
    struct PartitionData {
      int32_t partition_index{};
      int16_t error_code{};
      std::optional<std::string> error_message{std::in_place};
      int32_t leader_id{};
      int32_t leader_epoch{};
      int64_t high_watermark{};
      std::vector<ReplicaState> current_voters{};
      std::vector<ReplicaState> observers{};
      TaggedFields unknown_tags;
      bool operator==(const PartitionData&) const = default;
    };

    std::vector<PartitionData> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const TopicData&) const = default;
  };

  std::vector<TopicData> topics{};
  struct Node {
    int32_t node_id{};
    struct Listener {
      std::string name{};
      std::string host{};
      uint16_t port{};
      TaggedFields unknown_tags;
      bool operator==(const Listener&) const = default;
    };

    std::vector<Listener> listeners{};
    TaggedFields unknown_tags;
    bool operator==(const Node&) const = default;
  };

  std::vector<Node> nodes{};
  TaggedFields unknown_tags;
  bool operator==(const DescribeQuorumResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const DescribeQuorumResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, DescribeQuorumResponse* out);

}  // namespace dfly::cluster::raft::protocol
