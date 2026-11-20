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

struct FetchSnapshotRequest {
  static constexpr int16_t kApiKey = 59;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 1;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 2;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  std::optional<std::string> cluster_id{};
  int32_t replica_id = -1;
  int32_t max_bytes = 2147483647;
  struct TopicSnapshot {
    std::string name{};
    struct PartitionSnapshot {
      int32_t partition{};
      int32_t current_leader_epoch{};
      struct SnapshotId {
        int64_t end_offset{};
        int32_t epoch{};
        TaggedFields unknown_tags;
        bool operator==(const SnapshotId&) const = default;
      };

      SnapshotId snapshot_id{};
      int64_t position{};
      Uuid replica_directory_id{};
      TaggedFields unknown_tags;
      bool operator==(const PartitionSnapshot&) const = default;
    };

    std::vector<PartitionSnapshot> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const TopicSnapshot&) const = default;
  };

  std::vector<TopicSnapshot> topics{};
  TaggedFields unknown_tags;
  bool operator==(const FetchSnapshotRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const FetchSnapshotRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, FetchSnapshotRequest* out);

struct FetchSnapshotResponse {
  static constexpr int16_t kApiKey = 59;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 1;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int32_t throttle_time_ms{};
  int16_t error_code{};
  struct TopicSnapshot {
    std::string name{};
    struct PartitionSnapshot {
      int32_t index{};
      int16_t error_code{};
      struct SnapshotId {
        int64_t end_offset{};
        int32_t epoch{};
        TaggedFields unknown_tags;
        bool operator==(const SnapshotId&) const = default;
      };

      SnapshotId snapshot_id{};
      struct LeaderIdAndEpoch {
        int32_t leader_id{};
        int32_t leader_epoch{};
        TaggedFields unknown_tags;
        bool operator==(const LeaderIdAndEpoch&) const = default;
      };

      LeaderIdAndEpoch current_leader{};
      int64_t size{};
      int64_t position{};
      RecordBytes unaligned_records{};
      TaggedFields unknown_tags;
      bool operator==(const PartitionSnapshot&) const = default;
    };

    std::vector<PartitionSnapshot> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const TopicSnapshot&) const = default;
  };

  std::vector<TopicSnapshot> topics{};
  struct NodeEndpoint {
    int32_t node_id{};
    std::string host{};
    uint16_t port{};
    TaggedFields unknown_tags;
    bool operator==(const NodeEndpoint&) const = default;
  };

  std::vector<NodeEndpoint> node_endpoints{};
  TaggedFields unknown_tags;
  bool operator==(const FetchSnapshotResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const FetchSnapshotResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, FetchSnapshotResponse* out);

}  // namespace dfly::cluster::raft::protocol
