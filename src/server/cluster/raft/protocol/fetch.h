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

struct FetchRequest {
  static constexpr int16_t kApiKey = 1;
  static constexpr int16_t kMinVersion = 4;
  static constexpr int16_t kMaxVersion = 18;
  static constexpr int16_t kFirstFlexibleVersion = 12;
  static constexpr int16_t HeaderVersion(int16_t version) {
    return version < kFirstFlexibleVersion ? 1 : 2;
  }

  std::optional<std::string> cluster_id{};
  int32_t replica_id = -1;
  struct ReplicaState {
    int32_t replica_id = -1;
    int64_t replica_epoch = -1;
    TaggedFields unknown_tags;
    bool operator==(const ReplicaState&) const = default;
  };

  ReplicaState replica_state{};
  int32_t max_wait_ms{};
  int32_t min_bytes{};
  int32_t max_bytes = 2147483647;
  int8_t isolation_level = 0;
  int32_t session_id = 0;
  int32_t session_epoch = -1;
  struct FetchTopic {
    std::string topic{};
    Uuid topic_id{};
    struct FetchPartition {
      int32_t partition{};
      int32_t current_leader_epoch = -1;
      int64_t fetch_offset{};
      int32_t last_fetched_epoch = -1;
      int64_t log_start_offset = -1;
      int32_t partition_max_bytes{};
      Uuid replica_directory_id{};
      int64_t high_watermark = 9223372036854775807;
      TaggedFields unknown_tags;
      bool operator==(const FetchPartition&) const = default;
    };

    std::vector<FetchPartition> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const FetchTopic&) const = default;
  };

  std::vector<FetchTopic> topics{};
  struct ForgottenTopic {
    std::string topic{};
    Uuid topic_id{};
    std::vector<int32_t> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const ForgottenTopic&) const = default;
  };

  std::vector<ForgottenTopic> forgotten_topics_data{};
  std::string rack_id{};
  TaggedFields unknown_tags;
  bool operator==(const FetchRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const FetchRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, FetchRequest* out);

struct FetchResponse {
  static constexpr int16_t kApiKey = 1;
  static constexpr int16_t kMinVersion = 4;
  static constexpr int16_t kMaxVersion = 18;
  static constexpr int16_t kFirstFlexibleVersion = 12;
  static constexpr int16_t HeaderVersion(int16_t version) {
    return version < kFirstFlexibleVersion ? 0 : 1;
  }

  int32_t throttle_time_ms{};
  int16_t error_code{};
  int32_t session_id = 0;
  struct FetchableTopicResponse {
    std::string topic{};
    Uuid topic_id{};
    struct PartitionData {
      int32_t partition_index{};
      int16_t error_code{};
      int64_t high_watermark{};
      int64_t last_stable_offset = -1;
      int64_t log_start_offset = -1;
      struct EpochEndOffset {
        int32_t epoch = -1;
        int64_t end_offset = -1;
        TaggedFields unknown_tags;
        bool operator==(const EpochEndOffset&) const = default;
      };

      EpochEndOffset diverging_epoch{};
      struct LeaderIdAndEpoch {
        int32_t leader_id = -1;
        int32_t leader_epoch = -1;
        TaggedFields unknown_tags;
        bool operator==(const LeaderIdAndEpoch&) const = default;
      };

      LeaderIdAndEpoch current_leader{};
      struct SnapshotId {
        int64_t end_offset = -1;
        int32_t epoch = -1;
        TaggedFields unknown_tags;
        bool operator==(const SnapshotId&) const = default;
      };

      SnapshotId snapshot_id{};
      struct AbortedTransaction {
        int64_t producer_id{};
        int64_t first_offset{};
        TaggedFields unknown_tags;
        bool operator==(const AbortedTransaction&) const = default;
      };

      std::optional<std::vector<AbortedTransaction>> aborted_transactions{std::in_place};
      int32_t preferred_read_replica = -1;
      RecordBytes records{};
      TaggedFields unknown_tags;
      bool operator==(const PartitionData&) const = default;
    };

    std::vector<PartitionData> partitions{};
    TaggedFields unknown_tags;
    bool operator==(const FetchableTopicResponse&) const = default;
  };

  std::vector<FetchableTopicResponse> responses{};
  struct NodeEndpoint {
    int32_t node_id{};
    std::string host{};
    int32_t port{};
    std::optional<std::string> rack{};
    TaggedFields unknown_tags;
    bool operator==(const NodeEndpoint&) const = default;
  };

  std::vector<NodeEndpoint> node_endpoints{};
  TaggedFields unknown_tags;
  bool operator==(const FetchResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const FetchResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, FetchResponse* out);

// Decode records as views into immutable bytes retained by owner.
// capacity charges the whole receive allocation; failures preserve out.
CodecError DecodeShared(std::string_view bytes, int16_t version,
                        const std::shared_ptr<const void>& owner, size_t capacity,
                        FetchResponse* out);

// Encode a complete response frame without an intermediate body copy.
// On failure the destination keeps its previous value.
CodecError EncodeResponseFrame(const FetchResponse& message, int16_t version,
                               int32_t correlation_id, std::string* out);

}  // namespace dfly::cluster::raft::protocol
