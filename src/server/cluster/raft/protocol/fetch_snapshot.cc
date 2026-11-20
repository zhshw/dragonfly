// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/fetch_snapshot.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<FetchSnapshotRequest::TopicSnapshot::PartitionSnapshot::SnapshotId> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.end_offset))
      return false;
    if (!io.Field(m.epoch))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchSnapshotRequest::TopicSnapshot::PartitionSnapshot> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.partition))
      return false;
    if (!io.Field(m.current_leader_epoch))
      return false;
    if (!io.Field(m.snapshot_id))
      return false;
    if (!io.Field(m.position))
      return false;
    return io.Tags(m.unknown_tags,
                   Tagged{0, 1, m.replica_directory_id,
                          m.replica_directory_id != decltype(m.replica_directory_id){}, true});
  }
};

template <> struct Schema<FetchSnapshotRequest::TopicSnapshot> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.name))
      return false;
    if (!io.Field(m.partitions))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchSnapshotRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.replica_id))
      return false;
    if (!io.Field(m.max_bytes))
      return false;
    if (!io.Field(m.topics))
      return false;
    return io.Tags(m.unknown_tags, Tagged{0, 0, m.cluster_id, m.cluster_id.has_value(), false});
  }
};

template <> struct Schema<FetchSnapshotResponse::TopicSnapshot::PartitionSnapshot::SnapshotId> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.end_offset))
      return false;
    if (!io.Field(m.epoch))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <>
struct Schema<FetchSnapshotResponse::TopicSnapshot::PartitionSnapshot::LeaderIdAndEpoch> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.leader_id))
      return false;
    if (!io.Field(m.leader_epoch))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchSnapshotResponse::TopicSnapshot::PartitionSnapshot> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.index))
      return false;
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.snapshot_id))
      return false;
    if (!io.Field(m.size))
      return false;
    if (!io.Field(m.position))
      return false;
    if (!io.Field(m.unaligned_records))
      return false;
    if (!m.unaligned_records.data.has_value())
      return false;
    return io.Tags(m.unknown_tags, Tagged{0, 0, m.current_leader,
                                          m.current_leader != decltype(m.current_leader){}, false});
  }
};

template <> struct Schema<FetchSnapshotResponse::TopicSnapshot> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.name))
      return false;
    if (!io.Field(m.partitions))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchSnapshotResponse::NodeEndpoint> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 1) {
      if (!io.Field(m.node_id))
        return false;
    } else if (!io.Absent(m.node_id)) {
      return false;
    }
    if (io.version >= 1) {
      if (!io.Field(m.host))
        return false;
    } else if (!io.Absent(m.host)) {
      return false;
    }
    if (io.version >= 1) {
      if (!io.Field(m.port))
        return false;
    } else if (!io.Absent(m.port)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchSnapshotResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.throttle_time_ms))
      return false;
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.topics))
      return false;
    return io.Tags(m.unknown_tags,
                   Tagged{0, 1, m.node_endpoints, !m.node_endpoints.empty(), false});
  }
};

}  // namespace detail

CodecError Encode(const FetchSnapshotRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, FetchSnapshotRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const FetchSnapshotResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, FetchSnapshotResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
