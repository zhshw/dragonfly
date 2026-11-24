// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/fetch.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<FetchRequest::ReplicaState> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 15) {
      if (!io.Field(m.replica_id))
        return false;
    } else if (!io.Absent(m.replica_id, -1)) {
      return false;
    }
    if (io.version >= 15) {
      if (!io.Field(m.replica_epoch))
        return false;
    } else if (!io.Absent(m.replica_epoch, -1)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchRequest::FetchTopic::FetchPartition> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.partition))
      return false;
    if (io.version >= 9) {
      if (!io.Field(m.current_leader_epoch))
        return false;
    }
    if (!io.Field(m.fetch_offset))
      return false;
    if (io.version >= 12) {
      if (!io.Field(m.last_fetched_epoch))
        return false;
    } else if (!io.Absent(m.last_fetched_epoch, -1)) {
      return false;
    }
    if (io.version >= 5) {
      if (!io.Field(m.log_start_offset))
        return false;
    }
    if (!io.Field(m.partition_max_bytes))
      return false;
    return io.Tags(m.unknown_tags,
                   Tagged{0, 17, m.replica_directory_id,
                          m.replica_directory_id != decltype(m.replica_directory_id){}, true},
                   Tagged{1, 18, m.high_watermark, m.high_watermark != 9223372036854775807, true});
  }
};

template <> struct Schema<FetchRequest::FetchTopic> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version <= 12) {
      if (!io.Field(m.topic))
        return false;
    }
    if (io.version >= 13) {
      if (!io.Field(m.topic_id))
        return false;
    }
    if (!io.Field(m.partitions))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchRequest::ForgottenTopic> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 7 && io.version <= 12) {
      if (!io.Field(m.topic))
        return false;
    }
    if (io.version >= 13) {
      if (!io.Field(m.topic_id))
        return false;
    }
    if (io.version >= 7) {
      if (!io.Field(m.partitions))
        return false;
    } else if (!io.Absent(m.partitions)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version <= 14) {
      if (!io.Field(m.replica_id))
        return false;
    } else if (!io.Absent(m.replica_id, -1)) {
      return false;
    }
    if (!io.Field(m.max_wait_ms))
      return false;
    if (!io.Field(m.min_bytes))
      return false;
    if (io.version >= 3) {
      if (!io.Field(m.max_bytes))
        return false;
    }
    if (io.version >= 4) {
      if (!io.Field(m.isolation_level))
        return false;
    }
    if (io.version >= 7) {
      if (!io.Field(m.session_id))
        return false;
    }
    if (io.version >= 7) {
      if (!io.Field(m.session_epoch))
        return false;
    }
    if (!io.Field(m.topics))
      return false;
    if (io.version >= 7) {
      if (!io.Field(m.forgotten_topics_data))
        return false;
    } else if (!io.Absent(m.forgotten_topics_data)) {
      return false;
    }
    if (io.version >= 11) {
      if (!io.Field(m.rack_id))
        return false;
    }
    return io.Tags(
        m.unknown_tags, Tagged{0, 12, m.cluster_id, m.cluster_id.has_value(), true},
        Tagged{1, 15, m.replica_state, m.replica_state != decltype(m.replica_state){}, false});
  }
};

template <> struct Schema<FetchResponse::FetchableTopicResponse::PartitionData::EpochEndOffset> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 12) {
      if (!io.Field(m.epoch))
        return false;
    } else if (!io.Absent(m.epoch, -1)) {
      return false;
    }
    if (io.version >= 12) {
      if (!io.Field(m.end_offset))
        return false;
    } else if (!io.Absent(m.end_offset, -1)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchResponse::FetchableTopicResponse::PartitionData::LeaderIdAndEpoch> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 12) {
      if (!io.Field(m.leader_id))
        return false;
    } else if (!io.Absent(m.leader_id, -1)) {
      return false;
    }
    if (io.version >= 12) {
      if (!io.Field(m.leader_epoch))
        return false;
    } else if (!io.Absent(m.leader_epoch, -1)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchResponse::FetchableTopicResponse::PartitionData::SnapshotId> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.end_offset))
      return false;
    if (!io.Field(m.epoch))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <>
struct Schema<FetchResponse::FetchableTopicResponse::PartitionData::AbortedTransaction> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 4) {
      if (!io.Field(m.producer_id))
        return false;
    } else if (!io.Absent(m.producer_id)) {
      return false;
    }
    if (io.version >= 4) {
      if (!io.Field(m.first_offset))
        return false;
    } else if (!io.Absent(m.first_offset)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchResponse::FetchableTopicResponse::PartitionData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.partition_index))
      return false;
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.high_watermark))
      return false;
    if (io.version >= 4) {
      if (!io.Field(m.last_stable_offset))
        return false;
    }
    if (io.version >= 5) {
      if (!io.Field(m.log_start_offset))
        return false;
    }
    if (io.version >= 4) {
      if (!io.Field(m.aborted_transactions))
        return false;
    }
    if (io.version >= 11) {
      if (!io.Field(m.preferred_read_replica))
        return false;
    } else if (!io.Absent(m.preferred_read_replica, -1)) {
      return false;
    }
    if (!io.Field(m.records))
      return false;
    return io.Tags(
        m.unknown_tags,
        Tagged{0, 12, m.diverging_epoch, m.diverging_epoch != decltype(m.diverging_epoch){}, false},
        Tagged{1, 12, m.current_leader, m.current_leader != decltype(m.current_leader){}, false},
        Tagged{2, 12, m.snapshot_id, m.snapshot_id != decltype(m.snapshot_id){}, false});
  }
};

template <> struct Schema<FetchResponse::FetchableTopicResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version <= 12) {
      if (!io.Field(m.topic))
        return false;
    }
    if (io.version >= 13) {
      if (!io.Field(m.topic_id))
        return false;
    }
    if (!io.Field(m.partitions))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchResponse::NodeEndpoint> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 16) {
      if (!io.Field(m.node_id))
        return false;
    } else if (!io.Absent(m.node_id)) {
      return false;
    }
    if (io.version >= 16) {
      if (!io.Field(m.host))
        return false;
    } else if (!io.Absent(m.host)) {
      return false;
    }
    if (io.version >= 16) {
      if (!io.Field(m.port))
        return false;
    } else if (!io.Absent(m.port)) {
      return false;
    }
    if (io.version >= 16) {
      if (!io.Field(m.rack))
        return false;
    } else if (!io.Absent(m.rack)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 1) {
      if (!io.Field(m.throttle_time_ms))
        return false;
    }
    if (io.version >= 7) {
      if (!io.Field(m.error_code))
        return false;
    }
    if (io.version >= 7) {
      if (!io.Field(m.session_id))
        return false;
    } else if (!io.Absent(m.session_id)) {
      return false;
    }
    if (!io.Field(m.responses))
      return false;
    return io.Tags(m.unknown_tags,
                   Tagged{0, 16, m.node_endpoints, !m.node_endpoints.empty(), false});
  }
};

}  // namespace detail

CodecError Encode(const FetchRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, FetchRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const FetchResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, FetchResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError DecodeShared(std::string_view bytes, int16_t version,
                        const std::shared_ptr<const void>& owner, size_t capacity,
                        FetchResponse* out) {
  if (!owner)
    return CodecError::kInvalidData;
  return detail::DecodeMessage(bytes, version, owner, capacity, out);
}

CodecError EncodeResponseFrame(const FetchResponse& message, int16_t version,
                               int32_t correlation_id, std::string* out) {
  return detail::EncodeResponseFrameMessage(message, version, correlation_id, out);
}

}  // namespace dfly::cluster::raft::protocol
