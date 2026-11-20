// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/end_epoch.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<EndQuorumEpochRequest::TopicData::PartitionData::ReplicaInfo> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 1) {
      if (!io.Field(m.candidate_id))
        return false;
    } else if (!io.Absent(m.candidate_id)) {
      return false;
    }
    if (io.version >= 1) {
      if (!io.Field(m.candidate_directory_id))
        return false;
    } else if (!io.Absent(m.candidate_directory_id)) {
      return false;
    }
    if (io.version < 1)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<EndQuorumEpochRequest::TopicData::PartitionData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.partition_index))
      return false;
    if (!io.Field(m.leader_id))
      return false;
    if (!io.Field(m.leader_epoch))
      return false;
    if (io.version == 0) {
      if (!io.Field(m.preferred_successors))
        return false;
    }
    if (io.version >= 1) {
      if (!io.Field(m.preferred_candidates))
        return false;
    }
    if (io.version < 1)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<EndQuorumEpochRequest::TopicData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.topic_name))
      return false;
    if (!io.Field(m.partitions))
      return false;
    if (io.version < 1)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<EndQuorumEpochRequest::LeaderEndpoint> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 1) {
      if (!io.Field(m.name))
        return false;
    } else if (!io.Absent(m.name)) {
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
    if (io.version < 1)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<EndQuorumEpochRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.cluster_id))
      return false;
    if (!io.Field(m.topics))
      return false;
    if (io.version >= 1) {
      if (!io.Field(m.leader_endpoints))
        return false;
    }
    if (io.version < 1)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<EndQuorumEpochResponse::TopicData::PartitionData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.partition_index))
      return false;
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.leader_id))
      return false;
    if (!io.Field(m.leader_epoch))
      return false;
    if (io.version < 1)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<EndQuorumEpochResponse::TopicData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.topic_name))
      return false;
    if (!io.Field(m.partitions))
      return false;
    if (io.version < 1)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<EndQuorumEpochResponse::NodeEndpoint> {
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
    if (io.version < 1)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<EndQuorumEpochResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.topics))
      return false;
    if (io.version < 1)
      return io.Absent(m.unknown_tags);
    return io.Tags(m.unknown_tags, Tagged{0, 1, m.node_endpoints, !m.node_endpoints.empty()});
  }
};

}  // namespace detail

CodecError Encode(const EndQuorumEpochRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, EndQuorumEpochRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const EndQuorumEpochResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, EndQuorumEpochResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
