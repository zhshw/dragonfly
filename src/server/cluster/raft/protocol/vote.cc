// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/vote.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<VoteRequest::TopicData::PartitionData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.partition_index))
      return false;
    if (!io.Field(m.replica_epoch))
      return false;
    if (!io.Field(m.replica_id))
      return false;
    if (io.version >= 1) {
      if (!io.Field(m.replica_directory_id))
        return false;
    }
    if (io.version >= 1) {
      if (!io.Field(m.voter_directory_id))
        return false;
    }
    if (!io.Field(m.last_offset_epoch))
      return false;
    if (!io.Field(m.last_offset))
      return false;
    if (io.version >= 2) {
      if (!io.Field(m.pre_vote))
        return false;
    } else if (!io.Absent(m.pre_vote)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<VoteRequest::TopicData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.topic_name))
      return false;
    if (!io.Field(m.partitions))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<VoteRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.cluster_id))
      return false;
    if (io.version >= 1) {
      if (!io.Field(m.voter_id))
        return false;
    }
    if (!io.Field(m.topics))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<VoteResponse::TopicData::PartitionData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.partition_index))
      return false;
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.leader_id))
      return false;
    if (!io.Field(m.leader_epoch))
      return false;
    if (!io.Field(m.vote_granted))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<VoteResponse::TopicData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.topic_name))
      return false;
    if (!io.Field(m.partitions))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<VoteResponse::NodeEndpoint> {
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

template <> struct Schema<VoteResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.topics))
      return false;
    return io.Tags(m.unknown_tags, Tagged{0, 1, m.node_endpoints});
  }
};

}  // namespace detail

CodecError Encode(const VoteRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, VoteRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const VoteResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, VoteResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
