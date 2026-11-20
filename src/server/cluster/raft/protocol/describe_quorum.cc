// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/describe_quorum.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<DescribeQuorumRequest::TopicData::PartitionData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.partition_index))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<DescribeQuorumRequest::TopicData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.topic_name))
      return false;
    if (!io.Field(m.partitions))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<DescribeQuorumRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.topics))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<DescribeQuorumResponse::ReplicaState> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.replica_id))
      return false;
    if (io.version >= 2) {
      if (!io.Field(m.replica_directory_id))
        return false;
    } else if (!io.Absent(m.replica_directory_id)) {
      return false;
    }
    if (!io.Field(m.log_end_offset))
      return false;
    if (io.version >= 1) {
      if (!io.Field(m.last_fetch_timestamp))
        return false;
    }
    if (io.version >= 1) {
      if (!io.Field(m.last_caught_up_timestamp))
        return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<DescribeQuorumResponse::TopicData::PartitionData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.partition_index))
      return false;
    if (!io.Field(m.error_code))
      return false;
    if (io.version >= 2) {
      if (!io.Field(m.error_message))
        return false;
    }
    if (!io.Field(m.leader_id))
      return false;
    if (!io.Field(m.leader_epoch))
      return false;
    if (!io.Field(m.high_watermark))
      return false;
    if (!io.Field(m.current_voters))
      return false;
    if (!io.Field(m.observers))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<DescribeQuorumResponse::TopicData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.topic_name))
      return false;
    if (!io.Field(m.partitions))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<DescribeQuorumResponse::Node::Listener> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 2) {
      if (!io.Field(m.name))
        return false;
    } else if (!io.Absent(m.name)) {
      return false;
    }
    if (io.version >= 2) {
      if (!io.Field(m.host))
        return false;
    } else if (!io.Absent(m.host)) {
      return false;
    }
    if (io.version >= 2) {
      if (!io.Field(m.port))
        return false;
    } else if (!io.Absent(m.port)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<DescribeQuorumResponse::Node> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (io.version >= 2) {
      if (!io.Field(m.node_id))
        return false;
    } else if (!io.Absent(m.node_id)) {
      return false;
    }
    if (io.version >= 2) {
      if (!io.Field(m.listeners))
        return false;
    } else if (!io.Absent(m.listeners)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<DescribeQuorumResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.error_code))
      return false;
    if (io.version >= 2) {
      if (!io.Field(m.error_message))
        return false;
    }
    if (!io.Field(m.topics))
      return false;
    if (io.version >= 2) {
      if (!io.Field(m.nodes))
        return false;
    } else if (!io.Absent(m.nodes)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

}  // namespace detail

CodecError Encode(const DescribeQuorumRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, DescribeQuorumRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const DescribeQuorumResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, DescribeQuorumResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
