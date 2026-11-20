// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/add_voter.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<AddRaftVoterRequest::Listener> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.name))
      return false;
    if (!io.Field(m.host))
      return false;
    if (!io.Field(m.port))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<AddRaftVoterRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.cluster_id))
      return false;
    if (!io.Field(m.timeout_ms))
      return false;
    if (!io.Field(m.voter_id))
      return false;
    if (!io.Field(m.voter_directory_id))
      return false;
    if (!io.Field(m.listeners))
      return false;
    if (io.version >= 1) {
      if (!io.Field(m.ack_when_committed))
        return false;
    } else if (!io.Absent(m.ack_when_committed, true)) {
      return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<AddRaftVoterResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.throttle_time_ms))
      return false;
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.error_message))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

}  // namespace detail

CodecError Encode(const AddRaftVoterRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, AddRaftVoterRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const AddRaftVoterResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, AddRaftVoterResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
