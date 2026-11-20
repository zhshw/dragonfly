// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/update_voter.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<UpdateRaftVoterRequest::Listener> {
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

template <> struct Schema<UpdateRaftVoterRequest::KRaftVersionFeature> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.min_supported_version))
      return false;
    if (!io.Field(m.max_supported_version))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<UpdateRaftVoterRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.cluster_id))
      return false;
    if (!io.Field(m.current_leader_epoch))
      return false;
    if (!io.Field(m.voter_id))
      return false;
    if (!io.Field(m.voter_directory_id))
      return false;
    if (!io.Field(m.listeners))
      return false;
    if (!io.Field(m.k_raft_version_feature))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<UpdateRaftVoterResponse::CurrentLeader> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.leader_id))
      return false;
    if (!io.Field(m.leader_epoch))
      return false;
    if (!io.Field(m.host))
      return false;
    if (!io.Field(m.port))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<UpdateRaftVoterResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.throttle_time_ms))
      return false;
    if (!io.Field(m.error_code))
      return false;
    return io.Tags(m.unknown_tags, Tagged{0, 0, m.current_leader,
                                          m.current_leader != decltype(m.current_leader){}, false});
  }
};

}  // namespace detail

CodecError Encode(const UpdateRaftVoterRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, UpdateRaftVoterRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const UpdateRaftVoterResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, UpdateRaftVoterResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
