// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/voter_records.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<KRaftVersionRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.version))
      return false;
    if (!io.Field(m.k_raft_version))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<VotersRecord::Voter::Endpoint> {
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

template <> struct Schema<VotersRecord::Voter::KRaftVersionFeature> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.min_supported_version))
      return false;
    if (!io.Field(m.max_supported_version))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<VotersRecord::Voter> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.voter_id))
      return false;
    if (!io.Field(m.voter_directory_id))
      return false;
    if (!io.Field(m.endpoints))
      return false;
    if (!io.Field(m.k_raft_version_feature))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<VotersRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.version))
      return false;
    if (!io.Field(m.voters))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

}  // namespace detail

CodecError Encode(const KRaftVersionRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, KRaftVersionRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const VotersRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, VotersRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
