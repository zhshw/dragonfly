// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/broker_registration.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<BrokerRegistrationRequest::Listener> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.name))
      return false;
    if (!io.Field(m.host))
      return false;
    if (!io.Field(m.port))
      return false;
    if (!io.Field(m.security_protocol))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<BrokerRegistrationRequest::Feature> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.name))
      return false;
    if (!io.Field(m.min_supported_version))
      return false;
    if (!io.Field(m.max_supported_version))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<BrokerRegistrationRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.broker_id))
      return false;
    if (!io.Field(m.cluster_id))
      return false;
    if (!io.Field(m.incarnation_id))
      return false;
    if (!io.Field(m.listeners))
      return false;
    if (!io.Field(m.features))
      return false;
    if (!io.Field(m.rack))
      return false;
    if (io.version >= 1) {
      if (!io.Field(m.is_migrating_zk_broker))
        return false;
    } else if (!io.Absent(m.is_migrating_zk_broker)) {
      return false;
    }
    if (io.version >= 2) {
      if (!io.Field(m.log_dirs))
        return false;
    }
    if (io.version >= 3) {
      if (!io.Field(m.previous_broker_epoch))
        return false;
    }
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<BrokerRegistrationResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.throttle_time_ms))
      return false;
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.broker_epoch))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

}  // namespace detail

CodecError Encode(const BrokerRegistrationRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, BrokerRegistrationRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const BrokerRegistrationResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, BrokerRegistrationResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
