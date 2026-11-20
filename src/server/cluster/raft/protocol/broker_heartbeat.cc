// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#include "server/cluster/raft/protocol/broker_heartbeat.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<BrokerHeartbeatRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.broker_id))
      return false;
    if (!io.Field(m.broker_epoch))
      return false;
    if (!io.Field(m.current_metadata_offset))
      return false;
    if (!io.Field(m.want_fence))
      return false;
    if (!io.Field(m.want_shut_down))
      return false;
    return io.Tags(m.unknown_tags,
                   Tagged{0, 1, m.offline_log_dirs, !m.offline_log_dirs.empty(), false},
                   Tagged{1, 2, m.cordoned_log_dirs, m.cordoned_log_dirs.has_value(), false});
  }
};

template <> struct Schema<BrokerHeartbeatResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.throttle_time_ms))
      return false;
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.is_caught_up))
      return false;
    if (!io.Field(m.is_fenced))
      return false;
    if (!io.Field(m.should_shut_down))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

}  // namespace detail

CodecError Encode(const BrokerHeartbeatRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, BrokerHeartbeatRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const BrokerHeartbeatResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, BrokerHeartbeatResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
