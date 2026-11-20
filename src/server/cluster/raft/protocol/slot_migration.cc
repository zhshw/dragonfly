// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Dragonfly cluster extension schemas by generate.py.

#include "server/cluster/raft/protocol/slot_migration.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<SlotMigrationRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.cluster_id))
      return false;
    if (!io.Field(m.action))
      return false;
    if (!io.Field(m.migration_id))
      return false;
    if (!io.Field(m.slot))
      return false;
    if (!io.Field(m.source_group))
      return false;
    if (!io.Field(m.target_group))
      return false;
    if (!io.Field(m.generation))
      return false;
    if (!io.Field(m.group_id))
      return false;
    if (!io.Field(m.reporter_id))
      return false;
    if (!io.Field(m.broker_epoch))
      return false;
    if (!io.Field(m.leader_epoch))
      return false;
    if (!io.Field(m.record_epoch))
      return false;
    if (!io.Field(m.record_end_offset))
      return false;
    if (!io.Field(m.high_watermark))
      return false;
    if (!io.Field(m.applied_end_offset))
      return false;
    if (!io.Field(m.expected_phase))
      return false;
    if (!io.Field(m.source_epoch))
      return false;
    if (!io.Field(m.source_end_offset))
      return false;
    if (!io.Field(m.digest))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<SlotMigrationResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.error_message))
      return false;
    if (!io.Field(m.phase))
      return false;
    if (!io.Field(m.source_finished))
      return false;
    if (!io.Field(m.target_finished))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

}  // namespace detail

CodecError Encode(const SlotMigrationRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, SlotMigrationRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const SlotMigrationResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, SlotMigrationResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
