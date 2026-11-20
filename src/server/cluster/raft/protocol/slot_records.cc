// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Dragonfly cluster extension schemas by generate.py.

#include "server/cluster/raft/protocol/slot_records.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<RedisNodeRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.broker_id))
      return false;
    if (!io.Field(m.redis_node_id))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<DataGroupRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.group_id))
      return false;
    if (!io.Field(m.members))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<GroupLeaderRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.group_id))
      return false;
    if (!io.Field(m.leader_id))
      return false;
    if (!io.Field(m.leader_epoch))
      return false;
    if (!io.Field(m.voter_config_offset))
      return false;
    if (!io.Field(m.reporter_id))
      return false;
    if (!io.Field(m.reporter_epoch))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<SlotAssignmentRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.first_slot))
      return false;
    if (!io.Field(m.last_slot))
      return false;
    if (!io.Field(m.expected_group))
      return false;
    if (!io.Field(m.expected_generation))
      return false;
    if (!io.Field(m.group_id))
      return false;
    if (!io.Field(m.generation))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<SlotMigrationRecord::AdminRequestData> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.request_id))
      return false;
    if (!io.Field(m.value))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<SlotMigrationRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
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
    if (!io.Field(m.phase))
      return false;
    if (!io.Field(m.source_epoch))
      return false;
    if (!io.Field(m.source_end_offset))
      return false;
    if (!io.Field(m.target_epoch))
      return false;
    if (!io.Field(m.target_end_offset))
      return false;
    if (!io.Field(m.digest))
      return false;
    if (!io.Field(m.source_finished))
      return false;
    if (!io.Field(m.target_finished))
      return false;
    return io.Tags(m.unknown_tags, Tagged{0, 0, m.admin_request,
                                          m.admin_request != decltype(m.admin_request){}, false});
  }
};

}  // namespace detail

CodecError Encode(const RedisNodeRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, RedisNodeRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const DataGroupRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, DataGroupRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const GroupLeaderRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, GroupLeaderRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const SlotAssignmentRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, SlotAssignmentRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const SlotMigrationRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, SlotMigrationRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
