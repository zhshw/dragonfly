// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Dragonfly cluster extension schemas by generate.py.

#include "server/cluster/raft/protocol/slot_transfer_record.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<SlotTransferRecord> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.version))
      return false;
    if (!io.Field(m.kind))
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
    if (!io.Field(m.position))
      return false;
    if (!io.Field(m.source_epoch))
      return false;
    if (!io.Field(m.source_end_offset))
      return false;
    if (!io.Field(m.digest))
      return false;
    if (!io.Field(m.bytes))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

}  // namespace detail

CodecError Encode(const SlotTransferRecord& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, SlotTransferRecord* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
