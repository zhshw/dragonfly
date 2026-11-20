// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Dragonfly cluster extension schemas by generate.py.

#include "server/cluster/raft/protocol/fetch_slot_image.h"

#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft::protocol {
namespace detail {

template <> struct Schema<FetchSlotImageRequest> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.cluster_id))
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
    if (!io.Field(m.max_bytes))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

template <> struct Schema<FetchSlotImageResponse> {
  template <typename IO, typename T> static bool Visit(IO& io, T& m) {
    if (!io.Field(m.error_code))
      return false;
    if (!io.Field(m.source_epoch))
      return false;
    if (!io.Field(m.source_end_offset))
      return false;
    if (!io.Field(m.total_size))
      return false;
    if (!io.Field(m.bytes))
      return false;
    return io.Tags(m.unknown_tags);
  }
};

}  // namespace detail

CodecError Encode(const FetchSlotImageRequest& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, FetchSlotImageRequest* out) {
  return detail::DecodeMessage(bytes, version, out);
}

CodecError Encode(const FetchSlotImageResponse& message, int16_t version, std::string* out) {
  return detail::EncodeMessage(message, version, out);
}

CodecError Decode(std::string_view bytes, int16_t version, FetchSlotImageResponse* out) {
  return detail::DecodeMessage(bytes, version, out);
}

}  // namespace dfly::cluster::raft::protocol
