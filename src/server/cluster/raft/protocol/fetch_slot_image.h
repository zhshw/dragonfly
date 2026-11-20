// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Dragonfly cluster extension schemas by generate.py.

#pragma once

#include <optional>
#include <string_view>
#include <vector>

#include "server/cluster/raft/protocol/wire.h"

namespace dfly::cluster::raft::protocol {

struct FetchSlotImageRequest {
  static constexpr int16_t kApiKey = 20001;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 2;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  std::string cluster_id{};
  Uuid migration_id{};
  uint16_t slot{};
  int32_t source_group{};
  int32_t target_group{};
  int64_t generation{};
  int64_t position{};
  int32_t max_bytes{};
  TaggedFields unknown_tags;
  bool operator==(const FetchSlotImageRequest&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const FetchSlotImageRequest& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, FetchSlotImageRequest* out);

struct FetchSlotImageResponse {
  static constexpr int16_t kApiKey = 20001;
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;
  static constexpr int16_t kHeaderVersion = 1;
  static constexpr int16_t HeaderVersion(int16_t) {
    return kHeaderVersion;
  }

  int16_t error_code{};
  int32_t source_epoch = -1;
  int64_t source_end_offset = -1;
  int64_t total_size{};
  RecordBytes bytes{};
  TaggedFields unknown_tags;
  bool operator==(const FetchSlotImageResponse&) const = default;
};

// Encode or decode one complete body without the RPC header or frame length.
// On failure the destination keeps its previous value.
CodecError Encode(const FetchSlotImageResponse& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, FetchSlotImageResponse* out);

}  // namespace dfly::cluster::raft::protocol
