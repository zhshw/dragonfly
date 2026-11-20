// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//
// Generated from Kafka message schemas by generate.py.

#pragma once

#include <optional>
#include <string_view>
#include <vector>

#include "server/cluster/raft/protocol/wire.h"

namespace dfly::cluster::raft::protocol {

struct KRaftVersionRecord {
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int16_t version{};
  int16_t k_raft_version{};
  TaggedFields unknown_tags;
  bool operator==(const KRaftVersionRecord&) const = default;
};

// Encode or decode one control record value, including its embedded version.
// On failure the destination keeps its previous value.
CodecError Encode(const KRaftVersionRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, KRaftVersionRecord* out);

struct VotersRecord {
  static constexpr int16_t kMinVersion = 0;
  static constexpr int16_t kMaxVersion = 0;
  static constexpr int16_t kFirstFlexibleVersion = 0;

  int16_t version{};
  struct Voter {
    int32_t voter_id{};
    Uuid voter_directory_id{};
    struct Endpoint {
      std::string name{};
      std::string host{};
      uint16_t port{};
      TaggedFields unknown_tags;
      bool operator==(const Endpoint&) const = default;
    };

    std::vector<Endpoint> endpoints{};
    struct KRaftVersionFeature {
      int16_t min_supported_version{};
      int16_t max_supported_version{};
      TaggedFields unknown_tags;
      bool operator==(const KRaftVersionFeature&) const = default;
    };

    KRaftVersionFeature k_raft_version_feature{};
    TaggedFields unknown_tags;
    bool operator==(const Voter&) const = default;
  };

  std::vector<Voter> voters{};
  TaggedFields unknown_tags;
  bool operator==(const VotersRecord&) const = default;
};

// Encode or decode one control record value, including its embedded version.
// On failure the destination keeps its previous value.
CodecError Encode(const VotersRecord& message, int16_t version, std::string* out);
CodecError Decode(std::string_view bytes, int16_t version, VotersRecord* out);

}  // namespace dfly::cluster::raft::protocol
