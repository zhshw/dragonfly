// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/raft/quorum/membership_history.h"

namespace dfly::cluster::raft {

struct SnapshotSlotRange {
  uint16_t first;
  uint16_t last;
  int64_t generation;
  bool operator==(const SnapshotSlotRange&) const = default;
};

struct KvSnapshotScope {
  std::string cluster_id;
  int32_t group_id;
  LogPosition cut;
  std::vector<SnapshotSlotRange> slots;
  bool operator==(const KvSnapshotScope&) const = default;
};

struct KvSnapshot {
  KvSnapshotScope scope;
  std::string rdb;
  // Frozen and incoming migration state at the same cut, including any staged import bytes.
  std::string migrations;
};

// Use a Kafka snapshot envelope with the membership at cut-1. Local capture requires a whole
// committed batch boundary. Retain the old log prefix until installation has succeeded.
absl::Status WriteKvSnapshot(const KvSnapshotScope& scope, std::string_view rdb,
                             std::string_view migrations, const MembershipHistory& membership,
                             int64_t high_watermark, int64_t now_ms, PartitionLog* log);
absl::StatusOr<KvSnapshot> ReadKvSnapshot(const PartitionLog& log, LogPosition id,
                                          std::string_view cluster_id, int32_t group_id);

// Validate a completed download before publishing it or changing the retained log.
absl::StatusOr<KvSnapshot> ReadKvSnapshot(const SnapshotWriter& writer, std::string_view cluster_id,
                                          int32_t group_id);

}  // namespace dfly::cluster::raft
