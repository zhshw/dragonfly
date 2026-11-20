// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "server/cluster/raft/quorum/election.h"

namespace dfly::cluster::raft {

std::string SnapshotName(LogPosition id);

// Checks installed snapshots independently of any partial downloads. The caller holds
// the partition directory lock. IDs are sorted using Kafka OffsetAndEpoch ordering.
absl::StatusOr<std::vector<LogPosition>> RecoverSnapshots(const std::filesystem::path& dir,
                                                          LogPosition start);

struct SnapshotChunk {
  int64_t total_size;
  std::string bytes;
};

absl::StatusOr<SnapshotChunk> ReadSnapshotChunk(const std::filesystem::path& dir, LogPosition id,
                                                int64_t position, size_t max_bytes);

// Receives a Kafka raw snapshot, one FetchSnapshot chunk at a time. Destruction aborts
// an unfinished transfer. Only a fully validated and durable file gets an immutable name.
class SnapshotWriter {
 public:
  ~SnapshotWriter();
  SnapshotWriter(const SnapshotWriter&) = delete;
  SnapshotWriter& operator=(const SnapshotWriter&) = delete;

  int64_t position() const {
    return position_;
  }
  LogPosition id() const {
    return id_;
  }
  int64_t size() const {
    return size_;
  }

  absl::Status Append(int64_t position, std::string_view bytes);

 private:
  friend class PartitionLog;
  static absl::StatusOr<std::unique_ptr<SnapshotWriter>> Create(const std::filesystem::path& dir,
                                                                LogPosition id, int64_t size);
  absl::Status Freeze();
  SnapshotWriter(LogPosition id, int64_t size) : id_(id), size_(size) {
  }
  LogPosition id_;
  int64_t size_;
  int64_t position_ = 0;
  int dir_fd_ = -1;
  int fd_ = -1;
  std::string temp_;
  bool published_ = false;
};

}  // namespace dfly::cluster::raft
