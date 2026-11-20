// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <cstdint>
#include <optional>
#include <string_view>

#include "server/cluster/raft/quorum/election.h"

namespace dfly::cluster::raft::log_files {

absl::Status DiskError(std::string_view operation);
absl::Status ReadAt(int fd, int64_t position, char* dest, size_t size);
absl::Status WriteAt(int fd, int64_t position, std::string_view bytes);

// Per-partition, local crash-recovery checkpoint. Positions in Fetch frames are offsets.
struct DurableEnd {
  LogPosition start{0, 0};
  int64_t segment_base = 0;
  int64_t file_end = 0;
  int64_t offset = 0;
};

// One writer under the partition directory lock. Reads legacy DFE1/DFE2 checkpoints too.
class RecoveryMarker {
 public:
  RecoveryMarker() = default;
  ~RecoveryMarker();
  RecoveryMarker(const RecoveryMarker&) = delete;
  RecoveryMarker& operator=(const RecoveryMarker&) = delete;

  absl::StatusOr<std::optional<DurableEnd>> Read(int dir_fd);
  absl::Status Publish(int dir_fd, const DurableEnd& end);

 private:
  int fd_ = -1;
};

}  // namespace dfly::cluster::raft::log_files
