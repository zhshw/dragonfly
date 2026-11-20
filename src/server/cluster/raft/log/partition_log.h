// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "server/cluster/raft/quorum/election.h"

namespace dfly::cluster::raft {

// One writer per partition. Open and mutation are synchronous disk-worker operations.
// Each successful append is durable before its resulting LEO can be reported to a peer.
class PartitionLog {
 public:
  static absl::StatusOr<std::unique_ptr<PartitionLog>> Open(const std::filesystem::path& dir,
                                                            size_t segment_bytes = 1U << 30);
  ~PartitionLog();
  PartitionLog(const PartitionLog&) = delete;
  PartitionLog& operator=(const PartitionLog&) = delete;

  const absl::Status& io_failure() const {
    return io_failure_;
  }

  LogPosition End() const;
  LogPosition EndForEpoch(int32_t requested_epoch) const;

  // Leader appends contain whole batches of this epoch; follower appends may include
  // earlier epochs and a partial trailing batch from Fetch.
  absl::Status Append(std::string_view batches, int32_t leader_epoch);
  absl::Status AppendAsFollower(std::string_view batches, int32_t fetch_epoch);

  absl::Status AppendLeaderChange(int32_t epoch, int64_t timestamp_ms, int32_t leader_id,
                                  std::span<const int32_t> voters,
                                  std::span<const int32_t> granting_voters);

  // Bound a read by LEO (uncommitted Fetch) or by the exclusive HW (committed replay).
  absl::StatusOr<std::string> Read(int64_t offset, int64_t max_offset, size_t max_bytes) const;

  // Truncation is permitted only at complete batch boundaries at or above the group HW.
  absl::Status TruncateTo(int64_t offset, int64_t high_watermark);

 private:
  struct Batch {
    LogPosition end;
    int64_t segment_base;
    int64_t file_start;
    int64_t file_end;
  };

  PartitionLog(std::filesystem::path dir, size_t segment_bytes)
      : dir_(std::move(dir)), segment_bytes_(segment_bytes) {
  }
  absl::Status Recover();
  absl::Status RecoverSegment(std::optional<int64_t> durable_bytes);
  absl::Status SetActive(int64_t base, int flags);
  absl::Status RemoveSuffixSegments();
  absl::Status AppendBatches(std::string_view batches, int32_t epoch, bool follower);
  absl::Status PublishEnd(int64_t file_end, int64_t end_offset);

  std::filesystem::path dir_;
  int dir_fd_ = -1;
  int log_fd_ = -1;
  size_t segment_bytes_;
  int64_t active_base_ = 0;
  int64_t active_bytes_ = 0;
  std::vector<int64_t> segment_bases_;
  std::vector<Batch> batches_;
  absl::Status io_failure_;
};

}  // namespace dfly::cluster::raft
