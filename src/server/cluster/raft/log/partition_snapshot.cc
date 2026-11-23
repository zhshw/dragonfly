// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_format.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>

#include "server/cluster/raft/log/log_files.h"
#include "server/cluster/raft/log/partition_log.h"

namespace dfly::cluster::raft {

using namespace std;
using namespace log_files;

std::optional<LogPosition> PartitionLog::SnapshotAtOrBefore(int64_t end_offset) const {
  auto it = std::upper_bound(snapshots_.begin(), snapshots_.end(), end_offset,
                             [](int64_t offset, LogPosition id) { return offset < id.end_offset; });
  return it == snapshots_.begin() ? std::nullopt : std::optional{*std::prev(it)};
}

optional<LogPosition> PartitionLog::LatestSnapshot() const {
  if (snapshots_.empty())
    return nullopt;
  return snapshots_.back();
}

FetchPosition PartitionLog::ValidateOffsetAndEpoch(int64_t offset, int32_t epoch) const {
  using Kind = FetchPosition::Kind;
  if (StartOffset() == 0 && offset == 0)
    return {Kind::kValid, {0, 0}};
  if (!snapshots_.empty()) {
    const LogPosition earliest = snapshots_.front();
    if (offset < StartOffset() || (offset == StartOffset() && epoch != earliest.epoch) ||
        epoch < earliest.epoch)
      return {Kind::kSnapshot, snapshots_.back()};
  }
  LogPosition end = EndForEpoch(epoch);
  if (end.epoch != epoch || end.end_offset < offset)
    return {Kind::kDiverging, end};
  return {Kind::kValid, {epoch, offset}};
}

absl::StatusOr<unique_ptr<SnapshotWriter>> PartitionLog::BeginSnapshot(LogPosition id,
                                                                       int64_t size) const {
  if (!io_failure_.ok())
    return io_failure_;
  if (binary_search(snapshots_.begin(), snapshots_.end(), id))
    return absl::AlreadyExistsError("Kafka snapshot already installed");
  return SnapshotWriter::Create(dir_, id, size);
}

bool PartitionLog::IsLocalBoundary(LogPosition id) const {
  if (id == start_)
    return true;
  auto it = lower_bound(batches_.begin(), batches_.end(), id.end_offset,
                        [](const Batch& b, int64_t offset) { return b.end.end_offset < offset; });
  return it != batches_.end() && it->end == id;
}

absl::Status PartitionLog::InstallSnapshot(SnapshotWriter* writer, int64_t hw,
                                           const SnapshotValidator& validate) {
  if (!io_failure_.ok())
    return io_failure_;
  if (AppendPending())
    return absl::FailedPreconditionError("Drain quorum append before installing snapshot");
  if (!writer || hw < 0 || hw > End().end_offset)
    return absl::InvalidArgumentError("Invalid Kafka snapshot installation or high watermark");
  struct stat source, target;
  if (fstat(writer->dir_fd_, &source) == -1 || fstat(dir_fd_, &target) == -1)
    return DiskError("Checking snapshot partition directory");
  if (source.st_dev != target.st_dev || source.st_ino != target.st_ino)
    return absl::InvalidArgumentError("Kafka snapshot belongs to a different partition");
  LogPosition id = writer->id();
  if (binary_search(snapshots_.begin(), snapshots_.end(), id))
    return absl::OkStatus();
  bool resets_log = id > End();
  if (id.end_offset < StartOffset() ||
      (resets_log && (id.end_offset < hw || (hw > 0 && id.end_offset == hw))) ||
      (!resets_log && (id.end_offset > hw || !IsLocalBoundary(id))))
    return absl::InvalidArgumentError(
        "Kafka snapshot conflicts with retained log or committed prefix");
  auto status = writer->Freeze(validate);
  if (!status.ok()) {
    if (writer->published_)
      io_failure_ = status;
    return status;
  }
  snapshots_.insert(lower_bound(snapshots_.begin(), snapshots_.end(), id), id);
  if (resets_log)
    return io_failure_ = ResetToSnapshot(id);
  return absl::OkStatus();
}

absl::StatusOr<SnapshotChunk> PartitionLog::ReadSnapshot(LogPosition id, int64_t position,
                                                         size_t max_bytes) const {
  if (!io_failure_.ok())
    return io_failure_;
  if (!binary_search(snapshots_.begin(), snapshots_.end(), id))
    return absl::NotFoundError("Kafka snapshot ID is unavailable");
  return ReadSnapshotChunk(dir_, id, position, max_bytes);
}

absl::Status PartitionLog::RemovePrefix(int64_t first_base) {
  auto first = lower_bound(segment_bases_.begin(), segment_bases_.end(), first_base);
  for (auto it = segment_bases_.begin(); it != first; ++it) {
    if (unlinkat(dir_fd_, absl::StrFormat("%020d.log", *it).c_str(), 0) == -1)
      return DiskError("Removing old quorum log segment");
  }
  segment_bases_.erase(segment_bases_.begin(), first);
  auto snapshot = lower_bound(snapshots_.begin(), snapshots_.end(), start_);
  for (auto it = snapshots_.begin(); it != snapshot; ++it) {
    if (unlinkat(dir_fd_, SnapshotName(*it).c_str(), 0) == -1)
      return DiskError("Removing obsolete Kafka snapshot");
  }
  snapshots_.erase(snapshots_.begin(), snapshot);
  if (fsync(dir_fd_) == -1)
    return DiskError("Syncing retained quorum files");
  return absl::OkStatus();
}

absl::Status PartitionLog::ResetToSnapshot(LogPosition id) {
  // Commit the empty extent first. Recovery can finish replacing even a segment with the
  // same filename from an older epoch, regardless of which physical files survived.
  if (auto s = marker_.Publish(dir_fd_, {id, id.end_offset, 0, id.end_offset}); !s.ok())
    return s;
  if (auto s = SetActive(id.end_offset, O_CREAT); !s.ok())
    return s;
  if (ftruncate(log_fd_, 0) == -1 || fsync(log_fd_) == -1)
    return DiskError("Resetting quorum log to installed snapshot");
  auto base = lower_bound(segment_bases_.begin(), segment_bases_.end(), id.end_offset);
  if (base == segment_bases_.end() || *base != id.end_offset)
    segment_bases_.insert(base, id.end_offset);
  start_ = id;
  active_bytes_ = 0;
  batches_.clear();
  if (auto s = RemoveSuffixSegments(); !s.ok())
    return s;
  return RemovePrefix(active_base_);
}

absl::Status PartitionLog::DeleteBeforeSnapshot(LogPosition id, int64_t hw) {
  if (AppendPending())
    return absl::FailedPreconditionError("Drain quorum append before deleting prefix");
  if (!io_failure_.ok())
    return io_failure_;
  if (!binary_search(snapshots_.begin(), snapshots_.end(), id))
    return absl::NotFoundError("No Kafka snapshot at requested retention boundary");
  if (id.end_offset > hw || hw > End().end_offset || !IsLocalBoundary(id))
    return absl::InvalidArgumentError("Log retention requires a committed snapshot boundary");
  if (id == start_)
    return absl::OkStatus();
  if (id == End())
    return io_failure_ = ResetToSnapshot(id);

  // Keep the physical segment containing the first readable batch, even if it starts before
  // the cut. The durable logical boundary hides its earlier batches after a crash.
  auto first =
      upper_bound(batches_.begin(), batches_.end(), id.end_offset,
                  [](int64_t offset, const Batch& b) { return offset < b.end.end_offset; });
  int64_t first_base = first->segment_base;
  io_failure_ = marker_.Publish(dir_fd_, {id, active_base_, active_bytes_, End().end_offset});
  if (!io_failure_.ok())
    return io_failure_;
  start_ = id;
  batches_.erase(batches_.begin(), first);
  return io_failure_ = RemovePrefix(first_base);
}

}  // namespace dfly::cluster::raft
