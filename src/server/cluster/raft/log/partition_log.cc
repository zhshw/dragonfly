// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/log/partition_log.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/crc/crc32c.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>

#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace fs = filesystem;
namespace be = absl::big_endian;

constexpr char kSegment[] = "00000000000000000000.log";

// Private local recovery boundary. Only .log bytes travel in Fetch. The marker
// distinguishes a torn local append from damage within a previously durable batch.
constexpr char kDurableEnd[] = "dragonfly-log-end";
constexpr char kTemp[] = "dragonfly-log-end.tmp";
constexpr size_t kMarkerSize = 24;

absl::Status DiskError(string_view op) {
  return absl::ErrnoToStatus(errno, op);
}

absl::Status ReadAt(int fd, int64_t position, char* dest, size_t size) {
  while (size > 0) {
    ssize_t count = pread(fd, dest, size, position);
    if (count == -1 && errno == EINTR)
      continue;
    if (count < 0)
      return DiskError("Reading quorum log");
    if (count == 0)
      return absl::DataLossError("Truncated quorum log file");
    position += count;
    dest += count;
    size -= count;
  }
  return absl::OkStatus();
}

absl::Status WriteAt(int fd, int64_t position, string_view data) {
  while (!data.empty()) {
    ssize_t count = pwrite(fd, data.data(), data.size(), position);
    if (count == -1 && errno == EINTR)
      continue;
    if (count < 0)
      return DiskError("Writing quorum log");
    if (count == 0)
      return absl::InternalError("Zero-length quorum log write");
    position += count;
    data.remove_prefix(count);
  }
  return absl::OkStatus();
}

struct DurableEnd {
  int64_t file_end;
  int64_t offset;
};

absl::StatusOr<optional<DurableEnd>> ReadMarker(int dir_fd) {
  int fd = openat(dir_fd, kDurableEnd, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (fd == -1) {
    if (errno == ENOENT)
      return nullopt;
    return DiskError("Opening quorum log recovery marker");
  }
  absl::Cleanup cleanup = [fd] { close(fd); };
  struct stat st;
  if (fstat(fd, &st) != 0)
    return DiskError("Stat quorum log recovery marker");
  if (st.st_size != kMarkerSize)
    return absl::DataLossError("Invalid quorum log recovery marker length");
  string marker(kMarkerSize, '\0');
  if (auto status = ReadAt(fd, 0, marker.data(), marker.size()); !status.ok())
    return status;
  uint32_t crc = static_cast<uint32_t>(absl::ComputeCrc32c(string_view(marker).substr(0, 20)));
  if (!marker.starts_with("DFE1") || be::Load32(marker.data() + 20) != crc)
    return absl::DataLossError("Invalid quorum log recovery marker");
  return DurableEnd{static_cast<int64_t>(be::Load64(marker.data() + 4)),
                    static_cast<int64_t>(be::Load64(marker.data() + 12))};
}
}  // namespace

PartitionLog::~PartitionLog() {
  if (log_fd_ >= 0)
    close(log_fd_);
  if (dir_fd_ >= 0)
    close(dir_fd_);
}

absl::StatusOr<std::unique_ptr<PartitionLog>> PartitionLog::Open(const filesystem::path& dir) {
  if (mkdir(dir.c_str(), 0755) == -1) {
    if (errno != EEXIST)
      return DiskError("Creating quorum log directory");
  } else {
    int parent = open((dir / "..").c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (parent == -1)
      return DiskError("Opening quorum log parent");
    absl::Cleanup cleanup = [parent] { close(parent); };
    if (fsync(parent) == -1)
      return DiskError("Syncing quorum log parent");
  }
  unique_ptr<PartitionLog> log(new PartitionLog(dir));
  log->dir_fd_ = open(dir.c_str(), O_DIRECTORY | O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (log->dir_fd_ == -1)
    return DiskError("Opening quorum log directory");
  if (flock(log->dir_fd_, LOCK_EX | LOCK_NB) != 0)
    return DiskError("Acquiring quorum log directory lock");

  error_code ec;
  fs::directory_iterator files(dir, ec);
  for (; !ec && files != fs::directory_iterator(); files.increment(ec)) {
    const fs::path& path = files->path();
    if (path.extension() == ".checkpoint")
      return absl::FailedPreconditionError(
          "Snapshot must be recovered before opening the quorum log");
    if (path.extension() == ".log" && path.filename() != kSegment)
      return absl::FailedPreconditionError(
          "Multiple or nonzero-base segments require recovery support");
  }
  if (ec)
    return absl::ErrnoToStatus(ec.value(), "Listing quorum log segments");
  log->log_fd_ = openat(log->dir_fd_, kSegment, O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0644);
  if (log->log_fd_ == -1)
    return DiskError("Opening quorum log segment");
  if (auto status = log->Recover(); !status.ok())
    return status;
  return log;
}

absl::Status PartitionLog::Recover() {
  auto marker = ReadMarker(dir_fd_);
  if (!marker.ok())
    return marker.status();
  struct stat st;
  if (fstat(log_fd_, &st) != 0)
    return DiskError("Stat quorum log segment");
  if (!S_ISREG(st.st_mode))
    return absl::DataLossError("Quorum log segment is not a regular file");
  int64_t file_end = *marker ? (*marker)->file_end : st.st_size;
  if (file_end < 0 || file_end > st.st_size)
    return absl::DataLossError("Quorum log shorter than its durable boundary");
  int64_t pos = 0;
  while (pos < file_end) {
    char prefix[12];
    if (file_end - pos < static_cast<int64_t>(sizeof(prefix)))
      return absl::DataLossError("Incomplete durable batch prefix");
    if (auto status = ReadAt(log_fd_, pos, prefix, sizeof(prefix)); !status.ok())
      return status;
    auto size = RecordBatchSize(string_view(prefix, sizeof(prefix)));
    if (!size.ok() || *size > static_cast<uint64_t>(file_end - pos))
      return absl::DataLossError("Incomplete or invalid durable batch");
    string batch(*size, '\0');
    if (auto status = ReadAt(log_fd_, pos, batch.data(), batch.size()); !status.ok())
      return status;
    auto info = DecodeRecordBatch(batch);
    if (!info.ok())
      return info.status();
    if (info->base_offset != End().end_offset || info->end.epoch < End().epoch)
      return absl::DataLossError("Quorum log has an offset gap or regressing epoch");
    pos += *size;
    batches_.push_back({info->end, pos});
  }
  if (*marker && (*marker)->offset != End().end_offset)
    return absl::DataLossError("Quorum log durable offset disagrees with stored batches");
  if (st.st_size != file_end) {
    if (ftruncate(log_fd_, file_end) == -1 || fsync(log_fd_) == -1)
      return DiskError("Removing incomplete quorum log append");
  }
  return *marker ? absl::OkStatus() : PublishEnd(file_end, End().end_offset);
}

absl::Status PartitionLog::PublishEnd(int64_t file_end, int64_t offset) {
  string marker(kMarkerSize, '\0');
  marker.replace(0, 4, "DFE1");
  be::Store64(marker.data() + 4, file_end);
  be::Store64(marker.data() + 12, offset);
  be::Store32(marker.data() + 20,
              static_cast<uint32_t>(absl::ComputeCrc32c(string_view(marker).substr(0, 20))));
  int fd = openat(dir_fd_, kTemp, O_WRONLY | O_TRUNC | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0644);
  if (fd == -1)
    return DiskError("Opening temporary quorum log recovery marker");
  absl::Cleanup cleanup = [fd] { close(fd); };
  if (auto status = WriteAt(fd, 0, marker); !status.ok())
    return status;
  if (fsync(fd) == -1)
    return DiskError("Syncing quorum log recovery marker");
  std::move(cleanup).Cancel();
  if (close(fd) == -1 || renameat(dir_fd_, kTemp, dir_fd_, kDurableEnd) == -1 ||
      fsync(dir_fd_) == -1)
    return DiskError("Installing quorum log recovery marker");
  return absl::OkStatus();
}

LogPosition PartitionLog::End() const {
  return batches_.empty() ? LogPosition{0, 0} : batches_.back().end;
}

LogPosition PartitionLog::EndForEpoch(int32_t epoch) const {
  if (epoch == -1)
    return End();
  auto next = upper_bound(batches_.begin(), batches_.end(), epoch,
                          [](int32_t term, const Batch& b) { return term < b.end.epoch; });
  if (next == batches_.end())
    return End();
  return next == batches_.begin() ? LogPosition{epoch, 0} : prev(next)->end;
}

absl::Status PartitionLog::Append(string_view batches, int32_t epoch) {
  return AppendBatches(batches, epoch, false);
}

absl::Status PartitionLog::AppendAsFollower(string_view batches, int32_t epoch) {
  return AppendBatches(batches, epoch, true);
}

absl::Status PartitionLog::AppendBatches(string_view bytes, int32_t epoch, bool follower) {
  if (!io_failure_.ok())
    return io_failure_;
  vector<Batch> pending;
  LogPosition end = End();
  int64_t pos = batches_.empty() ? 0 : batches_.back().file_end;
  string_view remaining = bytes;
  while (!remaining.empty()) {
    if (follower && remaining.size() < 12)
      break;
    auto size = RecordBatchSize(remaining);
    if (!size.ok())
      return size.status();
    if (follower && *size > remaining.size())
      break;
    auto batch = DecodeRecordBatch(remaining.substr(0, *size));
    if (!batch.ok())
      return batch.status();
    if (follower && batch->end.epoch > epoch)
      break;
    if (batch->base_offset != end.end_offset || batch->end.epoch < end.epoch ||
        (!follower && batch->end.epoch != epoch))
      return absl::InvalidArgumentError("Append does not match quorum log offset or epoch");
    if (*size > static_cast<uint64_t>(INT64_MAX - pos))
      return absl::OutOfRangeError("Quorum log file offset overflow");
    pos += *size;
    end = batch->end;
    pending.push_back({end, pos});
    remaining.remove_prefix(*size);
  }
  if (pending.empty())
    return absl::InvalidArgumentError("No complete batch to append");
  const size_t length = bytes.size() - remaining.size();
  io_failure_ = WriteAt(log_fd_, pos - static_cast<int64_t>(length), bytes.substr(0, length));
  if (!io_failure_.ok())
    return io_failure_;
  if (fsync(log_fd_) != 0)
    return io_failure_ = DiskError("Syncing quorum log append");
  io_failure_ = PublishEnd(pos, end.end_offset);
  if (io_failure_.ok())
    batches_.insert(batches_.end(), pending.begin(), pending.end());
  return io_failure_;
}

absl::Status PartitionLog::AppendLeaderChange(int32_t epoch, int64_t timestamp_ms, int32_t leader,
                                              span<const int32_t> voters,
                                              span<const int32_t> granting_voters) {
  if (!batches_.empty() && epoch <= End().epoch)
    return absl::InvalidArgumentError("LeaderChange must begin a new epoch");
  auto bytes =
      MakeLeaderChangeBatch(End().end_offset, epoch, timestamp_ms, leader, voters, granting_voters);
  return bytes.ok() ? Append(*bytes, epoch) : bytes.status();
}

absl::StatusOr<string> PartitionLog::Read(int64_t offset, int64_t max_offset,
                                          size_t max_bytes) const {
  if (!io_failure_.ok())
    return io_failure_;
  if (offset < 0 || offset > End().end_offset || max_offset < 0 || max_offset > End().end_offset)
    return absl::OutOfRangeError("Read offset outside quorum log");
  if (offset >= max_offset)
    return string{};
  auto first = upper_bound(batches_.begin(), batches_.end(), offset,
                           [](int64_t key, const Batch& b) { return key < b.end.end_offset; });
  auto limit = upper_bound(first, batches_.end(), max_offset,
                           [](int64_t key, const Batch& b) { return key < b.end.end_offset; });
  if (first == limit)
    return string{};
  int64_t start = first == batches_.begin() ? 0 : prev(first)->file_end;
  size_t size =
      min<size_t>(prev(limit)->file_end - start, max<size_t>(max_bytes, first->file_end - start));
  string result(size, '\0');
  if (auto status = ReadAt(log_fd_, start, result.data(), result.size()); !status.ok())
    return status;
  return result;
}

absl::Status PartitionLog::TruncateTo(int64_t offset, int64_t hw) {
  if (!io_failure_.ok())
    return io_failure_;
  if (hw < 0 || offset < hw || offset > End().end_offset)
    return absl::InvalidArgumentError("Truncation outside uncommitted quorum log suffix");
  if (offset == End().end_offset)
    return absl::OkStatus();
  auto cut = upper_bound(batches_.begin(), batches_.end(), offset,
                         [](int64_t key, const Batch& b) { return key < b.end.end_offset; });
  if (offset != 0 && (cut == batches_.begin() || prev(cut)->end.end_offset != offset))
    return absl::InvalidArgumentError("Truncation splits a quorum log batch");
  int64_t pos = cut == batches_.begin() ? 0 : prev(cut)->file_end;
  io_failure_ = PublishEnd(pos, offset);
  if (!io_failure_.ok())
    return io_failure_;

  // A crash after publishing the shorter boundary completes the truncation on recovery.
  if (ftruncate(log_fd_, pos) != 0 || fsync(log_fd_) != 0)
    return io_failure_ = DiskError("Truncating quorum log suffix");
  batches_.erase(cut, batches_.end());
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
