// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/log/partition_log.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <charconv>

#include "server/cluster/raft/log/log_files.h"
#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace fs = filesystem;
using namespace log_files;

string SegmentName(int64_t base) {
  return absl::StrFormat("%020d.log", base);
}
}  // namespace

PartitionLog::~PartitionLog() {
  if (log_fd_ >= 0)
    close(log_fd_);
  if (dir_fd_ >= 0)
    close(dir_fd_);
}

absl::StatusOr<std::unique_ptr<PartitionLog>> PartitionLog::Open(const filesystem::path& dir,
                                                                 size_t segment_bytes) {
  if (segment_bytes == 0 || segment_bytes > INT32_MAX)
    return absl::InvalidArgumentError("Invalid quorum segment size");
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
  unique_ptr<PartitionLog> log(new PartitionLog(dir, segment_bytes));
  log->dir_fd_ = open(dir.c_str(), O_DIRECTORY | O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (log->dir_fd_ == -1)
    return DiskError("Opening quorum log directory");
  if (flock(log->dir_fd_, LOCK_EX | LOCK_NB) != 0)
    return DiskError("Acquiring quorum log directory lock");

  if (auto status = log->Recover(); !status.ok())
    return status;
  return log;
}

absl::Status PartitionLog::SetActive(int64_t base, int flags) {
  int fd =
      openat(dir_fd_, SegmentName(base).c_str(), O_RDWR | O_CLOEXEC | O_NOFOLLOW | flags, 0644);
  if (fd < 0)
    return DiskError("Opening quorum log segment");
  if ((flags & O_CREAT) && fsync(dir_fd_) == -1) {
    auto status = DiskError("Syncing new quorum log segment name");
    close(fd);
    return status;
  }
  if (log_fd_ >= 0)
    close(log_fd_);
  log_fd_ = fd;
  active_base_ = base;
  return absl::OkStatus();
}

absl::Status PartitionLog::Recover() {
  auto stored = ReadMarker(dir_fd_);
  if (!stored.ok())
    return stored.status();
  auto marker = *stored;
  if (marker && marker->start != LogPosition{0, 0})
    return absl::FailedPreconditionError("A nonzero log start requires a recovered snapshot");
  error_code ec;
  fs::directory_iterator files(dir_, ec);
  for (; !ec && files != fs::directory_iterator(); files.increment(ec)) {
    const fs::path& path = files->path();
    if (path.extension() == ".checkpoint")
      return absl::FailedPreconditionError(
          "Snapshot must be recovered before opening the quorum log");
    if (path.extension() != ".log")
      continue;
    string stem = path.stem().string();
    int64_t base;
    auto [end, err] = from_chars(stem.data(), stem.data() + stem.size(), base);
    if (stem.size() != 20 || err != errc{} || end != stem.data() + stem.size() || base < 0)
      return absl::DataLossError("Invalid Kafka segment filename");
    segment_bases_.push_back(base);
  }
  if (ec)
    return absl::ErrnoToStatus(ec.value(), "Listing quorum log segments");
  sort(segment_bases_.begin(), segment_bases_.end());
  if (segment_bases_.empty()) {
    if (marker)
      return absl::DataLossError("Missing durable quorum log segment");
    if (auto s = SetActive(0, O_CREAT | O_EXCL); !s.ok())
      return s;
    segment_bases_.push_back(0);
  }
  if (segment_bases_.front() != 0 ||
      (marker &&
       !binary_search(segment_bases_.begin(), segment_bases_.end(), marker->segment_base)))
    return absl::DataLossError("Missing first or active quorum segment");
  bool found_end = false;
  for (int64_t base : segment_bases_) {
    if (found_end)
      break;
    if (End().end_offset != base)
      return absl::DataLossError("Gap between quorum log segments");
    if (auto s = SetActive(base, 0); !s.ok())
      return s;
    found_end = marker && marker->segment_base == base;
    if (auto s = RecoverSegment(found_end ? optional<int64_t>(marker->file_end) : nullopt); !s.ok())
      return s;

    // Raw Kafka segments may predate our first recovery marker.
    if (!marker && fsync(log_fd_) == -1)
      return DiskError("Syncing imported quorum segment");
  }
  if (marker && marker->offset != End().end_offset)
    return absl::DataLossError("Quorum log durable offset disagrees with batches");
  if (!marker)
    return PublishEnd(active_bytes_, End().end_offset);
  return RemoveSuffixSegments();
}

absl::Status PartitionLog::RemoveSuffixSegments() {
  while (segment_bases_.back() > active_base_) {
    if (unlinkat(dir_fd_, SegmentName(segment_bases_.back()).c_str(), 0) != 0)
      return DiskError("Removing quorum log suffix segment");
    segment_bases_.pop_back();
  }
  if (fsync(dir_fd_) != 0)
    return DiskError("Syncing quorum log directory");
  return absl::OkStatus();
}

absl::Status PartitionLog::RecoverSegment(optional<int64_t> durable_bytes) {
  struct stat st;
  if (fstat(log_fd_, &st) != 0)
    return DiskError("Stat quorum log segment");
  if (!S_ISREG(st.st_mode))
    return absl::DataLossError("Quorum log segment is not a regular file");
  int64_t file_end = durable_bytes.value_or(st.st_size);
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
    batches_.push_back({info->end, active_base_, pos - static_cast<int64_t>(*size), pos});
  }
  if (st.st_size != file_end) {
    if (ftruncate(log_fd_, file_end) == -1 || fsync(log_fd_) == -1)
      return DiskError("Removing incomplete quorum log append");
  }
  active_bytes_ = file_end;
  return absl::OkStatus();
}

absl::Status PartitionLog::PublishEnd(int64_t file_end, int64_t offset) {
  return PublishMarker(dir_fd_, {{0, 0}, active_base_, file_end, offset});
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
  int64_t pos = 0;
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
    pending.push_back({end, 0, pos - static_cast<int64_t>(*size), pos});
    remaining.remove_prefix(*size);
  }
  if (pending.empty())
    return absl::InvalidArgumentError("No complete batch to append");
  const size_t length = bytes.size() - remaining.size();
  if (active_bytes_ > 0 && (static_cast<uint64_t>(active_bytes_) + length > segment_bytes_ ||
                            end.end_offset - active_base_ > INT32_MAX)) {
    auto status = SetActive(End().end_offset, O_CREAT | O_EXCL);
    if (!status.ok())
      return io_failure_ = status;
    segment_bases_.push_back(active_base_);
    active_bytes_ = 0;
  }
  for (Batch& batch : pending) {
    batch.segment_base = active_base_;
    batch.file_start += active_bytes_;
    batch.file_end += active_bytes_;
  }
  pos = pending.back().file_end;
  io_failure_ = WriteAt(log_fd_, active_bytes_, bytes.substr(0, length));
  if (!io_failure_.ok())
    return io_failure_;
  if (fsync(log_fd_) != 0)
    return io_failure_ = DiskError("Syncing quorum log append");
  io_failure_ = PublishEnd(pos, end.end_offset);
  if (io_failure_.ok()) {
    batches_.insert(batches_.end(), pending.begin(), pending.end());
    active_bytes_ = pos;
  }
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

  // Kafka serves one segment per Fetch, even when the byte budget spans two.
  limit = upper_bound(first, limit, first->segment_base,
                      [](int64_t key, const Batch& b) { return key < b.segment_base; });
  int64_t start = first->file_start;
  size_t size =
      min<size_t>(prev(limit)->file_end - start, max<size_t>(max_bytes, first->file_end - start));
  string result(size, '\0');
  int fd = log_fd_;
  if (first->segment_base != active_base_) {
    fd = openat(dir_fd_, SegmentName(first->segment_base).c_str(),
                O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd == -1)
      return DiskError("Reading retained quorum segment");
  }
  absl::Cleanup cleanup = [&] {
    if (fd != log_fd_)
      close(fd);
  };
  if (auto status = ReadAt(fd, start, result.data(), result.size()); !status.ok())
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
  int64_t base = cut == batches_.begin() ? 0 : prev(cut)->segment_base;
  if (active_base_ != base) {
    io_failure_ = SetActive(base, 0);
    if (!io_failure_.ok())
      return io_failure_;
  }
  io_failure_ = PublishEnd(pos, offset);
  if (!io_failure_.ok())
    return io_failure_;

  // A crash after publishing the shorter boundary completes the truncation on recovery.
  if (ftruncate(log_fd_, pos) != 0 || fsync(log_fd_) != 0)
    return io_failure_ = DiskError("Truncating quorum log suffix");
  batches_.erase(cut, batches_.end());
  active_bytes_ = pos;
  return io_failure_ = RemoveSuffixSegments();
}

}  // namespace dfly::cluster::raft
