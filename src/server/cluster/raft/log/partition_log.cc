// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/log/partition_log.h"

#include <absl/base/internal/endian.h>
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
                                                                 size_t segment_bytes,
                                                                 AckMode ack_mode) {
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
  unique_ptr<PartitionLog> log(new PartitionLog(dir, segment_bytes, ack_mode));
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
  auto stored = marker_.Read(dir_fd_);
  if (!stored.ok())
    return stored.status();
  auto marker = *stored;
  if (marker)
    start_ = marker->start;
  auto snapshots = RecoverSnapshots(dir_, start_);
  if (!snapshots.ok())
    return snapshots.status();
  snapshots_ = *std::move(snapshots);
  error_code ec;
  fs::directory_iterator files(dir_, ec);
  for (; !ec && files != fs::directory_iterator(); files.increment(ec)) {
    const fs::path& path = files->path();
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
  // The reset marker precedes segment replacement. Redo it after an interrupted reset.
  if (marker && marker->file_end == 0 && marker->segment_base == StartOffset() &&
      marker->EndOffset() == StartOffset() && start_ != LogPosition{0, 0}) {
    if (auto s = ResetToSnapshot(start_); !s.ok())
      return s;
  }
  if (segment_bases_.empty()) {
    if (marker)
      return absl::DataLossError("Missing durable quorum log segment");
    if (!snapshots_.empty())
      return ResetToSnapshot(snapshots_.back());
    if (auto s = SetActive(0, O_CREAT | O_EXCL); !s.ok())
      return s;
    segment_bases_.push_back(0);
  }
  if (!marker && segment_bases_.front() > 0) {
    // A native Kafka partition can retain a first segment that straddles its snapshot cut.
    auto id = find_if(snapshots_.begin(), snapshots_.end(), [&](LogPosition candidate) {
      return candidate.end_offset >= segment_bases_.front();
    });
    if (id == snapshots_.end())
      return absl::DataLossError("Missing snapshot covering the first Kafka segment");
    start_ = *id;
  }
  auto upper = upper_bound(segment_bases_.begin(), segment_bases_.end(), StartOffset());
  if (upper == segment_bases_.begin() ||
      (marker &&
       !binary_search(segment_bases_.begin(), segment_bases_.end(), marker->segment_base)))
    return absl::DataLossError("Missing first or active quorum segment");
  int64_t first = *prev(upper);
  LogPosition physical_end{first == StartOffset() ? start_.epoch : 0, first};
  bool found_end = false;
  for (int64_t base : segment_bases_) {
    if (found_end)
      break;
    if (base < first)
      continue;
    if (physical_end.end_offset != base)
      return absl::DataLossError("Gap between quorum log segments");
    if (auto s = SetActive(base, 0); !s.ok())
      return s;
    found_end = marker && marker->segment_base == base;
    if (auto s = RecoverSegment(found_end ? &*marker : nullptr, &physical_end); !s.ok())
      return s;

    // Raw Kafka segments may predate our first recovery marker.
    if (!marker && fsync(log_fd_) == -1)
      return DiskError("Syncing imported quorum segment");
  }
  if (marker && marker->EndOffset() != End().end_offset)
    return absl::DataLossError("Quorum log durable offset disagrees with batches");
  if (physical_end.end_offset < StartOffset()) {
    if (!marker && snapshots_.back() > physical_end)
      return ResetToSnapshot(snapshots_.back());
    return absl::DataLossError("Snapshot cut beyond the end of the retained Kafka segment");
  }
  if (!marker) {
    if (auto s = PublishEnd(active_bytes_, End().end_offset); !s.ok())
      return s;
  }
  if (!snapshots_.empty() && snapshots_.back() > End())
    return ResetToSnapshot(snapshots_.back());
  if (auto s = RemoveSuffixSegments(); !s.ok())
    return s;
  return RemovePrefix(first);
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

absl::Status PartitionLog::RecoverSegment(const log_files::DurableEnd* marker,
                                          LogPosition* physical_end) {
  struct stat st;
  if (fstat(log_fd_, &st) != 0)
    return DiskError("Stat quorum log segment");
  if (!S_ISREG(st.st_mode))
    return absl::DataLossError("Quorum log segment is not a regular file");
  if (marker && marker->file_end > st.st_size)
    return absl::DataLossError("Quorum log shorter than its durable boundary");
  int64_t pos = 0;
  while (true) {
    // Verify the exact checkpoint first. Its inode length can then select further *whole*
    // batches. Bytes beyond the selected offset are unpublished, even if they pass CRC.
    if (marker && pos == marker->file_end && physical_end->end_offset != marker->offset)
      return absl::DataLossError("Quorum log checkpoint offset disagrees with batches");
    if (marker && physical_end->end_offset > marker->EndOffset())
      return absl::DataLossError("Quorum log recovery boundary splits a batch");
    if (marker ? pos >= marker->file_end && physical_end->end_offset == marker->EndOffset()
               : pos == st.st_size)
      break;
    const int64_t file_end = marker && pos < marker->file_end ? marker->file_end : st.st_size;
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
    if (info->base_offset != physical_end->end_offset || info->end.epoch < physical_end->epoch)
      return absl::DataLossError("Quorum log has an offset gap or regressing epoch");
    pos += *size;
    *physical_end = info->end;
    if (info->end.end_offset <= StartOffset()) {
      if (info->end.end_offset == StartOffset() && info->end.epoch != start_.epoch)
        return absl::DataLossError("Snapshot epoch disagrees with retained segment");
      continue;
    }
    if (info->base_offset < StartOffset() || info->end.epoch < start_.epoch)
      return absl::DataLossError("Snapshot cut splits a batch or has a newer epoch than its tail");
    batches_.push_back({info->end, active_base_, pos - static_cast<int64_t>(*size), pos});
  }
  if (st.st_size != pos) {
    if (ftruncate(log_fd_, pos) == -1 || fsync(log_fd_) == -1)
      return DiskError("Removing incomplete quorum log append");
  }
  active_bytes_ = pos;
  return absl::OkStatus();
}

absl::Status PartitionLog::PublishEnd(int64_t file_end, int64_t offset, bool append) {
  return marker_.Publish(dir_fd_, {start_, active_base_, file_end, offset}, append);
}

LogPosition PartitionLog::End() const {
  return batches_.empty() ? start_ : batches_.back().end;
}

void PartitionLog::RecordBuffered(size_t bytes) {
  if (!buffered_durable_end_ || (flushing_ && buffered_bytes_ == flushing_->bytes))
    buffered_since_ = chrono::steady_clock::now();
  if (!buffered_durable_end_)
    buffered_durable_end_ = End();
  buffered_bytes_ += bytes;
}

bool PartitionLog::BufferedFlushDue() const {
  return !flushing_ && buffered_durable_end_ &&
         (buffered_bytes_ >= (64U << 20) ||
          chrono::steady_clock::now() - buffered_since_ >= chrono::milliseconds(100));
}

absl::Status PartitionLog::FlushBuffered() {
  if (!io_failure_.ok())
    return io_failure_;
  if (IoPending())
    return absl::FailedPreconditionError("Drain quorum append before flushing buffered data");
  if (!buffered_durable_end_)
    return absl::OkStatus();
  if (fsync(log_fd_) != 0)
    return io_failure_ = DiskError("Syncing buffered quorum log");
  io_failure_ = PublishEnd(active_bytes_, End().end_offset, true);
  if (io_failure_.ok()) {
    buffered_durable_end_.reset();
    buffered_bytes_ = 0;
  }
  return io_failure_;
}

absl::StatusOr<bool> PartitionLog::TryFlushBufferedAsync(AsyncIO submit, AppendDone done) {
  if (!io_failure_.ok())
    return io_failure_;
  if (IoPending())
    return absl::FailedPreconditionError("Quorum append already in flight");
  if (!submit || !buffered_durable_end_)
    return false;
  auto publication = marker_.PrepareAppend({start_, active_base_, active_bytes_, End().end_offset});
  if (!publication.ok())
    return io_failure_ = publication.status();
  if (!*publication)
    return false;
  flushing_ = make_unique<PendingFlush>(
      PendingFlush{**publication, End(), buffered_bytes_, std::move(submit), std::move(done)});
  flushing_->submit(log_fd_, -1, {}, [this](int result) {
    if (result != 0) {
      FinishFlush(absl::ErrnoToStatus(result < 0 ? -result : EIO, "Syncing buffered quorum log"));
      return;
    }
    if (!io_failure_.ok()) {
      FinishFlush(io_failure_);
      return;
    }
    auto status = marker_.SelectAppend(flushing_->publication);
    if (!status.ok()) {
      FinishFlush(status);
      return;
    }
    flushing_->submit(flushing_->publication.fd, -1, {}, [this](int result) {
      FinishFlush(result == 0 ? absl::OkStatus()
                              : absl::ErrnoToStatus(result < 0 ? -result : EIO,
                                                    "Syncing buffered quorum recovery marker"));
    });
  });
  return true;
}

void PartitionLog::FinishFlush(absl::Status status) {
  auto flush = std::move(flushing_);
  if (io_failure_.ok())
    io_failure_ = status;
  if (io_failure_.ok()) {
    marker_.FinishAppend(flush->publication);
    buffered_bytes_ -= flush->bytes;
    if (buffered_bytes_ == 0)
      buffered_durable_end_.reset();
    else
      buffered_durable_end_ = flush->end;
  }
  flush->done(io_failure_);
}

bool PartitionLog::CanAppendDuringFlush(string_view bytes) const {
  if (ack_mode_ != AckMode::kMemory || pending_ || !io_failure_.ok())
    return false;
  auto end = End();
  const size_t length = bytes.size();
  while (bytes.size() >= 12) {
    auto size = RecordBatchSize(bytes);
    if (!size.ok())
      return false;

    // Fetch can cut the last batch at its byte limit. The follower appends only whole batches.
    if (*size > bytes.size())
      break;
    const char* header = bytes.data();
    const uint32_t count = absl::big_endian::Load32(header + 57);
    if (header[16] != 2 || (absl::big_endian::Load16(header + 21) & 0x20) ||
        absl::big_endian::Load32(header + 12) != uint32_t(end.epoch) ||
        absl::big_endian::Load64(header) != uint64_t(end.end_offset) || count == 0 ||
        count > INT32_MAX || end.end_offset > INT64_MAX - count)
      return false;
    end.end_offset += count;
    bytes.remove_prefix(*size);
  }
  const size_t complete_bytes = length - bytes.size();
  return complete_bytes != 0 && FitsActiveSegment(complete_bytes, end.end_offset);
}

LogPosition PartitionLog::EndForEpoch(int32_t epoch) const {
  if (epoch == -1)
    return End();
  auto next = upper_bound(batches_.begin(), batches_.end(), epoch,
                          [](int32_t term, const Batch& b) { return term < b.end.epoch; });
  if (next == batches_.end())
    return End();
  if (next != batches_.begin())
    return prev(next)->end;
  if (!snapshots_.empty() && snapshots_.front().end_offset == StartOffset())
    return snapshots_.front();
  return {epoch, StartOffset()};
}

absl::Status PartitionLog::Append(string_view batches, int32_t epoch,
                                  optional<DataWritten> written) {
  return AppendBatches(batches, epoch, false, written);
}

absl::Status PartitionLog::AppendAsFollower(string_view batches, int32_t epoch,
                                            vector<ValidatedBatch>* appended) {
  return AppendBatches(batches, epoch, true, nullopt, appended);
}

bool PartitionLog::FitsActiveSegment(size_t bytes, int64_t end_offset) const {
  return !pending_ && static_cast<uint64_t>(active_bytes_) <= segment_bytes_ &&
         bytes <= segment_bytes_ - active_bytes_ && end_offset >= active_base_ &&
         end_offset - active_base_ <= INT32_MAX;
}

absl::Status PartitionLog::AppendBatches(string_view bytes, int32_t epoch, bool follower,
                                         optional<DataWritten> written,
                                         vector<ValidatedBatch>* appended) {
  if (!io_failure_.ok())
    return io_failure_;
  if (pending_)
    return absl::FailedPreconditionError("Quorum append already in flight");
  vector<ValidatedBatch> validated;
  vector<Batch> pending;
  bool memory = ack_mode_ == AckMode::kMemory;
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
    vector<RecordView> records;
    auto batch = DecodeRecordBatch(remaining.substr(0, *size), appended ? &records : nullptr);
    if (!batch.ok())
      return batch.status();
    if (batch->control) {
      written.reset();
      memory = false;
    }
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
    if (appended)
      validated.push_back({*batch, *size, std::move(records)});
    remaining.remove_prefix(*size);
  }
  if (pending.empty())
    return absl::InvalidArgumentError("No complete batch to append");
  const size_t length = bytes.size() - remaining.size();
  const bool roll = active_bytes_ > 0 && !FitsActiveSegment(length, end.end_offset);
  if (flushing_ && (!memory || roll || End().epoch != end.epoch))
    return absl::FailedPreconditionError("Drain buffered flush before changing quorum log extent");
  if (!memory || roll) {
    if (auto s = FlushBuffered(); !s.ok())
      return s;
  }
  if (roll) {
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
  if (written)
    (*written)(pending.front().file_end - pending.front().file_start);
  if (memory) {
    RecordBuffered(length);
  } else {
    if (fsync(log_fd_) != 0)
      return io_failure_ = DiskError("Syncing quorum log append");
    io_failure_ = PublishEnd(pos, end.end_offset, true);
  }
  if (io_failure_.ok()) {
    batches_.insert(batches_.end(), pending.begin(), pending.end());
    active_bytes_ = pos;
    if (appended)
      *appended = std::move(validated);
  }
  return io_failure_;
}

absl::StatusOr<bool> PartitionLog::TryAppendAsync(shared_ptr<const string> bytes, int32_t epoch,
                                                  AsyncIO submit, function<void(size_t)> written,
                                                  AppendDone done) {
  if (!io_failure_.ok())
    return io_failure_;
  if (pending_)
    return absl::FailedPreconditionError("Quorum append already in flight");
  if (!submit || !bytes || bytes->empty())
    return false;
  auto pending = make_unique<PendingAppend>();
  auto end = End();
  int64_t pos = active_bytes_;
  string_view remaining = *bytes;
  while (!remaining.empty()) {
    auto size = RecordBatchSize(remaining);
    if (!size.ok())
      return size.status();
    auto batch = DecodeRecordBatch(remaining.substr(0, *size));
    if (!batch.ok())
      return batch.status();
    if (batch->control)
      return false;
    if (batch->base_offset != end.end_offset || batch->end.epoch < end.epoch ||
        batch->end.epoch != epoch)
      return absl::InvalidArgumentError("Append does not match quorum log offset or epoch");
    if (*size > static_cast<uint64_t>(INT64_MAX - pos))
      return absl::OutOfRangeError("Quorum log file offset overflow");
    pending->batches.push_back({batch->end, active_base_, pos, pos + int64_t(*size)});
    pos += *size;
    end = batch->end;
    remaining.remove_prefix(*size);
  }

  // Segment creation and checkpoint replacement retain their synchronous directory barriers.
  if (!FitsActiveSegment(bytes->size(), end.end_offset))
    return false;
  pending->memory = ack_mode_ == AckMode::kMemory;
  if (flushing_ && (!pending->memory || End().epoch != end.epoch))
    return absl::FailedPreconditionError("Drain buffered flush before changing quorum log epoch");
  if (!pending->memory) {
    auto publication = marker_.PrepareAppend({start_, active_base_, pos, end.end_offset});
    if (!publication.ok())
      return io_failure_ = publication.status();
    if (!*publication)
      return false;
    pending->publication = **publication;
  }
  pending->bytes = std::move(bytes);
  pending->submit = std::move(submit);
  pending->written = std::move(written);
  pending->done = std::move(done);
  pending_ = std::move(pending);
  WritePending();
  return true;
}

void PartitionLog::WritePending() {
  auto* p = pending_.get();
  p->submit(log_fd_, active_bytes_ + p->progress, string_view(*p->bytes).substr(p->progress),
            [this](int result) {
              auto* p = pending_.get();
              if (!io_failure_.ok()) {
                FinishPending(io_failure_);
                return;
              }
              if (result == -EINTR) {
                WritePending();
                return;
              }
              if (result <= 0 || size_t(result) > p->bytes->size() - p->progress) {
                FinishPending(result < 0
                                  ? absl::ErrnoToStatus(-result, "Writing quorum log")
                                  : absl::DataLossError("Invalid asynchronous write length"));
                return;
              }
              p->progress += result;
              if (p->progress < p->bytes->size()) {
                WritePending();
                return;
              }
              p->readable = true;
              p->written(p->batches.front().file_end - p->batches.front().file_start);
              if (p->memory)
                FinishPending(absl::OkStatus());
              else
                SyncPending();
            });
}

void PartitionLog::SyncPending() {
  pending_->submit(log_fd_, -1, {}, [this](int result) {
    if (result != 0) {
      FinishPending(absl::ErrnoToStatus(result < 0 ? -result : EIO, "Syncing quorum log"));
      return;
    }
    auto status = marker_.SelectAppend(pending_->publication);
    if (!status.ok()) {
      FinishPending(status);
      return;
    }
    pending_->submit(pending_->publication.fd, -1, {}, [this](int result) {
      FinishPending(result == 0 ? absl::OkStatus()
                                : absl::ErrnoToStatus(result < 0 ? -result : EIO,
                                                      "Syncing quorum recovery marker"));
    });
  });
}

void PartitionLog::FinishPending(absl::Status status) {
  auto pending = std::move(pending_);
  if (io_failure_.ok())
    io_failure_ = status;
  if (io_failure_.ok()) {
    if (pending->memory) {
      RecordBuffered(pending->bytes->size());
    } else {
      marker_.FinishAppend(pending->publication);
      buffered_durable_end_.reset();
      buffered_bytes_ = 0;
    }
    active_bytes_ = pending->batches.back().file_end;
    batches_.insert(batches_.end(), pending->batches.begin(), pending->batches.end());
  }
  pending->done(io_failure_);
}

optional<string_view> PartitionLog::WrittenRecords(int64_t offset, size_t max_bytes) const {
  if (!pending_ || !pending_->readable || offset != End().end_offset)
    return nullopt;
  const auto& first = pending_->batches.front();
  return string_view(*pending_->bytes)
      .substr(0, max(max_bytes, size_t(first.file_end - first.file_start)));
}

bool PartitionLog::PendingContains(int64_t offset, int32_t epoch) const {
  return pending_ && pending_->readable && epoch == pending_->batches.back().end.epoch &&
         offset > End().end_offset && offset <= pending_->batches.back().end.end_offset;
}

absl::Status PartitionLog::AppendLeaderChange(int32_t epoch, int64_t timestamp_ms, int32_t leader,
                                              span<const int32_t> voters,
                                              span<const int32_t> granting_voters) {
  if (End().end_offset > 0 && epoch <= End().epoch)
    return absl::InvalidArgumentError("LeaderChange must begin a new epoch");
  auto bytes =
      MakeLeaderChangeBatch(End().end_offset, epoch, timestamp_ms, leader, voters, granting_voters);
  return bytes.ok() ? Append(*bytes, epoch) : bytes.status();
}

absl::StatusOr<string> PartitionLog::Read(int64_t offset, int64_t max_offset, size_t max_bytes,
                                          int64_t* batch_start) const {
  if (!io_failure_.ok())
    return io_failure_;
  if (offset < StartOffset() || offset > End().end_offset || max_offset < StartOffset() ||
      max_offset > End().end_offset)
    return absl::OutOfRangeError("Read offset outside quorum log");
  if (batch_start)
    *batch_start = offset;
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
  if (batch_start)
    *batch_start = first == batches_.begin() ? StartOffset() : prev(first)->end.end_offset;
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
  if (IoPending())
    return absl::FailedPreconditionError("Drain quorum append before truncation");
  if (!io_failure_.ok())
    return io_failure_;
  if (hw < 0 || offset < hw || (!snapshots_.empty() && offset < snapshots_.back().end_offset) ||
      offset < StartOffset() || offset > End().end_offset)
    return absl::InvalidArgumentError("Truncation outside uncommitted quorum log suffix");
  if (offset == End().end_offset)
    return absl::OkStatus();
  if (auto s = FlushBuffered(); !s.ok())
    return s;
  auto cut = upper_bound(batches_.begin(), batches_.end(), offset,
                         [](int64_t key, const Batch& b) { return key < b.end.end_offset; });
  if (offset != StartOffset() && (cut == batches_.begin() || prev(cut)->end.end_offset != offset))
    return absl::InvalidArgumentError("Truncation splits a quorum log batch");
  ++content_generation_;
  int64_t pos = cut == batches_.begin() ? cut->file_start : prev(cut)->file_end;
  int64_t base = cut == batches_.begin() ? cut->segment_base : prev(cut)->segment_base;
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
