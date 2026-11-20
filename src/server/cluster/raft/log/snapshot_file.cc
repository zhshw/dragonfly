// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/log/snapshot_file.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <charconv>

#include "server/cluster/raft/log/log_files.h"
#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/protocol/flexible_message.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace fs = filesystem;
using namespace log_files;

template <typename T> bool ParseNumber(string_view field, T* out) {
  auto [end, ec] = from_chars(field.data(), field.data() + field.size(), *out);
  return ec == errc{} && end == field.data() + field.size() && *out >= 0;
}

absl::StatusOr<LogPosition> ParseName(string_view name) {
  LogPosition id;
  if (name.size() != 42 || name[20] != '-' || !name.ends_with(".checkpoint") ||
      !ParseNumber(name.substr(0, 20), &id.end_offset) ||
      !ParseNumber(name.substr(21, 10), &id.epoch))
    return absl::DataLossError("Invalid Kafka snapshot name");
  return id;
}

absl::StatusOr<int64_t> FileSize(int fd) {
  struct stat st;
  if (fstat(fd, &st) == -1)
    return DiskError("Stat Kafka snapshot");
  if (!S_ISREG(st.st_mode) || st.st_size <= 0)
    return absl::DataLossError("Invalid Kafka snapshot file");
  return st.st_size;
}

bool ValidEnvelopeValue(string_view value, bool header) {
  protocol::detail::Reader reader(0, value);
  protocol::TaggedFields tags;
  int16_t version;
  int64_t timestamp;
  return reader.Field(version) && version == 0 && (!header || reader.Field(timestamp)) &&
         reader.Tags(tags) && reader.empty();
}

absl::Status ValidateFile(int fd, LogPosition id) {
  auto size = FileSize(fd);
  if (!size.ok())
    return size.status();
  int64_t position = 0, offset = 0;
  bool footer = false;
  while (position < *size) {
    char prefix[12];
    if (auto s = ReadAt(fd, position, prefix, sizeof(prefix)); !s.ok())
      return s;
    auto length = RecordBatchSize(string_view(prefix, sizeof(prefix)));
    if (!length.ok() || *length > static_cast<uint64_t>(*size - position))
      return absl::DataLossError("Invalid Kafka snapshot batch length");
    string bytes(*length, '\0');
    if (auto s = ReadAt(fd, position, bytes.data(), bytes.size()); !s.ok())
      return s;
    vector<RecordView> records;
    auto batch = DecodeRecordBatch(bytes, &records);
    if (!batch.ok())
      return batch.status();
    if (batch->base_offset != offset || batch->end.epoch != id.epoch)
      return absl::DataLossError("Inconsistent Kafka snapshot batch offset or epoch");
    for (auto& record : records) {
      uint16_t type = UINT16_MAX;
      if (batch->control) {
        if (record.key->size() != 4 || absl::big_endian::Load16(record.key->data()) != 0)
          return absl::DataLossError("Invalid Kafka snapshot control key");
        type = absl::big_endian::Load16(record.key->data() + 2);
      }
      if (footer || (record.offset == 0) != (type == 3))
        return absl::DataLossError("Missing or misplaced Kafka snapshot envelope record");
      if ((type == 3 || type == 4) && !ValidEnvelopeValue(*record.value, type == 3))
        return absl::DataLossError("Invalid Kafka snapshot header or footer value");
      footer = type == 4;
    }
    offset = batch->end.end_offset;
    position += *length;
  }
  return footer ? absl::OkStatus() : absl::DataLossError("Missing Kafka snapshot footer");
}

}  // namespace

std::string SnapshotName(LogPosition id) {
  return absl::StrFormat("%020d-%010d.checkpoint", id.end_offset, id.epoch);
}

absl::StatusOr<vector<LogPosition>> RecoverSnapshots(const fs::path& dir, LogPosition start) {
  vector<LogPosition> ids;
  vector<fs::path> obsolete;
  error_code ec;
  fs::directory_iterator files(dir, ec);
  for (; !ec && files != fs::directory_iterator(); files.increment(ec)) {
    auto& path = files->path();
    auto name = path.filename().string();
    if (name.ends_with(".checkpoint.part") || name.ends_with(".checkpoint.deleted")) {
      // Only collect this partition's Kafka snapshot names, including Java's unique temp suffix.
      if (name.size() >= 31 && ParseName(name.substr(0, 31) + ".checkpoint").ok())
        obsolete.push_back(path);
      continue;
    }
    if (path.extension() != ".checkpoint")
      continue;
    auto id = ParseName(path.filename().string());
    if (!id.ok())
      return id.status();
    if (id->end_offset < start.end_offset || *id < start) {
      obsolete.push_back(path);
      continue;
    }
    int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd == -1)
      return DiskError("Opening immutable Kafka snapshot");
    absl::Cleanup cleanup = [fd] { close(fd); };
    if (auto s = ValidateFile(fd, *id); !s.ok())
      return s;
    ids.push_back(*id);
  }
  if (ec)
    return absl::ErrnoToStatus(ec.value(), "Listing Kafka snapshots");
  sort(ids.begin(), ids.end());
  if (start != LogPosition{0, 0} && !binary_search(ids.begin(), ids.end(), start))
    return absl::DataLossError("Log start requires a matching immutable Kafka snapshot");
  for (const auto& path : obsolete) {
    if (unlink(path.c_str()) == -1)
      return DiskError("Removing obsolete or interrupted Kafka snapshot");
  }
  return ids;
}

absl::StatusOr<SnapshotChunk> ReadSnapshotChunk(const fs::path& dir, LogPosition id,
                                                int64_t position, size_t max_bytes) {
  int fd = open((dir / SnapshotName(id)).c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  if (fd == -1)
    return DiskError("Opening immutable Kafka snapshot for FetchSnapshot");
  absl::Cleanup cleanup = [fd] { close(fd); };
  auto size = FileSize(fd);
  if (!size.ok())
    return size.status();
  if (position < 0 || position >= *size)
    return absl::OutOfRangeError("FetchSnapshot position is outside snapshot");
  SnapshotChunk chunk{*size, string(min<size_t>(*size - position, max_bytes), '\0')};
  if (auto s = ReadAt(fd, position, chunk.bytes.data(), chunk.bytes.size()); !s.ok())
    return s;
  return chunk;
}

absl::StatusOr<unique_ptr<SnapshotWriter>> SnapshotWriter::Create(const fs::path& dir,
                                                                  LogPosition id, int64_t size) {
  if (id.end_offset < 0 || id.epoch < 0 || (id.end_offset == 0 && id.epoch != 0) || size <= 0)
    return absl::InvalidArgumentError("Invalid Kafka snapshot ID or size");
  unique_ptr<SnapshotWriter> writer(new SnapshotWriter(id, size));
  writer->dir_fd_ = open(dir.c_str(), O_DIRECTORY | O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
  if (writer->dir_fd_ == -1)
    return DiskError("Opening Kafka snapshot directory");
  string path =
      (dir / absl::StrFormat("%020d-%010d-XXXXXX.checkpoint.part", id.end_offset, id.epoch))
          .string();
  writer->fd_ = mkstemps(path.data(), sizeof(".checkpoint.part") - 1);
  if (writer->fd_ == -1)
    return DiskError("Creating temporary Kafka snapshot");
  writer->temp_ = fs::path(path).filename().string();
  if (fcntl(writer->fd_, F_SETFD, FD_CLOEXEC) == -1)
    return DiskError("Setting Kafka snapshot close-on-exec flag");
  return writer;
}

SnapshotWriter::~SnapshotWriter() {
  if (fd_ >= 0)
    close(fd_);
  if (!temp_.empty())
    unlinkat(dir_fd_, temp_.c_str(), 0);
  if (dir_fd_ >= 0)
    close(dir_fd_);
}

absl::Status SnapshotWriter::Append(int64_t position, string_view bytes) {
  if (published_ || position != position_ || bytes.empty() ||
      bytes.size() > static_cast<uint64_t>(size_ - position_))
    return absl::InvalidArgumentError("Out-of-order or oversized Kafka snapshot chunk");
  if (auto s = WriteAt(fd_, position, bytes); !s.ok())
    return s;
  position_ += bytes.size();
  return absl::OkStatus();
}

absl::Status SnapshotWriter::Freeze() {
  if (position_ != size_)
    return absl::FailedPreconditionError("Incomplete Kafka snapshot");
  if (auto s = ValidateFile(fd_, id_); !s.ok())
    return s;
  if (fsync(fd_) == -1)
    return DiskError("Syncing complete Kafka snapshot");

  // Multiple downloads of the same ID must never replace an already published file.
  if (linkat(dir_fd_, temp_.c_str(), dir_fd_, SnapshotName(id_).c_str(), 0) == -1)
    return DiskError("Publishing immutable Kafka snapshot");
  published_ = true;
  if (fsync(dir_fd_) == -1)
    return DiskError("Syncing published Kafka snapshot name");
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
