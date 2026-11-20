// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/log/log_files.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/crc/crc32c.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <string>

namespace dfly::cluster::raft::log_files {
using namespace std;
namespace be = absl::big_endian;
namespace {
constexpr char kDurableEnd[] = "dragonfly-log-end";
constexpr char kTemp[] = "dragonfly-log-end.tmp";
constexpr size_t kOldSize = 24;
constexpr size_t kSize = 44;

// The two records live in separate blocks. Both selector lengths include both slots and differ
// in several bits. Only inode size changes when selecting a slot.
constexpr off_t kSlotStride = 64 * 1024;
constexpr off_t kSelectFirst = 2 * kSlotStride;
constexpr off_t kSelectSecond = kSelectFirst + 15;

int SelectedSlot(off_t file_size) {
  if (file_size == kSelectFirst)
    return 0;
  if (file_size == kSelectSecond)
    return 1;
  return -1;
}
}  // namespace
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

RecoveryMarker::~RecoveryMarker() {
  if (fd_ != -1)
    close(fd_);
}

absl::StatusOr<optional<DurableEnd>> RecoveryMarker::Read(int dir_fd) {
  int fd = openat(dir_fd, kDurableEnd, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  if (fd == -1) {
    if (errno == ENOENT)
      return nullopt;
    return DiskError("Opening quorum log recovery marker");
  }
  absl::Cleanup cleanup = [fd] { close(fd); };
  struct stat st;
  if (fstat(fd, &st) != 0)
    return DiskError("Stat quorum log recovery marker");
  int slot = SelectedSlot(st.st_size);
  if (!S_ISREG(st.st_mode) || (slot < 0 && st.st_size != kOldSize && st.st_size != kSize))
    return absl::DataLossError("Invalid quorum log recovery marker length");
  string marker(slot >= 0 ? kSize : st.st_size, '\0');
  if (auto status = ReadAt(fd, slot >= 0 ? slot * kSlotStride : 0, marker.data(), marker.size());
      !status.ok())
    return status;
  size_t size = marker.size() - 4;
  uint32_t crc = static_cast<uint32_t>(absl::ComputeCrc32c(string_view(marker).substr(0, size)));
  if (be::Load32(marker.data() + size) != crc)
    return absl::DataLossError("Invalid quorum log recovery marker checksum");
  DurableEnd end;
  if (marker.size() == kOldSize && marker.starts_with("DFE1")) {
    end.file_end = static_cast<int64_t>(be::Load64(marker.data() + 4));
    end.offset = static_cast<int64_t>(be::Load64(marker.data() + 12));
  } else if (marker.size() == kSize && marker.starts_with(slot >= 0 ? "DFE3" : "DFE2")) {
    end.start.end_offset = static_cast<int64_t>(be::Load64(marker.data() + 4));
    end.start.epoch = static_cast<int32_t>(be::Load32(marker.data() + 12));
    end.segment_base = static_cast<int64_t>(be::Load64(marker.data() + 16));
    end.file_end = static_cast<int64_t>(be::Load64(marker.data() + 24));
    end.offset = static_cast<int64_t>(be::Load64(marker.data() + 32));
  } else {
    return absl::DataLossError("Unknown quorum log recovery marker");
  }
  if (end.start.epoch < 0 || end.start.end_offset < 0 || end.segment_base < 0 || end.file_end < 0 ||
      end.offset < end.start.end_offset || end.offset < end.segment_base)
    return absl::DataLossError("Invalid quorum log recovery boundary");
  if (slot >= 0) {
    fd_ = openat(dir_fd, kDurableEnd, O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd_ == -1)
      return DiskError("Opening writable quorum log recovery marker");
  }
  return end;
}

absl::Status RecoveryMarker::Publish(int dir_fd, const DurableEnd& end) {
  string marker(kSize, '\0');
  marker.replace(0, 4, "DFE3");
  be::Store64(marker.data() + 4, end.start.end_offset);
  be::Store32(marker.data() + 12, end.start.epoch);
  be::Store64(marker.data() + 16, end.segment_base);
  be::Store64(marker.data() + 24, end.file_end);
  be::Store64(marker.data() + 32, end.offset);
  be::Store32(marker.data() + 40,
              static_cast<uint32_t>(absl::ComputeCrc32c(string_view(marker).substr(0, 40))));
  if (fd_ != -1) {
    struct stat st;
    if (fstat(fd_, &st) != 0)
      return DiskError("Stat quorum log recovery marker");
    int active = SelectedSlot(st.st_size);
    if (active < 0)
      return absl::DataLossError("Invalid quorum log recovery marker selector");

    // Prepare the inactive record before switching the selector. A crash during either sync
    // can expose the old or the new boundary, but never selects an incompletely written slot.
    if (auto s = WriteAt(fd_, (1 - active) * kSlotStride, marker); !s.ok())
      return s;
    if (fsync(fd_) == -1)
      return DiskError("Syncing prepared quorum log recovery marker");
    if (ftruncate(fd_, active == 0 ? kSelectSecond : kSelectFirst) == -1 || fsync(fd_) == -1)
      return DiskError("Selecting quorum log recovery marker");
    return absl::OkStatus();
  }

  // Install the first checkpoint (or upgrade a legacy one) in one directory transaction.
  int fd = openat(dir_fd, kTemp, O_RDWR | O_TRUNC | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0644);
  if (fd == -1)
    return DiskError("Opening temporary quorum log recovery marker");
  absl::Cleanup cleanup = [fd] { close(fd); };
  if (auto status = WriteAt(fd, 0, marker); !status.ok())
    return status;
  if (ftruncate(fd, kSelectFirst) == -1 || fsync(fd) == -1)
    return DiskError("Syncing initial quorum log recovery marker");
  if (renameat(dir_fd, kTemp, dir_fd, kDurableEnd) == -1 || fsync(dir_fd) == -1)
    return DiskError("Installing quorum log recovery marker");
  fd_ = fd;
  std::move(cleanup).Cancel();
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft::log_files
