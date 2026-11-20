// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/bootstrap/raft_directory.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <random>

#include "server/cluster/raft/bootstrap/meta_properties.h"
#include "server/cluster/raft/bootstrap/quorum_bootstrap.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace fs = std::filesystem;

// Both hard links identify the same immutable identity. Publish the Dragonfly marker first
// so legacy startup also rejects an interrupted format that has not installed meta.properties.
constexpr char kMarker[] = ".dragonfly-raft";
constexpr char kProperties[] = "meta.properties";
constexpr char kTempPrefix[] = ".dragonfly-raft-format.";

absl::Status FileError(const fs::path& path, int error) {
  return absl::ErrnoToStatus(error, path.string());
}

}  // namespace

bool IsReservedDirectoryId(const DirectoryId& id) {
  // Kafka compares the low 64 bits as a signed Java long.
  return all_of(id.begin(), id.begin() + 8, [](uint8_t b) { return b == 0; }) &&
         ((id[8] & 0x80) ||
          (all_of(id.begin() + 8, id.end() - 1, [](uint8_t b) { return b == 0; }) &&
           id.back() < 100));
}

namespace {
DirectoryId NewDirectoryId() {
  random_device source;
  DirectoryId id;
  do {
    generate(id.begin(), id.end(), [&] { return static_cast<uint8_t>(source()); });
  } while (IsReservedDirectoryId(id) || FormatDirectoryId(id).starts_with('-'));
  return id;
}

absl::Status CheckIdentity(const RaftDirectoryIdentity& identity, const RaftDirectoryConfig& cfg) {
  if (cfg.cluster_id != identity.cluster_id)
    return absl::FailedPreconditionError("cluster.id differs from formatted directory");
  if (cfg.node_id != identity.node_id)
    return absl::FailedPreconditionError("node.id differs from formatted directory");
  if (cfg.directory_id && cfg.directory_id != identity.directory_id)
    return absl::FailedPreconditionError("directory.id differs from formatted directory");
  return absl::OkStatus();
}

absl::StatusOr<RaftDirectoryIdentity> ReadIdentity(const fs::path& path,
                                                   string* bootstrap = nullptr) {
  int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  if (fd == -1)
    return FileError(path, errno);
  absl::Cleanup close_fd = [fd] { close(fd); };
  struct stat st;
  if (fstat(fd, &st) != 0)
    return FileError(path, errno);
  if (!S_ISREG(st.st_mode))
    return absl::DataLossError(absl::StrCat(path.string(), " is not a regular identity file"));
  char buffer[4096];
  string contents;
  while (true) {
    ssize_t count = read(fd, buffer, sizeof(buffer));
    if (count == -1 && errno == EINTR)
      continue;
    if (count < 0)
      return FileError(path, errno);
    if (count == 0)
      break;
    contents.append(buffer, count);
  }

  auto identity = DecodeMetaProperties(contents, bootstrap);
  if (!identity.ok())
    return identity.status();
  if (!identity->directory_id || identity->node_id < 0 || identity->cluster_id.empty())
    return absl::DataLossError("Incomplete formatted Raft identity");
  if (IsReservedDirectoryId(*identity->directory_id))
    return absl::DataLossError("Reserved Kafka directory.id in meta.properties");
  return RaftDirectoryIdentity{std::move(identity->cluster_id), identity->node_id,
                               *identity->directory_id};
}

absl::Status SyncDirectory(const fs::path& dir) {
  int fd = open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd == -1)
    return FileError(dir, errno);
  absl::Cleanup close_fd = [fd] { close(fd); };
  if (fsync(fd) != 0)
    return FileError(dir, errno);
  return absl::OkStatus();
}

absl::Status InstallMarker(const fs::path& dir, string_view bytes) {
  string tmp = (dir / absl::StrCat(kTempPrefix, "XXXXXX")).string();
  int fd = mkstemp(tmp.data());
  if (fd == -1)
    return FileError(tmp, errno);
  absl::Cleanup unlink_temp = [&] { unlink(tmp.c_str()); };
  absl::Cleanup close_fd = [fd] { close(fd); };
  while (!bytes.empty()) {
    ssize_t count = write(fd, bytes.data(), bytes.size());
    if (count == -1 && errno == EINTR)
      continue;
    if (count <= 0)
      return FileError(tmp, count == 0 ? EIO : errno);
    bytes.remove_prefix(count);
  }
  if (fsync(fd) != 0)
    return FileError(tmp, errno);
  std::move(close_fd).Cancel();
  if (close(fd) != 0)
    return FileError(tmp, errno);
  if (link(tmp.c_str(), (dir / kMarker).c_str()) != 0)
    return FileError(dir / kMarker, errno);
  return SyncDirectory(dir);
}

absl::Status CheckUnformattedContents(const fs::path& dir) {
  error_code ec;
  fs::directory_iterator it(dir, ec);
  for (; !ec && it != fs::directory_iterator(); it.increment(ec)) {
    // A crash before installing the marker can leave the formatter's private scratch inode.
    // It is never a recovery source. Leave even a preexisting file of that name untouched.
    const string name = it->path().filename().string();
    if (!(name.starts_with(kTempPrefix) && name.size() == sizeof(kTempPrefix) - 1 + 6 &&
          fs::is_regular_file(it->symlink_status(ec))))
      return absl::FailedPreconditionError(
          absl::StrCat("Refusing to format nonempty ", dir.string(), ": found ", name));
  }
  return ec ? FileError(dir, ec.value()) : absl::OkStatus();
}

bool IsRemote(const fs::path& dir) {
  return dir.native().find("://") != string::npos;
}

}  // namespace

absl::StatusOr<RaftDirectoryIdentity> ReadRaftDirectory(const fs::path& dir) {
  if (IsRemote(dir))
    return absl::InvalidArgumentError("Raft storage requires a local data directory");
  const auto marker = ReadIdentity(dir / kMarker);
  if (!marker.ok())
    return marker.status();
  auto props = ReadIdentity(dir / kProperties);
  if (absl::IsNotFound(props.status()))
    return absl::FailedPreconditionError(
        "Raft format interrupted before installing meta.properties");
  if (!props.ok())
    return props.status();
  if (*props != *marker)
    return absl::FailedPreconditionError("meta.properties differs from Dragonfly Raft identity");
  return props;
}

absl::Status VerifyRaftDirectory(const fs::path& dir, const RaftDirectoryConfig& expected) {
  auto identity = ReadRaftDirectory(dir);
  return identity.ok() ? CheckIdentity(*identity, expected) : identity.status();
}

absl::Status CheckLegacyDirectory(const fs::path& dir) {
  if (!IsRemote(dir)) {
    struct stat st;
    if (lstat((dir / kMarker).c_str(), &st) == 0)
      return absl::FailedPreconditionError("Data directory belongs to a Dragonfly Raft node");
  }
  return absl::OkStatus();
}

absl::StatusOr<RaftDirectoryIdentity> FormatRaftDirectory(const fs::path& dir,
                                                          const RaftDirectoryConfig& supplied,
                                                          const QuorumBootstrap* bootstrap) {
  auto cfg = supplied;
  string bootstrap_id;
  if (bootstrap) {
    auto id = bootstrap->Identity();
    if (!id.ok())
      return id.status();
    bootstrap_id = *std::move(id);
    for (const auto& voter : bootstrap->voters.voters) {
      if (voter.voter_id == cfg.node_id) {
        if (cfg.directory_id && cfg.directory_id != voter.voter_directory_id)
          return absl::InvalidArgumentError("directory.id differs from the initial voter");
        cfg.directory_id = voter.voter_directory_id;
      }
    }
  }
  if (dir.empty() || IsRemote(dir) || cfg.node_id < 0 || cfg.cluster_id.empty())
    return absl::InvalidArgumentError(
        "Format needs a local directory, cluster.id and node.id >= 0");
  if (cfg.directory_id && IsReservedDirectoryId(*cfg.directory_id))
    return absl::InvalidArgumentError("Cannot assign a reserved Kafka directory.id");
  if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST)
    return FileError(dir, errno);

  // Serializes concurrent formatters without leaving a lock file in a legacy data directory.
  int fd = open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd == -1)
    return FileError(dir, errno);
  absl::Cleanup close_fd = [fd] { close(fd); };
  if (flock(fd, LOCK_EX) != 0)
    return FileError(dir, errno);
  string previous_bootstrap;
  auto marker = ReadIdentity(dir / kMarker, &previous_bootstrap);
  if (!marker.ok() && !absl::IsNotFound(marker.status()))
    return marker.status();
  if (marker.ok()) {
    if (auto status = CheckIdentity(*marker, cfg); !status.ok())
      return status;
    if (bootstrap_id != previous_bootstrap)
      return absl::FailedPreconditionError("Initial quorum differs from the formatted directory");
  } else {
    if (auto status = CheckUnformattedContents(dir); !status.ok())
      return status;
    RaftDirectoryConfig identity{cfg.cluster_id, cfg.node_id,
                                 cfg.directory_id ? *cfg.directory_id : NewDirectoryId()};
    auto bytes = EncodeMetaProperties(identity);
    if (!bytes.ok())
      return bytes.status();
    if (bootstrap)
      absl::StrAppend(&*bytes, "dragonfly.bootstrap=", bootstrap_id, "\n");
    if (auto status = InstallMarker(dir, *bytes); !status.ok())
      return status;
  }

  // Publish meta.properties only after the initial snapshot is durable. The ownership
  // marker binds retries to the original quorum even if the first format was interrupted.
  if (bootstrap) {
    struct stat properties;
    if (lstat((dir / kProperties).c_str(), &properties) != 0) {
      if (errno != ENOENT)
        return FileError(dir / kProperties, errno);
      if (auto s = bootstrap->Install(dir); !s.ok())
        return s;
    }
  }

  // link() never replaces an existing file. On retry, verify both names before returning.
  if (link((dir / kMarker).c_str(), (dir / kProperties).c_str()) != 0 && errno != EEXIST)
    return FileError(dir / kProperties, errno);
  if (fsync(fd) != 0)
    return FileError(dir, errno);
  if (auto status = SyncDirectory(dir.parent_path().empty() ? "." : dir.parent_path());
      !status.ok())
    return status;
  return ReadRaftDirectory(dir);
}

}  // namespace dfly::cluster::raft
