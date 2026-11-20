// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

#include "server/cluster/raft/quorum/election.h"

namespace dfly::cluster::raft {

struct QuorumBootstrap;

struct RaftDirectoryIdentity {
  std::string cluster_id;
  int32_t node_id;
  DirectoryId directory_id;
  bool operator==(const RaftDirectoryIdentity&) const = default;
};

struct RaftDirectoryConfig {
  std::string cluster_id;
  int32_t node_id;

  // Initial controller voters can specify this. Otherwise the formatter assigns one.
  std::optional<DirectoryId> directory_id;
  bool operator==(const RaftDirectoryConfig&) const = default;
};

// Offline format of an empty directory, or create the last path component. Retrying the
// same identity after an interruption finishes publication and returns the original ID.
absl::StatusOr<RaftDirectoryIdentity> FormatRaftDirectory(
    const std::filesystem::path& dir, const RaftDirectoryConfig& config,
    const QuorumBootstrap* bootstrap = nullptr);

// Reads an initialized Dragonfly Raft directory. Never initializes or repairs it.
absl::StatusOr<RaftDirectoryIdentity> ReadRaftDirectory(const std::filesystem::path& dir);
absl::Status VerifyRaftDirectory(const std::filesystem::path& dir,
                                 const RaftDirectoryConfig& expected);

// Legacy startup only tests for positive evidence of Dragonfly Raft ownership.
absl::Status CheckLegacyDirectory(const std::filesystem::path& dir);

absl::StatusOr<DirectoryId> ParseDirectoryId(std::string_view encoded);
std::string FormatDirectoryId(const DirectoryId& id);
bool IsReservedDirectoryId(const DirectoryId& id);

}  // namespace dfly::cluster::raft
