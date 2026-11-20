// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <filesystem>
#include <optional>
#include <set>
#include <system_error>

#include "server/cluster/raft/quorum/election.h"

namespace dfly::cluster::raft {

struct ElectionState {
  int32_t epoch = 0;
  std::optional<int32_t> leader_id;
  std::optional<ReplicaKey> voted_key;

  // Static quorum membership is present only in version 0 of quorum-state.
  std::set<int32_t> voters;

  bool operator==(const ElectionState&) const = default;
};

// Each quorum owns its own directory. Serialize mutations on its storage worker:
// writes wait for the file and directory fsync before a vote can be acknowledged.
class FileQuorumStateStore {
 public:
  explicit FileQuorumStateStore(const std::filesystem::path& quorum_dir)
      : path_(quorum_dir / "quorum-state") {
  }

  // A missing file is a successful read with no state. Corrupt data is an error.
  [[nodiscard]] std::error_code ReadElectionState(std::optional<ElectionState>* state) const;

  // KRaft feature levels 0 and 1 use file versions 0 and 1 respectively.
  [[nodiscard]] std::error_code WriteElectionState(const ElectionState& state,
                                                   int16_t kraft_version) const;

  [[nodiscard]] std::error_code Clear() const;

 private:
  std::filesystem::path path_;
};

}  // namespace dfly::cluster::raft
