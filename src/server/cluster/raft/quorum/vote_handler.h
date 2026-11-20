// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <optional>
#include <string>
#include <system_error>

#include "server/cluster/raft/protocol/vote.h"
#include "server/cluster/raft/storage/file_quorum_state_store.h"

namespace dfly::cluster::raft {

struct VoteTarget {
  std::string cluster_id;
  std::string topic;
  int32_t partition;
  ReplicaKey local_replica;
};

// The event loop owns one instance per quorum and serializes vote handling with all
// role, membership and log changes. Populate election.voters from the active voter set.
struct QuorumVoteState {
  ElectionState election;
  QuorumRole role = QuorumRole::kUnattached;
  bool fetched_from_leader = false;
  std::error_code storage_error{};
};

using PersistElection = std::function<std::error_code(const ElectionState&)>;

// Handle a decoded Vote message using the log's current end offset and last epoch
// (including an installed snapshot). Resolve leader_endpoint for this request's listener.
// persist must wait for durable storage. On a local error no response may be sent.
// A storage error fences all quorum operations until recovery from the durable state.
[[nodiscard]] std::error_code HandleVoteRequest(
    const VoteTarget& target, const protocol::VoteRequest& request, int16_t api_version,
    LogPosition local_log_end,
    const std::optional<protocol::VoteResponse::NodeEndpoint>& leader_endpoint,
    const PersistElection& persist, QuorumVoteState* state, protocol::VoteResponse* response);

}  // namespace dfly::cluster::raft
