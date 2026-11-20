// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/vote_handler.h"

namespace dfly::cluster::raft {
namespace {

using protocol::VoteRequest;
using protocol::VoteResponse;

// Error codes from Kafka's org.apache.kafka.common.protocol.Errors.
enum VoteError : int16_t {
  kNone = 0,
  kUnsupportedVersion = 35,
  kInvalidRequest = 42,
  kFencedLeaderEpoch = 74,
  kInconsistentClusterId = 104,
  kInvalidVoterKey = 125,
};

std::error_code Persist(const PersistElection& write, ElectionState next, QuorumVoteState* state) {
  state->storage_error = write(next);
  if (!state->storage_error)
    state->election = std::move(next);
  return state->storage_error;
}

}  // namespace

std::error_code HandleVoteRequest(const VoteTarget& target, const VoteRequest& request,
                                  int16_t api_version, LogPosition local_log_end,
                                  const std::optional<VoteResponse::NodeEndpoint>& leader_endpoint,
                                  const PersistElection& persist, QuorumVoteState* state,
                                  VoteResponse* response) {
  *response = {};
  if (state->storage_error)
    return state->storage_error;

  if (api_version < VoteRequest::kMinVersion || api_version > VoteRequest::kMaxVersion) {
    response->error_code = kUnsupportedVersion;
    return {};
  }
  if (request.cluster_id && *request.cluster_id != target.cluster_id) {
    response->error_code = kInconsistentClusterId;
    return {};
  }
  if (request.topics.size() != 1 || request.topics.front().topic_name != target.topic ||
      request.topics.front().partitions.size() != 1 ||
      request.topics.front().partitions.front().partition_index != target.partition) {
    response->error_code = kInvalidRequest;
    return {};
  }

  // The recovery path must reconcile the durable vote epoch with the recovered log.
  if (local_log_end.epoch < 0 || local_log_end.end_offset < 0 ||
      local_log_end.epoch > state->election.epoch)
    return std::make_error_code(std::errc::state_not_recoverable);

  const auto& part = request.topics.front().partitions.front();
  auto reply = [&](VoteError error, bool granted = false) {
    auto& topic = response->topics.emplace_back();
    topic.topic_name = target.topic;
    auto& result = topic.partitions.emplace_back();
    result.partition_index = target.partition;
    result.error_code = error;
    result.leader_epoch = state->election.epoch;
    result.leader_id = state->election.leader_id.value_or(-1);
    result.vote_granted = granted;
    if (api_version >= 1 && result.leader_id >= 0 && leader_endpoint &&
        leader_endpoint->node_id == result.leader_id)
      response->node_endpoints.push_back(*leader_endpoint);
    return std::error_code{};
  };

  const int32_t epoch = part.replica_epoch;
  const bool pre_vote = part.pre_vote;
  if (part.last_offset < 0 || part.last_offset_epoch < 0 ||
      (pre_vote ? part.last_offset_epoch > epoch : part.last_offset_epoch >= epoch))
    return reply(kInvalidRequest);
  if (epoch < state->election.epoch)
    return reply(kFencedLeaderEpoch);
  if (part.replica_id < 0)
    return reply(kInvalidRequest);

  // KRaft advances even for pre-votes and before validating the destination key.
  if (epoch > state->election.epoch) {
    ElectionState next{epoch, std::nullopt, std::nullopt, state->election.voters};
    if (auto ec = Persist(persist, std::move(next), state); ec)
      return ec;
    state->role = QuorumRole::kUnattached;
    state->fetched_from_leader = false;
  }

  // Version 0 has no recipient key; from v1 a missing directory still matches by id.
  if (request.voter_id >= 0 &&
      !ReplicaKey{request.voter_id, part.voter_directory_id}.Matches(target.local_replica))
    return reply(kInvalidVoterKey);

  const ReplicaKey candidate{part.replica_id, part.replica_directory_id};
  const VoteContext context{state->role, state->election.leader_id, state->election.voted_key,
                            state->fetched_from_leader};
  const bool granted =
      context.CanGrantVote(candidate, {part.last_offset_epoch, part.last_offset}, local_log_end,
                           pre_vote ? VoteType::kPreVote : VoteType::kVote);
  if (granted && !pre_vote && !state->election.voted_key) {
    // Only the local transition to candidate may persist a self vote.
    if (candidate.id == target.local_replica.id)
      return reply(kInvalidRequest);
    auto next = state->election;
    next.voted_key = candidate;
    if (auto ec = Persist(persist, std::move(next), state); ec)
      return ec;
  }
  return reply(kNone, granted);
}

}  // namespace dfly::cluster::raft
