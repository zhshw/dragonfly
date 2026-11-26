// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/campaign.h"

#include <algorithm>
#include <limits>

namespace dfly::cluster::raft {
namespace {

using namespace std;
using protocol::VoteResponse;

enum ResponseError : int16_t {
  kNotLeaderOrFollower = 6,
  kClusterAuthorizationFailed = 31,
  kUnsupportedVersion = 35,
  kInvalidRequest = 42,
  kFencedLeaderEpoch = 74,
  kUnknownLeaderEpoch = 75,
  kInvalidVoterKey = 125,
};

}  // namespace

ElectionCampaign::ElectionCampaign(VoteTarget target, vector<CampaignVoter> voters,
                                   PersistElection persist)
    : target_(std::move(target)), voters_(std::move(voters)), persist_(std::move(persist)) {
  for (const auto& voter : voters_)
    voter_keys_.push_back(voter.key);
}

optional<ElectionCampaign::Endpoint> ElectionCampaign::KnownEndpoint(int32_t node_id) const {
  for (const auto& voter : voters_) {
    if (voter.key.id == node_id) {
      Endpoint endpoint;
      endpoint.node_id = node_id;
      endpoint.host = voter.endpoint.host;
      endpoint.port = voter.endpoint.port;
      return endpoint;
    }
  }
  return nullopt;
}

error_code ElectionCampaign::InstallVoters(vector<CampaignVoter> voters) {
  if (state_.storage_error)
    return state_.storage_error;
  set<int32_t> ids;
  vector<ReplicaKey> keys;
  for (const auto& voter : voters) {
    if (voter.key.id < 0 || voter.endpoint.host.empty() || !voter.endpoint.port ||
        !ids.insert(voter.key.id).second)
      return make_error_code(errc::invalid_argument);
    keys.push_back(voter.key);
  }
  voters_ = std::move(voters);
  ++peer_revision_;
  voter_keys_ = std::move(keys);
  state_.election.voters = std::move(ids);
  if (state_.election.leader_id) {
    if (auto addr = KnownEndpoint(*state_.election.leader_id); addr)
      leader_endpoint_ = std::move(addr);
  }
  return {};
}

error_code ElectionCampaign::Save(ElectionState next, QuorumRole role) {
  if (auto ec = persist_(next); ec) {
    state_.storage_error = ec;
    return ec;
  }
  state_.election = std::move(next);
  state_.role = role;
  state_.fetched_from_leader = false;
  pending_.clear();
  return {};
}

error_code ElectionCampaign::Recover(ElectionState durable, LogPosition log_end) {
  if (state_.storage_error)
    return state_.storage_error;
  if (recovered_ || durable.epoch < 0 || log_end.epoch < 0 || log_end.end_offset < 0 ||
      target_.local_replica.id < 0 || !persist_)
    return make_error_code(errc::invalid_argument);

  durable.voters.clear();
  for (const auto& voter : voters_) {
    if (voter.key.id < 0 || voter.endpoint.host.empty() || voter.endpoint.port == 0 ||
        !durable.voters.insert(voter.key.id).second)
      return make_error_code(errc::invalid_argument);
  }

  QuorumRole role = QuorumRole::kUnattached;
  if (durable.epoch < log_end.epoch) {
    durable.epoch = log_end.epoch;
    durable.leader_id.reset();
    durable.voted_key.reset();
  } else if (durable.leader_id == target_.local_replica.id) {
    role = QuorumRole::kResigned;
    durable.voted_key.reset();
    leader_endpoint_ = KnownEndpoint(target_.local_replica.id);
  } else if (durable.voted_key && durable.voted_key->Matches(target_.local_replica)) {
    round_ = EpochElection::Start(voter_keys_, target_.local_replica, VoteType::kVote);
    if (!round_)
      return make_error_code(errc::state_not_recoverable);
    role = QuorumRole::kCandidate;
    durable.leader_id.reset();
    durable.voted_key = target_.local_replica;
  } else if (durable.leader_id) {
    leader_endpoint_ = KnownEndpoint(*durable.leader_id);
    if (leader_endpoint_)
      role = QuorumRole::kFollower;
  }

  if (auto ec = Save(std::move(durable), role); ec)
    return ec;
  recovered_ = true;
  if (role == QuorumRole::kCandidate && round_->GetOutcome() == EpochElection::Outcome::kGranted)
    return BecomeLeader();
  if (voter_keys_.size() == 1 && voter_keys_.front().Matches(target_.local_replica) &&
      role != QuorumRole::kCandidate)
    return StartRound(VoteType::kPreVote);
  return {};
}

error_code ElectionCampaign::StartRound(VoteType type) {
  auto next_round = EpochElection::Start(voter_keys_, target_.local_replica, type);
  if (!next_round)
    return make_error_code(errc::operation_not_permitted);
  if (type == VoteType::kVote) {
    const auto& current = state_.election;
    if (current.epoch == numeric_limits<int32_t>::max())
      return make_error_code(errc::value_too_large);
    ElectionState next{current.epoch + 1, nullopt, target_.local_replica, current.voters};
    if (auto ec = Save(std::move(next), QuorumRole::kCandidate); ec)
      return ec;
    leader_endpoint_.reset();
  } else {
    state_.role = QuorumRole::kProspective;
    state_.fetched_from_leader = false;
    pending_.clear();
  }
  round_ = std::move(next_round);

  // A one-voter quorum already has a majority from its own vote.
  if (round_->GetOutcome() == EpochElection::Outcome::kGranted) {
    if (type == VoteType::kPreVote)
      return StartRound(VoteType::kVote);
    return BecomeLeader();
  }
  return {};
}

error_code ElectionCampaign::BecomeLeader() {
  ElectionState next{state_.election.epoch, target_.local_replica.id, nullopt,
                     state_.election.voters};
  if (auto ec = Save(std::move(next), QuorumRole::kLeader); ec)
    return ec;
  leader_endpoint_ = KnownEndpoint(target_.local_replica.id);
  return {};
}

error_code ElectionCampaign::LosePreVote() {
  auto role = state_.election.leader_id && leader_endpoint_ ? QuorumRole::kFollower
                                                            : QuorumRole::kUnattached;
  if (auto ec = Save(state_.election, role); ec)
    return ec;
  round_.reset();
  return {};
}

error_code ElectionCampaign::OnElectionTimeout() {
  if (state_.storage_error)
    return state_.storage_error;
  if (!recovered_ || state_.role == QuorumRole::kLeader)
    return make_error_code(errc::operation_not_permitted);
  if (state_.role == QuorumRole::kProspective)
    return LosePreVote();
  if (state_.role == QuorumRole::kResigned) {
    if (state_.election.epoch == numeric_limits<int32_t>::max())
      return make_error_code(errc::value_too_large);
    return FollowLeader(state_.election.epoch + 1, nullopt, {});
  }
  return StartRound(VoteType::kPreVote);
}

vector<ReplicaKey> ElectionCampaign::UnrecordedVoters() const {
  if (state_.storage_error || !round_ || round_->GetOutcome() != EpochElection::Outcome::kPending)
    return {};
  return round_->UnrecordedVoters();
}

vector<int32_t> ElectionCampaign::GrantingVoters() const {
  if (state_.storage_error || state_.role != QuorumRole::kLeader || !round_)
    return {};
  return round_->GrantingVoters();
}

optional<OutboundVote> ElectionCampaign::PrepareVote(const ReplicaKey& recipient,
                                                     LogPosition log_end) {
  if (log_end.end_offset < 0 || log_end.epoch < 0 || log_end.epoch > state_.election.epoch)
    return nullopt;
  auto unrecorded = UnrecordedVoters();
  if (find(unrecorded.begin(), unrecorded.end(), recipient) == unrecorded.end())
    return nullopt;
  OutboundVote vote{recipient, ++sequence_, {}};
  pending_[recipient.id] = vote.sequence;
  auto& request = vote.request;
  request.cluster_id = target_.cluster_id;
  request.voter_id = recipient.id;
  auto& topic = request.topics.emplace_back();
  topic.topic_name = target_.topic;
  auto& part = topic.partitions.emplace_back();
  part.partition_index = target_.partition;
  part.replica_epoch = state_.election.epoch;
  part.replica_id = target_.local_replica.id;
  part.replica_directory_id = target_.local_replica.directory_id;
  part.voter_directory_id = recipient.directory_id;
  part.last_offset = log_end.end_offset;
  part.last_offset_epoch = log_end.epoch;
  part.pre_vote = state_.role == QuorumRole::kProspective;
  return vote;
}

error_code ElectionCampaign::FollowLeader(int32_t epoch, optional<int32_t> leader,
                                          const vector<Endpoint>& endpoints) {
  const auto& current = state_.election;
  if (leader && (leader == target_.local_replica.id ||
                 (epoch == current.epoch && current.leader_id && current.leader_id != leader)))
    return make_error_code(errc::protocol_error);

  if (epoch == current.epoch &&
      (!leader || (current.leader_id == leader && (leader_endpoint_ || endpoints.empty()))))
    return {};

  optional<Endpoint> endpoint;
  if (leader) {
    auto it = find_if(endpoints.begin(), endpoints.end(),
                      [&](const Endpoint& e) { return e.node_id == *leader; });
    endpoint = it == endpoints.end() ? KnownEndpoint(*leader) : optional<Endpoint>{*it};
    if (!endpoint || endpoint->host.empty() || endpoint->port == 0)
      return make_error_code(errc::protocol_error);
  }

  auto vote = current.epoch == epoch ? current.voted_key : nullopt;
  ElectionState next{epoch, leader, vote, current.voters};
  if (auto ec = Save(std::move(next), leader ? QuorumRole::kFollower : QuorumRole::kUnattached); ec)
    return ec;
  leader_endpoint_ = std::move(endpoint);
  ++peer_revision_;
  round_.reset();
  return {};
}

error_code ElectionCampaign::ReceiveVoteResponse(const ReplicaKey& source, uint64_t sequence,
                                                 const VoteResponse& response) {
  if (state_.storage_error)
    return state_.storage_error;
  auto pending = pending_.find(source.id);
  if (pending == pending_.end() || pending->second != sequence)
    return {};
  auto configured = find_if(voter_keys_.begin(), voter_keys_.end(),
                            [&](const ReplicaKey& voter) { return voter.Matches(source); });
  if (configured == voter_keys_.end())
    return {};
  pending_.erase(pending);
  if (response.error_code == kUnsupportedVersion && state_.role == QuorumRole::kProspective)
    return StartRound(VoteType::kVote);
  if (response.error_code == kClusterAuthorizationFailed)
    return make_error_code(errc::permission_denied);
  if (response.error_code != 0 || response.topics.size() != 1 ||
      response.topics.front().topic_name != target_.topic ||
      response.topics.front().partitions.size() != 1 ||
      response.topics.front().partitions.front().partition_index != target_.partition)
    return make_error_code(errc::resource_unavailable_try_again);

  const auto& result = response.topics.front().partitions.front();
  const auto error = result.error_code;
  const auto epoch = result.leader_epoch;
  if (epoch < state_.election.epoch || error == kUnknownLeaderEpoch)
    return {};
  optional<int32_t> leader;
  if (result.leader_id >= 0)
    leader = result.leader_id;
  if (epoch > state_.election.epoch || error == kFencedLeaderEpoch ||
      error == kNotLeaderOrFollower || (leader && !state_.election.leader_id))
    return FollowLeader(epoch, leader, response.node_endpoints);
  if (error == kInvalidVoterKey)
    return {};
  if (error == kInvalidRequest)
    return make_error_code(errc::protocol_error);
  if (error != 0)
    return make_error_code(errc::resource_unavailable_try_again);

  if (round_->RecordVote(source.id, result.vote_granted) == EpochElection::RecordResult::kInvalid)
    return make_error_code(errc::protocol_error);
  auto outcome = round_->GetOutcome();
  if (outcome == EpochElection::Outcome::kGranted) {
    if (state_.role == QuorumRole::kProspective)
      return StartRound(VoteType::kVote);
    return BecomeLeader();
  }
  if (outcome == EpochElection::Outcome::kRejected && state_.role == QuorumRole::kProspective)
    return LosePreVote();
  return {};
}

error_code ElectionCampaign::ReceiveVote(const protocol::VoteRequest& request, int16_t version,
                                         LogPosition log_end, VoteResponse* response) {
  if (!recovered_)
    return make_error_code(errc::operation_not_permitted);
  auto previous_epoch = state_.election.epoch;
  auto previous_vote = state_.election.voted_key;
  if (auto ec = HandleVoteRequest(target_, request, version, log_end, leader_endpoint_, persist_,
                                  &state_, response);
      ec)
    return ec;
  if (state_.election.epoch != previous_epoch) {
    leader_endpoint_.reset();
    round_.reset();
    pending_.clear();
  } else if (state_.role == QuorumRole::kProspective &&
             previous_vote != state_.election.voted_key) {
    return StartRound(VoteType::kPreVote);
  }
  return {};
}

error_code ElectionCampaign::ObserveLeader(int32_t epoch, std::optional<int32_t> leader_id,
                                           optional<QuorumEndpoint> address) {
  if (state_.storage_error)
    return state_.storage_error;
  if (!recovered_ || (leader_id && *leader_id < 0) || epoch < state_.election.epoch)
    return make_error_code(errc::invalid_argument);
  if (state_.role == QuorumRole::kLeader && leader_id == target_.local_replica.id &&
      epoch == state_.election.epoch)
    return {};
  vector<Endpoint> endpoints;
  if (address && leader_id) {
    auto& endpoint = endpoints.emplace_back();
    endpoint.node_id = *leader_id;
    endpoint.host = std::move(address->host);
    endpoint.port = address->port;
  }
  return FollowLeader(epoch, leader_id, endpoints);
}

error_code ElectionCampaign::Resign(int32_t epoch) {
  if (state_.storage_error)
    return state_.storage_error;
  if (state_.role != QuorumRole::kLeader || epoch != state_.election.epoch)
    return make_error_code(errc::operation_not_permitted);
  state_.role = QuorumRole::kResigned;
  pending_.clear();
  round_.reset();
  return {};
}

void ElectionCampaign::MarkFetchedFromLeader(int32_t leader_id, int32_t epoch) {
  if (state_.role == QuorumRole::kFollower && state_.election.leader_id == leader_id &&
      state_.election.epoch == epoch)
    state_.fetched_from_leader = true;
}

}  // namespace dfly::cluster::raft
