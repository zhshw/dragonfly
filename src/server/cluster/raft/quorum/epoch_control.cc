// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/epoch_control.h"

#include <algorithm>
#include <bit>

namespace dfly::cluster::raft {
namespace {

using namespace std;
using namespace protocol;

constexpr int16_t kInvalidRequest = 42;
constexpr int16_t kFencedLeaderEpoch = 74;
constexpr int16_t kInconsistentClusterId = 104;
constexpr int16_t kInvalidVoterKey = 125;

template <typename Message>
bool IsQuorumPartition(const Message& message, const VoteTarget& target) {
  return message.topics.size() == 1 && message.topics.front().topic_name == target.topic &&
         message.topics.front().partitions.size() == 1 &&
         message.topics.front().partitions.front().partition_index == target.partition;
}

template <typename Message> auto& AddPartition(const VoteTarget& target, Message* message) {
  auto& topic = message->topics.emplace_back();
  topic.topic_name = target.topic;
  auto& partition = topic.partitions.emplace_back();
  partition.partition_index = target.partition;
  return partition;
}

template <typename Message>
void AddLocalLeader(const ElectionCampaign& campaign, const string& listener, Message* msg) {
  const auto& target = campaign.target();
  msg->cluster_id = target.cluster_id;
  auto& part = AddPartition(target, msg);
  part.leader_id = target.local_replica.id;
  part.leader_epoch = campaign.state().election.epoch;
  if (const auto& addr = campaign.LeaderEndpoint(); addr) {
    auto& ep = msg->leader_endpoints.emplace_back();
    ep.name = listener;
    ep.host = addr->host;
    ep.port = addr->port;
  }
}

template <typename Request, typename Response>
error_code HandleNotification(ElectionCampaign& campaign, const string& listener,
                              const Request& request, int16_t version, Response* response) {
  *response = {};
  const auto& state = campaign.state();
  const auto& target = campaign.target();
  if (state.storage_error)
    return state.storage_error;
  if (version < Request::kMinVersion || version > Request::kMaxVersion) {
    response->error_code = 35;
    return {};
  }
  if (request.cluster_id && *request.cluster_id != target.cluster_id) {
    response->error_code = kInconsistentClusterId;
    return {};
  }
  if (!IsQuorumPartition(request, target)) {
    response->error_code = kInvalidRequest;
    return {};
  }

  int16_t error = 0;
  const auto& part = request.topics.front().partitions.front();
  if (part.leader_epoch < state.election.epoch) {
    error = kFencedLeaderEpoch;
  } else if (part.leader_id < 0) {
    error = kInvalidRequest;
  } else {
    optional<QuorumEndpoint> address;
    for (const auto& ep : request.leader_endpoints) {
      if (ep.name == listener) {
        address = {ep.host, ep.port};
        break;
      }
    }
    if (auto ec = campaign.ObserveLeader(part.leader_epoch, part.leader_id, address); ec)
      return ec;

    // BeginQuorumEpoch validates the intended recipient after learning the current leader.
    if constexpr (is_same_v<Request, BeginQuorumEpochRequest>) {
      if (request.voter_id >= 0 &&
          !ReplicaKey{request.voter_id, part.voter_directory_id}.Matches(target.local_replica))
        error = kInvalidVoterKey;
    }
  }
  auto& result = AddPartition(target, response);
  result.error_code = error;
  result.leader_epoch = state.election.epoch;
  result.leader_id = state.election.leader_id.value_or(-1);
  if (const auto& ep = campaign.LeaderEndpoint();
      version >= 1 && ep && result.leader_id == ep->node_id) {
    auto& advertised = response->node_endpoints.emplace_back();
    advertised.node_id = ep->node_id;
    advertised.host = ep->host;
    advertised.port = ep->port;
  }
  return {};
}

template <typename Response>
error_code ApplyNotificationResponse(ElectionCampaign& campaign, const Response& response,
                                     bool* acknowledged) {
  *acknowledged = false;
  if (response.error_code == 31)
    return make_error_code(errc::permission_denied);
  if (response.error_code != 0 || !IsQuorumPartition(response, campaign.target()))
    return make_error_code(errc::resource_unavailable_try_again);
  const auto& part = response.topics.front().partitions.front();
  const auto& current = campaign.state().election;
  if (part.leader_epoch < current.epoch || part.error_code == 75)
    return {};
  if (part.leader_epoch > current.epoch || part.error_code == kFencedLeaderEpoch ||
      part.error_code == 6 || (part.leader_id >= 0 && !current.leader_id)) {
    optional<int32_t> leader;
    optional<QuorumEndpoint> address;
    if (part.leader_id >= 0) {
      leader = part.leader_id;
      for (const auto& endpoint : response.node_endpoints) {
        if (endpoint.node_id == *leader) {
          address = {endpoint.host, endpoint.port};
          break;
        }
      }
    }
    return campaign.ObserveLeader(part.leader_epoch, leader, address);
  }
  if (part.error_code == kInvalidVoterKey)
    return {};
  if (part.error_code == kInvalidRequest)
    return make_error_code(errc::protocol_error);
  if (part.error_code != 0)
    return make_error_code(errc::resource_unavailable_try_again);
  *acknowledged = true;
  return {};
}

}  // namespace

EpochControl::EpochControl(ElectionCampaign& campaign, string listener, int32_t fetch_timeout_ms,
                           int32_t max_election_backoff_ms)
    : campaign_(campaign),
      listener_(std::move(listener)),
      fetch_timeout_ms_(fetch_timeout_ms),
      max_backoff_ms_(max_election_backoff_ms) {
}

optional<ReplicaKey> EpochControl::VoterKey(int32_t id) const {
  for (const auto& voter : campaign_.voters()) {
    if (voter.key.id == id)
      return voter.key;
  }
  return nullopt;
}

void EpochControl::TrackRole(int64_t now_ms) {
  const auto& state = campaign_.state();
  if (tracked_epoch_ == state.election.epoch && tracked_role_ == state.role)
    return;

  const bool just_resigned = state.role == QuorumRole::kResigned &&
                             tracked_role_ == QuorumRole::kLeader &&
                             tracked_epoch_ == state.election.epoch;
  successors_.clear();
  if (just_resigned) {
    for (const auto& [id, peer] : peers_)
      successors_.push_back(*VoterKey(id));
    sort(successors_.begin(), successors_.end(),
         [&](const ReplicaKey& left, const ReplicaKey& right) {
           auto lhs = peers_.at(left.id).end_offset, rhs = peers_.at(right.id).end_offset;
           return lhs == rhs ? left.id < right.id : lhs > rhs;
         });
  }
  tracked_role_ = state.role;
  tracked_epoch_ = state.election.epoch;
  peers_.clear();
  fetched_voters_.clear();
  last_majority_ms_ = now_ms;
  last_begin_ms_.reset();
  if (state.role == QuorumRole::kLeader || state.role == QuorumRole::kResigned) {
    for (const auto& voter : campaign_.voters()) {
      if (voter.key.id != campaign_.target().local_replica.id)
        peers_.try_emplace(voter.key.id);
    }
  }
}

BeginQuorumEpochRequest EpochControl::MakeBegin(const ReplicaKey& recipient) const {
  BeginQuorumEpochRequest request;
  AddLocalLeader(campaign_, listener_, &request);
  request.voter_id = recipient.id;
  request.topics.front().partitions.front().voter_directory_id = recipient.directory_id;
  return request;
}

EndQuorumEpochRequest EpochControl::MakeEnd() const {
  EndQuorumEpochRequest request;
  AddLocalLeader(campaign_, listener_, &request);
  auto& part = request.topics.front().partitions.front();
  for (const auto& successor : successors_) {
    part.preferred_successors.push_back(successor.id);
    auto& candidate = part.preferred_candidates.emplace_back();
    candidate.candidate_id = successor.id;
    candidate.candidate_directory_id = successor.directory_id;
  }
  return request;
}

error_code EpochControl::Poll(int64_t now_ms, vector<EpochNotification>* notifications) {
  notifications->clear();
  if (campaign_.state().storage_error)
    return campaign_.state().storage_error;
  if (now_ms < 0 || fetch_timeout_ms_ <= 0 || max_backoff_ms_ < 0)
    return make_error_code(errc::invalid_argument);
  TrackRole(now_ms);
  if (tracked_role_ == QuorumRole::kLeader && !peers_.empty() &&
      now_ms - last_majority_ms_ >= int64_t{fetch_timeout_ms_} * 3 / 2) {
    if (auto ec = Resign(now_ms); ec)
      return ec;
  }
  if (tracked_role_ == QuorumRole::kResigned) {
    for (const auto& voter : campaign_.voters()) {
      auto it = peers_.find(voter.key.id);
      if (it != peers_.end() && !it->second.acknowledged)
        notifications->push_back({voter.key, tracked_epoch_, MakeEnd()});
    }
  }
  if (tracked_role_ != QuorumRole::kLeader)
    return {};

  const int64_t interval = fetch_timeout_ms_ / 2;
  if (last_begin_ms_ && now_ms - *last_begin_ms_ < interval)
    return {};
  last_begin_ms_ = now_ms;
  for (const auto& voter : campaign_.voters()) {
    auto it = peers_.find(voter.key.id);
    if (it == peers_.end())
      continue;
    auto last_fetch = it->second.last_fetch;
    if (!last_fetch || now_ms - *last_fetch >= interval)
      notifications->push_back({voter.key, tracked_epoch_, MakeBegin(voter.key)});
  }
  return {};
}

error_code EpochControl::Resign(int64_t now_ms) {
  TrackRole(now_ms);
  if (auto ec = campaign_.Resign(tracked_epoch_); ec)
    return ec;
  TrackRole(now_ms);
  return {};
}

void EpochControl::RecordFetch(ReplicaKey replica, int32_t epoch, int64_t now_ms,
                               optional<int64_t> end_offset) {
  TrackRole(now_ms);
  if (campaign_.state().storage_error || tracked_role_ != QuorumRole::kLeader ||
      epoch != tracked_epoch_)
    return;
  auto it = peers_.find(replica.id);
  if (it == peers_.end() || !VoterKey(replica.id)->Matches(replica))
    return;
  if (end_offset) {
    it->second.acknowledged = true;
    it->second.last_fetch = now_ms;
    it->second.end_offset = end_offset;
  }
  fetched_voters_.insert(replica.id);

  // A fixed quorum's leader contributes one. Only distinct voter Fetch or FetchSnapshot
  // contacts may extend the deadline; a successful BeginQuorumEpoch response cannot.
  if (1 + fetched_voters_.size() >= campaign_.voters().size() / 2 + 1) {
    last_majority_ms_ = now_ms;
    fetched_voters_.clear();
  }
}

int32_t EpochControl::SuccessorBackoff(
    const EndQuorumEpochRequest::TopicData::PartitionData& part) const {
  const auto& local = campaign_.target().local_replica;
  size_t position = 0;
  auto count = part.preferred_candidates.size();
  if (count > 0) {
    for (const auto& candidate : part.preferred_candidates) {
      if (ReplicaKey{candidate.candidate_id, candidate.candidate_directory_id}.Matches(local))
        break;
      ++position;
    }
  } else {
    count = part.preferred_successors.size();
    position = find(part.preferred_successors.begin(), part.preferred_successors.end(), local.id) -
               part.preferred_successors.begin();
  }

  if (position == 0)
    return 0;
  if (position == count)
    return max_backoff_ms_;

  // This is Kafka's strict exponential delay: an integer base rounded down before
  // scaling it by the successor position.
  // Java int shifts use the low five bits of the shift distance.
  const auto base = static_cast<uint32_t>(max_backoff_ms_) >> ((count - 1) & 31);
  return min(max_backoff_ms_, bit_cast<int32_t>(base << ((position - 1) & 31)));
}

vector<int32_t> EpochControl::UnacknowledgedVoters() const {
  vector<int32_t> result;
  for (const auto& [id, peer] : peers_) {
    if (!peer.acknowledged)
      result.push_back(id);
  }
  return result;
}

error_code EpochControl::ReceiveBegin(const BeginQuorumEpochRequest& request, int16_t version,
                                      BeginQuorumEpochResponse* response) {
  return HandleNotification(campaign_, listener_, request, version, response);
}

error_code EpochControl::ReceiveEnd(const EndQuorumEpochRequest& request, int16_t version,
                                    EndQuorumEpochResponse* response,
                                    optional<int32_t>* override_ms) {
  override_ms->reset();
  if (auto ec = HandleNotification(campaign_, listener_, request, version, response); ec)
    return ec;
  if (response->error_code == 0 && response->topics.front().partitions.front().error_code == 0 &&
      campaign_.state().role == QuorumRole::kFollower)
    *override_ms = SuccessorBackoff(request.topics.front().partitions.front());
  return {};
}

error_code EpochControl::ReceiveBeginResponse(const ReplicaKey& source, int32_t sent_epoch,
                                              const BeginQuorumEpochResponse& response) {
  if (campaign_.state().storage_error)
    return campaign_.state().storage_error;
  auto key = VoterKey(source.id);
  if (!key || !key->Matches(source) || campaign_.state().election.epoch != sent_epoch ||
      campaign_.state().role != QuorumRole::kLeader)
    return {};
  bool acknowledged;
  if (auto ec = ApplyNotificationResponse(campaign_, response, &acknowledged); ec)
    return ec;
  if (acknowledged && peers_.contains(source.id))
    peers_.at(source.id).acknowledged = true;
  return {};
}

error_code EpochControl::ReceiveEndResponse(const ReplicaKey& source, int32_t sent_epoch,
                                            const EndQuorumEpochResponse& response) {
  if (campaign_.state().storage_error)
    return campaign_.state().storage_error;
  auto peer = peers_.find(source.id);
  if (peer == peers_.end() || !VoterKey(source.id)->Matches(source) ||
      campaign_.state().election.epoch != sent_epoch ||
      campaign_.state().role != QuorumRole::kResigned)
    return {};
  bool acknowledged;
  if (auto ec = ApplyNotificationResponse(campaign_, response, &acknowledged); ec)
    return ec;
  if (acknowledged)
    peer->second.acknowledged = true;
  return {};
}

}  // namespace dfly::cluster::raft
