// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/replica_progress.h"

#include <algorithm>

namespace dfly::cluster::raft {
using namespace std;

void QuorumReplicaProgress::Reset() {
  voters_.clear();
  observers_.clear();
}

void QuorumReplicaProgress::Reconfigure(span<const CampaignVoter> previous) {
  auto old = std::move(voters_);
  voters_.clear();
  for (const auto& voter : campaign_.voters()) {
    auto same = find_if(previous.begin(), previous.end(),
                        [&](const auto& node) { return node.key.Matches(voter.key); });
    const auto identity = pair{voter.key.id, voter.key.directory_id};
    if (auto it = old.find(voter.key.id); same != previous.end() && it != old.end())
      voters_.emplace(voter.key.id, it->second);
    else if (auto observed = observers_.find(identity); observed != observers_.end())
      voters_.emplace(voter.key.id, observed->second);
    observers_.erase(identity);
    old.erase(voter.key.id);
  }
  for (const auto& voter : previous) {
    if (auto it = old.find(voter.key.id); it != old.end())
      observers_.try_emplace({voter.key.id, voter.key.directory_id}, it->second);
  }
}

optional<int64_t> QuorumReplicaProgress::VoterOffset(int32_t id) const {
  auto it = voters_.find(id);
  return it == voters_.end() ? nullopt : optional<int64_t>{it->second.offset};
}

bool QuorumReplicaProgress::IsObserverCaughtUp(ReplicaKey key, int64_t wall_ms) const {
  auto it = observers_.find({key.id, key.directory_id});
  constexpr int64_t kMaxFetchAgeMs = 3'600'000;
  return it != observers_.end() && it->second.last_caught_up > 0 && it->second.last_fetch > 0 &&
         it->second.last_fetch > wall_ms - kMaxFetchAgeMs;
}

void QuorumReplicaProgress::Record(ReplicaKey source, int64_t fetch_offset, int64_t leader_end,
                                   int64_t wall_ms) {
  if (source.id < 0 || source.id == campaign_.target().local_replica.id)
    return;
  const auto& voters = campaign_.voters();
  bool voter =
      any_of(voters.begin(), voters.end(), [&](const auto& v) { return v.key.Matches(source); });
  Progress& progress = voter ? voters_[source.id] : observers_[{source.id, source.directory_id}];

  // Kafka credits a replica that reaches the previous Fetch's leader end at the
  // timestamp of that previous Fetch, even when the leader has appended since.
  if (fetch_offset >= leader_end)
    progress.last_caught_up = max(progress.last_caught_up, wall_ms);
  else if (progress.previous_leader_end > 0 && fetch_offset >= progress.previous_leader_end)
    progress.last_caught_up = max(progress.last_caught_up, progress.last_fetch);
  progress.previous_leader_end = leader_end;
  progress.last_fetch = max(progress.last_fetch, wall_ms);
  progress.offset = fetch_offset;
}

void QuorumReplicaProgress::Describe(
    int16_t version, int64_t leader_end, int64_t wall_ms,
    protocol::DescribeQuorumResponse::TopicData::PartitionData* part) {
  auto describe = [version](const ReplicaKey& key, const Progress& progress) {
    protocol::DescribeQuorumResponse::ReplicaState state;
    state.replica_id = key.id;
    if (version >= 2)
      state.replica_directory_id = key.directory_id;
    state.log_end_offset = progress.offset;
    state.last_caught_up_timestamp = progress.last_caught_up;
    state.last_fetch_timestamp = progress.last_fetch;
    return state;
  };

  for (const auto& voter : campaign_.voters()) {
    Progress progress;
    if (voter.key.Matches(campaign_.target().local_replica)) {
      progress = {leader_end, wall_ms, wall_ms};
    } else if (auto it = voters_.find(voter.key.id); it != voters_.end()) {
      progress = it->second;
    }
    part->current_voters.push_back(describe(voter.key, progress));
  }

  erase_if(observers_, [wall_ms](const auto& observer) {
    constexpr int64_t kObserverSessionTimeoutMs = 300'000;
    return wall_ms - observer.second.last_fetch >= kObserverSessionTimeoutMs;
  });
  for (const auto& [identity, progress] : observers_) {
    part->observers.push_back(describe({identity.first, identity.second}, progress));
  }
}

}  // namespace dfly::cluster::raft
