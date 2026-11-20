// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/election.h"

namespace dfly::cluster::raft {

bool ReplicaKey::Matches(const ReplicaKey& replica) const {
  return id == replica.id &&
         (directory_id == DirectoryId{} || directory_id == replica.directory_id);
}

bool VoteContext::CanGrantVote(const ReplicaKey& candidate, LogPosition candidate_log,
                               LogPosition local_log, VoteType type) const {
  const bool up_to_date = candidate_log >= local_log;
  switch (role) {
    case QuorumRole::kUnattached:
    case QuorumRole::kProspective:
      if (type == VoteType::kPreVote)
        return up_to_date;

      // A retransmission of an already persisted vote may arrive after the local log advances.
      if (voted_key)
        return voted_key->Matches(candidate);
      return !leader_id && up_to_date;

    case QuorumRole::kCandidate:
    case QuorumRole::kResigned:
      return type == VoteType::kPreVote && up_to_date;

    case QuorumRole::kFollower:
      return type == VoteType::kPreVote && !fetched_from_leader && up_to_date;

    case QuorumRole::kLeader:
      return false;
  }
  return false;
}

std::optional<EpochElection> EpochElection::Start(std::span<const ReplicaKey> voters,
                                                  const ReplicaKey& local, VoteType type) {
  EpochElection election(local.id, type);
  bool found_local = false;
  for (const auto& voter : voters) {
    const bool is_local = voter.Matches(local);
    Voter state{voter.directory_id, std::nullopt};
    if (is_local) {
      state.granted = true;
      found_local = true;
    }
    if (voter.id < 0 || !election.voters_.emplace(voter.id, state).second)
      return std::nullopt;
  }
  if (!found_local)
    return std::nullopt;
  return election;
}

EpochElection::RecordResult EpochElection::RecordVote(int32_t voter_id, bool granted) {
  auto it = voters_.find(voter_id);
  if (it == voters_.end() || (voter_id == local_id_ && !granted))
    return RecordResult::kInvalid;
  auto& vote = it->second.granted;
  if (vote.has_value() && type_ == VoteType::kVote && *vote != granted)
    return RecordResult::kInvalid;
  auto result = vote.has_value() ? RecordResult::kRepeat : RecordResult::kFirst;
  vote = granted;
  return result;
}

EpochElection::Outcome EpochElection::GetOutcome() const {
  size_t granted = 0, unrecorded = 0;
  for (const auto& [id, voter] : voters_) {
    if (!voter.granted.has_value())
      ++unrecorded;
    else if (*voter.granted)
      ++granted;
  }

  // For an even-sized quorum, a tie already rules out a majority of grants.
  const auto majority = voters_.size() / 2 + 1;
  if (granted >= majority)
    return Outcome::kGranted;
  if (granted + unrecorded < majority)
    return Outcome::kRejected;
  return Outcome::kPending;
}

std::vector<ReplicaKey> EpochElection::UnrecordedVoters() const {
  std::vector<ReplicaKey> voters;
  for (const auto& [id, voter] : voters_) {
    if (!voter.granted.has_value())
      voters.push_back({id, voter.directory_id});
  }
  return voters;
}

std::vector<int32_t> EpochElection::GrantingVoters() const {
  std::vector<int32_t> voters;
  for (const auto& [id, voter] : voters_) {
    if (voter.granted.value_or(false))
      voters.push_back(id);
  }
  return voters;
}

}  // namespace dfly::cluster::raft
