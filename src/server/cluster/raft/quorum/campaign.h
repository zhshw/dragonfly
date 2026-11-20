// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <map>

#include "server/cluster/raft/config/quorum_config.h"
#include "server/cluster/raft/quorum/vote_handler.h"

namespace dfly::cluster::raft {

// A campaign uses the quorum's current fixed voter configuration and one internal listener.
struct CampaignVoter {
  ReplicaKey key;
  QuorumEndpoint endpoint;
};

struct OutboundVote {
  ReplicaKey recipient;
  uint64_t sequence;
  protocol::VoteRequest request;
};

class ElectionCampaign {
 public:
  ElectionCampaign(VoteTarget target, std::vector<CampaignVoter> voters, PersistElection persist);
  ElectionCampaign(const ElectionCampaign&) = delete;
  ElectionCampaign& operator=(const ElectionCampaign&) = delete;

  // Initialize a fresh instance from the recovered quorum-state and log/snapshot.
  // A leader restarts resigned. A durable self vote resumes its existing candidacy.
  [[nodiscard]] std::error_code Recover(ElectionState durable, LogPosition log_end);

  // The event loop supplies the election/fetch deadline. On expiry a candidate starts
  // a new pre-vote in the same epoch; a lost candidate waits until that deadline.
  [[nodiscard]] std::error_code OnElectionTimeout();

  // Eligible destinations only. PrepareVote replaces any timed-out request to that peer.
  // After a pre-vote rejected by version negotiation, pass UNSUPPORTED_VERSION to
  // ReceiveVoteResponse with that request's sequence.
  std::vector<ReplicaKey> UnrecordedVoters() const;
  [[nodiscard]] std::optional<OutboundVote> PrepareVote(const ReplicaKey& recipient,
                                                        LogPosition log_end);
  [[nodiscard]] std::error_code ReceiveVoteResponse(const ReplicaKey& source, uint64_t sequence,
                                                    const protocol::VoteResponse& response);

  [[nodiscard]] std::error_code ReceiveVote(const protocol::VoteRequest& request, int16_t version,
                                            LogPosition log_end, protocol::VoteResponse* response);

  // Called only when Fetch succeeds from the current leader in the specified epoch.
  void MarkFetchedFromLeader(int32_t leader_id, int32_t epoch);

  const QuorumVoteState& state() const {
    return state_;
  }
  const auto& LeaderEndpoint() const {
    return leader_endpoint_;
  }

  // Voters needed for the new leader's LeaderChangeMessage.
  std::vector<int32_t> GrantingVoters() const;

 private:
  using Endpoint = protocol::VoteResponse::NodeEndpoint;
  std::optional<Endpoint> KnownEndpoint(int32_t node_id) const;
  std::error_code Save(ElectionState next, QuorumRole role);
  std::error_code StartRound(VoteType type);
  std::error_code BecomeLeader();
  std::error_code LosePreVote();
  std::error_code FollowLeader(int32_t epoch, std::optional<int32_t> leader,
                               const std::vector<Endpoint>& endpoints);

  VoteTarget target_;
  std::vector<CampaignVoter> voters_;
  std::vector<ReplicaKey> voter_keys_;
  PersistElection persist_;
  QuorumVoteState state_;
  bool recovered_ = false;
  std::optional<Endpoint> leader_endpoint_;
  std::optional<EpochElection> round_;
  uint64_t sequence_ = 0;
  std::map<int32_t, uint64_t> pending_;
};

}  // namespace dfly::cluster::raft
