// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <set>
#include <variant>

#include "server/cluster/raft/protocol/begin_epoch.h"
#include "server/cluster/raft/protocol/end_epoch.h"
#include "server/cluster/raft/quorum/campaign.h"

namespace dfly::cluster::raft {

using EpochRequest =
    std::variant<protocol::BeginQuorumEpochRequest, protocol::EndQuorumEpochRequest>;

struct EpochNotification {
  ReplicaKey recipient;
  int32_t epoch;
  EpochRequest request;
};

// The owner polls immediately on role change, then on its timer. As with Vote, the
// transport correlates each response with its originating request before delivery.
class EpochControl {
 public:
  EpochControl(ElectionCampaign& campaign, std::string listener, int32_t fetch_timeout_ms,
               int32_t max_election_backoff_ms);

  // Caller sends the eligible notifications subject to per-peer transport backoff.
  // A check-quorum timeout first resigns the leader and returns EndQuorumEpoch messages.
  [[nodiscard]] std::error_code Poll(int64_t now_ms, std::vector<EpochNotification>* notifications);

  // Accepted Fetch and FetchSnapshot requests establish liveness for this epoch. Pass
  // the follower's known end offset on Fetch to order preferred successors on resignation.
  void RecordFetch(ReplicaKey replica, int32_t epoch, int64_t now_ms,
                   std::optional<int64_t> end_offset);
  [[nodiscard]] std::error_code Resign(int64_t now_ms);
  std::vector<int32_t> UnacknowledgedVoters() const;

  [[nodiscard]] std::error_code ReceiveBegin(const protocol::BeginQuorumEpochRequest& request,
                                             int16_t version,
                                             protocol::BeginQuorumEpochResponse* response);
  [[nodiscard]] std::error_code ReceiveEnd(const protocol::EndQuorumEpochRequest& request,
                                           int16_t version,
                                           protocol::EndQuorumEpochResponse* response,
                                           std::optional<int32_t>* fetch_timeout_override_ms);

  [[nodiscard]] std::error_code ReceiveBeginResponse(
      const ReplicaKey& source, int32_t sent_epoch,
      const protocol::BeginQuorumEpochResponse& response);
  [[nodiscard]] std::error_code ReceiveEndResponse(
      const ReplicaKey& source, int32_t sent_epoch,
      const protocol::EndQuorumEpochResponse& response);

 private:
  struct PeerState {
    std::optional<int64_t> last_fetch;
    std::optional<int64_t> end_offset;
    bool acknowledged = false;
  };

  void TrackRole(int64_t now_ms);
  std::optional<ReplicaKey> VoterKey(int32_t id) const;
  protocol::BeginQuorumEpochRequest MakeBegin(const ReplicaKey& recipient) const;
  protocol::EndQuorumEpochRequest MakeEnd() const;
  int32_t SuccessorBackoff(
      const protocol::EndQuorumEpochRequest::TopicData::PartitionData& part) const;

  ElectionCampaign& campaign_;
  std::string listener_;
  int32_t fetch_timeout_ms_;
  int32_t max_backoff_ms_;
  QuorumRole tracked_role_ = QuorumRole::kUnattached;
  int32_t tracked_epoch_ = -1;
  int64_t last_majority_ms_ = 0;
  std::optional<int64_t> last_begin_ms_;
  std::map<int32_t, PeerState> peers_;
  std::set<int32_t> fetched_voters_;
  std::vector<ReplicaKey> successors_;
};

}  // namespace dfly::cluster::raft
