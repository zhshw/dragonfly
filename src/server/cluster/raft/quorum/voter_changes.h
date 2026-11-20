// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/raft/protocol/add_voter.h"
#include "server/cluster/raft/protocol/api_versions.h"
#include "server/cluster/raft/protocol/remove_voter.h"
#include "server/cluster/raft/protocol/update_voter.h"
#include "server/cluster/raft/quorum/fetch_replication.h"

namespace dfly::cluster::raft {

struct VoterChangeResult {
  // Empty while waiting for an ApiVersions reply or a new majority to commit.
  std::optional<int16_t> error;
  std::optional<std::string> message;
};

// No group identifier exists in these Kafka RPCs. Each instance belongs to the
// quorum on its dedicated administrative listener.
class VoterChanges {
 public:
  struct Probe {
    uint64_t sequence;
    ReplicaKey key;
    QuorumEndpoint endpoint;
  };
  using Result = std::shared_ptr<const VoterChangeResult>;

  VoterChanges(ElectionCampaign& campaign, EpochControl& epochs, FetchReplication& fetch,
               int32_t timeout_ms)
      : campaign_(campaign), epochs_(epochs), fetch_(fetch), timeout_ms_(timeout_ms) {
  }

  Result Begin(const protocol::AddRaftVoterRequest& request, int64_t now_ms, int64_t wall_ms);
  Result Begin(const protocol::RemoveRaftVoterRequest& request, int64_t now_ms, int64_t wall_ms);
  Result Begin(const protocol::UpdateRaftVoterRequest& request, int64_t now_ms, int64_t wall_ms);

  std::optional<Probe> TakeProbe();
  // On transport failure pass an ApiVersionsResponse bearing the transport error code.
  [[nodiscard]] absl::Status OnApiVersions(const Probe& probe,
                                           const protocol::ApiVersionsResponse& response,
                                           int64_t now_ms, int64_t wall_ms);
  void Poll(int64_t now_ms);

 private:
  enum class Operation { kAdd, kRemove, kUpdate };
  struct Pending {
    Operation op;
    protocol::VotersRecord::Voter voter;
    bool wait_for_commit = true;
    bool probe_sent = false;
    std::optional<int64_t> offset;
    int32_t epoch;
    int64_t deadline;
    uint64_t sequence;
    QuorumEndpoint endpoint;
    std::shared_ptr<VoterChangeResult> result;
  };

  template <typename Request>
  Result Start(const Request& request, Operation op, int64_t now_ms, int64_t wall_ms);
  [[nodiscard]] absl::Status Apply(int64_t now_ms, int64_t wall_ms);
  void Finish(int16_t error, std::optional<std::string> message = std::nullopt);

  ElectionCampaign& campaign_;
  EpochControl& epochs_;
  FetchReplication& fetch_;
  int32_t timeout_ms_;
  uint64_t sequence_ = 0;
  std::optional<Pending> pending_;

  // Kafka keeps updates to a static quorum's advertised identity and supported
  // feature range in memory, scoped to the leadership epoch.
  int32_t volatile_epoch_ = -1;
  protocol::VotersRecord volatile_voters_;
};

}  // namespace dfly::cluster::raft
