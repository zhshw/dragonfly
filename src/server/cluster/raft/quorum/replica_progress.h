// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <map>

#include "server/cluster/raft/protocol/describe_quorum.h"
#include "server/cluster/raft/quorum/campaign.h"

namespace dfly::cluster::raft {

// Fetch progress belongs to one leadership epoch. Observers are keyed by their full
// replica identity. A configured voter with no directory ID accepts any directory.
class QuorumReplicaProgress {
 public:
  explicit QuorumReplicaProgress(const ElectionCampaign& campaign) : campaign_(campaign) {
  }

  void Reset();
  void Record(ReplicaKey source, int64_t fetch_offset, int64_t leader_end, int64_t wall_ms);
  void Describe(int16_t version, int64_t leader_end, int64_t wall_ms,
                protocol::DescribeQuorumResponse::TopicData::PartitionData* partition);

 private:
  struct Progress {
    int64_t offset = -1;
    int64_t last_fetch = -1;
    int64_t last_caught_up = -1;
    int64_t previous_leader_end = -1;
  };

  const ElectionCampaign& campaign_;
  std::map<int32_t, Progress> voters_;
  std::map<std::pair<int32_t, DirectoryId>, Progress> observers_;
};

}  // namespace dfly::cluster::raft
