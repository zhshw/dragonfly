// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <map>

#include "server/cluster/raft/protocol/fetch.h"
#include "server/cluster/raft/quorum/election.h"

namespace dfly::cluster::raft {

// Dragonfly data-group extension on flexible Fetch partitions. A metadata quorum must never
// attach these hooks. Each data quorum owns a separate tracker on its serialized disk worker.
class DataApplyProgress {
 public:
  using FetchPartition = protocol::FetchRequest::FetchTopic::FetchPartition;
  static void Advertise(int64_t applied_offset, FetchPartition* partition);

  // Called only after a valid current-voter Fetch. durable_offset is the common prefix
  // actually verified by the leader, including the adjustment for a mid-batch request.
  void Observe(ReplicaKey source, const FetchPartition& partition, int64_t durable_offset,
               int64_t now_ms);

  // The caller supplies a current group voter, the local leadership epoch and monotonic time.
  std::optional<int64_t> Applied(ReplicaKey voter, int32_t epoch, int64_t now_ms,
                                 int64_t max_age_ms) const;

 private:
  struct Report {
    ReplicaKey source;
    int32_t epoch;
    int64_t applied;
    int64_t time_ms;
  };
  std::map<int32_t, Report> peers_;
};

}  // namespace dfly::cluster::raft
