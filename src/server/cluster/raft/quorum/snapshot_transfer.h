// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/raft/protocol/fetch_snapshot.h"
#include "server/cluster/raft/quorum/fetch_replication.h"

namespace dfly::cluster::raft {

struct OutboundSnapshotFetch {
  int32_t peer_id;
  int32_t epoch;
  uint64_t sequence;
  int16_t version;
  protocol::FetchSnapshotRequest request;
};

// One transfer per partition. Serialize calls with FetchReplication and partition log I/O.
class SnapshotTransfer {
 public:
  SnapshotTransfer(ElectionCampaign& campaign, EpochControl& epochs, FetchReplication& fetch,
                   PartitionLog& log, SnapshotValidator validate = {})
      : campaign_(campaign),
        epochs_(epochs),
        fetch_(fetch),
        log_(log),
        validate_(std::move(validate)) {
  }

  // Call after FetchReplication::SyncRole to drop stale requests and partial snapshots.
  void SyncRole();

  // Next byte to fetch in the current partial snapshot, or zero if there is no download.
  int64_t DownloadPosition() const {
    return writer_ ? writer_->position() : 0;
  }
  [[nodiscard]] absl::Status HandleRequest(const protocol::FetchSnapshotRequest& request,
                                           int16_t version, int64_t monotonic_ms,
                                           protocol::FetchSnapshotResponse* response);
  [[nodiscard]] std::optional<OutboundSnapshotFetch> Prepare(int16_t version, int32_t max_bytes);
  [[nodiscard]] absl::Status ReceiveResponse(int32_t source_id, uint64_t sequence,
                                             const protocol::FetchSnapshotResponse& response,
                                             bool* reset_fetch_timeout);

 private:
  ElectionCampaign& campaign_;
  EpochControl& epochs_;
  FetchReplication& fetch_;
  PartitionLog& log_;
  SnapshotValidator validate_;
  std::optional<LogPosition> target_;
  std::unique_ptr<SnapshotWriter> writer_;
  int32_t leader_ = -1;
  int32_t term_ = -1;
  int32_t max_bytes_ = 0;
  uint64_t sequence_ = 0;
  bool pending_ = false;
};

}  // namespace dfly::cluster::raft
