// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include "server/cluster/raft/log/partition_log.h"
#include "server/cluster/raft/protocol/fetch.h"
#include "server/cluster/raft/quorum/epoch_control.h"
#include "server/cluster/raft/quorum/membership_history.h"
#include "server/cluster/raft/quorum/replica_progress.h"
#include "server/cluster/raft/replication/high_watermark.h"

namespace dfly::cluster::raft {

struct OutboundFetch {
  int32_t peer_id;
  int32_t epoch;
  uint64_t sequence;
  int16_t version;
  protocol::FetchRequest request;
};

// Per-quorum log replication. Run all methods on the partition's serialized I/O
// worker. The event loop polls SyncRole immediately after a role change.
class FetchReplication {
 public:
  FetchReplication(ElectionCampaign& campaign, EpochControl& epochs, PartitionLog& log,
                   protocol::Uuid topic_id, std::optional<int64_t> recovered_hw = std::nullopt);

  // Also invoked before Recover on startup with an existing log or a bootstrap snapshot.
  [[nodiscard]] absl::Status RefreshMembership(bool force = false);
  const MembershipHistory& membership() const {
    return membership_;
  }

  // On becoming leader append and flush its LeaderChangeMessage before serving Fetch.
  [[nodiscard]] absl::Status SyncRole(int64_t timestamp_ms);
  [[nodiscard]] absl::Status Append(std::string_view batches, int32_t expected_epoch);
  std::optional<int64_t> HighWatermark() const;
  int64_t LogEndOffset() const {
    return log_.End().end_offset;
  }
  std::string_view ListenerName() const {
    return epochs_.ListenerName();
  }
  QuorumReplicaProgress& replica_progress() {
    return replica_progress_;
  }

  using FetchPartition = protocol::FetchRequest::FetchTopic::FetchPartition;
  using AdvertiseApplied = std::function<void(FetchPartition*)>;
  using ObserveApplied = std::function<void(ReplicaKey, const FetchPartition&, int64_t, int64_t)>;
  void SetDataApplyHooks(AdvertiseApplied advertise, ObserveApplied observe) {
    advertise_applied_ = std::move(advertise);
    observe_applied_ = std::move(observe);
  }

  // can_wait means a valid empty Fetch can remain outstanding. On a log append, HW advance
  // or role change, recompute without a new contact_ms to count the original contact
  // only once. On timeout send the retained response.
  [[nodiscard]] absl::Status HandleFetch(const protocol::FetchRequest& request, int16_t version,
                                         std::optional<int64_t> contact_ms, int64_t wall_ms,
                                         protocol::FetchResponse* response, bool* can_wait);

  // A probe from an unattached replica can learn the current leader; data is installed
  // only from a matched follower fetch. Superseding an outstanding fetch fences its reply.
  [[nodiscard]] std::optional<OutboundFetch> PrepareFetch(int32_t peer_id, int16_t version,
                                                          int32_t max_wait_ms, int32_t max_bytes);
  [[nodiscard]] absl::Status ReceiveFetchResponse(int32_t source_id, uint64_t sequence,
                                                  const protocol::FetchResponse& response,
                                                  bool* reset_fetch_timeout = nullptr);
  std::optional<LogPosition> RequestedSnapshot() const {
    const auto& state = campaign_.state();
    if (state.role != QuorumRole::kFollower || state.election.epoch != request_epoch_ ||
        state.storage_error)
      return std::nullopt;
    return requested_snapshot_;
  }
  void AbandonSnapshot(LogPosition snapshot_id) {
    if (requested_snapshot_ == snapshot_id)
      requested_snapshot_.reset();
  }

  // Decode the downloaded quorum configuration before replacing the previous durable log.
  [[nodiscard]] absl::Status ValidateSnapshot(const SnapshotWriter& writer) const;

  // Resume polling after the owner has atomically installed the requested snapshot.
  [[nodiscard]] absl::Status SnapshotInstalled(LogPosition snapshot_id);

 private:
  absl::StatusOr<std::vector<CampaignVoter>> ConfiguredVoters(
      const MembershipHistory& history) const;
  void UpdateHighWatermark();
  absl::Status DurableFailure(absl::Status status);

  ElectionCampaign& campaign_;
  EpochControl& epochs_;
  PartitionLog& log_;
  protocol::Uuid topic_id_;
  QuorumReplicaProgress replica_progress_;
  AdvertiseApplied advertise_applied_;
  ObserveApplied observe_applied_;
  const std::vector<CampaignVoter> static_voters_;
  MembershipHistory membership_;
  std::optional<int64_t> active_voter_offset_;
  int16_t active_kraft_version_ = 0;
  std::optional<int64_t> committed_;
  std::optional<LeaderHighWatermark> leader_hw_;
  std::vector<std::optional<LogOffsetMetadata>> voter_ends_;
  int32_t leader_epoch_ = -1;
  std::optional<LogPosition> requested_snapshot_;
  int32_t request_epoch_ = -1;
  QuorumRole request_role_ = QuorumRole::kUnattached;
  std::optional<int32_t> request_peer_;
  uint64_t sequence_ = 0;
  absl::Status io_error_;
};

}  // namespace dfly::cluster::raft
