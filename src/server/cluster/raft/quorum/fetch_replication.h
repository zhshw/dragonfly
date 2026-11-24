// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <atomic>

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
  // A successful data write can survive a later flush error or be replicated before it.
  // may_have_appended lets the client distinguish an uncertain outcome from a rejection.
  // With AsyncIO enabled success may mean accepted: callers must still wait for HW/application.
  // The owner drains the pending append before changing term, truncating or installing snapshots.
  // Move the encoded batches in to share their storage with asynchronous I/O callbacks.
  [[nodiscard]] absl::Status Append(std::string batches, int32_t expected_epoch,
                                    bool* may_have_appended = nullptr);

  // Retain the caller's immutable aggregate, also shared with its committed-effect cache.
  [[nodiscard]] absl::Status Append(std::shared_ptr<const std::string> batches,
                                    int32_t expected_epoch, bool* may_have_appended = nullptr);
  void SetAsyncIO(PartitionLog::AsyncIO submit) {
    async_io_ = std::move(submit);
  }
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

  // Called on the group worker after a matched follower append and membership validation.
  // Views refer to this immutable buffer and cover only the successfully appended prefix.
  using FetchedRecords =
      std::function<void(const PartitionLog&, const std::shared_ptr<const std::string>&,
                         std::span<const PartitionLog::ValidatedBatch>)>;
  void SetFetchedRecordsHook(FetchedRecords hook) {
    fetched_records_ = std::move(hook);
  }

  // A group of valid, empty Fetches at the same leader epoch and tail can share one copy of
  // the next written data. Readers only access it after publication on the disk worker.
  class ParkedFetch {
   public:
    std::optional<std::string_view> Records(int32_t max_bytes) const;

   private:
    friend class FetchReplication;
    struct Tail {
      LogPosition end;
      int64_t start_offset;
      int64_t high_watermark;
      bool operator==(const Tail&) const = default;
    };
    Tail tail_{};
    size_t max_bytes_ = 0;
    size_t first_batch_size_ = 0;
    std::string bytes_;
    std::atomic_bool ready_{false};
  };

  void SetFetchWakeup(std::function<void()> wakeup) {
    wake_fetch_ = std::move(wakeup);
  }

  // can_wait means a valid empty Fetch can remain outstanding. On a log append, HW advance
  // or role change, recompute without a new contact_ms to count the original contact
  // only once. On timeout send the retained response.
  [[nodiscard]] absl::Status HandleFetch(const protocol::FetchRequest& request, int16_t version,
                                         std::optional<int64_t> contact_ms, int64_t wall_ms,
                                         protocol::FetchResponse* response, bool* can_wait,
                                         std::shared_ptr<const ParkedFetch>* parked = nullptr);

  // A probe from an unattached replica can learn the current leader; data is installed
  // only from a matched follower fetch. Superseding an outstanding fetch fences its reply.
  [[nodiscard]] std::optional<OutboundFetch> PrepareFetch(int32_t peer_id, int16_t version,
                                                          int32_t max_wait_ms, int32_t max_bytes);
  [[nodiscard]] absl::Status ReceiveFetchResponse(int32_t source_id, uint64_t sequence,
                                                  protocol::FetchResponse response,
                                                  bool* reset_fetch_timeout = nullptr);

  // Admission only. Defer role changes, controls and extent replacement until the flush drains.
  bool CanReceiveDuringFlush(int32_t source_id, uint64_t sequence,
                             const protocol::FetchResponse& response) const;
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
  void PublishWritten(std::string_view batches, size_t first_batch_size);
  absl::Status Appended(std::string_view batches);
  ParkedFetch::Tail CurrentFetchTail() const;
  absl::Status DurableFailure(absl::Status status);

  ElectionCampaign& campaign_;
  EpochControl& epochs_;
  PartitionLog& log_;
  protocol::Uuid topic_id_;
  QuorumReplicaProgress replica_progress_;
  AdvertiseApplied advertise_applied_;
  ObserveApplied observe_applied_;
  FetchedRecords fetched_records_;
  std::weak_ptr<ParkedFetch> parked_fetch_;
  std::function<void()> wake_fetch_;
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
  PartitionLog::AsyncIO async_io_;
};

}  // namespace dfly::cluster::raft
