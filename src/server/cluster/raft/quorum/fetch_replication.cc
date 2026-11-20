// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/fetch_replication.h"

#include <algorithm>

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

constexpr int16_t kMinVersion = 13;  // Fetch by topic UUID
constexpr int16_t kNotLeaderOrFollower = 6;
constexpr int16_t kFencedLeaderEpoch = 74;
constexpr int16_t kUnknownLeaderEpoch = 75;

absl::Status StorageStatus(const QuorumVoteState& state) {
  if (state.storage_error)
    return absl::UnavailableError(state.storage_error.message());
  return absl::OkStatus();
}

optional<QuorumEndpoint> LeaderAddress(int32_t leader_id, const FetchResponse& response) {
  for (const auto& node : response.node_endpoints) {
    if (node.node_id == leader_id && !node.host.empty() && node.port > 0 && node.port <= UINT16_MAX)
      return QuorumEndpoint{node.host, static_cast<uint16_t>(node.port)};
  }
  return nullopt;
}
}  // namespace

FetchReplication::FetchReplication(ElectionCampaign& campaign, EpochControl& epochs,
                                   PartitionLog& log, Uuid topic_id, optional<int64_t> recovered_hw)
    : campaign_(campaign),
      epochs_(epochs),
      log_(log),
      topic_id_(topic_id),
      replica_progress_(campaign),
      committed_(recovered_hw) {
  if (committed_ && (*committed_ < 0 || *committed_ > log_.End().end_offset)) {
    DurableFailure(absl::DataLossError("Recovered quorum high watermark outside durable log"))
        .IgnoreError();
  } else if (log_.StartOffset() > committed_.value_or(0)) {
    committed_ = log_.StartOffset();
  }
}

absl::Status FetchReplication::DurableFailure(absl::Status failure) {
  io_error_ = std::move(failure);
  campaign_.FenceStorage();
  return io_error_;
}

absl::Status FetchReplication::SyncRole(int64_t timestamp_ms) {
  const auto& state = campaign_.state();
  if (!io_error_.ok())
    return io_error_;
  if (auto status = StorageStatus(state); !status.ok())
    return status;
  if (state.role != request_role_ || state.election.epoch != request_epoch_) {
    request_peer_.reset();
    requested_snapshot_.reset();
  }
  if (state.role != QuorumRole::kLeader) {
    if (leader_hw_)
      replica_progress_.Reset();
    leader_hw_.reset();
    voter_ends_.clear();
    leader_epoch_ = -1;
    return absl::OkStatus();
  }
  if (leader_epoch_ == state.election.epoch)
    return absl::OkStatus();

  vector<int32_t> voters;
  for (const auto& voter : campaign_.voters())
    voters.push_back(voter.key.id);
  sort(voters.begin(), voters.end());
  const auto start = log_.End().end_offset;
  auto status = log_.AppendLeaderChange(state.election.epoch, timestamp_ms,
                                        campaign_.target().local_replica.id, voters,
                                        campaign_.GrantingVoters());
  if (!status.ok())
    return DurableFailure(status);
  leader_epoch_ = state.election.epoch;
  replica_progress_.Reset();
  leader_hw_.emplace(start);
  voter_ends_.assign(voters.size(), nullopt);
  UpdateHighWatermark();
  return absl::OkStatus();
}

void FetchReplication::UpdateHighWatermark() {
  const auto& voters = campaign_.voters();
  for (size_t i = 0; i < voters.size(); ++i) {
    if (voters[i].key.Matches(campaign_.target().local_replica))
      voter_ends_[i] = LogOffsetMetadata{log_.End().end_offset};
  }
  if (leader_hw_->Update(voter_ends_))
    committed_ = max(committed_.value_or(0), leader_hw_->value()->offset);
}

optional<int64_t> FetchReplication::HighWatermark() const {
  if (campaign_.state().storage_error)
    return nullopt;
  if (campaign_.state().role != QuorumRole::kLeader)
    return committed_;
  if (!leader_hw_ || leader_epoch_ != campaign_.state().election.epoch || !leader_hw_->value())
    return nullopt;
  return leader_hw_->value()->offset;
}

absl::Status FetchReplication::Append(string_view batches, int32_t expected_epoch) {
  const auto& state = campaign_.state();
  if (auto s = StorageStatus(state); !s.ok())
    return s;
  if (state.role != QuorumRole::kLeader || state.election.epoch != expected_epoch ||
      leader_epoch_ != expected_epoch)
    return absl::FailedPreconditionError(
        "Append requires an initialized leader of the specified epoch");
  if (auto s = log_.Append(batches, expected_epoch); !s.ok())
    return log_.io_failure().ok() ? s : DurableFailure(s);
  UpdateHighWatermark();
  return absl::OkStatus();
}

absl::Status FetchReplication::HandleFetch(const FetchRequest& request, int16_t version,
                                           int64_t now_ms, int64_t wall_ms, FetchResponse* response,
                                           bool* can_wait) {
  *response = {};
  *can_wait = false;
  const auto& state = campaign_.state();
  if (auto s = StorageStatus(state); !s.ok())
    return s;
  if (version < kMinVersion || version > FetchRequest::kMaxVersion) {
    response->error_code = 35;
    return absl::OkStatus();
  }
  const auto& target = campaign_.target();
  if (request.cluster_id && *request.cluster_id != target.cluster_id) {
    response->error_code = 104;
    return absl::OkStatus();
  }
  if (request.topics.size() != 1 || request.topics.front().topic_id != topic_id_ ||
      request.topics.front().partitions.size() != 1 ||
      request.topics.front().partitions.front().partition != target.partition) {
    response->error_code = 42;
    return absl::OkStatus();
  }
  auto& topic = response->responses.emplace_back();
  topic.topic_id = topic_id_;
  auto& part = topic.partitions.emplace_back();
  part.partition_index = target.partition;
  part.high_watermark = -1;
  part.log_start_offset = log_.StartOffset();
  part.records.data.emplace();
  part.current_leader.leader_id = state.election.leader_id.value_or(-1);
  part.current_leader.leader_epoch = state.election.epoch;
  if (const auto& addr = campaign_.LeaderEndpoint();
      version >= 17 && addr && part.current_leader.leader_id == addr->node_id) {
    auto& node = response->node_endpoints.emplace_back();
    node.node_id = addr->node_id;
    node.host = addr->host;
    node.port = addr->port;
  }
  const auto& fetch = request.topics.front().partitions.front();
  if (request.max_wait_ms < 0 || fetch.fetch_offset < 0 || fetch.last_fetched_epoch < 0 ||
      fetch.last_fetched_epoch > fetch.current_leader_epoch) {
    part.error_code = 42;
  } else if (fetch.current_leader_epoch < state.election.epoch) {
    part.error_code = kFencedLeaderEpoch;
  } else if (fetch.current_leader_epoch > state.election.epoch) {
    part.error_code = kUnknownLeaderEpoch;
  } else if (state.role != QuorumRole::kLeader) {
    part.error_code = kNotLeaderOrFollower;
  }
  if (part.error_code)
    return absl::OkStatus();
  if (leader_epoch_ != state.election.epoch)
    return absl::FailedPreconditionError("Leader has not initialized its log");
  ReplicaKey source{version >= 15 ? request.replica_state.replica_id : request.replica_id};
  if (version >= 17)
    source.directory_id = fetch.replica_directory_id;
  auto position = log_.ValidateOffsetAndEpoch(fetch.fetch_offset, fetch.last_fetched_epoch);
  auto snapshot = log_.LatestSnapshot();
  if (fetch.fetch_offset == 0 && snapshot && *snapshot != LogPosition{0, 0})
    position = {FetchPosition::Kind::kSnapshot, *snapshot};
  switch (position.kind) {
    case FetchPosition::Kind::kSnapshot:
      part.snapshot_id.epoch = position.position.epoch;
      part.snapshot_id.end_offset = position.position.end_offset;
      break;
    case FetchPosition::Kind::kDiverging:
      part.diverging_epoch.epoch = position.position.epoch;
      part.diverging_epoch.end_offset = position.position.end_offset;
      break;
    case FetchPosition::Kind::kValid: {
      if (source.id == target.local_replica.id) {
        part.error_code = -1;
        return absl::OkStatus();
      }
      int64_t batch_start;
      auto bytes = log_.Read(fetch.fetch_offset, log_.End().end_offset, max(0, request.max_bytes),
                             &batch_start);
      if (!bytes.ok())
        return DurableFailure(bytes.status());
      part.records.data = *std::move(bytes);
      replica_progress_.Record(source, batch_start, log_.End().end_offset, wall_ms);
      const auto& voters = campaign_.voters();
      for (size_t i = 0; i < voters.size(); ++i) {
        if (voters[i].key.Matches(source)) {
          voter_ends_[i] = LogOffsetMetadata{batch_start};
          epochs_.RecordFetch(source, state.election.epoch, now_ms, batch_start);
          UpdateHighWatermark();
          break;
        }
      }
      break;
    }
  }
  part.high_watermark = HighWatermark().value_or(-1);
  *can_wait = position.kind == FetchPosition::Kind::kValid && part.records.data->empty() &&
              request.max_wait_ms > 0 &&
              (version < 18 || fetch.high_watermark >= part.high_watermark);
  return absl::OkStatus();
}

optional<OutboundFetch> FetchReplication::PrepareFetch(int32_t peer_id, int16_t version,
                                                       int32_t max_wait_ms, int32_t max_bytes) {
  const auto& state = campaign_.state();
  const auto& target = campaign_.target();
  if (state.storage_error || requested_snapshot_ || peer_id == target.local_replica.id ||
      version < kMinVersion || version > FetchRequest::kMaxVersion || max_wait_ms < 0 ||
      max_bytes < 0 ||
      (state.role != QuorumRole::kFollower && state.role != QuorumRole::kUnattached))
    return nullopt;

  request_peer_ = peer_id;
  request_epoch_ = state.election.epoch;
  request_role_ = state.role;
  OutboundFetch fetch{peer_id, request_epoch_, ++sequence_, version, {}};
  auto& req = fetch.request;
  req.cluster_id = target.cluster_id;
  req.max_bytes = max_bytes;
  req.max_wait_ms = max_wait_ms;
  if (version < 15)
    req.replica_id = target.local_replica.id;
  else
    req.replica_state.replica_id = target.local_replica.id;
  auto& topic = req.topics.emplace_back();
  topic.topic_id = topic_id_;
  auto& part = topic.partitions.emplace_back();
  part.partition = target.partition;
  part.fetch_offset = log_.End().end_offset;
  part.last_fetched_epoch = log_.End().epoch;
  part.current_leader_epoch = request_epoch_;
  if (version >= 17)
    part.replica_directory_id = target.local_replica.directory_id;
  if (version >= 18)
    part.high_watermark = HighWatermark().value_or(-1);
  return fetch;
}

absl::Status FetchReplication::ReceiveFetchResponse(int32_t source_id, uint64_t sequence,
                                                    const FetchResponse& response,
                                                    bool* reset_fetch_timeout) {
  if (reset_fetch_timeout)
    *reset_fetch_timeout = false;
  const auto& state = campaign_.state();
  if (auto s = StorageStatus(state); !s.ok())
    return s;
  if (request_peer_ != source_id || sequence != sequence_)
    return absl::OkStatus();
  request_peer_.reset();
  if (state.role != request_role_ || state.election.epoch != request_epoch_)
    return absl::OkStatus();
  if (response.error_code == 31)
    return absl::PermissionDeniedError("Quorum Fetch authorization failed");
  if (response.error_code != 0)
    return absl::UnavailableError("Quorum Fetch request was rejected");
  if (response.responses.size() != 1 || response.responses.front().topic_id != topic_id_ ||
      response.responses.front().partitions.size() != 1 ||
      response.responses.front().partitions.front().partition_index != campaign_.target().partition)
    return absl::InvalidArgumentError("Invalid Fetch topic or partition");

  const auto& part = response.responses.front().partitions.front();
  const auto leader = part.current_leader.leader_id;
  const auto epoch = part.current_leader.leader_epoch;
  if (epoch < state.election.epoch || part.error_code == kUnknownLeaderEpoch)
    return absl::OkStatus();
  if (epoch > state.election.epoch || part.error_code == kFencedLeaderEpoch ||
      part.error_code == kNotLeaderOrFollower || (leader >= 0 && !state.election.leader_id)) {
    optional<int32_t> id = leader >= 0 ? optional<int32_t>{leader} : nullopt;
    auto ec = campaign_.ObserveLeader(epoch, id, LeaderAddress(leader, response));
    return ec ? absl::ErrnoToStatus(ec.value(), "Observing leader in Fetch response")
              : absl::OkStatus();
  }
  if (part.error_code != 0)
    return absl::UnavailableError("Quorum Fetch partition was rejected");
  if (state.role != QuorumRole::kFollower || state.election.leader_id != source_id ||
      leader != source_id)
    return absl::OkStatus();

  const auto floor = committed_.value_or(0);
  const auto& divergence = part.diverging_epoch;
  const auto& snapshot = part.snapshot_id;
  if (divergence.epoch >= 0 || divergence.end_offset >= 0) {
    if (divergence.epoch < 0 || divergence.end_offset < floor)
      return absl::InvalidArgumentError("Invalid divergence or truncation below the quorum HW");
    int64_t offset;
    if (divergence.epoch == 0) {
      offset = min(divergence.end_offset, log_.End().end_offset);
    } else {
      const auto common = log_.EndForEpoch(divergence.epoch);
      offset = common.epoch == divergence.epoch ? min(common.end_offset, divergence.end_offset)
                                                : common.end_offset;
    }
    if (auto s = log_.TruncateTo(offset, floor); !s.ok())
      return log_.io_failure().ok() ? s : DurableFailure(s);
  } else if (snapshot.epoch >= 0 || snapshot.end_offset >= 0) {
    if (snapshot.epoch < 0 || snapshot.end_offset < floor || snapshot.epoch > epoch)
      return absl::InvalidArgumentError("Invalid snapshot hint in Fetch response");
    LogPosition cut{snapshot.epoch, snapshot.end_offset};
    if (log_.LatestSnapshot() != cut)
      requested_snapshot_ = cut;
  } else {
    if (part.records.data && !part.records.data->empty()) {
      // Kafka discards an invalid records payload and retries from the durable LEO.
      auto status = log_.AppendAsFollower(*part.records.data, epoch);
      if (!status.ok() && !log_.io_failure().ok())
        return DurableFailure(status);
    }
    if (part.high_watermark >= 0)
      committed_ = max(floor, min(part.high_watermark, log_.End().end_offset));
  }
  campaign_.MarkFetchedFromLeader(source_id, epoch);
  if (reset_fetch_timeout)
    *reset_fetch_timeout = true;
  return absl::OkStatus();
}

absl::Status FetchReplication::SnapshotInstalled(LogPosition snapshot_id) {
  const auto& state = campaign_.state();
  if (auto s = StorageStatus(state); !s.ok())
    return s;
  if (requested_snapshot_ != snapshot_id || request_epoch_ != state.election.epoch ||
      state.role != QuorumRole::kFollower || log_.LatestSnapshot() != snapshot_id ||
      log_.End().end_offset < snapshot_id.end_offset)
    return absl::FailedPreconditionError(
        "Snapshot does not match this follower's requested log cut");
  committed_ = max(committed_.value_or(0), snapshot_id.end_offset);
  requested_snapshot_.reset();
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
