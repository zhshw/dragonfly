// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/snapshot_transfer.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

constexpr int16_t kNotLeader = 6;
constexpr int16_t kFencedEpoch = 74;
constexpr int16_t kUnknownEpoch = 75;
constexpr int16_t kSnapshotNotFound = 98;

template <typename Message> bool IsSingleton(const Message& msg) {
  return msg.topics.size() == 1 && msg.topics.front().partitions.size() == 1;
}
}  // namespace

absl::Status SnapshotTransfer::HandleRequest(const FetchSnapshotRequest& request, int16_t version,
                                             int64_t now_ms, FetchSnapshotResponse* response) {
  *response = {};
  const auto& state = campaign_.state();
  const auto& target = campaign_.target();
  if (state.storage_error)
    return absl::UnavailableError(state.storage_error.message());
  if (version < request.kMinVersion || version > request.kMaxVersion) {
    response->error_code = 35;
    return absl::OkStatus();
  }
  if (request.cluster_id && *request.cluster_id != target.cluster_id) {
    response->error_code = 104;
    return absl::OkStatus();
  }
  if (!IsSingleton(request)) {
    response->error_code = 42;
    return absl::OkStatus();
  }
  const auto& topic = request.topics.front();
  const auto& input = topic.partitions.front();
  auto& out = response->topics.emplace_back();
  out.name = topic.name;
  auto& part = out.partitions.emplace_back();
  part.index = input.partition;
  part.unaligned_records.data.emplace();
  int32_t leader = state.election.leader_id.value_or(-1);
  if (const auto& addr = campaign_.LeaderEndpoint();
      version >= 1 && addr && addr->node_id == leader) {
    auto& node = response->node_endpoints.emplace_back();
    node.node_id = leader;
    node.host = addr->host;
    node.port = addr->port;
  }
  if (topic.name != target.topic || input.partition != target.partition) {
    part.error_code = 3;
    return absl::OkStatus();
  }
  part.current_leader.leader_id = leader;
  part.current_leader.leader_epoch = state.election.epoch;
  const LogPosition id{input.snapshot_id.epoch, input.snapshot_id.end_offset};
  if (input.current_leader_epoch < state.election.epoch) {
    part.error_code = kFencedEpoch;
  } else if (input.current_leader_epoch > state.election.epoch) {
    part.error_code = kUnknownEpoch;
  } else if (state.role != QuorumRole::kLeader) {
    part.error_code = kNotLeader;
  } else if (id == LogPosition{0, 0}) {
    part.error_code = kSnapshotNotFound;
  } else if (request.max_bytes < 0) {
    part.error_code = 42;
  }
  if (part.error_code)
    return absl::OkStatus();

  // Upstream serializes positions as longs but slices raw snapshot files with a Java int.
  auto chunk = log_.ReadSnapshot(id, input.position, request.max_bytes);
  if (!chunk.ok()) {
    if (absl::IsNotFound(chunk.status()))
      part.error_code = kSnapshotNotFound;
    else if (absl::IsOutOfRange(chunk.status()))
      part.error_code = 99;
    else {
      campaign_.FenceStorage();
      return chunk.status();
    }
    return absl::OkStatus();
  }
  if (input.position > INT32_MAX) {
    part.error_code = -1;
    return absl::OkStatus();
  }
  part.snapshot_id.epoch = id.epoch;
  part.snapshot_id.end_offset = id.end_offset;
  part.size = chunk->total_size;
  part.position = input.position;
  part.unaligned_records.data = std::move(chunk->bytes);

  // Snapshot requests maintain check-quorum but carry no evidence of a durable log offset.
  ReplicaKey replica{request.replica_id};
  if (version >= 1)
    replica.directory_id = input.replica_directory_id;
  epochs_.RecordFetch(replica, state.election.epoch, now_ms, nullopt);
  return absl::OkStatus();
}

void SnapshotTransfer::SyncRole() {
  const auto& state = campaign_.state();
  const auto& hint = fetch_.RequestedSnapshot();
  if (state.role == QuorumRole::kFollower && state.election.epoch == term_ &&
      state.election.leader_id == leader_ && hint == target_ && !state.storage_error)
    return;
  writer_.reset();
  pending_ = false;
  target_.reset();
  if (state.role == QuorumRole::kFollower && hint && state.election.leader_id &&
      !state.storage_error) {
    leader_ = *state.election.leader_id;
    term_ = state.election.epoch;
    target_ = *hint;
  }
}

optional<OutboundSnapshotFetch> SnapshotTransfer::Prepare(int16_t version, int32_t max_bytes) {
  SyncRole();
  if (!target_ || max_bytes <= 0 || version < FetchSnapshotRequest::kMinVersion ||
      version > FetchSnapshotRequest::kMaxVersion)
    return nullopt;
  pending_ = true;
  max_bytes_ = max_bytes;
  OutboundSnapshotFetch out{leader_, term_, ++sequence_, version, {}};
  const auto& target = campaign_.target();
  auto& request = out.request;
  request.cluster_id = target.cluster_id;
  request.replica_id = target.local_replica.id;
  request.max_bytes = max_bytes;
  auto& topic = request.topics.emplace_back();
  topic.name = target.topic;
  auto& part = topic.partitions.emplace_back();
  part.partition = target.partition;
  part.current_leader_epoch = term_;
  part.snapshot_id.epoch = target_->epoch;
  part.snapshot_id.end_offset = target_->end_offset;
  part.position = writer_ ? writer_->position() : 0;
  if (version >= 1)
    part.replica_directory_id = target.local_replica.directory_id;
  return out;
}

absl::Status SnapshotTransfer::ReceiveResponse(int32_t source_id, uint64_t sequence,
                                               const FetchSnapshotResponse& response,
                                               bool* reset_fetch_timeout) {
  *reset_fetch_timeout = false;
  SyncRole();
  if (!pending_ || !target_ || leader_ != source_id || sequence_ != sequence)
    return absl::OkStatus();
  pending_ = false;
  if (response.error_code == 31)
    return absl::PermissionDeniedError("FetchSnapshot authorization failed");
  if (response.error_code)
    return absl::UnavailableError("FetchSnapshot request rejected");
  if (!IsSingleton(response) || response.topics.front().name != campaign_.target().topic ||
      response.topics.front().partitions.front().index != campaign_.target().partition)
    return absl::InvalidArgumentError("FetchSnapshot topic partition mismatch");
  const auto& part = response.topics.front().partitions.front();
  const auto leader = part.current_leader.leader_id;
  const auto epoch = part.current_leader.leader_epoch;
  if (epoch < term_ || part.error_code == kUnknownEpoch)
    return absl::OkStatus();
  if (epoch > term_ || part.error_code == kFencedEpoch || part.error_code == kNotLeader) {
    optional<int32_t> id = leader >= 0 ? optional<int32_t>{leader} : nullopt;
    optional<QuorumEndpoint> address;
    for (const auto& node : response.node_endpoints) {
      if (node.node_id == id && !node.host.empty() && node.port) {
        address = {node.host, node.port};
        break;
      }
    }
    if (auto ec = campaign_.ObserveLeader(epoch, id, address); ec)
      return absl::ErrnoToStatus(ec.value(), "Observing leader in FetchSnapshot response");
    if (epoch != term_) {
      fetch_.AbandonSnapshot(*target_);
      SyncRole();
    }
    return absl::OkStatus();
  }
  if (leader != source_id)
    return absl::OkStatus();
  const LogPosition id{part.snapshot_id.epoch, part.snapshot_id.end_offset};
  if (part.error_code == kSnapshotNotFound || id.epoch < 0 || id.end_offset < 0) {
    fetch_.AbandonSnapshot(*target_);
    SyncRole();
  } else {
    if (part.error_code)
      return absl::UnavailableError("FetchSnapshot partition request rejected");
    if (id != *target_ || part.position != (writer_ ? writer_->position() : 0) ||
        part.size <= part.position || (writer_ && part.size != writer_->size()) ||
        !part.unaligned_records.data || part.unaligned_records.data->empty() ||
        part.unaligned_records.data->size() > static_cast<size_t>(max_bytes_) ||
        part.unaligned_records.data->size() > static_cast<uint64_t>(part.size - part.position))
      return absl::InvalidArgumentError(
          "Unexpected snapshot identity, byte position or chunk length");
    if (!writer_) {
      auto writer = log_.BeginSnapshot(id, part.size);
      if (!writer.ok())
        return writer.status();
      writer_ = *std::move(writer);
    }
    if (auto status = writer_->Append(part.position, *part.unaligned_records.data); !status.ok())
      return status;
    if (writer_->position() == writer_->size()) {
      auto status = log_.InstallSnapshot(writer_.get(), fetch_.HighWatermark().value_or(0));

      // A failed pre-publication validation discards this partial file. The next request
      // restarts the same immutable snapshot at byte zero. An uncertain publication fences I/O.
      writer_.reset();
      if (!status.ok()) {
        if (!log_.io_failure().ok())
          campaign_.FenceStorage();
        return status;
      }
      if (auto installed = fetch_.SnapshotInstalled(id); !installed.ok())
        return installed;
      SyncRole();
    }
  }
  campaign_.MarkFetchedFromLeader(source_id, epoch);
  *reset_fetch_timeout = true;
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
