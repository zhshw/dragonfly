// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/slot_migration_driver.h"

#include <algorithm>
#include <chrono>

#include "base/logging.h"
#include "io/io.h"
#include "server/cluster/raft/data/kv_rdb.h"
#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster/raft/data/slot_transfer_state.h"
#include "server/cluster/raft/quorum/fetch_replication.h"
#include "server/cluster/raft/transport/peer_connection.h"
#include "server/cluster/slot_set.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

constexpr int32_t kChunkSize = 256 * 1024;

template <typename T> T ForMigration(const SlotMigrationRecord& migration) {
  T result;
  result.migration_id = migration.migration_id;
  result.source_group = migration.source_group;
  result.target_group = migration.target_group;
  result.slot = migration.slot;
  result.generation = migration.generation;
  return result;
}

bool Active(const MetadataImage& image, const SlotMigrationRecord& migration) {
  const auto active = image.migrations.active_slots().find(migration.slot);
  if (active == image.migrations.active_slots().end() || active->second != migration.migration_id)
    return false;
  const auto& current = image.migrations.records().at(active->second);
  const auto owner = static_cast<MigrationPhase>(current.phase) == MigrationPhase::kCutover
                         ? SlotOwner{current.target_group, current.generation + 1}
                         : SlotOwner{current.source_group, current.generation};
  return current == migration && image.directory.Lookup(current.slot) == owner;
}

const RegisterBrokerRecord* Leader(const MetadataImage& image, int32_t group) {
  auto it = image.directory.groups().find(group);
  if (it == image.directory.groups().end() || !it->second.leadership)
    return nullptr;
  const auto& leadership = *it->second.leadership;
  auto broker = image.brokers.find(leadership.leader_id);
  if (broker == image.brokers.end() || broker->second.fenced ||
      broker->second.broker_epoch != leadership.reporter_epoch)
    return nullptr;
  return &broker->second;
}

optional<QuorumEndpoint> SourceEndpoint(const MetadataImage& image, int32_t source_group) {
  if (auto leader = Leader(image, source_group)) {
    for (const auto& endpoint : leader->end_points)
      if (endpoint.name == "RAFT")
        return QuorumEndpoint{endpoint.host, endpoint.port};
  }
  return nullopt;
}

bool Matches(const TransferProgress& progress, const SlotMigrationRecord& migration) {
  return progress.slot == migration.slot && progress.source_group == migration.source_group &&
         progress.target_group == migration.target_group &&
         progress.generation == migration.generation;
}

LogPosition SourceCut(const SlotMigrationRecord& migration) {
  return {migration.source_epoch, migration.source_end_offset};
}

}  // namespace

SlotMigrationDriver::SlotMigrationDriver(int32_t group_id, int32_t node_id, string cluster_id,
                                         Uuid incarnation, RaftGroup& group,
                                         const CommittedEffects& effects, ReadImage read_image,
                                         Service& service)
    : group_id_(group_id),
      node_id_(node_id),
      cluster_id_(std::move(cluster_id)),
      incarnation_(incarnation),
      group_(group),
      effects_(effects),
      read_image_(std::move(read_image)),
      service_(service) {
}

bool SlotMigrationDriver::Ready(RaftGroup::State state, const MetadataImage& image) const {
  const auto& election = state.election.state();
  auto hw = state.replication.HighWatermark();
  if (election.role != QuorumRole::kLeader || !hw || effects_.applied_offset() != *hw)
    return false;
  auto broker = Leader(image, group_id_);
  return broker && broker->broker_id == node_id_ && broker->incarnation_id == incarnation_ &&
         image.directory.groups().at(group_id_).leadership->leader_epoch == election.election.epoch;
}

absl::Status SlotMigrationDriver::Append(RaftGroup::State state, const SlotTransferRecord& record) {
  using namespace chrono;
  int32_t epoch = state.election.state().election.epoch;
  auto offset = state.log.End().end_offset;
  auto wall = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  auto batch = MakeSlotTransferBatch(offset, epoch, wall, record);
  if (!batch.ok())
    return batch.status();
  auto status = state.replication.Append(*batch, epoch);
  if (status.ok())
    pending_ = {epoch, offset + 1};
  return status;
}

void SlotMigrationDriver::Tick(PeerConnection& controller) {
  auto image = read_image_();
  if (!image)
    return;
  const auto* leader = Leader(*image, group_id_);
  if (!leader || leader->broker_id != node_id_ || leader->incarnation_id != incarnation_) {
    capture_.reset();
    return;
  }
  if (capture_) {
    auto it = image->migrations.records().find(capture_->id);
    if (it == image->migrations.records().end() ||
        static_cast<MigrationPhase>(it->second.phase) != MigrationPhase::kPreparing)
      capture_.reset();
  }
  for (const auto& [slot, id] : image->migrations.active_slots()) {
    const auto& migration = image->migrations.records().at(id);
    if ((group_id_ == migration.source_group && !migration.source_finished) ||
        (group_id_ == migration.target_group && !migration.target_finished))
      Advance(migration, controller);
  }
}

void SlotMigrationDriver::Advance(const SlotMigrationRecord& migration,
                                  PeerConnection& controller) {
  int32_t epoch = -1;
  int64_t position = 0;
  bool capture = false;
  optional<QuorumEndpoint> source;
  optional<SlotMigrationRequest> report;
  auto status = group_.Visit([&](RaftGroup::State state) {
    auto image = read_image_();
    if (!image || !Active(*image, migration) || !Ready(state, *image))
      return absl::OkStatus();
    epoch = state.election.state().election.epoch;
    if (pending_.epoch == epoch && pending_.end_offset > effects_.applied_offset())
      return absl::OkStatus();

    const bool exporting = group_id_ == migration.source_group;
    const auto* progress = effects_.transfers().Find(migration.migration_id);
    if (progress && !Matches(*progress, migration))
      return absl::DataLossError("Applied transfer does not match committed migration");

    auto record = ForMigration<SlotTransferRecord>(migration);
    auto append = [&](TransferKind kind) {
      record.kind = static_cast<int8_t>(kind);
      return Append(state, record);
    };
    auto certify = [&](MigrationAction action, LogPosition cut) {
      auto& req = report.emplace(ForMigration<SlotMigrationRequest>(migration));
      req.cluster_id = cluster_id_;
      req.action = static_cast<int8_t>(action);
      req.expected_phase = migration.phase;
      req.group_id = group_id_;
      req.reporter_id = node_id_;
      req.broker_epoch = image->brokers.at(node_id_).broker_epoch;
      req.leader_epoch = epoch;
      req.record_epoch = cut.epoch;
      req.record_end_offset = cut.end_offset;
      req.high_watermark = *state.replication.HighWatermark();
      req.applied_end_offset = effects_.applied_offset();
    };
    const auto phase = static_cast<MigrationPhase>(migration.phase);
    if (phase == MigrationPhase::kCutover || phase == MigrationPhase::kCancelled) {
      const bool cancel = phase == MigrationPhase::kCancelled;
      if (progress && progress->finish.end_offset) {
        if (progress->cancelled != cancel)
          return absl::DataLossError("Conflicting slot migration decision");
        certify(MigrationAction::kFinished, progress->finish);
        return absl::OkStatus();
      }
      if (!cancel) {
        LogPosition installed{migration.target_epoch, migration.target_end_offset};
        if (!progress ||
            (exporting ? progress->seal != SourceCut(migration) : progress->install != installed))
          return absl::DataLossError("Cutover does not match the local committed transfer");
      }
      return append(cancel ? TransferKind::kCancel : TransferKind::kFinish);
    }

    // An observer can temporarily trail a locally applied cleanup or its own previous report.
    if (progress && progress->finish.end_offset)
      return absl::OkStatus();
    if (exporting && phase == MigrationPhase::kPreparing) {
      if (!progress)
        return append(TransferKind::kFreeze);
      if (progress->seal.end_offset) {
        if (capture_ && capture_->id == migration.migration_id)
          capture_.reset();
        certify(MigrationAction::kFrozen, progress->seal);
        report->digest = progress->digest;
        return absl::OkStatus();
      }
      if (!progress->freeze.end_offset)
        return absl::DataLossError("Source transfer has no applied freeze");
      if (capture_ && capture_->epoch != epoch)
        capture_.reset();
      if (!capture_) {
        capture = true;
        return absl::OkStatus();
      }
      if (capture_->id != migration.migration_id)
        return absl::OkStatus();

      // RDB encoders on successive leaders may choose different byte encodings. Start the new
      // capture at byte zero to replace any unsealed prefix before appending its remaining chunks.
      const auto next = capture_->restart ? 0 : progress->bytes.size();
      record.position = next;
      if (next < capture_->bytes.size()) {
        record.bytes.data = capture_->bytes.substr(next, kChunkSize);
        auto appended = append(TransferKind::kSourceChunk);
        if (appended.ok())
          capture_->restart = false;
        return appended;
      }
      if (next != capture_->bytes.size())
        return absl::DataLossError("Source staging prefix exceeds its capture");
      record.digest = capture_->digest;
      return append(TransferKind::kSeal);
    }
    if (!exporting && phase == MigrationPhase::kFrozen) {
      if (progress && progress->source != SourceCut(migration))
        return absl::DataLossError("Target staging references another source seal");
      if (progress && progress->install.end_offset) {
        if (progress->digest != migration.digest)
          return absl::DataLossError("Installed slot image digest differs from the source");
        certify(MigrationAction::kImported, progress->install);
        report->source_epoch = migration.source_epoch;
        report->source_end_offset = migration.source_end_offset;
        report->digest = migration.digest;
      } else {
        source = SourceEndpoint(*image, migration.source_group);
        position = progress ? progress->bytes.size() : 0;
      }
    }
    return absl::OkStatus();
  });
  if (!status.ok()) {
    VLOG(1) << "Advancing slot " << migration.slot << ": " << status;
    return;
  }
  if (capture) {
    io::StringSink sink;
    const SlotSet slots(SlotRanges({{migration.slot, migration.slot}}));
    if (auto s = SaveKvRdb(service_, slots, &sink, [] {}); !s.ok()) {
      LOG(WARNING) << "Capturing frozen slot " << migration.slot << ": " << s;
      return;
    }
    string digest = KvRdbDigest(sink.str());
    capture_.emplace(Capture{migration.migration_id, epoch, true, std::move(sink).str(),
                             vector<int8_t>(digest.begin(), digest.end())});
  } else if (source) {
    Pull(migration, *source, epoch, position);
  } else if (report) {
    SlotMigrationResponse res;
    auto ec = controller.Call(*report, 0, &res);
    if (ec || res.error_code)
      VLOG(1) << "Reporting slot " << migration.slot << ": "
              << (ec ? ec.message() : res.error_message);
  }
}

void SlotMigrationDriver::Pull(const SlotMigrationRecord& migration, QuorumEndpoint source,
                               int32_t epoch, int64_t position) {
  auto request = ForMigration<FetchSlotImageRequest>(migration);
  request.cluster_id = cluster_id_;
  request.position = position;
  request.max_bytes = kChunkSize;
  FetchSlotImageResponse response;
  PeerConnection peer(std::move(source), 2000, kMaxRaftFrame);
  auto ec = peer.Call(request, 0, &response);
  peer.Close();
  if (ec || response.error_code || !response.bytes.data ||
      response.source_epoch != migration.source_epoch ||
      response.source_end_offset != migration.source_end_offset || response.total_size <= 0 ||
      response.total_size < position ||
      int64_t(response.bytes.data->size()) !=
          min<int64_t>(kChunkSize, response.total_size - position))
    return;

  auto status = group_.Visit([&](RaftGroup::State state) {
    auto image = read_image_();
    if (!image || !Active(*image, migration) || !Ready(state, *image) ||
        state.election.state().election.epoch != epoch)
      return absl::OkStatus();
    const auto* progress = effects_.transfers().Find(migration.migration_id);
    if ((progress && (!Matches(*progress, migration) || progress->install.end_offset ||
                      progress->finish.end_offset || progress->source != SourceCut(migration))) ||
        position != (progress ? int64_t(progress->bytes.size()) : 0))
      return absl::OkStatus();

    auto record = ForMigration<SlotTransferRecord>(migration);
    record.source_epoch = response.source_epoch;
    record.source_end_offset = response.source_end_offset;
    record.position = position;
    if (position == response.total_size) {
      const string digest(migration.digest.begin(), migration.digest.end());
      if (!progress || KvRdbDigest(progress->bytes) != digest)
        return absl::DataLossError("Staged target RDB digest differs from committed source seal");
      record.kind = static_cast<int8_t>(TransferKind::kInstall);
      record.digest = migration.digest;
    } else {
      record.kind = static_cast<int8_t>(TransferKind::kTargetChunk);
      record.bytes = std::move(response.bytes);
    }
    return Append(state, record);
  });
  if (!status.ok())
    VLOG(1) << "Importing slot " << migration.slot << ": " << status;
}

void SlotMigrationDriver::Fetch(const FetchSlotImageRequest& request, FetchSlotImageResponse* res) {
  *res = {};
  if (request.cluster_id != cluster_id_) {
    res->error_code = 104;
    return;
  }
  if (request.source_group != group_id_ || request.position < 0 || request.max_bytes <= 0) {
    res->error_code = 42;
    return;
  }
  auto status = group_.Visit([&](RaftGroup::State state) {
    auto image = read_image_();
    if (!image || !Ready(state, *image)) {
      res->error_code = 6;
      return absl::OkStatus();
    }
    auto it = image->migrations.records().find(request.migration_id);
    if (it == image->migrations.records().end()) {
      res->error_code = 42;
      return absl::OkStatus();
    }
    const auto& migration = it->second;
    const auto* progress = effects_.transfers().Find(request.migration_id);
    if (static_cast<MigrationPhase>(migration.phase) != MigrationPhase::kFrozen ||
        !Active(*image, migration) || migration.slot != request.slot ||
        migration.source_group != request.source_group ||
        migration.target_group != request.target_group ||
        migration.generation != request.generation || !progress || !Matches(*progress, migration) ||
        progress->seal != SourceCut(migration) || progress->finish.end_offset ||
        progress->digest != migration.digest ||
        request.position > int64_t(progress->bytes.size())) {
      res->error_code = 42;
      return absl::OkStatus();
    }
    res->source_epoch = progress->seal.epoch;
    res->source_end_offset = progress->seal.end_offset;
    res->total_size = progress->bytes.size();
    res->bytes.data = progress->bytes.substr(request.position, min(request.max_bytes, kChunkSize));
    return absl::OkStatus();
  });
  if (!status.ok())
    res->error_code = 6;
}

}  // namespace dfly::cluster::raft
