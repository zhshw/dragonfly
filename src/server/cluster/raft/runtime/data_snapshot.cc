// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/data_snapshot.h"

#include "base/flags.h"
#include "io/io.h"
#include "server/cluster/raft/data/kv_rdb.h"
#include "server/cluster/raft/data/kv_snapshot.h"
#include "server/cluster/raft/quorum/fetch_replication.h"
#include "server/cluster/slot_set.h"
#include "util/fibers/proactor_base.h"

ABSL_FLAG(int64_t, raft_data_snapshot_bytes, 20 << 20,
          "Applied data log bytes between data group snapshots.");
ABSL_FLAG(int64_t, raft_data_snapshot_interval_ms, 3600000,
          "Maximum time between snapshots of newly applied data. Zero disables the timer.");

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace chrono;

SlotSet MaterializedSlots(const KvSnapshotScope& scope) {
  vector<SlotRange> ranges;
  for (auto r : scope.slots)
    ranges.push_back({r.first, r.last});
  return SlotSet(SlotRanges(std::move(ranges)));
}

}  // namespace

DataSnapshotter::DataSnapshotter(string cluster_id, int32_t group_id, Service& service,
                                 util::fb2::ProactorBase* owner)
    : cluster_id_(std::move(cluster_id)), group_id_(group_id), service_(service), owner_(owner) {
}

absl::Status DataSnapshotter::Validate(const SnapshotWriter& writer) {
  auto snapshot = ReadKvSnapshot(writer, cluster_id_, group_id_);
  if (!snapshot.ok())
    return snapshot.status();
  auto restored = CommittedEffects::FromSnapshot(group_id_, snapshot->scope.cut,
                                                 snapshot->migrations, snapshot->scope.slots);
  if (!restored.ok())
    return restored.status();
  return owner_->Await(
      [&] { return ValidateKvRdb(service_, MaterializedSlots(snapshot->scope), snapshot->rdb); });
}

absl::Status DataSnapshotter::Restore(const PartitionLog& log, int64_t hw,
                                      CommittedEffects* effects) {
  auto cut = log.SnapshotAtOrBefore(hw);
  if (!cut || cut->end_offset <= effects->applied_offset())
    return absl::OkStatus();
  auto snapshot = ReadKvSnapshot(log, *cut, cluster_id_, group_id_);
  if (!snapshot.ok())
    return snapshot.status();
  auto restored =
      CommittedEffects::FromSnapshot(group_id_, *cut, snapshot->migrations, snapshot->scope.slots);
  if (!restored.ok())
    return restored.status();
  auto status = owner_->Await([&] {
    return RestoreGroupKvRdb(service_, MaterializedSlots(snapshot->scope), snapshot->rdb);
  });
  if (status.ok()) {
    *effects = *std::move(restored);
    last_bytes_ = 0;
    last_snapshot_ = steady_clock::now();
  }
  return status;
}

absl::Status DataSnapshotter::MaybeSave(RaftGroup& group, const CommittedEffects& effects) {
  KvSnapshotScope scope{cluster_id_, group_id_, {}, {}};
  string migrations;
  io::StringSink sink;
  uint64_t cut_bytes = 0;
  util::fb2::Fiber traversal;
  absl::Status saved;
  auto status = group.Visit([&](RaftGroup::State state) {
    auto hw = state.replication.HighWatermark();
    auto cut = effects.applied_position();
    if (!hw || !cut.end_offset || cut.end_offset > *hw)
      return absl::OkStatus();
    if (auto prior = state.log.SnapshotAtOrBefore(cut.end_offset); prior) {
      // Finish retention if the process stopped after publishing the snapshot.
      if (prior->end_offset > state.log.StartOffset()) {
        if (auto s = state.log.DeleteBeforeSnapshot(*prior, *hw); !s.ok())
          return s;
      }
      if (prior->end_offset == cut.end_offset)
        return absl::OkStatus();
    }
    auto interval = absl::GetFlag(FLAGS_raft_data_snapshot_interval_ms);
    cut_bytes = effects.applied_bytes();
    if (cut_bytes - last_bytes_ < uint64_t(absl::GetFlag(FLAGS_raft_data_snapshot_bytes)) &&
        (!interval || steady_clock::now() - last_snapshot_ < milliseconds(interval)))
      return absl::OkStatus();

    scope.cut = cut;
    scope.slots = effects.SnapshotSlots();
    auto progress = effects.SnapshotMigrations(cut);
    if (!progress.ok())
      return progress.status();
    migrations = *std::move(progress);

    // Serialize the cut with apply only until every shard has installed its write callback.
    util::fb2::Done registered;
    traversal = owner_->LaunchFiber([&, registered]() mutable {
      auto slots = MaterializedSlots(scope);
      saved = SaveKvRdb(service_, slots, &sink, [&] { registered.Notify(); });
      registered.Notify();
    });
    registered.Wait();
    return absl::OkStatus();
  });
  if (!traversal.IsJoinable())
    return status;
  traversal.Join();
  if (!saved.ok())
    return saved;

  return group.Visit([&](RaftGroup::State state) {
    auto hw = state.replication.HighWatermark();
    if (!hw || *hw < scope.cut.end_offset)
      return absl::OkStatus();

    // A received snapshot may already cover the cut while the RDB was streaming.
    if (auto prior = state.log.LatestSnapshot(); prior && prior->end_offset >= scope.cut.end_offset)
      return absl::OkStatus();

    auto wall = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    auto s = WriteKvSnapshot(scope, sink.str(), migrations, state.replication.membership(), *hw,
                             wall, &state.log);
    if (!s.ok())
      return s;
    last_bytes_ = cut_bytes;
    last_snapshot_ = steady_clock::now();
    return state.log.DeleteBeforeSnapshot(scope.cut, *hw);
  });
}

}  // namespace dfly::cluster::raft
