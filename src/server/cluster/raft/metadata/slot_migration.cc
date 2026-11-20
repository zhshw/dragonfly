// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/slot_migration.h"

#include <limits>

#include "server/cluster/raft/metadata/metadata_image.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;
using Phase = MigrationPhase;
using Action = MigrationAction;

bool PhaseIs(const SlotMigrationRecord& record, Phase phase) {
  return record.phase == static_cast<int8_t>(phase);
}

bool Finished(const SlotMigrationRecord& record) {
  return record.source_finished && record.target_finished;
}

template <typename T> bool SameMigration(const SlotMigrationRecord& record, const T& other) {
  return record.migration_id == other.migration_id && record.slot == other.slot &&
         record.source_group == other.source_group && record.target_group == other.target_group &&
         record.generation == other.generation;
}

bool Valid(const SlotMigrationRecord& record, const SlotDirectory& directory) {
  if (record.migration_id == Uuid{} || record.slot > kMaxSlotNum ||
      record.admin_request.request_id.empty() != record.admin_request.value.empty() ||
      record.source_group == record.target_group ||
      !directory.groups().contains(record.source_group) ||
      !directory.groups().contains(record.target_group) || record.generation < 1 ||
      record.generation == numeric_limits<int64_t>::max())
    return false;
  const bool source =
      record.source_epoch >= 0 && record.source_end_offset > 0 && record.digest.size() == 32;
  const bool no_source =
      record.source_epoch == -1 && record.source_end_offset == -1 && record.digest.empty();
  const bool target = record.target_epoch >= 0 && record.target_end_offset > 0;
  const bool no_target = record.target_epoch == -1 && record.target_end_offset == -1;
  if (PhaseIs(record, Phase::kCutover))
    return source && target;
  if (PhaseIs(record, Phase::kCancelled))
    return (source || no_source) && no_target;
  if (record.source_finished || record.target_finished)
    return false;
  return (PhaseIs(record, Phase::kPreparing) && no_source && no_target) ||
         (PhaseIs(record, Phase::kFrozen) && source && no_target);
}

bool ValidChange(const SlotMigrationRecord& old, const SlotMigrationRecord& next) {
  if (!SameMigration(old, next) || old.admin_request != next.admin_request ||
      (old.source_finished && !next.source_finished) ||
      (old.target_finished && !next.target_finished))
    return false;
  if (old.source_end_offset >= 0 &&
      (old.source_end_offset != next.source_end_offset || old.source_epoch != next.source_epoch ||
       old.digest != next.digest))
    return false;
  if (old.target_end_offset >= 0 &&
      (old.target_end_offset != next.target_end_offset || old.target_epoch != next.target_epoch))
    return false;
  if (old.phase == next.phase) {
    if (old.source_end_offset != next.source_end_offset)
      return false;
    return true;
  }
  if (next.source_finished || next.target_finished)
    return false;
  if (PhaseIs(old, Phase::kPreparing))
    return PhaseIs(next, Phase::kFrozen) ||
           (PhaseIs(next, Phase::kCancelled) && next.source_end_offset == -1);
  return PhaseIs(old, Phase::kFrozen) &&
         (PhaseIs(next, Phase::kCutover) || PhaseIs(next, Phase::kCancelled));
}

void Describe(const SlotMigrationRecord& mig, SlotMigrationResponse* res) {
  res->phase = mig.phase;
  res->source_finished = mig.source_finished;
  res->target_finished = mig.target_finished;
}

vector<MetadataRecord> Fail(int16_t code, string message, SlotMigrationResponse* res) {
  res->error_code = code;
  res->error_message = std::move(message);
  return {};
}

int16_t ValidateReport(const MetadataImage& image, const SlotMigrationRequest& req) {
  auto broker = image.brokers.find(req.reporter_id);
  if (broker == image.brokers.end() || broker->second.fenced ||
      broker->second.broker_epoch != req.broker_epoch)
    return 77;
  auto group = image.directory.groups().find(req.group_id);
  if (group == image.directory.groups().end() || !group->second.leadership)
    return 6;
  const auto& leader = *group->second.leadership;
  if (leader.leader_id != req.reporter_id || leader.reporter_epoch != req.broker_epoch ||
      leader.leader_epoch != req.leader_epoch)
    return 74;
  if (req.record_epoch < 0 || req.record_epoch > req.leader_epoch || req.record_end_offset <= 0 ||
      req.applied_end_offset < req.record_end_offset || req.high_watermark < req.applied_end_offset)
    return 42;
  return 0;
}
}  // namespace

bool SlotMigrations::BlocksAssignment(SlotId first, SlotId last) const {
  auto it = active_slots_.lower_bound(first);
  return it != active_slots_.end() && it->first <= last;
}

const SlotMigrationRecord* SlotMigrations::LatestCutover(SlotId slot) const {
  auto it = latest_cutovers_.find(slot);
  return it == latest_cutovers_.end() ? nullptr : &records_.at(it->second);
}

const SlotMigrationRecord* SlotMigrations::FindAdminRequest(string_view request_id) const {
  auto it = admin_requests_.find(request_id);
  return it == admin_requests_.end() ? nullptr : &records_.at(it->second);
}

absl::StatusOr<bool> SlotMigrations::Check(const SlotMigrationRecord& record, bool snapshot,
                                           const SlotDirectory& directory) const {
  if (!Valid(record, directory))
    return absl::DataLossError("Invalid slot migration record");
  if (auto request = FindAdminRequest(record.admin_request.request_id);
      request && request->migration_id != record.migration_id)
    return absl::DataLossError("Management request ID already belongs to another migration");
  auto old = records_.find(record.migration_id);
  if (snapshot) {
    if (old != records_.end())
      return absl::DataLossError("Repeated migration in metadata snapshot");

    // Terminal decisions can precede later migrations of the same slot.
    if (Finished(record))
      return false;
  } else if (old == records_.end()) {
    if (!PhaseIs(record, Phase::kPreparing))
      return absl::FailedPreconditionError("Slot migration must start in preparing");
  } else if (!ValidChange(old->second, record)) {
    return absl::FailedPreconditionError("Invalid slot migration transition");
  }
  auto active = active_slots_.find(record.slot);
  if (active != active_slots_.end() && active->second != record.migration_id)
    return absl::FailedPreconditionError("Another migration owns this slot");
  bool switch_owner = !snapshot && PhaseIs(record, Phase::kCutover) && old != records_.end() &&
                      !PhaseIs(old->second, Phase::kCutover);
  SlotOwner expected{record.source_group, record.generation};
  if (!switch_owner && PhaseIs(record, Phase::kCutover))
    expected = {record.target_group, record.generation + 1};
  if (directory.Lookup(record.slot) != expected)
    return absl::FailedPreconditionError("Slot owner or generation changed");
  return switch_owner;
}

absl::Status SlotMigrations::Replay(const SlotMigrationRecord& record, bool snapshot,
                                    SlotDirectory* directory) {
  auto switch_owner = Check(record, snapshot, *directory);
  if (!switch_owner.ok())
    return switch_owner.status();
  if (*switch_owner) {
    SlotAssignmentRecord assignment{record.slot,
                                    record.slot,
                                    record.source_group,
                                    record.generation,
                                    record.target_group,
                                    record.generation + 1,
                                    {}};
    if (auto s = directory->Replay(assignment); !s.ok())
      return s;
  }
  records_.insert_or_assign(record.migration_id, record);

  // Snapshot records arrive in UUID order. Keep the newest ownership generation per slot.
  if (PhaseIs(record, Phase::kCutover)) {
    const auto* previous = LatestCutover(record.slot);
    if (!previous || previous->generation < record.generation)
      latest_cutovers_[record.slot] = record.migration_id;
  }
  if (!record.admin_request.request_id.empty())
    admin_requests_.try_emplace(record.admin_request.request_id, record.migration_id);
  if (!Finished(record)) {
    active_slots_[record.slot] = record.migration_id;
  } else {
    auto active = active_slots_.find(record.slot);
    if (active != active_slots_.end() && active->second == record.migration_id)
      active_slots_.erase(active);
  }
  return absl::OkStatus();
}

vector<MetadataRecord> ProposeSlotMigration(const MetadataImage& image, string_view cluster_id,
                                            const SlotMigrationRequest& req,
                                            SlotMigrationResponse* res) {
  *res = {};
  if (req.cluster_id != cluster_id)
    return Fail(104, "Inconsistent cluster ID", res);
  auto feature = image.features.find("dragonfly.cluster");
  if (feature == image.features.end() || feature->second != 1)
    return Fail(35, "Slot migration requires dragonfly.cluster=1", res);
  auto action = static_cast<Action>(req.action);
  auto old = image.migrations.records().find(req.migration_id);
  SlotMigrationRecord next;
  if (old == image.migrations.records().end()) {
    if (action != Action::kStart)
      return Fail(42, "Unknown migration ID", res);
    next.migration_id = req.migration_id;
    next.slot = req.slot;
    next.source_group = req.source_group;
    next.target_group = req.target_group;
    next.generation = req.generation;
    next.phase = static_cast<int8_t>(Phase::kPreparing);
  } else {
    next = old->second;
    Describe(next, res);
    if (!SameMigration(next, req))
      return Fail(42, "Migration ID already belongs to a different slot, group or generation", res);
    if (action == Action::kStatus || action == Action::kStart)
      return {};
    if (action == Action::kCancel) {
      if (PhaseIs(next, Phase::kCutover))
        return Fail(42, "Slot has already switched owner", res);
      next.phase = static_cast<int8_t>(Phase::kCancelled);
    } else {
      if (auto code = ValidateReport(image, req); code)
        return Fail(code, "Stale data leader or invalid applied group log position", res);
      if (action == Action::kFrozen) {
        if (req.group_id != next.source_group || req.digest.size() != 32)
          return Fail(42, "Expected the frozen source group and SHA-256", res);
        if (PhaseIs(next, Phase::kCancelled))
          return Fail(42, "Cancelled migration", res);
        if (!PhaseIs(next, Phase::kPreparing)) {
          if (req.record_epoch != next.source_epoch ||
              req.record_end_offset != next.source_end_offset || req.digest != next.digest)
            return Fail(42, "Conflicting freeze certificate", res);
          return {};
        }
        next.phase = static_cast<int8_t>(Phase::kFrozen);
        next.source_epoch = req.record_epoch;
        next.source_end_offset = req.record_end_offset;
        next.digest = req.digest;
      } else if (action == Action::kImported) {
        if (req.group_id != next.target_group || req.source_epoch != next.source_epoch ||
            req.source_end_offset != next.source_end_offset || req.digest != next.digest ||
            next.digest.size() != 32 ||
            (!PhaseIs(next, Phase::kFrozen) && !PhaseIs(next, Phase::kCutover)))
          return Fail(42, "Import does not match the frozen source", res);
        next.phase = static_cast<int8_t>(Phase::kCutover);
        next.target_epoch = req.record_epoch;
        next.target_end_offset = req.record_end_offset;
      } else if (action == Action::kFinished) {
        if (req.expected_phase != next.phase ||
            (!PhaseIs(next, Phase::kCancelled) && !PhaseIs(next, Phase::kCutover)) ||
            (req.group_id != next.source_group && req.group_id != next.target_group))
          return Fail(42, "Wrong participant or migration decision for cleanup", res);
        bool source = req.group_id == next.source_group;
        auto cut = source ? next.source_end_offset : next.target_end_offset;
        auto term = source ? next.source_epoch : next.target_epoch;
        if (req.record_end_offset <= cut || req.record_epoch < term)
          return Fail(42, "Cleanup must follow the original group marker", res);
        (source ? next.source_finished : next.target_finished) = true;
      } else {
        return Fail(42, "Unknown slot migration action", res);
      }
    }
    if (next == old->second)
      return {};
  }

  // Validate exactly the record and the owner CAS that committed replay will perform.
  if (auto checked = image.migrations.Check(next, false, image.directory); !checked.ok())
    return Fail(42, string(checked.status().message()), res);
  Describe(next, res);
  if (action == Action::kImported) {
    BeginTransactionRecord begin;
    begin.name = "Switch Redis slot owner";
    return {{std::move(begin)}, {std::move(next)}, {EndTransactionRecord{}}};
  }
  return {{std::move(next)}};
}
}  // namespace dfly::cluster::raft
