// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/slot_directory.h"

#include <algorithm>
#include <limits>
#include <set>

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

bool Contains(const vector<int32_t>& members, int32_t id) {
  return find(members.begin(), members.end(), id) != members.end();
}

}  // namespace

absl::Status SlotDirectory::Replay(const RedisNodeRecord& record, const RegisteredBrokers& brokers,
                                   bool snapshot) {
  if (record.broker_id < 0 || record.redis_node_id.empty() ||
      (!snapshot && !brokers.contains(record.broker_id)))
    return absl::InvalidArgumentError(
        "Redis node binding requires a registered broker and a node ID");
  for (const auto& [broker_id, redis_id] : nodes_) {
    if (record.broker_id == broker_id && record.redis_node_id != redis_id)
      return absl::AlreadyExistsError("Broker already has a stable Redis node ID");
    if (record.redis_node_id == redis_id && record.broker_id != broker_id)
      return absl::AlreadyExistsError("Redis node ID is bound to another broker");
  }
  nodes_[record.broker_id] = record.redis_node_id;
  return absl::OkStatus();
}

absl::Status SlotDirectory::Replay(const DataGroupRecord& record) {
  if (record.group_id < 0 || record.members.empty())
    return absl::InvalidArgumentError("Data group requires a non-negative ID and members");
  if (auto it = groups_.find(record.group_id); it != groups_.end()) {
    if (it->second.members == record.members)
      return absl::OkStatus();
    return absl::AlreadyExistsError("Bootstrap membership cannot replace an existing data group");
  }
  set<int32_t> members;
  for (int32_t member : record.members) {
    if (!nodes_.contains(member) || !members.insert(member).second)
      return absl::InvalidArgumentError("Duplicate member or missing Redis node identity");
  }
  for (const auto& [id, group] : groups_) {
    for (int32_t member : group.members) {
      if (members.contains(member))
        return absl::AlreadyExistsError("A Redis node can belong to only one data group");
    }
  }
  groups_.emplace(record.group_id, DataGroup{record.members, nullopt});
  return absl::OkStatus();
}

absl::StatusOr<bool> SlotDirectory::CheckLeaderReport(const GroupLeaderRecord& record,
                                                      const RegisteredBrokers& brokers) const {
  auto group = groups_.find(record.group_id);
  if (group == groups_.end() || record.leader_epoch < 0 || record.voter_config_offset < -1 ||
      record.leader_id < -1 || !Contains(group->second.members, record.reporter_id) ||
      (record.leader_id != -1 && record.leader_id != record.reporter_id))
    return absl::InvalidArgumentError("Invalid data group leader report");
  auto reporter = brokers.find(record.reporter_id);
  if (reporter == brokers.end() || reporter->second.broker_epoch != record.reporter_epoch ||
      reporter->second.fenced)
    return absl::FailedPreconditionError("Fenced or stale broker reported data group leadership");
  auto& previous = group->second.leadership;
  if (previous) {
    if (record.leader_epoch < previous->leader_epoch ||
        record.voter_config_offset < previous->voter_config_offset)
      return false;
    if (record.leader_epoch == previous->leader_epoch && previous->leader_id >= 0 &&
        record.leader_id != previous->leader_id)
      return absl::FailedPreconditionError(
          "Conflicting leader report for the same data group epoch");
  }
  return previous != record;
}

absl::Status SlotDirectory::Replay(const GroupLeaderRecord& record,
                                   const RegisteredBrokers& brokers, bool snapshot) {
  if (snapshot) {
    auto it = groups_.find(record.group_id);
    if (it == groups_.end() || it->second.leadership || record.leader_epoch < 0 ||
        record.voter_config_offset < -1 || record.leader_id < -1 ||
        !Contains(it->second.members, record.reporter_id) ||
        (record.leader_id != -1 && record.leader_id != record.reporter_id))
      return absl::DataLossError("Invalid group leader in metadata snapshot");
    it->second.leadership = record;
    return absl::OkStatus();
  }
  auto change = CheckLeaderReport(record, brokers);
  if (!change.ok())
    return change.status();
  if (*change)
    groups_.at(record.group_id).leadership = record;
  return absl::OkStatus();
}

absl::Status SlotDirectory::Replay(const SlotAssignmentRecord& record, bool snapshot) {
  if (record.first_slot > record.last_slot || record.last_slot > kMaxSlotNum ||
      !groups_.contains(record.group_id) || record.expected_group == record.group_id ||
      record.expected_generation < 0 ||
      record.expected_generation == numeric_limits<int64_t>::max() ||
      (snapshot ? (record.expected_group != -1 || record.expected_generation != 0 ||
                   record.generation < 1)
                : record.generation != record.expected_generation + 1))
    return absl::InvalidArgumentError("Invalid slot assignment range, group or generation");
  SlotOwner expected{record.expected_group, record.expected_generation};
  const auto first = prev(ranges_.upper_bound(record.first_slot));
  auto after = ranges_.upper_bound(record.last_slot);
  for (auto it = first; it != after; ++it) {
    if (it->second != expected)
      return absl::FailedPreconditionError("Slot owner or generation changed");
  }

  // Preserve the right-hand tail before removing range boundaries inside the assignment.
  if (record.last_slot < kMaxSlotNum)
    ranges_.try_emplace(record.last_slot + 1, prev(after)->second);
  auto start = ranges_.lower_bound(record.first_slot);
  ranges_.erase(start, ranges_.upper_bound(record.last_slot));
  auto assigned =
      ranges_.emplace(record.first_slot, SlotOwner{record.group_id, record.generation}).first;
  auto next_range = next(assigned);
  if (next_range != ranges_.end() && next_range->second == assigned->second)
    ranges_.erase(next_range);
  if (assigned != ranges_.begin() && prev(assigned)->second == assigned->second)
    ranges_.erase(assigned);
  if (record.expected_group < 0)
    unassigned_slots_ -= record.last_slot - record.first_slot + 1;
  return absl::OkStatus();
}

optional<SlotOwner> SlotDirectory::Lookup(SlotId slot) const {
  if (slot > kMaxSlotNum)
    return nullopt;
  const auto& owner = prev(ranges_.upper_bound(slot))->second;
  return owner.group_id < 0 ? nullopt : optional{owner};
}

optional<RedisRoute> SlotDirectory::Route(SlotId slot, const RegisteredBrokers& brokers) const {
  if (!HasCompleteCoverage())
    return nullopt;
  auto owner = Lookup(slot);
  if (!owner)
    return nullopt;
  const auto& report = groups_.at(owner->group_id).leadership;
  if (!report || report->leader_id < 0)
    return nullopt;
  auto broker = brokers.find(report->leader_id);
  if (broker == brokers.end() || broker->second.fenced || broker->second.in_controlled_shutdown ||
      broker->second.broker_epoch != report->reporter_epoch)
    return nullopt;
  for (const auto& ep : broker->second.end_points) {
    if (ep.name == "RESP" && !ep.host.empty() && ep.port > 0)
      return RedisRoute{*owner, nodes_.at(broker->first), ep.host, ep.port};
  }
  return nullopt;
}

}  // namespace dfly::cluster::raft
