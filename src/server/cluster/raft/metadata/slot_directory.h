// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <map>
#include <optional>

#include "server/cluster/raft/protocol/metadata_records.h"
#include "server/cluster/raft/protocol/slot_records.h"
#include "server/cluster_support.h"

namespace dfly::cluster::raft {

using RegisteredBrokers = std::map<int32_t, protocol::RegisterBrokerRecord>;

struct SlotOwner {
  int32_t group_id = -1;
  int64_t generation = 0;
  bool operator==(const SlotOwner&) const = default;
};

struct DataGroup {
  std::vector<int32_t> members;
  std::optional<protocol::GroupLeaderRecord> leadership;
  bool operator==(const DataGroup&) const = default;
};

struct RedisRoute {
  SlotOwner owner;
  std::string node_id;
  std::string host;
  uint16_t port;
};

// Dragonfly's cluster metadata extension. Groups use stable IDs across elections.
// Adjacent slots with the same group and generation share one range in the image.
class SlotDirectory {
 public:
  [[nodiscard]] absl::Status Replay(const protocol::RedisNodeRecord& record,
                                    const RegisteredBrokers& brokers, bool snapshot = false);
  [[nodiscard]] absl::Status Replay(const protocol::DataGroupRecord& record);
  [[nodiscard]] absl::Status Replay(const protocol::GroupLeaderRecord& record,
                                    const RegisteredBrokers& brokers, bool snapshot = false);
  [[nodiscard]] absl::Status Replay(const protocol::SlotAssignmentRecord& record,
                                    bool snapshot = false);

  absl::StatusOr<bool> CheckLeaderReport(const protocol::GroupLeaderRecord& report,
                                         const RegisteredBrokers& brokers) const;

  // A complete map may contain a group with no reachable leader. Other groups can
  // continue to serve. A returned route is only a hint for reaching a data quorum.
  bool HasCompleteCoverage() const {
    return unassigned_slots_ == 0;
  }
  std::optional<SlotOwner> Lookup(SlotId slot) const;
  std::optional<RedisRoute> Route(SlotId slot, const RegisteredBrokers& brokers) const;

  const auto& nodes() const {
    return nodes_;
  }
  const auto& groups() const {
    return groups_;
  }
  const auto& ranges() const {
    return ranges_;
  }
  bool operator==(const SlotDirectory&) const = default;

 private:
  std::map<int32_t, std::string> nodes_;
  std::map<int32_t, DataGroup> groups_;
  std::map<SlotId, SlotOwner> ranges_{{0, SlotOwner{}}};
  uint32_t unassigned_slots_ = kMaxSlotNum + 1;
};

}  // namespace dfly::cluster::raft
