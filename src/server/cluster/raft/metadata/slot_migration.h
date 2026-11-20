// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/raft/metadata/record_serde.h"
#include "server/cluster/raft/metadata/slot_directory.h"
#include "server/cluster/raft/protocol/slot_migration.h"

namespace dfly::cluster::raft {

enum class MigrationAction : int8_t {
  kStart = 1,
  kFrozen = 2,
  kImported = 3,
  kCancel = 4,
  kFinished = 5,
  kStatus = 6,
};
enum class MigrationPhase : int8_t { kPreparing = 1, kFrozen = 2, kCutover = 3, kCancelled = 4 };

class SlotMigrations {
 public:
  // On cutover the record applies both the old-owner CAS and the phase change to the
  // unpublished metadata image. Store each UUID's final decision to recognize late retries.
  [[nodiscard]] absl::Status Replay(const protocol::SlotMigrationRecord& record, bool snapshot,
                                    SlotDirectory* directory);
  // Returns true when applying this valid transition must also switch slot ownership.
  [[nodiscard]] absl::StatusOr<bool> Check(const protocol::SlotMigrationRecord& record,
                                           bool snapshot, const SlotDirectory& directory) const;
  bool BlocksAssignment(SlotId first, SlotId last) const;
  const protocol::SlotMigrationRecord* LatestCutover(SlotId slot) const;
  const protocol::SlotMigrationRecord* FindAdminRequest(std::string_view request_id) const;
  const auto& records() const {
    return records_;
  }
  const auto& active_slots() const {
    return active_slots_;
  }
  bool operator==(const SlotMigrations&) const = default;

 private:
  std::map<protocol::Uuid, protocol::SlotMigrationRecord> records_;
  std::map<std::string, protocol::Uuid, std::less<>> admin_requests_;
  std::map<SlotId, protocol::Uuid> active_slots_;
  std::map<SlotId, protocol::Uuid> latest_cutovers_;
};

struct MetadataImage;
std::vector<MetadataRecord> ProposeSlotMigration(const MetadataImage& image,
                                                 std::string_view cluster_id,
                                                 const protocol::SlotMigrationRequest& req,
                                                 protocol::SlotMigrationResponse* res);

}  // namespace dfly::cluster::raft
