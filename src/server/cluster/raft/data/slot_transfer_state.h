// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/status/statusor.h>

#include "server/cluster/raft/protocol/slot_transfer_record.h"
#include "server/cluster/raft/quorum/election.h"
#include "server/common_types.h"

namespace dfly::cluster::raft {

enum class TransferKind : int8_t {
  kFreeze = 1,
  kSourceChunk = 2,
  kSeal = 3,
  kTargetChunk = 4,
  kInstall = 5,
  kFinish = 6,
  kCancel = 7,
};

struct TransferProgress {
  SlotId slot{};
  int32_t source_group{};
  int32_t target_group{};
  int64_t generation{};
  LogPosition freeze{}, seal{}, install{}, finish{};

  // Source seal referenced by the target's chunks and install.
  LogPosition source{};
  std::string bytes;
  std::vector<int8_t> digest;
  bool cancelled = false;
};

// For an install, replace the slot with the validated RDB. For a clear, rdb is nullopt.
using ApplySlotImage = absl::FunctionRef<absl::Status(SlotId, std::optional<std::string_view> rdb,
                                                      std::string_view digest)>;

absl::StatusOr<std::string> MakeSlotTransferBatch(int64_t offset, int32_t epoch,
                                                  int64_t timestamp_ms,
                                                  const protocol::SlotTransferRecord& record);
inline constexpr std::string_view kSlotTransferMagic = "DFST";

class SlotTransferState {
 public:
  const TransferProgress* Find(const protocol::Uuid& id) const;
  bool BlocksAccess(SlotId slot, int64_t generation) const;
  absl::Status Replay(int32_t group, const protocol::SlotTransferRecord& record, LogPosition end,
                      ApplySlotImage apply_image);

  // The snapshot stores each migration once. Restore derives active slots and retired
  // generation fences from those records before accepting more log entries.
  absl::StatusOr<std::string> Snapshot(int32_t group, LogPosition cut) const;
  static absl::StatusOr<SlotTransferState> FromSnapshot(int32_t group, LogPosition cut,
                                                        std::string_view bytes);

 private:
  std::map<protocol::Uuid, TransferProgress> transfers_;
  std::map<SlotId, protocol::Uuid> active_;
  std::map<SlotId, int64_t> retired_generations_;
};

}  // namespace dfly::cluster::raft
