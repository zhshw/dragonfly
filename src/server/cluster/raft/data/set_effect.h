// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/status/statusor.h>

#include <string>

#include "server/cluster/raft/data/slot_transfer_state.h"
#include "server/cluster/raft/log/partition_log.h"
#include "server/common_types.h"

namespace dfly::cluster::raft {

// A resolved SET result. A replacement clears the old value and TTL. A failed condition
// commits a no-op so that its reply is authorized by the same quorum as a successful write.
// The group log supplies the epoch and offset; the effect identifies the addressed slot generation.
struct SetEffect {
  enum class Action : uint8_t { kReplace = 1, kNoop = 2 };

  int32_t group_id;
  DbIndex db;
  SlotId slot;
  int64_t generation;
  std::string key;
  std::string value;
  Action action = Action::kReplace;

  bool operator==(const SetEffect&) const = default;
};

// Produces one data record in a Kafka magic-v2 batch. The caller must check the group and
// generation before proposing via FetchReplication::Append with the same expected leader epoch.
absl::StatusOr<std::string> MakeSetEffectBatch(int64_t offset, int32_t epoch, int64_t timestamp_ms,
                                               const SetEffect& effect);

// One instance per serialized group apply loop. Start at zero over an empty database, or at
// the exclusive applied offset of an already restored KV image with the same group identity.
class CommittedEffects {
 public:
  explicit CommittedEffects(int32_t group_id, int64_t applied_offset = 0)
      : group_id_(group_id), applied_(applied_offset) {
  }

  int64_t applied_offset() const {
    return applied_;
  }
  using ApplySet = absl::FunctionRef<absl::Status(const SetEffect&)>;

  const SlotTransferState& transfers() const {
    return transfers_;
  }

  // Apply complete, committed batches in order. The callback checks historical slot ownership
  // and returns OK only after the replacement is visible on its engine shard.
  absl::Status ApplyTo(const PartitionLog& log, int64_t high_watermark, ApplySet apply);
  absl::Status ApplyTo(const PartitionLog& log, int64_t high_watermark, ApplySet apply,
                       ApplySlotImage apply_image);

 private:
  absl::Status ApplyRecord(std::string_view payload, LogPosition end, ApplySet apply,
                           ApplySlotImage apply_image);

  int32_t group_id_;
  int64_t applied_;
  bool applying_ = false;
  SlotTransferState transfers_;
};

}  // namespace dfly::cluster::raft
