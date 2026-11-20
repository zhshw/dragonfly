// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/status/statusor.h>

#include <string>

#include "server/cluster/raft/log/partition_log.h"
#include "server/common_types.h"

namespace dfly::cluster::raft {

// A resolved, unconditional string replacement. SET clears the old value and TTL.
// The group log supplies the epoch and offset; the effect identifies the addressed slot generation.
struct SetEffect {
  int32_t group_id;
  DbIndex db;
  SlotId slot;
  int64_t generation;
  std::string key;
  std::string value;

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

  // Apply complete, committed batches in order. The callback checks historical slot ownership
  // and returns OK only after the replacement is visible on its engine shard.
  absl::Status ApplyTo(const PartitionLog& log, int64_t high_watermark, ApplySet apply);

 private:
  int32_t group_id_;
  int64_t applied_;
  bool applying_ = false;
};

}  // namespace dfly::cluster::raft
