// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/status/statusor.h>

#include <deque>
#include <string>

#include "server/cluster/raft/data/kv_snapshot.h"
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
  explicit CommittedEffects(int32_t group_id, int64_t applied_offset = 0);

  int64_t applied_offset() const {
    return applied_.end_offset;
  }
  LogPosition applied_position() const {
    return applied_;
  }
  uint64_t applied_bytes() const {
    return applied_bytes_;
  }
  using ApplySet = absl::FunctionRef<absl::Status(const SetEffect&)>;

  // Execute in order and report the completed prefix, including on failure. Success may
  // consume a nonempty prefix to yield after a time budget. Views live until the call returns.
  using ApplySetBatch =
      absl::FunctionRef<absl::Status(std::span<const SetEffect* const>, size_t* applied)>;

  const SlotTransferState& transfers() const {
    return transfers_;
  }

  // Capture at the same applied boundary as the RDB. Validate the restored cursor and
  // migration state before installing the RDB and replacing the running state machine.
  std::vector<SnapshotSlotRange> SnapshotSlots() const;
  absl::StatusOr<std::string> SnapshotMigrations(LogPosition cut) const;
  static absl::StatusOr<CommittedEffects> FromSnapshot(int32_t group_id, LogPosition cut,
                                                       std::string_view migrations,
                                                       std::span<const SnapshotSlotRange> slots);

  // Keep the resolved effect of a successful leader append for delivery on commit.
  // encoded_bytes is the size of its single-record WAL batch, used by snapshot accounting.
  void RememberAppended(LogPosition end, size_t encoded_bytes, SetEffect effect);

  // Apply complete, committed batches in order. The callback checks historical slot ownership
  // and returns OK only after the replacement is visible on its engine shard.
  absl::Status ApplyTo(const PartitionLog& log, int64_t high_watermark, ApplySet apply);
  absl::Status ApplyTo(const PartitionLog& log, int64_t high_watermark, ApplySet apply,
                       ApplySlotImage apply_image);

  // Coalesce ready replacements up to 32 entries / 1 MiB (one larger entry is allowed).
  // Control, no-op and migration records end a batch. State changes stay on the group worker.
  absl::Status ApplyToBatched(const PartitionLog& log, int64_t high_watermark, ApplySetBatch apply,
                              ApplySlotImage apply_image);

 private:
  absl::Status ApplyTransferRecord(std::string_view payload, LogPosition end,
                                   ApplySlotImage apply_image);

  absl::Status ValidateEffect(const SetEffect& effect) const;

  struct PendingEffect {
    LogPosition end;
    size_t encoded_bytes;
    SetEffect effect;
  };

  std::deque<PendingEffect> pending_;
  size_t pending_bytes_ = 0;
  int32_t group_id_;
  LogPosition applied_;
  uint64_t applied_bytes_ = 0;
  bool applying_ = false;

  // Generation of the materialized KV for each slot, or -1 after removal. Routing metadata
  // advances independently of group apply and cannot determine which slots to snapshot.
  std::vector<int64_t> slot_generations_;
  SlotTransferState transfers_;
};

}  // namespace dfly::cluster::raft
