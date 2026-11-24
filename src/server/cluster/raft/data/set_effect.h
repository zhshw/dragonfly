// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/status/statusor.h>

#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>

#include "server/cluster/raft/data/kv_snapshot.h"
#include "server/cluster/raft/data/slot_transfer_state.h"
#include "server/cluster/raft/log/partition_log.h"
#include "server/common_types.h"

namespace dfly::cluster::raft {

struct SetEffectView;

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
  operator SetEffectView() const;
};

// Borrowed only for encoding or synchronous application; the caller retains the backing owner.
struct SetEffectView {
  int32_t group_id;
  DbIndex db;
  SlotId slot;
  int64_t generation;
  std::string_view key;
  std::string_view value;
  SetEffect::Action action = SetEffect::Action::kReplace;
};

inline SetEffect::operator SetEffectView() const {
  return {group_id, db, slot, generation, key, value, action};
}

// Produces one data record in a Kafka magic-v2 batch. The caller must check the group and
// generation before proposing via FetchReplication::Append with the same expected leader epoch.
absl::StatusOr<std::string> MakeSetEffectBatch(int64_t offset, int32_t epoch, int64_t timestamp_ms,
                                               const SetEffect& effect);

// Size and append the same wire representation into a preallocated aggregate. The effect
// must not borrow output; validation failures leave the aggregate unchanged. On success
// key_offset, if requested, locates the contiguous key/value bytes in the aggregate.
absl::StatusOr<size_t> SetEffectBatchSize(const SetEffectView& effect);
absl::Status AppendSetEffectBatch(int64_t offset, int32_t epoch, int64_t timestamp_ms,
                                  const SetEffectView& effect, std::string* output,
                                  size_t* key_offset = nullptr);

// One instance per serialized group apply loop. Start at zero over an empty database, or at
// the exclusive applied offset of an already restored KV image with the same group identity.
class CommittedEffects {
 public:
  class ApplyBatch;
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
      absl::FunctionRef<absl::Status(std::span<const SetEffectView* const>, size_t* applied)>;

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

  // Rebind the effect's key/value lengths to contiguous bytes in an immutable WAL aggregate.
  // Cache admission charges each backing capacity once; rejected entries replay from the WAL.
  void RememberAppended(LogPosition end, size_t encoded_bytes, const SetEffectView& effect,
                        std::shared_ptr<const std::string> buffer, size_t key_offset);

  // Cache ordinary effects from AppendAsFollower's validated output, borrowing the exact
  // immutable bytes it validated. Control/migration/unsupported records still replay from WAL.
  void RememberFetched(const PartitionLog& log, const std::shared_ptr<const std::string>& buffer,
                       std::span<const PartitionLog::ValidatedBatch> batches);
  void RememberFetched(const PartitionLog& log, const std::shared_ptr<const void>& buffer,
                       size_t capacity, std::span<const PartitionLog::ValidatedBatch> batches);

  // Pending-cache budget: unique backing capacity for shared entries, encoded size for owned
  // effects. I/O and the currently applying batch can retain additional references.
  size_t cached_bytes() const {
    return pending_bytes_;
  }

  // Apply complete, committed batches in order. The callback checks historical slot ownership
  // and returns OK only after the replacement is visible on its engine shard.
  absl::Status ApplyTo(const PartitionLog& log, int64_t high_watermark, ApplySet apply);
  absl::Status ApplyTo(const PartitionLog& log, int64_t high_watermark, ApplySet apply,
                       ApplySlotImage apply_image);

  // Coalesce ready replacements up to 32 entries / 1 MiB (one larger entry is allowed).
  // Control, no-op and migration records end a batch. State changes stay on the group worker.
  absl::Status ApplyToBatched(const PartitionLog& log, int64_t high_watermark, ApplySetBatch apply,
                              ApplySlotImage apply_image);

  // Detach a bounded committed ordinary prefix on the serial owner. Its immutable backing stays
  // alive in ApplyBatch while the engine works; only FinishApply advances state on the owner.
  // Null asks the caller to process a control/recovery barrier through ApplyToBatched.
  absl::StatusOr<std::shared_ptr<ApplyBatch>> PrepareApply(const PartitionLog& log, int64_t hw);
  absl::Status FinishApply(const ApplyBatch& batch, size_t completed);

 private:
  absl::Status ApplyTransferRecord(std::string_view payload, LogPosition end,
                                   ApplySlotImage apply_image);

  absl::Status ValidateEffect(const SetEffectView& effect) const;

  struct SharedEffect {
    std::shared_ptr<const void> buffer;
    size_t capacity;
    SetEffectView view;
  };

  struct PendingEffect {
    LogPosition end;
    size_t encoded_bytes;
    std::variant<SetEffect, SharedEffect> effect;

    SetEffectView View() const {
      if (auto* shared = std::get_if<SharedEffect>(&effect))
        return shared->view;
      return std::get<SetEffect>(effect);
    }
  };

  void Remember(PendingEffect effect);
  void ForgetFront();
  void ClearCache();
  void SynchronizeCache(const PartitionLog& log);

  std::deque<PendingEffect> pending_;
  std::unordered_map<const void*, size_t> buffer_uses_;
  size_t pending_bytes_ = 0;
  uint64_t cache_generation_ = 0;
  int32_t group_id_;
  LogPosition applied_;
  uint64_t applied_bytes_ = 0;
  bool applying_ = false;

  // Generation of the materialized KV for each slot, or -1 after removal. Routing metadata
  // advances independently of group apply and cannot determine which slots to snapshot.
  std::vector<int64_t> slot_generations_;
  SlotTransferState transfers_;
};

class CommittedEffects::ApplyBatch {
 public:
  size_t size() const {
    return records_.size();
  }
  SetEffectView View(size_t i) const {
    return records_[i].View();
  }
  int64_t end_offset() const {
    return records_.back().end.end_offset;
  }

 private:
  friend class CommittedEffects;
  std::vector<PendingEffect> records_;
  int64_t begin_offset_;
};

}  // namespace dfly::cluster::raft
