// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/set_effect.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>

#include <algorithm>

#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster_support.h"

namespace dfly::cluster::raft {
namespace {

using namespace std;
namespace be = absl::big_endian;

// Dragonfly KV payload v0: magic, version, opcode, group, DB, slot, generation, key length,
// key and value. The enclosing Kafka DefaultRecord supplies the total value length.
constexpr string_view kMagic = "DFKV";
constexpr uint8_t kVersion = 0;
constexpr size_t kHeader = 26;

bool ValidAddress(const SetEffect& effect) {
  return effect.group_id >= 0 && effect.db != kInvalidDbId && effect.slot <= kMaxSlotNum &&
         effect.generation >= 0;
}

absl::StatusOr<SetEffect> DecodeSetEffect(string_view payload) {
  if (payload.size() < kHeader || !payload.starts_with(kMagic))
    return absl::DataLossError("Invalid Dragonfly KV record header");
  auto action = static_cast<SetEffect::Action>(static_cast<uint8_t>(payload[5]));
  if (payload[4] != kVersion ||
      (action != SetEffect::Action::kReplace && action != SetEffect::Action::kNoop))
    return absl::UnimplementedError("Unsupported Dragonfly KV record version or operation");
  SetEffect effect;
  effect.action = action;
  effect.group_id = static_cast<int32_t>(be::Load32(payload.data() + 6));
  effect.db = be::Load16(payload.data() + 10);
  effect.slot = be::Load16(payload.data() + 12);
  effect.generation = static_cast<int64_t>(be::Load64(payload.data() + 14));
  size_t key_size = be::Load32(payload.data() + 22);
  payload.remove_prefix(kHeader);
  if (!ValidAddress(effect) || key_size > payload.size() ||
      (action == SetEffect::Action::kNoop && payload.size() != key_size))
    return absl::DataLossError("Invalid Dragonfly KV address or key length");
  effect.key.assign(payload.substr(0, key_size));
  effect.value.assign(payload.substr(key_size));
  return effect;
}

}  // namespace

absl::StatusOr<string> MakeSetEffectBatch(int64_t offset, int32_t epoch, int64_t timestamp_ms,
                                          const SetEffect& effect) {
  if (!ValidAddress(effect))
    return absl::InvalidArgumentError("Invalid Dragonfly SET effect address");
  if ((effect.action == SetEffect::Action::kNoop && !effect.value.empty()) ||
      (effect.action != SetEffect::Action::kNoop && effect.action != SetEffect::Action::kReplace))
    return absl::InvalidArgumentError("Invalid Dragonfly SET effect operation or value");
  if (effect.key.size() > kMaxBatchSize - kHeader ||
      effect.value.size() > kMaxBatchSize - kHeader - effect.key.size())
    return absl::ResourceExhaustedError("Dragonfly SET effect exceeds Kafka batch size");
  array<char, kHeader> payload{};
  copy(kMagic.begin(), kMagic.end(), payload.begin());
  payload[4] = kVersion;
  payload[5] = static_cast<char>(effect.action);
  be::Store32(payload.data() + 6, effect.group_id);
  be::Store16(payload.data() + 10, effect.db);
  be::Store16(payload.data() + 12, effect.slot);
  be::Store64(payload.data() + 14, effect.generation);
  be::Store32(payload.data() + 22, effect.key.size());
  const array<string_view, 3> parts{string_view(payload.data(), payload.size()), effect.key,
                                    effect.value};
  return MakeSingleRecordBatch(offset, epoch, timestamp_ms, parts);
}

CommittedEffects::CommittedEffects(int32_t group_id, int64_t applied_offset)
    : group_id_(group_id),
      applied_{applied_offset ? -1 : 0, applied_offset},
      slot_generations_(kMaxSlotNum + 1, -1) {
}

absl::Status CommittedEffects::ApplyTo(const PartitionLog& log, int64_t hw, ApplySet apply) {
  return ApplyTo(log, hw, apply, [](SlotId, optional<string_view>, string_view) {
    return absl::FailedPreconditionError("No slot image installer is attached");
  });
}

vector<SnapshotSlotRange> CommittedEffects::SnapshotSlots() const {
  vector<SnapshotSlotRange> slots;
  for (SlotId slot = 0; slot <= kMaxSlotNum; ++slot) {
    int64_t gen = slot_generations_[slot];
    if (gen < 0)
      continue;
    if (!slots.empty() && slots.back().last + 1 == slot && slots.back().generation == gen)
      slots.back().last = slot;
    else
      slots.push_back({slot, slot, gen});
  }
  return slots;
}

absl::StatusOr<string> CommittedEffects::SnapshotMigrations(LogPosition cut) const {
  if (applying_ || applied_ != cut)
    return absl::FailedPreconditionError("KV snapshot cut is not the applied group offset");
  return transfers_.Snapshot(group_id_, cut);
}

absl::StatusOr<CommittedEffects> CommittedEffects::FromSnapshot(
    int32_t group_id, LogPosition cut, string_view migrations,
    span<const SnapshotSlotRange> slots) {
  auto transfers = SlotTransferState::FromSnapshot(group_id, cut, migrations);
  if (!transfers.ok())
    return transfers.status();
  CommittedEffects restored(group_id);
  int previous = -1;
  for (const auto& r : slots) {
    if (r.first <= previous || r.last < r.first || r.last > kMaxSlotNum || r.generation < 0)
      return absl::DataLossError("Invalid materialized slots in group snapshot");
    fill(restored.slot_generations_.begin() + r.first,
         restored.slot_generations_.begin() + r.last + 1, r.generation);
    previous = r.last;
  }
  restored.transfers_ = *std::move(transfers);
  restored.applied_ = cut;
  return restored;
}

absl::Status CommittedEffects::ApplyTransferRecord(string_view payload, LogPosition end,
                                                   ApplySlotImage apply_image) {
  protocol::SlotTransferRecord record;
  if (protocol::Decode(payload.substr(kSlotTransferMagic.size()), 0, &record) !=
      protocol::CodecError::kOk)
    return absl::DataLossError("Invalid Dragonfly slot transfer payload");
  return transfers_.Replay(group_id_, record, end,
                           [&](SlotId slot, optional<string_view> image, string_view digest) {
                             auto status = apply_image(slot, image, digest);
                             if (status.ok())
                               slot_generations_[slot] = image ? record.generation + 1 : -1;
                             return status;
                           });
}

absl::Status CommittedEffects::ValidateEffect(const SetEffect& effect) const {
  if (effect.group_id != group_id_)
    return absl::FailedPreconditionError("SET effect belongs to another replication group");
  if (transfers_.BlocksAccess(effect.slot, effect.generation))
    return absl::FailedPreconditionError("SET effect addresses a fenced slot generation");
  return absl::OkStatus();
}

void CommittedEffects::RememberAppended(LogPosition end, size_t encoded_bytes, SetEffect effect) {
  if (!pending_.empty() && (pending_.back().end.epoch != end.epoch ||
                            pending_.back().end.end_offset >= end.end_offset)) {
    pending_.clear();
    pending_bytes_ = 0;
  }

  // When a quorum lags, subsequent proposals can still commit via the WAL.
  if (pending_.size() >= 4096 || encoded_bytes > kMaxBatchSize - pending_bytes_)
    return;
  pending_.push_back({end, encoded_bytes, std::move(effect)});
  pending_bytes_ += encoded_bytes;
}

absl::Status CommittedEffects::ApplyTo(const PartitionLog& log, int64_t hw, ApplySet apply,
                                       ApplySlotImage apply_image) {
  return ApplyToBatched(
      log, hw,
      [&](span<const SetEffect* const> effects, size_t* applied) {
        for (const auto* effect : effects) {
          if (auto s = apply(*effect); !s.ok())
            return s;
          ++*applied;
        }
        return absl::OkStatus();
      },
      apply_image);
}

absl::Status CommittedEffects::ApplyToBatched(const PartitionLog& log, int64_t hw,
                                              ApplySetBatch apply, ApplySlotImage apply_image) {
  if (!log.io_failure().ok())
    return log.io_failure();
  if (applied_offset() < log.StartOffset() || applied_offset() > log.End().end_offset)
    return absl::FailedPreconditionError(
        "KV image applied offset is outside the retained group log");
  if (hw < 0 || hw > log.End().end_offset)
    return absl::InvalidArgumentError("Invalid data group high watermark");
  if (applying_)
    return absl::FailedPreconditionError("Group apply is already in progress");
  applying_ = true;
  absl::Cleanup reset = [this] { applying_ = false; };

  // A new epoch can replace the old leader's uncommitted suffix. Replay that prefix
  // from the authoritative WAL even if some of its old entries survived the election.
  if (!pending_.empty() && pending_.front().end.epoch != log.End().epoch) {
    pending_.clear();
    pending_bytes_ = 0;
  }
  constexpr size_t kMaxEntries = 32;
  constexpr size_t kMaxBytes = 1 << 20;
  vector<PendingEffect> ready;
  ready.reserve(kMaxEntries);
  size_t ready_bytes = 0;
  auto flush = [&]() -> absl::Status {
    array<const SetEffect*, kMaxEntries> effects;
    for (size_t i = 0; i < ready.size(); ++i)
      effects[i] = &ready[i].effect;
    size_t done = 0;
    while (done < ready.size()) {
      size_t completed = 0;
      auto status = apply(span(effects.data() + done, ready.size() - done), &completed);
      if (completed > ready.size() - done || (status.ok() && completed == 0))
        return absl::InternalError("Invalid completed SET prefix");
      for (size_t i = done; i < done + completed; ++i) {
        const auto& item = ready[i];
        slot_generations_[item.effect.slot] = item.effect.generation;
        applied_ = item.end;
        applied_bytes_ += item.encoded_bytes;
      }
      if (!status.ok())
        return status;
      done += completed;
    }
    ready.clear();
    ready_bytes = 0;
    return absl::OkStatus();
  };

  // Reading ahead must not discard a valid prefix when a later record is corrupt or fenced.
  auto fail = [&](absl::Status status) {
    auto applied = flush();
    return applied.ok() ? status : applied;
  };
  int64_t next_offset = applied_offset();
  while (next_offset < hw) {
    PendingEffect next;
    while (!pending_.empty() && pending_.front().end.end_offset <= next_offset) {
      pending_bytes_ -= pending_.front().encoded_bytes;
      pending_.pop_front();
    }
    if (!pending_.empty() && pending_.front().end.end_offset == next_offset + 1) {
      pending_bytes_ -= pending_.front().encoded_bytes;
      next = std::move(pending_.front());
      pending_.pop_front();
    } else {
      // A one-byte budget returns one complete batch, even across a segment boundary.
      int64_t base;
      auto bytes = log.Read(next_offset, hw, 1, &base);
      if (!bytes.ok())
        return fail(bytes.status());
      if (bytes->empty())
        break;
      if (base != next_offset)
        return fail(absl::DataLossError("KV image applied offset splits a group log batch"));
      vector<RecordView> records;
      auto batch = DecodeRecordBatch(*bytes, &records);
      if (!batch.ok())
        return fail(batch.status());
      if (!batch->control && (records.size() != 1 || records.front().key || !records.front().value))
        return fail(absl::DataLossError("Expected one Dragonfly KV effect in a data batch"));
      if (batch->control || records.front().value->starts_with(kSlotTransferMagic)) {
        if (auto s = flush(); !s.ok())
          return s;
        if (!batch->control) {
          if (auto s = ApplyTransferRecord(*records.front().value, batch->end, apply_image);
              !s.ok())
            return s;
        }
        applied_ = batch->end;
        applied_bytes_ += bytes->size();
        next_offset = applied_offset();
        continue;
      }
      auto effect = DecodeSetEffect(*records.front().value);
      if (!effect.ok())
        return fail(effect.status());
      next = {batch->end, bytes->size(), *std::move(effect)};
    }
    if (auto s = ValidateEffect(next.effect); !s.ok())
      return fail(s);
    next_offset = next.end.end_offset;
    if (next.effect.action == SetEffect::Action::kNoop) {
      if (auto s = flush(); !s.ok())
        return s;
      applied_ = next.end;
      applied_bytes_ += next.encoded_bytes;
      continue;
    }
    if (!ready.empty() && (ready.size() == kMaxEntries ||
                           next.encoded_bytes > kMaxBytes - min(ready_bytes, kMaxBytes))) {
      if (auto s = flush(); !s.ok())
        return s;
    }
    ready_bytes += next.encoded_bytes;
    ready.push_back(std::move(next));
  }
  return flush();
}

}  // namespace dfly::cluster::raft
