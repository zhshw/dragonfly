// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/slot_transfer_state.h"

#include <openssl/sha.h>

#include <limits>

#include "server/cluster/raft/data/kv_rdb.h"
#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster_support.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using protocol::SlotTransferRecord;
using protocol::Uuid;
using Kind = TransferKind;

bool Valid(const SlotTransferRecord& r) {
  if (r.version != 0 || r.migration_id == Uuid{} || r.slot > kMaxSlotNum || r.source_group < 0 ||
      r.target_group < 0 || r.source_group == r.target_group || r.generation < 1 ||
      r.generation == numeric_limits<int64_t>::max() || r.position < 0)
    return false;
  const auto kind = static_cast<Kind>(r.kind);
  if (kind < Kind::kFreeze || kind > Kind::kCancel)
    return false;
  const bool target_image = kind == Kind::kTargetChunk || kind == Kind::kInstall;
  if (target_image ? (r.source_epoch < 0 || r.source_end_offset <= 0)
                   : (r.source_epoch != -1 || r.source_end_offset != -1))
    return false;
  const bool chunk = kind == Kind::kSourceChunk || kind == Kind::kTargetChunk;
  const bool verify = kind == Kind::kSeal || kind == Kind::kInstall;
  return (chunk ? r.bytes.data && !r.bytes.data->empty() : !r.bytes.data) &&
         (verify ? r.digest.size() == SHA256_DIGEST_LENGTH : r.digest.empty()) &&
         (chunk || (verify ? r.position > 0 : r.position == 0));
}

bool SameSlot(const TransferProgress& p, const SlotTransferRecord& r) {
  return p.slot == r.slot && p.source_group == r.source_group && p.target_group == r.target_group &&
         p.generation == r.generation;
}
}  // namespace

std::string KvRdbDigest(std::string_view bytes) {
  std::string digest(SHA256_DIGEST_LENGTH, '\0');
  SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(),
         reinterpret_cast<unsigned char*>(digest.data()));
  return digest;
}

absl::StatusOr<string> MakeSlotTransferBatch(int64_t offset, int32_t epoch, int64_t timestamp_ms,
                                             const SlotTransferRecord& record) {
  if (!Valid(record))
    return absl::InvalidArgumentError("Invalid slot transfer record");
  array<string, 1> payload;
  if (Encode(record, 0, &payload.front()) != protocol::CodecError::kOk)
    return absl::InvalidArgumentError("Cannot encode slot transfer record");
  payload.front().insert(0, kSlotTransferMagic);
  return MakeDataBatch(offset, epoch, timestamp_ms, payload);
}

const TransferProgress* SlotTransferState::Find(const Uuid& id) const {
  auto it = transfers_.find(id);
  return it == transfers_.end() ? nullptr : &it->second;
}

bool SlotTransferState::BlocksAccess(SlotId slot, int64_t generation) const {
  auto retired = retired_generations_.find(slot);
  if (retired != retired_generations_.end() && generation <= retired->second)
    return true;
  auto active = active_.find(slot);
  if (active == active_.end())
    return false;
  const auto& p = transfers_.at(active->second);
  return p.install.end_offset == 0 || generation != p.generation + 1;
}

absl::Status SlotTransferState::Replay(int32_t group, const SlotTransferRecord& r, LogPosition end,
                                       ApplySlotImage apply_image) {
  if (!Valid(r) || end.epoch < 0 || end.end_offset <= 0)
    return absl::DataLossError("Invalid committed slot transfer record");
  const auto kind = static_cast<Kind>(r.kind);
  const bool source = r.source_group == group;
  if ((!source && r.target_group != group) ||
      (source && (kind == Kind::kTargetChunk || kind == Kind::kInstall)) ||
      (!source && (kind == Kind::kFreeze || kind == Kind::kSourceChunk || kind == Kind::kSeal)))
    return absl::FailedPreconditionError("Slot transfer record belongs to another data group");

  // Construct the first marker off-map so a rejected record never acquires the slot.
  TransferProgress first;
  auto it = transfers_.find(r.migration_id);
  const bool creating = it == transfers_.end();
  if (creating) {
    if (kind != Kind::kFreeze && kind != Kind::kTargetChunk && kind != Kind::kCancel)
      return absl::FailedPreconditionError("Slot transfer has no initial marker");
    first.slot = r.slot;
    first.source_group = r.source_group;
    first.target_group = r.target_group;
    first.generation = r.generation;
  }
  auto& p = creating ? first : it->second;
  if (!SameSlot(p, r))
    return absl::DataLossError("Slot transfer identity changed");
  if (p.finish.end_offset > 0) {
    if ((p.cancelled && kind == Kind::kCancel) || (!p.cancelled && kind == Kind::kFinish))
      return absl::OkStatus();
    return absl::FailedPreconditionError("Slot transfer is already finished");
  }
  auto active = active_.find(r.slot);
  const bool cancel_unstarted = creating && kind == Kind::kCancel;
  if (!cancel_unstarted && active != active_.end() && active->second != r.migration_id)
    return absl::FailedPreconditionError("Another slot transfer is active");
  if (creating && !cancel_unstarted) {
    auto retired = retired_generations_.find(r.slot);
    if (retired != retired_generations_.end() && r.generation <= retired->second)
      return absl::FailedPreconditionError("Slot transfer generation already retired");
  }
  const LogPosition source_cut{r.source_epoch, r.source_end_offset};
  switch (kind) {
    case Kind::kFreeze:
      if (!p.freeze.end_offset)
        p.freeze = end;
      break;
    case Kind::kSourceChunk:
    case Kind::kTargetChunk: {
      if (p.seal.end_offset || p.install.end_offset || (source && !p.freeze.end_offset))
        return absl::FailedPreconditionError("Cannot stage another slot image chunk");
      if (!source && p.source.end_offset && source_cut != p.source)
        return absl::DataLossError("Slot chunks refer to different source cuts");

      // A new source leader may serialize the same frozen keys in a different order.
      // Its position-zero record replaces the unfinished serialization on every replica.
      if (source && r.position == 0)
        p.bytes.assign(*r.bytes.data);
      else {
        if (static_cast<uint64_t>(r.position) != p.bytes.size())
          return absl::DataLossError("Gap or overlap in slot image");
        p.bytes.append(*r.bytes.data);
      }
      if (!source)
        p.source = source_cut;
      break;
    }
    case Kind::kSeal:
    case Kind::kInstall: {
      if ((source && !p.freeze.end_offset) || (!source && source_cut != p.source))
        return absl::FailedPreconditionError("Missing slot image source cut");
      if (static_cast<uint64_t>(r.position) != p.bytes.size())
        return absl::DataLossError("Incomplete slot image");
      const auto marker = source ? p.seal : p.install;
      if (marker.end_offset > 0)
        return p.digest == r.digest ? absl::OkStatus() : absl::DataLossError("Slot digest changed");
      string_view digest(reinterpret_cast<const char*>(r.digest.data()), r.digest.size());
      if (KvRdbDigest(p.bytes) != digest)
        return absl::DataLossError("Slot image SHA256 mismatch");
      if (source) {
        p.seal = end;
      } else {
        if (auto s = apply_image(r.slot, p.bytes, digest); !s.ok())
          return s;
        p.install = end;
      }
      p.digest = r.digest;
      break;
    }
    case Kind::kFinish:
    case Kind::kCancel: {
      const bool cancel = kind == Kind::kCancel;
      if (!cancel && !(source ? p.seal : p.install).end_offset)
        return absl::FailedPreconditionError("Cannot finish a slot transfer before its image");
      if ((!cancel && source) || (cancel && p.install.end_offset)) {
        if (auto s = apply_image(r.slot, nullopt, {}); !s.ok())
          return s;
      }
      if (!cancel && source)
        retired_generations_[r.slot] = r.generation;
      p.cancelled = cancel;
      p.finish = end;
      string{}.swap(p.bytes);
      if (!cancel_unstarted)
        active_.erase(r.slot);
      break;
    }
  }
  if (creating) {
    transfers_.emplace(r.migration_id, std::move(first));
    if (!cancel_unstarted)
      active_.emplace(r.slot, r.migration_id);
  }
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
