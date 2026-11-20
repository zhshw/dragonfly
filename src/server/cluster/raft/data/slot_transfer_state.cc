// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/slot_transfer_state.h"

#include <openssl/sha.h>

#include <algorithm>
#include <limits>

#include "server/cluster/raft/data/kv_rdb.h"
#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/protocol/flexible_message.h"
#include "server/cluster_support.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using protocol::SlotTransferRecord;
using protocol::Uuid;
using Kind = TransferKind;
constexpr string_view kSnapshotMagic = "DFTM\x01";

bool ValidIdentity(const Uuid& id, SlotId slot, int32_t source, int32_t target,
                   int64_t generation) {
  return id != Uuid{} && slot <= kMaxSlotNum && source >= 0 && target >= 0 && source != target &&
         generation > 0 && generation < numeric_limits<int64_t>::max();
}

bool Valid(const SlotTransferRecord& r) {
  if (r.version != 0 || r.position < 0 ||
      !ValidIdentity(r.migration_id, r.slot, r.source_group, r.target_group, r.generation))
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

template <typename Stream, typename Position> bool PositionFields(Stream& wire, Position& pos) {
  return wire.Field(pos.epoch) && wire.Field(pos.end_offset);
}

template <typename Stream, typename Id, typename Progress>
bool ProgressFields(Stream& wire, Id& id, Progress& p) {
  return wire.Field(id) && wire.Field(p.slot) && wire.Field(p.source_group) &&
         wire.Field(p.target_group) && wire.Field(p.generation) && PositionFields(wire, p.freeze) &&
         PositionFields(wire, p.seal) && PositionFields(wire, p.install) &&
         PositionFields(wire, p.finish) && PositionFields(wire, p.source) &&
         wire.Field(p.cancelled) && wire.Field(p.digest);
}

bool ValidProgress(const Uuid& id, const TransferProgress& p, int32_t group, LogPosition cut) {
  if (!ValidIdentity(id, p.slot, p.source_group, p.target_group, p.generation))
    return false;
  for (auto pos : {p.freeze, p.seal, p.install, p.finish}) {
    if (pos != LogPosition{} && (pos.end_offset <= 0 || pos.end_offset > cut.end_offset ||
                                 pos.epoch < 0 || pos.epoch > cut.epoch))
      return false;
  }
  const bool source = group == p.source_group;
  if (source ? p.source != LogPosition{} || p.install.end_offset
             : group != p.target_group || p.freeze.end_offset || p.seal.end_offset)
    return false;

  // The source seal in a target chunk is an offset in a different quorum.
  if (p.source != LogPosition{} && (p.source.epoch < 0 || p.source.end_offset <= 0))
    return false;
  const bool started = source ? p.freeze.end_offset != 0 : p.source.end_offset != 0;
  const auto verified = source ? p.seal : p.install;
  if ((!started && (!p.cancelled || verified.end_offset)) || (p.cancelled && !p.finish.end_offset))
    return false;
  if (source && p.seal.end_offset &&
      (p.seal.end_offset <= p.freeze.end_offset || p.seal.epoch < p.freeze.epoch))
    return false;
  if (p.finish.end_offset) {
    auto last = verified.end_offset ? verified : p.freeze;
    if (!p.bytes.empty() || (!p.cancelled && !verified.end_offset) ||
        last.end_offset >= p.finish.end_offset || last.epoch > p.finish.epoch)
      return false;
  } else if ((!source || verified.end_offset) && p.bytes.empty()) {
    return false;
  }
  return verified.end_offset ? p.digest.size() == SHA256_DIGEST_LENGTH : p.digest.empty();
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

absl::StatusOr<string> SlotTransferState::Snapshot(int32_t group, LogPosition cut) const {
  if (group < 0 || cut.epoch < 0 || cut.end_offset <= 0)
    return absl::InvalidArgumentError("Invalid slot transfer snapshot cut");
  if (transfers_.empty())
    return string{};
  string bytes(kSnapshotMagic);
  protocol::detail::Writer wire(0, &bytes);
  wire.Field(group);
  PositionFields(wire, cut);
  wire.Field(static_cast<uint64_t>(transfers_.size()));
  for (const auto& [id, p] : transfers_) {
    if (!ValidProgress(id, p, group, cut) || !ProgressFields(wire, id, p))
      return absl::FailedPreconditionError("Slot transfer state does not match the snapshot cut");
    wire.Field(static_cast<uint64_t>(p.bytes.size()));
    bytes.append(p.bytes);
  }
  return bytes;
}

absl::StatusOr<SlotTransferState> SlotTransferState::FromSnapshot(int32_t group, LogPosition cut,
                                                                  string_view bytes) {
  if (group < 0 || cut.epoch < 0 || cut.end_offset <= 0)
    return absl::InvalidArgumentError("Invalid slot transfer snapshot cut");
  SlotTransferState state;
  if (bytes.empty())
    return state;
  if (!bytes.starts_with(kSnapshotMagic))
    return absl::DataLossError("Unknown slot transfer snapshot format");
  protocol::detail::Reader header(0, bytes.substr(kSnapshotMagic.size()));
  int32_t saved_group;
  LogPosition saved_cut;
  uint64_t count;
  if (!header.Field(saved_group) || !PositionFields(header, saved_cut) || !header.Field(count) ||
      saved_group != group || saved_cut != cut || count > static_cast<uint64_t>(cut.end_offset))
    return absl::DataLossError("Invalid slot transfer snapshot header");
  bytes = header.remaining();
  for (uint64_t i = 0; i < count; ++i) {
    Uuid id;
    TransferProgress p;
    uint64_t length;
    protocol::detail::Reader wire(0, bytes);
    if (!ProgressFields(wire, id, p) || !wire.Field(length) || length > wire.remaining().size())
      return absl::DataLossError("Truncated slot transfer snapshot");
    bytes = wire.remaining();
    p.bytes.assign(bytes.substr(0, length));
    bytes.remove_prefix(length);
    if (!ValidProgress(id, p, group, cut))
      return absl::DataLossError("Invalid slot transfer snapshot progress");
    if (!p.finish.end_offset) {
      if (!p.digest.empty() &&
          KvRdbDigest(p.bytes) !=
              string_view(reinterpret_cast<const char*>(p.digest.data()), p.digest.size()))
        return absl::DataLossError("Sealed slot transfer snapshot digest mismatch");
      if (!state.active_.emplace(p.slot, id).second)
        return absl::DataLossError("Two active transfers for a snapshot slot");
    } else if (!p.cancelled && group == p.source_group) {
      auto& retired = state.retired_generations_[p.slot];
      retired = max(retired, p.generation);
    }
    if (!state.transfers_.emplace(id, std::move(p)).second)
      return absl::DataLossError("Duplicate slot transfer ID in snapshot");
  }
  if (!bytes.empty())
    return absl::DataLossError("Trailing bytes in slot transfer snapshot");
  for (const auto& [slot, id] : state.active_) {
    auto fence = state.retired_generations_.find(slot);
    if (fence != state.retired_generations_.end() && state.Find(id)->generation <= fence->second)
      return absl::DataLossError("Active snapshot transfer belongs to a retired slot generation");
  }
  return state;
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
