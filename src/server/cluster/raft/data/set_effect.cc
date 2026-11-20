// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/set_effect.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>

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
constexpr uint8_t kSet = 1;
constexpr size_t kHeader = 26;

bool ValidAddress(const SetEffect& effect) {
  return effect.group_id >= 0 && effect.db != kInvalidDbId && effect.slot <= kMaxSlotNum &&
         effect.generation >= 0;
}

absl::StatusOr<SetEffect> DecodeSetEffect(string_view payload) {
  if (payload.size() < kHeader || !payload.starts_with(kMagic))
    return absl::DataLossError("Invalid Dragonfly KV record header");
  if (payload[4] != kVersion || payload[5] != kSet)
    return absl::UnimplementedError("Unsupported Dragonfly KV record version or operation");
  SetEffect effect;
  effect.group_id = static_cast<int32_t>(be::Load32(payload.data() + 6));
  effect.db = be::Load16(payload.data() + 10);
  effect.slot = be::Load16(payload.data() + 12);
  effect.generation = static_cast<int64_t>(be::Load64(payload.data() + 14));
  size_t key_size = be::Load32(payload.data() + 22);
  payload.remove_prefix(kHeader);
  if (!ValidAddress(effect) || key_size > payload.size())
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
  if (effect.key.size() > kMaxBatchSize - kHeader ||
      effect.value.size() > kMaxBatchSize - kHeader - effect.key.size())
    return absl::ResourceExhaustedError("Dragonfly SET effect exceeds Kafka batch size");
  array<string, 1> records;
  string& payload = records.front();
  payload.assign(kHeader, '\0');
  payload.replace(0, kMagic.size(), kMagic);
  payload[4] = kVersion;
  payload[5] = kSet;
  be::Store32(payload.data() + 6, effect.group_id);
  be::Store16(payload.data() + 10, effect.db);
  be::Store16(payload.data() + 12, effect.slot);
  be::Store64(payload.data() + 14, effect.generation);
  be::Store32(payload.data() + 22, effect.key.size());
  payload += effect.key;
  payload += effect.value;
  return MakeDataBatch(offset, epoch, timestamp_ms, records);
}

absl::Status CommittedEffects::ApplyTo(const PartitionLog& log, int64_t hw, ApplySet apply) {
  if (applied_ < log.StartOffset() || applied_ > log.End().end_offset)
    return absl::FailedPreconditionError(
        "KV image applied offset is outside the retained group log");
  if (hw < 0 || hw > log.End().end_offset)
    return absl::InvalidArgumentError("Invalid data group high watermark");
  if (applying_)
    return absl::FailedPreconditionError("Group apply is already in progress");
  applying_ = true;
  absl::Cleanup reset = [this] { applying_ = false; };
  while (applied_ < hw) {
    // A one-byte budget returns just the first complete batch, even across a segment boundary.
    int64_t base;
    auto bytes = log.Read(applied_, hw, 1, &base);
    if (!bytes.ok())
      return bytes.status();
    if (bytes->empty())
      break;
    if (base != applied_)
      return absl::DataLossError("KV image applied offset splits a group log batch");
    vector<RecordView> records;
    auto batch = DecodeRecordBatch(*bytes, &records);
    if (!batch.ok())
      return batch.status();
    if (!batch->control) {
      if (records.size() != 1 || records.front().key || !records.front().value)
        return absl::DataLossError("Expected one Dragonfly KV effect in a data batch");
      auto effect = DecodeSetEffect(*records.front().value);
      if (!effect.ok())
        return effect.status();
      if (effect->group_id != group_id_)
        return absl::FailedPreconditionError("SET effect belongs to another replication group");
      if (auto s = apply(*effect); !s.ok())
        return s;
    }
    applied_ = batch->end.end_offset;
  }
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
