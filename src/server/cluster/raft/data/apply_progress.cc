// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/apply_progress.h"

#include <absl/base/internal/endian.h>

namespace dfly::cluster::raft {
namespace {

// Private extension of FetchPartition for __dragonfly_data, independent of Kafka's standard tags.
constexpr uint32_t kAppliedOffsetTag = 0x44464b56;  // DFKV
constexpr size_t kSize = 9;
constexpr uint8_t kVersion = 0;
}  // namespace

void DataApplyProgress::Advertise(int64_t applied, FetchPartition* partition) {
  if (applied < 0 || applied > partition->fetch_offset || partition->current_leader_epoch < 0) {
    partition->unknown_tags.erase(kAppliedOffsetTag);
    return;
  }
  std::string value(kSize, '\0');
  value[0] = kVersion;
  absl::big_endian::Store64(value.data() + 1, applied);
  partition->unknown_tags[kAppliedOffsetTag] = std::move(value);
}

void DataApplyProgress::Observe(ReplicaKey source, const FetchPartition& partition,
                                int64_t durable_offset, int64_t now_ms) {
  auto tag = partition.unknown_tags.find(kAppliedOffsetTag);
  if (tag != partition.unknown_tags.end() && tag->second.size() == kSize &&
      tag->second[0] == kVersion && durable_offset >= 0 && now_ms >= 0 &&
      partition.current_leader_epoch >= 0) {
    uint64_t offset = absl::big_endian::Load64(tag->second.data() + 1);
    if (offset <= static_cast<uint64_t>(durable_offset)) {
      peers_[source.id] = {source, partition.current_leader_epoch, static_cast<int64_t>(offset),
                           now_ms};
      return;
    }
  }

  // A restarted or older replica may no longer be able to confirm its previous apply position.
  peers_.erase(source.id);
}

std::optional<int64_t> DataApplyProgress::Applied(ReplicaKey voter, int32_t epoch, int64_t now_ms,
                                                  int64_t max_age_ms) const {
  auto it = peers_.find(voter.id);
  if (it == peers_.end())
    return std::nullopt;
  const Report& report = it->second;
  if (!voter.Matches(report.source) || report.epoch != epoch || now_ms < report.time_ms ||
      now_ms - report.time_ms > max_age_ms)
    return std::nullopt;
  return report.applied;
}

}  // namespace dfly::cluster::raft
