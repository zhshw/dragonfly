// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/replication/high_watermark.h"

#include <algorithm>
#include <vector>

namespace dfly::cluster::raft {

bool LeaderHighWatermark::Update(std::span<const std::optional<LogOffsetMetadata>> voter_offsets) {
  std::vector<LogOffsetMetadata> known;
  known.reserve(voter_offsets.size());
  for (const auto& entry : voter_offsets) {
    if (entry)
      known.push_back(*entry);
  }

  size_t index = voter_offsets.size() / 2;
  if (known.size() <= index)
    return false;

  // Preserve voter order for tied offsets, as in Kafka LeaderState's stable sort.
  std::stable_sort(known.begin(), known.end(),
                   [](const auto& a, const auto& b) { return a.offset > b.offset; });
  const auto& candidate = known[index];
  if (candidate.offset <= epoch_start_offset_)
    return false;
  if (high_watermark_ &&
      (candidate.offset < high_watermark_->offset || candidate == *high_watermark_))
    return false;

  high_watermark_ = candidate;
  return true;
}

}  // namespace dfly::cluster::raft
