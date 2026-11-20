// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <span>

namespace dfly::cluster::raft {

// The physical log position may change when a segment rolls at the same end offset.
struct SegmentPosition {
  int64_t base_offset;
  int32_t relative_position;

  bool operator==(const SegmentPosition&) const = default;
};

struct LogOffsetMetadata {
  int64_t offset;
  std::optional<SegmentPosition> position = std::nullopt;

  bool operator==(const LogOffsetMetadata&) const = default;
};

// One instance per leadership epoch. The initial HW is unknown until a majority
// replicates past the epoch start, including at least one record of this term.
class LeaderHighWatermark {
 public:
  explicit LeaderHighWatermark(int64_t epoch_start_offset)
      : epoch_start_offset_(epoch_start_offset) {
  }

  // Input has one entry per configured voter, including the leader if it is a
  // voter. Missing positions (offline voters) must be included as nullopt.
  // Returns true when the commit boundary or its current disk position changes.
  bool Update(std::span<const std::optional<LogOffsetMetadata>> voter_offsets);

  const std::optional<LogOffsetMetadata>& value() const {
    return high_watermark_;
  }

 private:
  int64_t epoch_start_offset_;
  std::optional<LogOffsetMetadata> high_watermark_;
};

}  // namespace dfly::cluster::raft
