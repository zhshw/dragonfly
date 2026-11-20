// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <map>
#include <memory>

#include "server/cluster/raft/metadata/record_serde.h"

namespace dfly::cluster::raft {

class PartitionLog;

// Consumers retain a shared immutable image. Offsets are exclusive, so stable_end_offset
// may trail the consumed prefix while the controller has an open metadata transaction.
struct MetadataImage {
  int64_t stable_end_offset = 0;
  std::map<std::string, int16_t, std::less<>> features;
  std::map<int32_t, protocol::RegisterBrokerRecord> brokers;

  bool operator==(const MetadataImage&) const = default;
};

class MetadataLoader {
 public:
  // Consume whole committed batches through high_watermark on the quorum's serialized worker.
  // A malformed batch leaves the previously installed image and replay cursor intact.
  [[nodiscard]] absl::Status Advance(const PartitionLog& log, int64_t high_watermark);
  std::shared_ptr<const MetadataImage> image() const {
    return image_;
  }
  int64_t consumed_end_offset() const {
    return consumed_end_offset_;
  }

 private:
  absl::Status ApplyBatch(std::string_view bytes);

  std::shared_ptr<const MetadataImage> image_ = std::make_shared<MetadataImage>();
  std::unique_ptr<MetadataImage> transaction_;
  int64_t consumed_end_offset_ = 0;
};

}  // namespace dfly::cluster::raft
