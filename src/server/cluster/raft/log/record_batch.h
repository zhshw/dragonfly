// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/status/statusor.h>

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "server/cluster/raft/quorum/election.h"

namespace dfly::cluster::raft {

constexpr size_t kBatchHeaderSize = 61;
constexpr size_t kMaxBatchSize = 8 << 20;  // KafkaRaftClient.MAX_BATCH_SIZE_BYTES

struct BatchInfo {
  int64_t base_offset;
  LogPosition end;
  bool control;
};

struct RecordView {
  int64_t offset;
  std::optional<std::string_view> key;
  std::optional<std::string_view> value;
};

// Reads the size from the first 12 bytes, including the base offset and length fields.
absl::StatusOr<size_t> RecordBatchSize(std::string_view prefix);

// Validates one complete uncompressed magic-v2 KRaft batch, including CRC32C,
// record boundaries and contiguous offsets. Views borrow the caller's batch bytes.
absl::StatusOr<BatchInfo> DecodeRecordBatch(std::string_view batch,
                                            std::vector<RecordView>* records = nullptr);

absl::StatusOr<std::string> MakeDataBatch(int64_t offset, int32_t epoch, int64_t timestamp_ms,
                                          std::span<const std::string> values);
absl::StatusOr<std::string> MakeLeaderChangeBatch(int64_t offset, int32_t epoch,
                                                  int64_t timestamp_ms, int32_t leader,
                                                  std::span<const int32_t> voters,
                                                  std::span<const int32_t> granting_voters);

}  // namespace dfly::cluster::raft
