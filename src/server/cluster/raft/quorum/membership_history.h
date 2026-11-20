// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <map>

#include "server/cluster/raft/log/partition_log.h"
#include "server/cluster/raft/protocol/voter_records.h"

namespace dfly::cluster::raft {

// KRaftControlRecordStateMachine reads the uncommitted log, unlike a metadata image.
// Only values received in a snapshot or log record have a record offset. The caller
// uses its static voter configuration while VotersAt() has no value.
class MembershipHistory {
 public:
  // Recover from the latest snapshot and log, or incrementally scan newly appended batches.
  [[nodiscard]] absl::Status Update(const PartitionLog& log);
  // Called only after PartitionLog has validated and fsynced these bytes. A Fetch can
  // include a partial trailing batch which lies beyond the newly durable end.
  [[nodiscard]] absl::Status Appended(std::string_view batches, int64_t durable_end);

  // Call after replacing a divergent suffix and before Update reads the replacement.
  void TruncateNewEntries(int64_t end_offset);
  void TruncateOldEntries(int64_t start_offset);

  int64_t next_offset() const {
    return next_offset_;
  }
  int16_t KraftVersionAt(int64_t offset) const;
  const protocol::VotersRecord* VotersAt(int64_t offset) const;
  std::optional<int64_t> LastVoterOffset() const;
  const protocol::VotersRecord* LatestVoters() const {
    return VotersAt(next_offset_ - 1);
  }
  int16_t KraftVersion() const {
    return KraftVersionAt(next_offset_ - 1);
  }

 private:
  absl::Status ReplayBatch(std::string_view bytes, std::optional<int64_t> snapshot_offset);
  absl::Status LoadSnapshot(const PartitionLog& log, LogPosition id);

  std::map<int64_t, protocol::VotersRecord> voters_;
  std::map<int64_t, int16_t> versions_;
  int64_t next_offset_ = -1;
};

}  // namespace dfly::cluster::raft
