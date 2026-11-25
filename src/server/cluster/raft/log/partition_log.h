// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/status/statusor.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "server/cluster/raft/log/log_files.h"
#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/log/snapshot_file.h"
#include "server/cluster/raft/quorum/election.h"
#include "server/cluster/raft/trace.h"

namespace dfly::cluster::raft {

struct FetchPosition {
  enum class Kind { kValid, kDiverging, kSnapshot };
  Kind kind;
  LogPosition position;
};

// One writer per partition. Mutations and I/O completions run on the serialized group worker.
// End() reports the acknowledgement boundary: durable by default, written in memory mode.
// DurableEnd() always reports the prefix covered by completed durability barriers.
class PartitionLog {
 public:
  enum class AckMode { kFlush, kMemory };

  static absl::StatusOr<std::unique_ptr<PartitionLog>> Open(const std::filesystem::path& dir,
                                                            size_t segment_bytes = 1U << 30,
                                                            AckMode ack_mode = AckMode::kFlush);
  ~PartitionLog();
  PartitionLog(const PartitionLog&) = delete;
  PartitionLog& operator=(const PartitionLog&) = delete;

  const absl::Status& io_failure() const {
    return io_failure_;
  }

  int64_t StartOffset() const {
    return start_.end_offset;
  }
  std::optional<LogPosition> LatestSnapshot() const;
  std::optional<LogPosition> SnapshotAtOrBefore(int64_t end_offset) const;

  // Kafka RaftLog's Fetch validation returns the common prefix or a snapshot to download.
  FetchPosition ValidateOffsetAndEpoch(int64_t offset, int32_t epoch) const;
  LogPosition End() const;
  LogPosition DurableEnd() const {
    return buffered_durable_end_.value_or(End());
  }
  LogPosition EndForEpoch(int32_t requested_epoch) const;

  // Invalidates borrowed log caches when truncation or a snapshot can replace records.
  // Ordinary appends and durability publication do not change this generation.
  uint64_t content_generation() const {
    return content_generation_;
  }

  // Memory mode batches ordinary data barriers. Structural changes must flush first.
  bool BufferedFlushDue() const;
  absl::Status FlushBuffered();

  // Check an encoded append against the active segment's byte and relative-offset limits.
  // Call on the group worker after the previous append has completed.
  bool FitsActiveSegment(size_t bytes, int64_t end_offset) const;

  // Leader appends contain whole batches of this epoch; follower appends may include
  // earlier epochs and a partial trailing batch from Fetch.
  // For an all-data append, notify after the complete write and before its durability barrier.
  // The callback receives the first complete batch's byte size before End() advances.
  using DataWritten = absl::FunctionRef<void(size_t)>;
  absl::Status Append(std::string_view batches, int32_t leader_epoch,
                      std::optional<DataWritten> written = std::nullopt);
  // The submitter must invoke callbacks on the serialized group worker. Offset -1 requests
  // fsync(flags=0); other offsets request a write. Return false for the synchronous slow path.
  using IoCallback = std::function<void(int)>;
  using AsyncIO = std::function<void(int, int64_t, std::string_view, IoCallback)>;
  using AppendDone = std::function<void(absl::Status)>;

  // Retain the immutable bytes through short writes and the final completion callback.
  absl::StatusOr<bool> TryAppendAsync(std::shared_ptr<const std::string> bytes, int32_t epoch,
                                      AsyncIO submit, std::function<void(size_t)> written,
                                      AppendDone done);
  absl::StatusOr<bool> TryFlushBufferedAsync(AsyncIO submit, AppendDone done);
  bool AppendPending() const {
    return pending_ != nullptr;
  }
  bool FlushPending() const {
    return flushing_ != nullptr;
  }
  bool IoPending() const {
    return AppendPending() || FlushPending();
  }

  // Admission check only: same-term data in the active segment may overlap a buffered flush.
  // Append still validates the complete batch, including its CRC and record boundaries.
  bool CanAppendDuringFlush(std::string_view bytes) const;
  trace::Gate FlushGate(std::string_view bytes) const;
  void SetWalCompleteHook(std::function<void(int64_t)> hook) {
    wal_complete_ = std::move(hook);
  }
  void SetTraceCapture(std::shared_ptr<trace::Capture> capture) {
    capture_ = std::move(capture);
  }
  uint64_t TraceFlushId() const {
    return flushing_ ? flushing_->event.id : 0;
  }
  std::optional<std::string_view> WrittenRecords(int64_t offset, size_t max_bytes) const;
  bool PendingContains(int64_t offset, int32_t epoch) const;

  struct ValidatedBatch {
    BatchInfo info;
    size_t encoded_bytes;
    std::vector<RecordView> records;
  };

  // Optionally return the already validated, successfully appended complete prefix.
  // Record views borrow batches; output is replaced only on success. No second CRC pass is needed
  // while the caller retains the same immutable bytes. Partial/future-epoch suffixes are excluded.
  absl::Status AppendAsFollower(std::string_view batches, int32_t fetch_epoch,
                                std::vector<ValidatedBatch>* appended = nullptr,
                                size_t* appended_bytes = nullptr);

  absl::Status AppendLeaderChange(int32_t epoch, int64_t timestamp_ms, int32_t leader_id,
                                  std::span<const int32_t> voters,
                                  std::span<const int32_t> granting_voters);

  // Bound a read by LEO (uncommitted Fetch) or by the exclusive HW (committed replay).
  absl::StatusOr<std::string> Read(int64_t offset, int64_t max_offset, size_t max_bytes,
                                   int64_t* batch_start = nullptr) const;

  // Truncation is permitted only at complete batch boundaries at or above the group HW.
  absl::Status TruncateTo(int64_t offset, int64_t high_watermark);

  // Install a received leader snapshot ahead of the local log, or freeze an image at a
  // locally committed batch boundary. Only DeleteBeforeSnapshot discards a matching prefix.
  absl::StatusOr<std::unique_ptr<SnapshotWriter>> BeginSnapshot(LogPosition id, int64_t size) const;
  absl::Status InstallSnapshot(SnapshotWriter* writer, int64_t high_watermark,
                               const SnapshotValidator& validate = {});
  absl::StatusOr<SnapshotChunk> ReadSnapshot(LogPosition id, int64_t position,
                                             size_t max_bytes) const;
  // The caller has applied the snapshot and supplies the group's exclusive high watermark.
  absl::Status DeleteBeforeSnapshot(LogPosition id, int64_t high_watermark);

 private:
  struct Batch {
    LogPosition end;
    int64_t segment_base;
    int64_t file_start;
    int64_t file_end;
  };

  struct PendingAppend {
    std::shared_ptr<const std::string> bytes;
    std::vector<Batch> batches;
    log_files::RecoveryMarker::AppendPublication publication;
    AsyncIO submit;
    std::function<void(size_t)> written;
    AppendDone done;
    size_t progress = 0;
    bool readable = false;
    bool memory = false;
  };
  struct PendingFlush {
    trace::Event event;
    log_files::RecoveryMarker::AppendPublication publication;
    LogPosition end;
    size_t bytes;
    AsyncIO submit;
    AppendDone done;
  };
  void SubmitFlushSync(int fd, std::string_view role, IoCallback callback);
  void WritePending();
  void SyncPending();
  void FinishPending(absl::Status status);
  void FinishFlush(absl::Status status);
  void RecordBuffered(size_t bytes);
  void ObserveWalComplete(LogPosition end);

  PartitionLog(std::filesystem::path dir, size_t segment_bytes, AckMode ack_mode)
      : dir_(std::move(dir)), segment_bytes_(segment_bytes), ack_mode_(ack_mode) {
  }
  absl::Status Recover();
  absl::Status RecoverSegment(const log_files::DurableEnd* marker, LogPosition* physical_end);
  absl::Status ResetToSnapshot(LogPosition id);
  absl::Status RemovePrefix(int64_t first_base);
  bool IsLocalBoundary(LogPosition id) const;
  absl::Status SetActive(int64_t base, int flags);
  absl::Status RemoveSuffixSegments();
  absl::Status AppendBatches(std::string_view batches, int32_t epoch, bool follower,
                             std::optional<DataWritten> written,
                             std::vector<ValidatedBatch>* appended = nullptr,
                             size_t* appended_bytes = nullptr);
  absl::Status PublishEnd(int64_t file_end, int64_t end_offset, bool append = false);

  std::shared_ptr<trace::Capture> capture_;
  std::function<void(int64_t)> wal_complete_;
  std::filesystem::path dir_;
  int dir_fd_ = -1;
  int log_fd_ = -1;
  log_files::RecoveryMarker marker_;
  size_t segment_bytes_;
  const AckMode ack_mode_;
  std::optional<LogPosition> buffered_durable_end_;
  size_t buffered_bytes_ = 0;
  std::chrono::steady_clock::time_point buffered_since_;
  int64_t active_base_ = 0;
  int64_t active_bytes_ = 0;
  LogPosition start_{0, 0};
  std::vector<LogPosition> snapshots_;
  std::vector<int64_t> segment_bases_;
  std::vector<Batch> batches_;
  uint64_t content_generation_ = 0;
  absl::Status io_failure_;
  std::unique_ptr<PendingAppend> pending_;
  std::unique_ptr<PendingFlush> flushing_;
};

}  // namespace dfly::cluster::raft
