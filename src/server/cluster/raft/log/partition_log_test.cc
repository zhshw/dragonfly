// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/log/partition_log.h"

#include <absl/base/internal/endian.h>
#include <absl/crc/crc32c.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <sys/wait.h>

#include <fstream>

#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/log/testdata/controlled_io.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace fs = std::filesystem;

string LegacyEnd(string_view magic, int64_t file_end, int64_t offset) {
  string record(44, '\0');
  record.replace(0, 4, magic);
  absl::big_endian::Store64(record.data() + 24, file_end);
  absl::big_endian::Store64(record.data() + 32, offset);
  absl::big_endian::Store32(
      record.data() + 40,
      static_cast<uint32_t>(absl::ComputeCrc32c(string_view(record).substr(0, 40))));
  return record;
}

class PartitionLogTest : public testing::Test {
 protected:
  void SetUp() override {
    string temp = (fs::temp_directory_path() / "raft-partition-XXXXXX").string();
    ASSERT_NE(mkdtemp(temp.data()), nullptr);
    dir_ = temp;
    Open();
  }

  void Open(size_t segment_bytes = 1U << 30,
            PartitionLog::AckMode mode = PartitionLog::AckMode::kFlush) {
    auto result = PartitionLog::Open(dir_, segment_bytes, mode);
    ASSERT_TRUE(result.ok()) << result.status();
    log_ = *std::move(result);
  }

  void TearDown() override {
    log_.reset();
    if (!dir_.empty())
      fs::remove_all(dir_);
  }

  fs::path Segment() const {
    return dir_ / "00000000000000000000.log";
  }

  const array<string, 2> values_{"foo", "bar"};
  fs::path dir_;
  unique_ptr<PartitionLog> log_;
};

TEST_F(PartitionLogTest, AsyncAppendPublishesOnlyAfterBothBarriersAndHandlesShortWrites) {
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  ASSERT_TRUE(log_->Append(first, 3).ok());
  auto bytes = MakeDataBatch(2, 3, 1, values_).value();
  test::ControlledIO io;
  bool written = false, completed = false;
  EXPECT_FALSE(log_->TryAppendAsync(
                       make_shared<const string>(bytes), 4, io.Submitter(), [](size_t) {},
                       [](absl::Status) { ADD_FAILURE(); })
                   .ok());
  EXPECT_FALSE(log_->TryAppendAsync(
                       make_shared<const string>(bytes.substr(0, bytes.size() - 1)), 3,
                       io.Submitter(), [](size_t) {}, [](absl::Status) { ADD_FAILURE(); })
                   .ok());
  EXPECT_TRUE(io.requests.empty());
  auto payload = make_shared<const string>(bytes);
  weak_ptr<const string> retained = payload;
  const char* data = payload->data();
  auto started = log_->TryAppendAsync(
      std::move(payload), 3, io.Submitter(),
      [&](size_t first_size) {
        written = true;
        EXPECT_EQ(first_size, bytes.size());
      },
      [&](absl::Status status) {
        completed = true;
        EXPECT_TRUE(status.ok());
        EXPECT_FALSE(retained.expired());
      });
  ASSERT_TRUE(started.ok() && *started);
  ASSERT_EQ(io.requests.size(), 1u);
  EXPECT_EQ(io.requests.front().bytes.data(), data);
  EXPECT_FALSE(log_->TruncateTo(0, 0).ok());
  EXPECT_FALSE(log_->Append(bytes, 3).ok());
  EXPECT_FALSE(log_->WrittenRecords(2, 1));
  io.Complete(-EINTR);
  io.Complete(7);
  EXPECT_FALSE(written);
  ASSERT_EQ(io.requests.size(), 1u);
  EXPECT_EQ(io.requests.front().offset, int64_t(first.size() + 7));
  EXPECT_EQ(io.requests.front().bytes.data(), data + 7);
  io.Complete();
  EXPECT_TRUE(written);
  EXPECT_EQ(log_->WrittenRecords(2, 1), bytes);
  EXPECT_TRUE(log_->PendingContains(4, 3));
  EXPECT_FALSE(log_->PendingContains(4, 4));
  EXPECT_FALSE(log_->WrittenRecords(1, 100));
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  io.Complete();
  EXPECT_FALSE(completed);
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  io.Complete();
  EXPECT_TRUE(completed);
  EXPECT_TRUE(retained.expired());
  EXPECT_FALSE(log_->AppendPending());
  EXPECT_EQ(log_->End(), (LogPosition{3, 4}));
  log_.reset();
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{3, 4}));
  EXPECT_EQ(log_->Read(0, 4, SIZE_MAX).value(), first + bytes);
}

TEST_F(PartitionLogTest, MemoryAppendAcknowledgesWritesBeforeSharedDurabilityBarriers) {
  log_.reset();
  Open(1U << 30, PartitionLog::AckMode::kMemory);
  const auto marker_size = fs::file_size(dir_ / "dragonfly-log-end");
  const auto first = MakeDataBatch(0, 3, 1, values_).value();
  const auto second = MakeDataBatch(2, 3, 1, values_).value();
  test::ControlledIO io;
  bool completed = false;
  auto payload = make_shared<const string>(first);
  weak_ptr<const string> retained = payload;
  ASSERT_TRUE(log_->TryAppendAsync(
                      std::move(payload), 3, io.Submitter(), [](size_t) {},
                      [&](absl::Status s) {
                        completed = s.ok();
                        EXPECT_FALSE(retained.expired());
                      })
                  .value());
  io.Complete(7);
  EXPECT_FALSE(completed);
  EXPECT_EQ(log_->End().end_offset, 0);
  io.Complete();
  EXPECT_TRUE(completed);
  EXPECT_TRUE(retained.expired());
  EXPECT_TRUE(io.requests.empty());
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  EXPECT_EQ(log_->DurableEnd(), (LogPosition{0, 0}));
  ASSERT_TRUE(log_->AppendAsFollower(second, 3).ok());
  EXPECT_EQ(log_->End(), (LogPosition{3, 4}));
  EXPECT_EQ(log_->DurableEnd(), (LogPosition{0, 0}));
  EXPECT_EQ(fs::file_size(dir_ / "dragonfly-log-end"), marker_size);
  EXPECT_EQ(log_->Read(0, 4, SIZE_MAX).value(), first + second);

  // Receiving both writes does not publish a recovery marker or issue an implicit fsync.
  completed = false;
  ASSERT_TRUE(
      log_->TryFlushBufferedAsync(io.Submitter(), [&](absl::Status s) { completed = s.ok(); })
          .value());
  ASSERT_EQ(io.requests.size(), 1u);
  EXPECT_EQ(io.requests.front().offset, -1);
  EXPECT_FALSE(log_->Append(second, 3).ok());
  io.Complete();
  EXPECT_FALSE(completed);
  EXPECT_EQ(log_->DurableEnd().end_offset, 0);
  ASSERT_EQ(io.requests.size(), 1u);
  EXPECT_EQ(io.requests.front().offset, -1);
  io.Complete();
  EXPECT_TRUE(completed);
  EXPECT_EQ(log_->DurableEnd(), log_->End());
  EXPECT_FALSE(log_->BufferedFlushDue());
  log_.reset();
  Open();
  EXPECT_EQ(log_->Read(0, 4, SIZE_MAX).value(), first + second);
}

TEST_F(PartitionLogTest, FollowerAppendsCompletePrefixDuringMemoryFlush) {
  const auto first = MakeDataBatch(0, 3, 1, values_).value();
  const auto second = MakeDataBatch(2, 3, 1, values_).value();
  const auto third = MakeDataBatch(4, 3, 1, values_).value();
  const string response = second + third.substr(0, third.size() - 1);
  log_.reset();
  Open(first.size() + second.size(), PartitionLog::AckMode::kMemory);
  ASSERT_TRUE(log_->AppendAsFollower(first, 3).ok());
  test::ControlledIO flush;
  ASSERT_TRUE(log_->TryFlushBufferedAsync(flush.Submitter(),
                                          [](absl::Status status) { EXPECT_TRUE(status.ok()); })
                  .value());
  ASSERT_TRUE(log_->FlushPending());
  EXPECT_FALSE(log_->CanAppendDuringFlush(second.substr(0, second.size() - 1)));
  EXPECT_FALSE(log_->CanAppendDuringFlush(second + third));
  EXPECT_TRUE(log_->CanAppendDuringFlush(response));
  EXPECT_TRUE(log_->CanAppendDuringFlush(second + third.substr(0, 11)));
  ASSERT_TRUE(log_->AppendAsFollower(response, 3).ok());
  EXPECT_EQ(log_->End(), (LogPosition{3, 4}));
  while (!flush.requests.empty())
    flush.Complete();
  EXPECT_EQ(log_->DurableEnd(), (LogPosition{3, 2}));
  EXPECT_EQ(log_->Read(0, 4, SIZE_MAX).value(), first + second);
}

TEST_F(PartitionLogTest, MemoryTailWithoutPublicationIsDiscardedOnRecovery) {
  const auto first = MakeDataBatch(0, 3, 1, values_).value();
  const auto second = MakeDataBatch(2, 3, 1, values_).value();
  ASSERT_TRUE(log_->Append(first, 3).ok());
  log_.reset();
  auto pid = fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    auto log = PartitionLog::Open(dir_, 1U << 30, PartitionLog::AckMode::kMemory).value();
    if (!log->AppendAsFollower(second, 3).ok() || log->End().end_offset != 4 ||
        log->DurableEnd().end_offset != 2)
      _exit(1);
    _exit(0);
  }
  int status;
  ASSERT_EQ(waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  EXPECT_EQ(log_->Read(0, 2, SIZE_MAX).value(), first);
  EXPECT_EQ(fs::file_size(Segment()), first.size());
}

TEST_F(PartitionLogTest, MemoryFlushPublishesCapturedCutWhileAppendContinues) {
  for (int phase = 0; phase < 2; ++phase) {
    for (bool flush_first : {false, true}) {
      SCOPED_TRACE(testing::Message() << phase << ":" << flush_first);
      log_.reset();
      fs::remove_all(dir_);
      Open(1U << 30, PartitionLog::AckMode::kMemory);
      const auto first = MakeDataBatch(0, 3, 1, values_).value();
      const auto second = MakeDataBatch(2, 3, 1, values_).value();
      ASSERT_TRUE(log_->Append(first, 3).ok());
      test::ControlledIO flush, append;
      bool flushed = false, appended = false;
      ASSERT_TRUE(log_->TryFlushBufferedAsync(flush.Submitter(),
                                              [&](absl::Status s) {
                                                EXPECT_TRUE(s.ok());
                                                flushed = true;
                                              })
                      .value());
      EXPECT_TRUE(log_->FlushPending());
      EXPECT_FALSE(log_->AppendPending());
      EXPECT_TRUE(log_->IoPending());
      EXPECT_TRUE(log_->CanAppendDuringFlush(second));
      EXPECT_FALSE(log_->CanAppendDuringFlush(second.substr(0, second.size() - 1)));
      EXPECT_FALSE(log_->CanAppendDuringFlush(MakeDataBatch(2, 4, 1, values_).value()));
      if (phase)
        flush.Complete();
      ASSERT_TRUE(log_->TryAppendAsync(
                          make_shared<const string>(second), 3, append.Submitter(), [](size_t) {},
                          [&](absl::Status s) {
                            EXPECT_TRUE(s.ok());
                            appended = true;
                          })
                      .value());
      EXPECT_TRUE(log_->AppendPending());
      EXPECT_FALSE(log_->FlushBuffered().ok());
      EXPECT_FALSE(log_->TruncateTo(0, 0).ok());
      append.Complete(7);
      EXPECT_FALSE(appended);
      if (!flush_first) {
        append.Complete();
        EXPECT_TRUE(appended);
        EXPECT_EQ(log_->End().end_offset, 4);
        EXPECT_EQ(log_->DurableEnd().end_offset, 0);
      }
      while (!flush.requests.empty())
        flush.Complete();
      EXPECT_TRUE(flushed);
      EXPECT_EQ(log_->DurableEnd(), (LogPosition{3, 2}));
      if (flush_first)
        append.Complete();
      EXPECT_TRUE(appended);
      EXPECT_EQ(log_->End().end_offset, 4);
      EXPECT_EQ(log_->DurableEnd().end_offset, 2);
      EXPECT_FALSE(log_->IoPending());

      // Recovery must discard the later complete write, even if the shared fsync covered it.
      log_.reset();
      Open(1U << 30, PartitionLog::AckMode::kMemory);
      EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
      EXPECT_EQ(log_->Read(0, 2, SIZE_MAX).value(), first);
    }
  }
}

TEST_F(PartitionLogTest, MemoryFlushRetainsNewBytesAndTimer) {
  log_.reset();
  Open(1U << 30, PartitionLog::AckMode::kMemory);
  ASSERT_TRUE(log_->Append(MakeDataBatch(0, 3, 1, values_).value(), 3).ok());
  test::ControlledIO io;
  ASSERT_TRUE(
      log_->TryFlushBufferedAsync(io.Submitter(), [](absl::Status s) { EXPECT_TRUE(s.ok()); })
          .value());
  const array<string, 1> large{string(1U << 20, 'x')};
  for (int i = 0; i < 64; ++i)
    ASSERT_TRUE(log_->AppendAsFollower(MakeDataBatch(2 + i, 3, 1, large).value(), 3).ok());
  io.Complete();
  io.Complete();
  EXPECT_EQ(log_->DurableEnd().end_offset, 2);
  EXPECT_TRUE(log_->BufferedFlushDue());
  ASSERT_TRUE(log_->FlushBuffered().ok());
  EXPECT_EQ(log_->DurableEnd().end_offset, 66);
  EXPECT_FALSE(log_->BufferedFlushDue());

  // A sub-threshold tail still retains its age after the earlier cut is published.
  ASSERT_TRUE(log_->Append(MakeDataBatch(66, 3, 1, values_).value(), 3).ok());
  ASSERT_TRUE(
      log_->TryFlushBufferedAsync(io.Submitter(), [](absl::Status s) { EXPECT_TRUE(s.ok()); })
          .value());
  ASSERT_TRUE(log_->Append(MakeDataBatch(68, 3, 1, values_).value(), 3).ok());
  usleep(110000);
  io.Complete();
  io.Complete();
  EXPECT_EQ(log_->DurableEnd().end_offset, 68);
  EXPECT_TRUE(log_->BufferedFlushDue());
  ASSERT_TRUE(log_->FlushBuffered().ok());
  EXPECT_EQ(log_->DurableEnd().end_offset, 70);
}

TEST_F(PartitionLogTest, MemoryAppendAndFlushFailuresCannotClearEachOther) {
  for (bool append_fails : {false, true}) {
    for (int phase = 0; phase < 2; ++phase) {
      SCOPED_TRACE(testing::Message() << append_fails << ":" << phase);
      log_.reset();
      fs::remove_all(dir_);
      Open(1U << 30, PartitionLog::AckMode::kMemory);
      const auto first = MakeDataBatch(0, 3, 1, values_).value();
      const auto second = MakeDataBatch(2, 3, 1, values_).value();
      ASSERT_TRUE(log_->Append(first, 3).ok());
      test::ControlledIO flush, append;
      optional<absl::Status> flushed, appended;
      ASSERT_TRUE(
          log_->TryFlushBufferedAsync(flush.Submitter(), [&](absl::Status s) { flushed = s; })
              .value());
      ASSERT_TRUE(log_->TryAppendAsync(
                          make_shared<const string>(second), 3, append.Submitter(), [](size_t) {},
                          [&](absl::Status s) { appended = s; })
                      .value());
      if (phase)
        flush.Complete();
      if (append_fails)
        append.Complete(-EIO);
      else
        flush.Complete(-EIO);
      const auto failure = log_->io_failure();
      ASSERT_FALSE(failure.ok());
      while (!append.requests.empty())
        append.Complete();
      while (!flush.requests.empty())
        flush.Complete();
      ASSERT_TRUE(appended && flushed);
      EXPECT_EQ(*appended, failure);
      EXPECT_EQ(*flushed, failure);
      EXPECT_EQ(log_->io_failure(), failure);
      EXPECT_FALSE(log_->IoPending());
      EXPECT_EQ(log_->End().end_offset, 2);
      EXPECT_FALSE(log_->Append(second, 3).ok());
      EXPECT_FALSE(log_->FlushBuffered().ok());
      log_.reset();
      Open();
      EXPECT_LE(log_->End().end_offset, 2);
    }
  }
}

TEST_F(PartitionLogTest, MemoryControlAndRolloverFlushEarlierData) {
  const auto first = MakeDataBatch(0, 3, 1, values_).value();
  log_.reset();
  Open(first.size(), PartitionLog::AckMode::kMemory);
  ASSERT_TRUE(log_->Append(first, 3).ok());
  EXPECT_EQ(log_->DurableEnd().end_offset, 0);
  test::ControlledIO io;
  ASSERT_TRUE(
      log_->TryFlushBufferedAsync(io.Submitter(), [](absl::Status s) { EXPECT_TRUE(s.ok()); })
          .value());
  const array<int32_t, 1> voters{1};
  EXPECT_FALSE(log_->CanAppendDuringFlush(MakeDataBatch(2, 3, 1, values_).value()));
  EXPECT_FALSE(log_->Append(MakeDataBatch(2, 3, 1, values_).value(), 3).ok());
  EXPECT_FALSE(log_->AppendLeaderChange(4, 1, 1, voters, voters).ok());
  EXPECT_TRUE(log_->io_failure().ok());
  io.Complete();
  io.Complete();
  ASSERT_TRUE(log_->Append(MakeDataBatch(2, 3, 1, values_).value(), 3).ok());
  EXPECT_EQ(log_->End().end_offset, 4);
  EXPECT_EQ(log_->DurableEnd().end_offset, 2);
  ASSERT_TRUE(log_->AppendLeaderChange(4, 1, 1, voters, voters).ok());
  EXPECT_EQ(log_->DurableEnd(), log_->End());
  log_.reset();
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{4, 5}));
}

TEST_F(PartitionLogTest, CrashDuringOverlappingMemoryFlushRecoversOnlyCapturedCut) {
  for (int phase = 0; phase < 3; ++phase) {
    SCOPED_TRACE(phase);
    log_.reset();
    fs::remove_all(dir_);
    Open();
    const auto first = MakeDataBatch(0, 3, 1, values_).value();
    const auto second = MakeDataBatch(2, 3, 1, values_).value();
    const auto third = MakeDataBatch(4, 3, 1, values_).value();
    ASSERT_TRUE(log_->Append(first, 3).ok());
    log_.reset();
    auto pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      auto log = PartitionLog::Open(dir_, 1U << 30, PartitionLog::AckMode::kMemory).value();
      test::ControlledIO io;
      if (!log->Append(second, 3).ok() ||
          !log->TryFlushBufferedAsync(io.Submitter(), [](absl::Status) {}).value() ||
          !log->AppendAsFollower(third, 3).ok())
        _exit(1);
      if (phase == 0) {
        if (fsync(io.requests.front().fd) != 0)
          _exit(2);
      } else {
        io.Complete();
        if (phase == 2)
          io.Complete();
      }
      _exit(0);
    }
    int status;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    Open();
    EXPECT_EQ(log_->End().end_offset, phase == 0 ? 2 : 4);
    EXPECT_EQ(log_->Read(0, log_->End().end_offset, SIZE_MAX).value(),
              phase == 0 ? first : first + second);
  }
}

TEST_F(PartitionLogTest, FailedMemoryFlushFencesFurtherWrites) {
  for (int step = 0; step < 2; ++step) {
    SCOPED_TRACE(step);
    log_.reset();
    fs::remove_all(dir_);
    Open(1U << 30, PartitionLog::AckMode::kMemory);
    ASSERT_TRUE(log_->Append(MakeDataBatch(0, 3, 1, values_).value(), 3).ok());
    test::ControlledIO io;
    absl::Status completed;
    ASSERT_TRUE(log_->TryFlushBufferedAsync(io.Submitter(), [&](absl::Status s) { completed = s; })
                    .value());
    if (step)
      io.Complete();
    io.Complete(-EIO);
    EXPECT_FALSE(completed.ok());
    EXPECT_FALSE(log_->io_failure().ok());
    EXPECT_EQ(log_->DurableEnd().end_offset, 0);
    EXPECT_FALSE(log_->Append(MakeDataBatch(2, 3, 1, values_).value(), 3).ok());
    EXPECT_FALSE(log_->FlushBuffered().ok());
  }
}

TEST_F(PartitionLogTest, AsyncFailuresFenceAppendAndPreserveAcknowledgedPrefix) {
  for (int step = 0; step < 4; ++step) {
    SCOPED_TRACE(step);
    log_.reset();
    fs::remove_all(dir_);
    Open();
    const auto first = MakeDataBatch(0, 3, 1, values_).value();
    ASSERT_TRUE(log_->Append(first, 3).ok());
    const auto bytes = MakeDataBatch(2, 3, 1, values_).value();
    test::ControlledIO io;
    bool failed = false;
    auto payload = make_shared<const string>(bytes);
    weak_ptr<const string> retained = payload;
    ASSERT_TRUE(log_->TryAppendAsync(
                        std::move(payload), 3, io.Submitter(), [](size_t) {},
                        [&](absl::Status status) {
                          failed = !status.ok();
                          EXPECT_FALSE(retained.expired());
                        })
                    .value());
    for (int i = 0; i < (step == 3 ? 0 : step); ++i)
      io.Complete();
    io.Complete(step == 3 ? 0 : -EIO);
    EXPECT_TRUE(failed);
    EXPECT_TRUE(retained.expired());
    EXPECT_FALSE(log_->AppendPending());
    EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
    EXPECT_FALSE(log_->Append(bytes, 3).ok());
    log_.reset();
    Open();
    EXPECT_EQ(log_->Read(0, 2, SIZE_MAX).value(), first);
    EXPECT_TRUE(log_->End().end_offset == 2 || log_->End().end_offset == 4);
  }
}

TEST_F(PartitionLogTest, CrashAtAsyncPublicationBoundariesRecoversCompletePrefixes) {
  for (int phase = 0; phase < 3; ++phase) {
    SCOPED_TRACE(phase);
    log_.reset();
    fs::remove_all(dir_);
    Open();
    auto first = MakeDataBatch(0, 3, 1, values_).value();
    auto bytes = MakeDataBatch(2, 3, 1, values_).value();
    ASSERT_TRUE(log_->Append(first, 3).ok());
    log_.reset();
    auto pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      auto log = PartitionLog::Open(dir_).value();
      test::ControlledIO io;
      if (!log->TryAppendAsync(
                  make_shared<const string>(bytes), 3, io.Submitter(), [](size_t) {},
                  [](absl::Status) {})
               .value())
        _exit(1);
      io.Complete();
      if (phase == 0) {
        // Crash after segment sync, before delivering its completion to select the marker.
        if (fsync(io.requests.front().fd) != 0)
          _exit(2);
      } else {
        io.Complete();
        if (phase == 2)
          io.Complete();
      }
      _exit(0);
    }
    int status;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    Open();
    EXPECT_EQ(log_->Read(0, 2, SIZE_MAX).value(), first);
    if (phase == 0)
      EXPECT_EQ(log_->End().end_offset, 2);
    else
      EXPECT_EQ(log_->Read(2, 4, SIZE_MAX).value(), bytes);
  }
}

TEST_F(PartitionLogTest, AsyncSlowPathsKeepSynchronousOrdering) {
  test::ControlledIO io;
  const array<int32_t, 1> voters{1};
  auto bytes = MakeLeaderChangeBatch(0, 3, 1, 1, voters, voters).value();
  auto start = [&] {
    return log_->TryAppendAsync(
        make_shared<const string>(bytes), 3, io.Submitter(), [](size_t) {},
        [](absl::Status) { ADD_FAILURE(); });
  };
  EXPECT_FALSE(start().value());
  ASSERT_TRUE(log_->Append(bytes, 3).ok());
  log_.reset();
  Open(bytes.size());
  bytes = MakeDataBatch(1, 3, 1, values_).value();
  EXPECT_FALSE(start().value());
  EXPECT_TRUE(io.requests.empty());
}

TEST_F(PartitionLogTest, SegmentPrefixSharesBarriersAndPublishesOnlyItsTail) {
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  auto second = MakeDataBatch(2, 3, 1, values_).value();
  auto third = MakeDataBatch(4, 3, 1, values_).value();
  auto fourth = MakeDataBatch(6, 3, 1, values_).value();
  const size_t capacity = first.size() + second.size() + third.size();
  log_.reset();
  Open(capacity);
  ASSERT_TRUE(log_->Append(first, 3).ok());
  EXPECT_TRUE(log_->FitsActiveSegment(second.size() + third.size(), 6));
  EXPECT_FALSE(log_->FitsActiveSegment(second.size() + third.size() + 1, 6));
  EXPECT_FALSE(log_->FitsActiveSegment(1, int64_t{INT32_MAX} + 1));
  test::ControlledIO io;
  bool completed = false;
  ASSERT_TRUE(log_->TryAppendAsync(
                      make_shared<const string>(second + third), 3, io.Submitter(), [](size_t) {},
                      [&](absl::Status status) {
                        EXPECT_TRUE(status.ok());
                        completed = true;
                      })
                  .value());
  EXPECT_FALSE(log_->FitsActiveSegment(1, 6));
  ASSERT_EQ(io.requests.size(), 1u);
  EXPECT_EQ(io.requests.front().bytes, second + third);
  io.Complete();
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));

  // The entire prefix shares one segment fsync and one marker fsync.
  ASSERT_EQ(io.requests.size(), 1u);
  EXPECT_EQ(io.requests.front().offset, -1);
  io.Complete();
  EXPECT_FALSE(completed);
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  ASSERT_EQ(io.requests.size(), 1u);
  EXPECT_EQ(io.requests.front().offset, -1);
  io.Complete();
  EXPECT_TRUE(completed);
  EXPECT_TRUE(io.requests.empty());
  EXPECT_EQ(log_->End(), (LogPosition{3, 6}));
  EXPECT_EQ(fs::file_size(Segment()), capacity);

  // A full segment cannot admit even the first batch; the next round must roll and progress.
  EXPECT_FALSE(log_->FitsActiveSegment(fourth.size(), 8));
  EXPECT_FALSE(log_->TryAppendAsync(
                       make_shared<const string>(fourth), 3, io.Submitter(), [](size_t) {},
                       [](absl::Status) { ADD_FAILURE(); })
                   .value());
  ASSERT_TRUE(log_->Append(fourth, 3).ok());
  EXPECT_EQ(fs::file_size(dir_ / "00000000000000000006.log"), fourth.size());
  log_.reset();
  Open(capacity);
  EXPECT_EQ(log_->End(), (LogPosition{3, 8}));
  EXPECT_EQ(log_->Read(0, 6, SIZE_MAX).value(), first + second + third);
  EXPECT_EQ(log_->Read(6, 8, SIZE_MAX).value(), fourth);
}

TEST_F(PartitionLogTest, ReadAppendRecoverAndEpochLookup) {
  ASSERT_EQ(log_->End(), (LogPosition{0, 0}));
  EXPECT_FALSE(PartitionLog::Open(dir_).ok());
  const array<int32_t, 3> voters{1, 2, 3};
  ASSERT_TRUE(log_->AppendLeaderChange(2, 123, 1, voters, span(voters).first(2)).ok());
  auto batch = MakeDataBatch(1, 2, 456, values_).value();
  auto next = MakeDataBatch(3, 4, 456, values_).value();
  ASSERT_TRUE(log_->Append(batch, 2).ok());
  ASSERT_TRUE(log_->Append(next, 4).ok());
  log_.reset();
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{4, 5}));
  EXPECT_EQ(log_->EndForEpoch(1), (LogPosition{1, 0}));
  EXPECT_EQ(log_->EndForEpoch(3), (LogPosition{2, 3}));
  EXPECT_EQ(log_->EndForEpoch(9), log_->End());
  EXPECT_EQ(log_->Read(5, 5, 100).value(), "");
  EXPECT_FALSE(log_->Read(6, 5, 100).ok());
  EXPECT_EQ(log_->Read(1, 3, SIZE_MAX).value(), batch);
  EXPECT_EQ(log_->Read(2, 5, 1).value(), batch);
  EXPECT_EQ(log_->Read(1, 5, batch.size() + 3).value(), batch + next.substr(0, 3));
}

TEST_F(PartitionLogTest, AppendValidationAndFutureEpochInFetch) {
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  auto second = MakeDataBatch(2, 4, 2, values_).value();
  EXPECT_FALSE(log_->Append({}, 3).ok());
  EXPECT_FALSE(log_->Append(second, 4).ok());
  EXPECT_FALSE(log_->Append(first + second, 3).ok());
  EXPECT_FALSE(log_->Append(first.substr(0, 35), 3).ok());
  EXPECT_EQ(log_->End().end_offset, 0);
  const string future_suffix = first + second;
  vector<PartitionLog::ValidatedBatch> appended;
  ASSERT_TRUE(log_->AppendAsFollower(future_suffix, 3, &appended).ok());
  ASSERT_EQ(appended.size(), 1u);
  EXPECT_EQ(appended[0].info.end, (LogPosition{3, 2}));
  EXPECT_EQ(appended[0].encoded_bytes, first.size());
  ASSERT_EQ(appended[0].records.size(), 2u);
  EXPECT_EQ(appended[0].records[0].value, "foo");
  EXPECT_GE(appended[0].records[0].value->data(), future_suffix.data());
  EXPECT_LT(appended[0].records[0].value->data(), future_suffix.data() + first.size());
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  EXPECT_FALSE(log_->AppendAsFollower(first, 3, &appended).ok());
  EXPECT_EQ(appended.size(), 1u);

  // A Fetch size limit can include part of the next batch after whole batches.
  const string partial_suffix = second + first.substr(0, 20);
  ASSERT_TRUE(log_->AppendAsFollower(partial_suffix, 4, &appended).ok());
  ASSERT_EQ(appended.size(), 1u);
  EXPECT_EQ(appended[0].info.end, (LogPosition{4, 4}));
  EXPECT_EQ(log_->End(), (LogPosition{4, 4}));
  auto corrupt = MakeDataBatch(4, 4, 2, values_).value();
  corrupt.back() ^= 1;
  EXPECT_FALSE(log_->AppendAsFollower(corrupt, 4, &appended).ok());
  EXPECT_EQ(appended[0].info.end, (LogPosition{4, 4}));
  EXPECT_EQ(log_->End(), (LogPosition{4, 4}));
}

TEST_F(PartitionLogTest, TruncateOnlyUncommittedBatches) {
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  auto second = MakeDataBatch(2, 4, 2, values_).value();
  ASSERT_TRUE(log_->AppendAsFollower(first + second, 4).ok());
  const auto generation = log_->content_generation();
  EXPECT_FALSE(log_->TruncateTo(0, 2).ok());
  EXPECT_FALSE(log_->TruncateTo(3, 2).ok());
  EXPECT_EQ(log_->content_generation(), generation);
  ASSERT_TRUE(log_->TruncateTo(2, 2).ok());
  EXPECT_GT(log_->content_generation(), generation);
  log_.reset();
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  EXPECT_EQ(log_->Read(0, 2, SIZE_MAX).value(), first);
  auto replacement = MakeDataBatch(2, 5, 3, values_).value();
  ASSERT_TRUE(log_->Append(replacement, 5).ok());
  EXPECT_EQ(log_->Read(2, 4, SIZE_MAX).value(), replacement);
}

TEST_F(PartitionLogTest, RecoveryDropsUnpublishedSuffix) {
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  auto next = MakeDataBatch(2, 3, 2, values_).value();
  ASSERT_TRUE(log_->Append(first, 3).ok());
  log_.reset();
  ofstream(Segment(), ios::app | ios::binary) << next << "torn";
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  EXPECT_EQ(fs::file_size(Segment()), first.size());
}

TEST_F(PartitionLogTest, CorruptDurablePrefixStopsRecovery) {
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  ASSERT_TRUE(log_->Append(first, 3).ok());
  log_.reset();
  fstream file(Segment(), ios::in | ios::out | ios::binary);
  file.seekp(first.size() - 1);
  file.put(1);
  file.close();
  EXPECT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));
  fs::resize_file(Segment(), first.size() - 1);
  EXPECT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));
}

TEST_F(PartitionLogTest, PartialDurabilityFailureRequiresReopen) {
  auto batch = MakeDataBatch(0, 1, 1, values_).value();
  const auto marker = dir_ / "dragonfly-log-end";
  const auto published = fs::file_size(marker);
  fs::resize_file(marker, published + 1);
  vector<PartitionLog::ValidatedBatch> appended;
  EXPECT_FALSE(log_->AppendAsFollower(batch, 1, &appended).ok());
  EXPECT_TRUE(appended.empty());
  EXPECT_EQ(log_->End().end_offset, 0);
  EXPECT_FALSE(log_->Read(0, 0, SIZE_MAX).ok());
  EXPECT_FALSE(log_->Append(batch, 1).ok());
  log_.reset();
  fs::resize_file(marker, published);
  Open();
  EXPECT_EQ(log_->End().end_offset, 0);
  EXPECT_EQ(fs::file_size(Segment()), 0u);
  EXPECT_TRUE(log_->Append(batch, 1).ok());
}

TEST_F(PartitionLogTest, RecoversSelectedLegacySlotAndUpgradesOnAppend) {
  log_.reset();
  constexpr size_t kStride = 64 * 1024;
  const auto marker = dir_ / "dragonfly-log-end";
  auto first = MakeDataBatch(0, 1, 1, values_).value();
  auto next = MakeDataBatch(2, 1, 2, values_).value();
  auto selected = LegacyEnd("DFE3", first.size(), 2);
  auto prepared = LegacyEnd("DFE3", first.size() + next.size(), 4);
  ofstream(Segment(), ios::binary) << first << next;
  ofstream(marker, ios::binary) << prepared;
  fstream(marker, ios::in | ios::out | ios::binary).seekp(kStride) << selected;
  fs::resize_file(marker, 2 * kStride + 15);

  // A valid inactive record cannot replace a damaged selected record.
  fstream(marker, ios::in | ios::out | ios::binary).seekp(kStride).put('!');
  ASSERT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));
  fstream(marker, ios::in | ios::out | ios::binary).seekp(kStride) << selected;

  // The previous writer had synced both the next batch and its as-yet unselected record.
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{1, 2}));
  EXPECT_EQ(fs::file_size(Segment()), first.size());
  log_.reset();
  fstream(marker, ios::in | ios::out | ios::binary).put('!');
  Open();
  ASSERT_TRUE(log_->Append(next, 1).ok());
  string magic(4, '\0');
  ASSERT_TRUE(ifstream(marker, ios::binary).read(magic.data(), magic.size()));
  EXPECT_EQ(magic, "DFE4");
  log_.reset();
  Open();
  EXPECT_EQ(log_->Read(0, 4, SIZE_MAX).value(), first + next);
}

TEST_F(PartitionLogTest, PublishedLengthMustSelectACompleteBatch) {
  const auto marker = dir_ / "dragonfly-log-end";
  auto one = MakeDataBatch(0, 1, 1, span(values_).first(1)).value();
  ASSERT_TRUE(log_->Append(one, 1).ok());
  const auto length_for_one = fs::file_size(marker);
  ASSERT_TRUE(log_->TruncateTo(0, 0).ok());
  auto two = MakeDataBatch(0, 1, 2, values_).value();
  ASSERT_TRUE(log_->Append(two, 1).ok());
  log_.reset();

  // Reject a damaged size checksum independently of the log contents.
  fs::resize_file(marker, fs::file_size(marker) + 1);
  EXPECT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));

  // This selector encodes a valid offset, but the segment has no batch ending there.
  fs::resize_file(marker, length_for_one);
  EXPECT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));
}

TEST_F(PartitionLogTest, ReplacingOffsetWindowPreservesBothCrashBoundaries) {
  const auto marker = dir_ / "dragonfly-log-end";
  const auto previous = dir_ / "previous-window";
  fs::create_hard_link(marker, previous);
  vector<string> values(4095, "a");
  auto first = MakeDataBatch(0, 1, 1, values).value();
  ASSERT_TRUE(log_->Append(first, 1).ok());
  EXPECT_TRUE(fs::equivalent(marker, previous));
  auto next = MakeDataBatch(4095, 1, 2, values_).value();
  ASSERT_TRUE(log_->AppendAsFollower(next, 1).ok());
  EXPECT_FALSE(fs::equivalent(marker, previous));
  log_.reset();
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{1, 4097}));
  EXPECT_EQ(log_->Read(4095, 4097, SIZE_MAX).value(), next);
  log_.reset();

  // If the name switch did not survive a crash, the old entry still selects the old end.
  fs::rename(previous, marker);
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{1, 4095}));
  EXPECT_EQ(fs::file_size(Segment()), first.size());
  EXPECT_EQ(log_->Read(0, 4095, SIZE_MAX).value(), first);
}

TEST_F(PartitionLogTest, RawKafkaSegmentMustBeContinuous) {
  log_.reset();
  fs::remove(dir_ / "dragonfly-log-end");
  auto first = MakeDataBatch(0, 1, 1, values_).value();
  auto gap = MakeDataBatch(8, 2, 1, values_).value();
  ofstream(Segment(), ios::binary) << first << gap;
  EXPECT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));
  ofstream(Segment(), ios::binary) << first;
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{1, 2}));
}

TEST_F(PartitionLogTest, RollReadAndReplaceSuffixAcrossSegments) {
  log_.reset();
  Open(100);
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  auto second = MakeDataBatch(2, 5, 2, values_).value();
  auto third = MakeDataBatch(4, 5, 3, values_).value();
  ASSERT_TRUE(log_->Append(first, 3).ok());
  ASSERT_TRUE(log_->Append(second, 5).ok());
  ASSERT_TRUE(log_->Append(third, 5).ok());
  auto middle = dir_ / "00000000000000000002.log";
  auto last = dir_ / "00000000000000000004.log";
  EXPECT_EQ(fs::file_size(middle), second.size());
  EXPECT_EQ(fs::file_size(last), third.size());
  log_.reset();
  Open(100);
  EXPECT_EQ(log_->EndForEpoch(4), (LogPosition{3, 2}));
  EXPECT_EQ(log_->Read(0, 6, SIZE_MAX).value(), first);
  EXPECT_EQ(log_->Read(2, 6, SIZE_MAX).value(), second);
  EXPECT_EQ(log_->Read(4, 6, SIZE_MAX).value(), third);
  ASSERT_TRUE(log_->TruncateTo(2, 2).ok());
  EXPECT_FALSE(fs::exists(middle));
  EXPECT_FALSE(fs::exists(last));
  auto replacement = MakeDataBatch(2, 7, 4, values_).value();
  ASSERT_TRUE(log_->Append(replacement, 7).ok());
  log_.reset();
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{7, 4}));
  EXPECT_EQ(log_->Read(2, 4, SIZE_MAX).value(), replacement);
}

TEST_F(PartitionLogTest, UnpublishedRollAndMissingDurableMiddleSegment) {
  log_.reset();
  Open(100);
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  auto second = MakeDataBatch(2, 3, 2, values_).value();
  auto third = MakeDataBatch(4, 3, 3, values_).value();
  ASSERT_TRUE(log_->Append(first, 3).ok());
  const auto marker = dir_ / "dragonfly-log-end";
  const auto published = fs::file_size(marker);
  fs::resize_file(marker, published + 1);
  EXPECT_FALSE(log_->Append(second, 3).ok());
  EXPECT_FALSE(log_->io_failure().ok());
  auto middle = dir_ / "00000000000000000002.log";
  EXPECT_TRUE(fs::exists(middle));
  log_.reset();
  fs::resize_file(marker, published);
  Open(100);
  EXPECT_FALSE(fs::exists(middle));
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  ASSERT_TRUE(log_->Append(second, 3).ok());
  ASSERT_TRUE(log_->Append(third, 3).ok());
  log_.reset();
  ASSERT_TRUE(fs::remove(middle));
  EXPECT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));
}

TEST_F(PartitionLogTest, UpgradeSingleSegmentMarker) {
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  ASSERT_TRUE(log_->Append(first, 3).ok());
  log_.reset();
  string legacy(24, '\0');
  legacy.replace(0, 4, "DFE1");
  absl::big_endian::Store64(legacy.data() + 4, first.size());
  absl::big_endian::Store64(legacy.data() + 12, 2);
  absl::big_endian::Store32(
      legacy.data() + 20,
      static_cast<uint32_t>(absl::ComputeCrc32c(string_view(legacy).substr(0, 20))));
  ofstream(dir_ / "dragonfly-log-end", ios::binary) << legacy;
  Open(100);
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  auto next = MakeDataBatch(2, 4, 2, values_).value();
  ASSERT_TRUE(log_->Append(next, 4).ok());
  log_.reset();
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{4, 4}));
}

TEST_F(PartitionLogTest, FailedUpgradeKeepsSegmentedLegacyBoundary) {
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  ASSERT_TRUE(log_->Append(first, 3).ok());
  log_.reset();
  const auto marker = dir_ / "dragonfly-log-end";
  auto legacy = LegacyEnd("DFE2", first.size(), 2);
  ofstream(marker, ios::binary) << legacy;
  Open();

  // The old checkpoint must remain authoritative until the new inode is ready to install.
  const auto temporary = dir_ / "dragonfly-log-end.tmp";
  ASSERT_TRUE(fs::create_directory(temporary));
  auto next = MakeDataBatch(2, 4, 2, values_).value();
  EXPECT_FALSE(log_->Append(next, 4).ok());
  EXPECT_EQ(fs::file_size(marker), legacy.size());
  log_.reset();
  fs::remove(temporary);
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  EXPECT_EQ(fs::file_size(Segment()), first.size());
  ASSERT_TRUE(log_->Append(next, 4).ok());
  log_.reset();
  Open();
  EXPECT_EQ(log_->End(), (LogPosition{4, 4}));
}

}  // namespace
}  // namespace dfly::cluster::raft
