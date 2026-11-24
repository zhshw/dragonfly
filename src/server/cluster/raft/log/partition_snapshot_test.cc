// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/base/internal/endian.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include <fstream>

#include "server/cluster/raft/log/partition_log.h"
#include "server/cluster/raft/log/testdata/controlled_io.h"
#include "server/cluster/raft/log/testdata/snapshot_fixture.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace fs = filesystem;
using Kind = FetchPosition::Kind;

class PartitionSnapshotTest : public testing::Test {
 protected:
  void SetUp() override {
    string temp = (fs::temp_directory_path() / "raft-snapshot-XXXXXX").string();
    ASSERT_NE(mkdtemp(temp.data()), nullptr);
    dir_ = temp;
    Reopen();
  }

  void TearDown() override {
    log_.reset();
    if (!dir_.empty())
      fs::remove_all(dir_);
  }

  void Reopen(size_t segment_bytes = 1U << 30,
              PartitionLog::AckMode mode = PartitionLog::AckMode::kFlush) {
    log_.reset();
    auto log = PartitionLog::Open(dir_, segment_bytes, mode);
    ASSERT_TRUE(log.ok()) << log.status();
    log_ = *std::move(log);
  }

  string Data(int64_t offset, int32_t epoch) {
    const array<string, 2> values{"a", "b"};
    return MakeDataBatch(offset, epoch, 456, values).value();
  }

  absl::Status Install(LogPosition id, int64_t hw) {
    string bytes = test::SnapshotBytes(id.epoch);
    auto writer = log_->BeginSnapshot(id, bytes.size());
    if (!writer.ok())
      return writer.status();
    if (auto s = (*writer)->Append(0, bytes); !s.ok())
      return s;
    return log_->InstallSnapshot(writer->get(), hw);
  }

  void ExpectFetch(int64_t offset, int32_t epoch, Kind kind, LogPosition expected) {
    auto result = log_->ValidateOffsetAndEpoch(offset, epoch);
    EXPECT_EQ(result.kind, kind);
    EXPECT_EQ(result.position, expected);
  }

  fs::path dir_;
  unique_ptr<PartitionLog> log_;
};

TEST_F(PartitionSnapshotTest, MemorySnapshotAndRetentionPublishOnlyDurablePrefixes) {
  Reopen(1U << 30, PartitionLog::AckMode::kMemory);
  ASSERT_TRUE(log_->Append(Data(0, 3), 3).ok());
  EXPECT_EQ(log_->DurableEnd().end_offset, 0);
  test::ControlledIO io;
  ASSERT_TRUE(
      log_->TryFlushBufferedAsync(io.Submitter(), [](absl::Status s) { EXPECT_TRUE(s.ok()); })
          .value());
  EXPECT_FALSE(Install({3, 2}, 2).ok());
  EXPECT_TRUE(log_->io_failure().ok());
  io.Complete();
  io.Complete();
  ASSERT_TRUE(Install({3, 2}, 2).ok());
  EXPECT_EQ(log_->DurableEnd().end_offset, 2);
  ASSERT_TRUE(log_->Append(Data(2, 3), 3).ok());
  EXPECT_EQ(log_->DurableEnd().end_offset, 2);
  ASSERT_TRUE(
      log_->TryFlushBufferedAsync(io.Submitter(), [](absl::Status s) { EXPECT_TRUE(s.ok()); })
          .value());
  EXPECT_FALSE(log_->DeleteBeforeSnapshot({3, 2}, 4).ok());
  EXPECT_TRUE(log_->io_failure().ok());
  io.Complete();
  io.Complete();
  ASSERT_TRUE(log_->DeleteBeforeSnapshot({3, 2}, 4).ok());
  EXPECT_EQ(log_->DurableEnd().end_offset, 4);
  Reopen();
  EXPECT_EQ(log_->StartOffset(), 2);
  EXPECT_EQ(log_->End(), (LogPosition{3, 4}));
  EXPECT_EQ(log_->Read(2, 4, SIZE_MAX).value(), Data(2, 3));
}

TEST_F(PartitionSnapshotTest, ReceiveFreezeReadAndResumeAfterRestart) {
  const auto generation = log_->content_generation();
  LogPosition id{2, 122};
  string bytes = test::SnapshotBytes(id.epoch);
  auto writer = log_->BeginSnapshot(id, bytes.size()).value();
  EXPECT_FALSE(log_->LatestSnapshot());
  EXPECT_FALSE(writer->Append(1, bytes).ok());
  EXPECT_TRUE(writer->Append(0, string_view(bytes).substr(0, 30)).ok());
  EXPECT_FALSE(log_->InstallSnapshot(writer.get(), 0).ok());
  EXPECT_EQ(log_->content_generation(), generation);
  EXPECT_FALSE(log_->ReadSnapshot(id, 0, 10).ok());
  EXPECT_EQ(writer->position(), 30);
  ASSERT_TRUE(writer->Append(30, string_view(bytes).substr(30)).ok());
  ASSERT_TRUE(log_->InstallSnapshot(writer.get(), 0).ok());
  EXPECT_GT(log_->content_generation(), generation);
  const auto installed_generation = log_->content_generation();
  EXPECT_TRUE(log_->InstallSnapshot(writer.get(), 0).ok());
  EXPECT_EQ(log_->content_generation(), installed_generation);
  EXPECT_EQ(log_->LatestSnapshot().value(), id);
  EXPECT_EQ(log_->StartOffset(), 122);
  EXPECT_EQ(log_->End(), id);
  auto chunk = log_->ReadSnapshot(id, 30, 47).value();
  EXPECT_EQ(chunk.total_size, static_cast<int64_t>(bytes.size()));
  EXPECT_EQ(chunk.bytes, bytes.substr(30, 47));
  EXPECT_EQ(log_->ReadSnapshot(id, bytes.size() - 4, 12)->bytes, bytes.substr(bytes.size() - 4));
  EXPECT_FALSE(log_->ReadSnapshot(id, bytes.size(), 10).ok());
  EXPECT_FALSE(log_->ReadSnapshot(id, -1, 10).ok());
  EXPECT_TRUE(absl::IsAlreadyExists(log_->BeginSnapshot(id, 9).status()));
  writer.reset();
  Reopen();
  ExpectFetch(0, 0, Kind::kSnapshot, id);
  ExpectFetch(122, 1, Kind::kSnapshot, id);
  ExpectFetch(122, 2, Kind::kValid, id);
  ExpectFetch(124, 9, Kind::kDiverging, id);
  auto batch = Data(122, 3);
  ASSERT_TRUE(log_->Append(batch, 3).ok());
  Reopen();
  EXPECT_EQ(log_->Read(122, 124, 1).value(), batch);
  EXPECT_FALSE(log_->Read(121, 124, 100).ok());
}

TEST_F(PartitionSnapshotTest, SnapshotInsideSegmentHidesOnlyLogicalPrefix) {
  Reopen(175);
  ASSERT_TRUE(log_->Append(Data(0, 1), 1).ok());
  ASSERT_TRUE(log_->Append(Data(2, 3), 3).ok());
  ASSERT_TRUE(log_->Append(Data(4, 4), 4).ok());
  ASSERT_TRUE(log_->Append(Data(6, 7), 7).ok());
  ASSERT_TRUE(fs::exists(dir_ / "00000000000000000004.log"));
  LogPosition first{1, 2}, latest{4, 6};
  EXPECT_FALSE(Install(first, 0).ok());
  EXPECT_FALSE(Install({3, 3}, 8).ok());
  ASSERT_TRUE(Install(first, 8).ok());
  ASSERT_TRUE(Install(latest, 8).ok());
  EXPECT_FALSE(log_->TruncateTo(2, 2).ok());
  EXPECT_FALSE(log_->DeleteBeforeSnapshot(first, 0).ok());
  ASSERT_TRUE(log_->DeleteBeforeSnapshot(first, 4).ok());
  Reopen();
  EXPECT_EQ(log_->StartOffset(), 2);
  EXPECT_TRUE(fs::exists(dir_ / "00000000000000000000.log"));
  EXPECT_TRUE(absl::IsOutOfRange(log_->Read(0, 8, 10).status()));
  EXPECT_EQ(log_->Read(2, 8, SIZE_MAX).value(), Data(2, 3));
  ExpectFetch(0, 0, Kind::kSnapshot, latest);
  ExpectFetch(2, 3, Kind::kSnapshot, latest);
  ExpectFetch(6, 0, Kind::kSnapshot, latest);
  ExpectFetch(3, 2, Kind::kDiverging, first);
  ExpectFetch(2, 1, Kind::kValid, first);
  ExpectFetch(7, 4, Kind::kDiverging, latest);
  ExpectFetch(8, 7, Kind::kValid, {7, 8});
  ASSERT_TRUE(log_->DeleteBeforeSnapshot(latest, 8).ok());
  EXPECT_FALSE(fs::exists(dir_ / SnapshotName(first)));
  EXPECT_FALSE(fs::exists(dir_ / "00000000000000000000.log"));
  // A crash after the retention marker can leave an older snapshot name in the directory.
  ofstream(dir_ / SnapshotName(first), ios::binary) << test::SnapshotBytes(first.epoch);
  Reopen();
  EXPECT_FALSE(fs::exists(dir_ / SnapshotName(first)));
  EXPECT_EQ(log_->StartOffset(), 6);
  int64_t batch_start = -1;
  EXPECT_EQ(log_->Read(7, 8, 1, &batch_start).value(), Data(6, 7));
  EXPECT_EQ(batch_start, 6);
  ASSERT_TRUE(log_->TruncateTo(6, 6).ok());
  Reopen();
  EXPECT_EQ(log_->End(), latest);
  ASSERT_TRUE(log_->Append(Data(6, 9), 9).ok());
  Reopen();
  EXPECT_EQ(log_->End(), (LogPosition{9, 8}));
}

TEST_F(PartitionSnapshotTest, NewerEpochSnapshotCanReplaceOlderUncommittedTail) {
  Reopen(100);
  ASSERT_TRUE(log_->Append(Data(0, 1), 1).ok());
  ASSERT_TRUE(log_->Append(Data(2, 1), 1).ok());
  ASSERT_TRUE(log_->Append(Data(4, 1), 1).ok());
  EXPECT_FALSE(Install({2, 2}, 4).ok());
  ASSERT_TRUE(Install({2, 4}, 2).ok());
  EXPECT_EQ(log_->End(), (LogPosition{2, 4}));
  EXPECT_EQ(fs::file_size(dir_ / "00000000000000000004.log"), 0u);
  EXPECT_FALSE(fs::exists(dir_ / "00000000000000000002.log"));
  log_.reset();

  // Simulate a restart after publishing the empty segment intent, before its creation.
  fs::remove(dir_ / "00000000000000000004.log");
  ofstream(dir_ / "00000000000000000006.log", ios::binary) << "old suffix";
  Reopen();
  EXPECT_EQ(log_->StartOffset(), 4);
  EXPECT_FALSE(fs::exists(dir_ / "00000000000000000006.log"));
  ASSERT_TRUE(log_->Append(Data(4, 3), 3).ok());
  Reopen();
  EXPECT_EQ(log_->Read(4, 6, SIZE_MAX).value(), Data(4, 3));
}

TEST_F(PartitionSnapshotTest, RejectsInvalidDownloadedEnvelopeAndCorruptInstalledFile) {
  LogPosition id{2, 42};
  string good = test::SnapshotBytes(2);
  size_t header_length = RecordBatchSize(good).value();
  string bad_crc = good;
  bad_crc.back() ^= 1;
  string gap = good;
  absl::big_endian::Store64(gap.data() + header_length, 9);
  for (const auto& bad : {bad_crc, gap, good.substr(header_length), good.substr(0, header_length),
                          test::SnapshotBytes(3), good + good}) {
    auto writer = log_->BeginSnapshot(id, bad.size()).value();
    ASSERT_TRUE(writer->Append(0, bad).ok());
    EXPECT_FALSE(log_->InstallSnapshot(writer.get(), 0).ok());
    EXPECT_FALSE(log_->LatestSnapshot());
    EXPECT_FALSE(fs::exists(dir_ / SnapshotName(id)));
    EXPECT_TRUE(log_->io_failure().ok());
  }
  auto partial = dir_ / (SnapshotName(id) + ".part");
  ofstream(partial) << "partial download";
  Reopen();
  EXPECT_FALSE(fs::exists(partial));
  ASSERT_TRUE(Install(id, 0).ok());
  log_.reset();
  ofstream(dir_ / SnapshotName(id), ios::binary) << bad_crc;
  EXPECT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));
  fs::remove(dir_ / SnapshotName(id));
  EXPECT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));
}

TEST_F(PartitionSnapshotTest, NativeSnapshotAndKafkaSegmentsRecoverWithoutLocalMarker) {
  LogPosition id{2, 42};
  log_.reset();
  fs::remove(dir_ / "dragonfly-log-end");
  fs::remove(dir_ / "00000000000000000000.log");
  ofstream(dir_ / SnapshotName(id), ios::binary) << test::SnapshotBytes(2);
  ofstream(dir_ / "00000000000000000042.log", ios::binary) << Data(42, 3);
  Reopen();
  EXPECT_EQ(log_->StartOffset(), 42);
  EXPECT_EQ(log_->End(), (LogPosition{3, 44}));
  Reopen();
  EXPECT_EQ(log_->Read(42, 44, SIZE_MAX).value(), Data(42, 3));
}

}  // namespace
}  // namespace dfly::cluster::raft
