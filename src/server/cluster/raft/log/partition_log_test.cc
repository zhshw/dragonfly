// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/log/partition_log.h"

#include <absl/base/internal/endian.h>
#include <absl/crc/crc32c.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include <fstream>

#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace fs = std::filesystem;

class PartitionLogTest : public testing::Test {
 protected:
  void SetUp() override {
    string temp = (fs::temp_directory_path() / "raft-partition-XXXXXX").string();
    ASSERT_NE(mkdtemp(temp.data()), nullptr);
    dir_ = temp;
    Open();
  }

  void Open(size_t segment_bytes = 1U << 30) {
    auto result = PartitionLog::Open(dir_, segment_bytes);
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
  ASSERT_TRUE(log_->AppendAsFollower(first + second, 3).ok());
  EXPECT_EQ(log_->End(), (LogPosition{3, 2}));
  EXPECT_FALSE(log_->AppendAsFollower(first, 3).ok());

  // A Fetch size limit can include part of the next batch after whole batches.
  ASSERT_TRUE(log_->AppendAsFollower(second + first.substr(0, 20), 4).ok());
  EXPECT_EQ(log_->End(), (LogPosition{4, 4}));
}

TEST_F(PartitionLogTest, TruncateOnlyUncommittedBatches) {
  auto first = MakeDataBatch(0, 3, 1, values_).value();
  auto second = MakeDataBatch(2, 4, 2, values_).value();
  ASSERT_TRUE(log_->AppendAsFollower(first + second, 4).ok());
  EXPECT_FALSE(log_->TruncateTo(0, 2).ok());
  EXPECT_FALSE(log_->TruncateTo(3, 2).ok());
  ASSERT_TRUE(log_->TruncateTo(2, 2).ok());
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
  ASSERT_TRUE(log_->Append(first, 3).ok());
  log_.reset();
  ofstream(Segment(), ios::app | ios::binary).write("torn", 4);
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
  EXPECT_FALSE(log_->Append(batch, 1).ok());
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

TEST_F(PartitionLogTest, PreparedSlotIsInvisibleUntilSelectorIsDurable) {
  const auto marker = dir_ / "dragonfly-log-end";
  auto empty = fs::file_size(marker);
  fs::create_hard_link(marker, dir_ / "same-checkpoint");
  auto batch = MakeDataBatch(0, 1, 1, values_).value();
  ASSERT_TRUE(log_->Append(batch, 1).ok());
  ASSERT_TRUE(fs::equivalent(marker, dir_ / "same-checkpoint"));
  ASSERT_NE(fs::file_size(marker), empty);
  log_.reset();

  // Model a crash after syncing the new record, with the previous selector still on disk.
  fs::resize_file(marker, empty);
  Open();
  EXPECT_EQ(log_->End().end_offset, 0);
  EXPECT_EQ(fs::file_size(Segment()), 0u);

  // Recovery also ignores a torn preparation in the inactive block.
  log_.reset();
  fstream(marker, ios::in | ios::out | ios::binary).seekp(64 * 1024).put('!');
  Open();
  ASSERT_TRUE(log_->Append(batch, 1).ok());
  log_.reset();

  // Once selected, a damaged record is fatal even if the other slot is intact.
  fstream(marker, ios::in | ios::out | ios::binary).seekp(64 * 1024 + 40).write("bad!", 4);
  EXPECT_TRUE(absl::IsDataLoss(PartitionLog::Open(dir_).status()));
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
  string legacy(44, '\0');
  legacy.replace(0, 4, "DFE2");
  absl::big_endian::Store64(legacy.data() + 24, first.size());
  absl::big_endian::Store64(legacy.data() + 32, 2);
  absl::big_endian::Store32(
      legacy.data() + 40,
      static_cast<uint32_t>(absl::ComputeCrc32c(string_view(legacy).substr(0, 40))));
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
