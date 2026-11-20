// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/metadata_image.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include "server/cluster/raft/log/partition_log.h"
#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

FeatureLevelRecord Feature(string name, int16_t level) {
  FeatureLevelRecord record;
  record.name = std::move(name);
  record.feature_level = level;
  return record;
}

class MetadataImageTest : public testing::Test {
 protected:
  void SetUp() override {
    string tmp = (filesystem::temp_directory_path() / "dfly-image-XXXXXX").string();
    ASSERT_NE(mkdtemp(tmp.data()), nullptr);
    dir_ = std::move(tmp);
    log_ = PartitionLog::Open(dir_).value();
  }
  void TearDown() override {
    log_.reset();
    filesystem::remove_all(dir_);
  }
  void Append(initializer_list<MetadataRecord> records) {
    vector<string> values;
    for (const auto& record : records) {
      auto value = EncodeMetadataRecord(record);
      ASSERT_TRUE(value.ok()) << value.status();
      values.push_back(*std::move(value));
    }
    auto batch = MakeDataBatch(log_->End().end_offset, 1, 1000, values);
    ASSERT_TRUE(batch.ok());
    ASSERT_TRUE(log_->Append(*batch, 1).ok());
  }
  absl::Status CatchUp() {
    return loader_.Advance(*log_, log_->End().end_offset);
  }

  filesystem::path dir_;
  unique_ptr<PartitionLog> log_;
  MetadataLoader loader_;
};

TEST_F(MetadataImageTest, CommittedOpenTransactionSurvivesRestartWithoutPublishingItsRecords) {
  ASSERT_TRUE(
      log_->AppendLeaderChange(1, 1000, 1, array<int32_t, 1>{1}, array<int32_t, 1>{1}).ok());
  Append({{Feature("metadata.version", 33)}, {BeginTransactionRecord{}}, {Feature("first", 1)}});
  Append({{Feature("second", 2)}});
  ASSERT_TRUE(CatchUp().ok());
  auto before = loader_.image();
  EXPECT_EQ(before->stable_end_offset, 2);
  EXPECT_EQ(before->features.size(), 1u);
  EXPECT_EQ(loader_.consumed_end_offset(), 5);
  Append({{EndTransactionRecord{}}});
  ASSERT_TRUE(loader_.Advance(*log_, 5).ok());
  EXPECT_EQ(loader_.image(), before);

  log_.reset();
  log_ = PartitionLog::Open(dir_).value();
  MetadataLoader recovered;
  ASSERT_TRUE(recovered.Advance(*log_, 5).ok());
  EXPECT_EQ(*recovered.image(), *before);
  ASSERT_TRUE(recovered.Advance(*log_, 6).ok());
  ASSERT_TRUE(CatchUp().ok());
  EXPECT_EQ(*recovered.image(), *loader_.image());
  EXPECT_EQ(loader_.image()->features.at("second"), 2);
  EXPECT_EQ(loader_.image()->stable_end_offset, 6);
  EXPECT_EQ(before->features.size(), 1u);
}

TEST_F(MetadataImageTest, AbortDropsOnlyOpenTransactionAcrossAndInsideBatch) {
  Append({{BeginTransactionRecord{}},
          {Feature("metadata.version", 33)},
          {EndTransactionRecord{}},
          {BeginTransactionRecord{}},
          {Feature("aborted", 1)}});
  ASSERT_TRUE(CatchUp().ok());
  EXPECT_EQ(loader_.image()->stable_end_offset, 3);
  Append({{AbortTransactionRecord{}},
          {Feature("kept", 1)},
          {BeginTransactionRecord{}},
          {Feature("also-aborted", 1)},
          {AbortTransactionRecord{}}});
  ASSERT_TRUE(CatchUp().ok());
  auto image = loader_.image();
  EXPECT_EQ(image->stable_end_offset, 10);
  EXPECT_EQ(image->features.size(), 2u);
  EXPECT_TRUE(image->features.contains("metadata.version"));
  EXPECT_TRUE(image->features.contains("kept"));
}

TEST_F(MetadataImageTest, BrokerFencingAndShutdownValidateEpochAndKeepPublishedViewOnFault) {
  RegisterBrokerRecord broker;
  broker.broker_id = 7;
  broker.broker_epoch = 42;
  broker.incarnation_id.back() = 5;
  broker.log_dirs.emplace_back()[0] = 9;
  broker.cordoned_log_dirs = broker.log_dirs;
  Append({{broker, 4}});
  BrokerRegistrationChangeRecord change;
  change.broker_id = 7;
  change.broker_epoch = 42;
  change.fenced = -1;
  change.in_controlled_shutdown = 1;
  change.cordoned_log_dirs.emplace();
  Append({{change, 3}});
  ASSERT_TRUE(CatchUp().ok());
  auto before = loader_.image();
  EXPECT_FALSE(before->brokers.at(7).fenced);
  EXPECT_TRUE(before->brokers.at(7).in_controlled_shutdown);
  EXPECT_EQ(before->brokers.at(7).log_dirs, broker.log_dirs);
  EXPECT_TRUE(before->brokers.at(7).cordoned_log_dirs.value().empty());
  change.broker_epoch = 41;
  Append({{Feature("would-leak", 1)}, {change, 3}});
  EXPECT_FALSE(CatchUp().ok());
  EXPECT_FALSE(CatchUp().ok());
  EXPECT_EQ(loader_.image(), before);
  EXPECT_EQ(loader_.consumed_end_offset(), 2);
}

TEST_F(MetadataImageTest, RejectsMalformedTransactionsAndPartialCommitBoundaries) {
  Append({{BeginTransactionRecord{}}, {NoOpRecord{}}, {BeginTransactionRecord{}}});
  EXPECT_FALSE(loader_.Advance(*log_, 2).ok());
  EXPECT_FALSE(CatchUp().ok());
  EXPECT_EQ(loader_.consumed_end_offset(), 0);
  ASSERT_TRUE(log_->TruncateTo(0, 0).ok());
  Append({{EndTransactionRecord{}}});
  EXPECT_FALSE(CatchUp().ok());
  ASSERT_TRUE(log_->TruncateTo(0, 0).ok());
  Append({{AbortTransactionRecord{}}});
  EXPECT_FALSE(CatchUp().ok());
  ASSERT_TRUE(log_->TruncateTo(0, 0).ok());
  Append({{Feature("metadata.version", 12)}, {BeginTransactionRecord{}}});
  EXPECT_FALSE(CatchUp().ok());
  EXPECT_TRUE(loader_.image()->features.empty());
}

}  // namespace
}  // namespace dfly::cluster::raft
