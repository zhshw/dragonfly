// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/slot_migration.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/metadata/metadata_snapshot.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

class SlotMigrationTest : public testing::Test {
 protected:
  void SetUp() override {
    auto path = (filesystem::temp_directory_path() / "dfly-migration-XXXXXX").string();
    ASSERT_NE(mkdtemp(path.data()), nullptr);
    dir_ = path;
    log_ = PartitionLog::Open(dir_).value();
    Apply({{FeatureLevelRecord{"metadata.version", 33, {}}},
           {FeatureLevelRecord{"dragonfly.cluster", 1, {}}}});
    for (int32_t broker : {7, 8}) {
      RegisterBrokerRecord r;
      r.broker_id = broker;
      r.broker_epoch = 9;
      r.incarnation_id.back() = broker;
      r.fenced = false;
      int group = broker == 7 ? 10 : 20;
      Apply({{r, 4},
             {RedisNodeRecord{broker, to_string(broker), {}}},
             {DataGroupRecord{group, {broker}, {}}},
             {GroupLeaderRecord{group, broker, 4, -1, broker, 9, {}}}});
    }
    Apply({{SlotAssignmentRecord{0, 8191, -1, 0, 10, 1, {}}},
           {SlotAssignmentRecord{8192, 16383, -1, 0, 20, 1, {}}}});
    req_.cluster_id = "c";
    req_.slot = 42;
    req_.source_group = 10;
    req_.target_group = 20;
    req_.generation = 1;
    req_.migration_id.back() = 1;
    req_.action = static_cast<int8_t>(MigrationAction::kStart);
  }
  void TearDown() override {
    log_.reset();
    filesystem::remove_all(dir_);
  }
  void Apply(vector<MetadataRecord> records) {
    for (const auto& record : records) {
      auto value = EncodeMetadataRecord(record).value();
      EXPECT_EQ(DecodeMetadataRecord(value).value(), record);
      auto batch = MakeDataBatch(log_->End().end_offset, 1, 500, span(&value, 1)).value();
      ASSERT_TRUE(log_->Append(batch, 1).ok());
      auto status = loader_.Advance(*log_, log_->End().end_offset);
      ASSERT_TRUE(status.ok()) << status;
    }
  }
  vector<MetadataRecord> Plan(MigrationAction action, int16_t error = 0) {
    req_.action = static_cast<int8_t>(action);
    auto records = ProposeSlotMigration(*loader_.image(), "c", req_, &reply_);
    EXPECT_EQ(reply_.error_code, error) << reply_.error_message;
    return records;
  }
  void Report(int group, int64_t end) {
    req_.group_id = group;
    req_.reporter_id = group == 10 ? 7 : 8;
    req_.broker_epoch = 9;
    req_.record_epoch = 3;
    req_.leader_epoch = 4;
    req_.record_end_offset = req_.applied_end_offset = req_.high_watermark = end;
  }
  void RestartAtSnapshot() {
    const auto cut = log_->End();
    MembershipHistory history;
    ASSERT_TRUE(history.Update(*log_).ok());
    auto before = loader_.image();
    auto status = WriteMetadataSnapshot(*before, history, cut.end_offset, 900, log_.get());
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(log_->DeleteBeforeSnapshot(cut, cut.end_offset).ok());
    log_.reset();
    log_ = PartitionLog::Open(dir_).value();
    loader_ = MetadataLoader{};
    status = loader_.Advance(*log_, cut.end_offset);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(loader_.image()->migrations, before->migrations);
    EXPECT_EQ(loader_.image()->directory, before->directory);
  }
  filesystem::path dir_;
  unique_ptr<PartitionLog> log_;
  MetadataLoader loader_;
  SlotMigrationRequest req_;
  SlotMigrationResponse reply_;
};

TEST_F(SlotMigrationTest, RejectsStaleProofsAndPublishesCutoverOnlyAtTransactionEnd) {
  req_.cluster_id = "wrong";
  EXPECT_TRUE(Plan(MigrationAction::kStart, 104).empty());
  req_.cluster_id = "c";
  req_.generation = 2;
  EXPECT_TRUE(Plan(MigrationAction::kStart, 42).empty());
  req_.generation = 1;
  Apply(Plan(MigrationAction::kStart));
  EXPECT_TRUE(Plan(MigrationAction::kStart).empty());
  req_.migration_id.back()++;
  EXPECT_TRUE(Plan(MigrationAction::kStart, 42).empty());
  req_.migration_id.back()--;
  Report(10, 100);
  req_.digest.assign(32, 42);
  req_.broker_epoch--;
  EXPECT_TRUE(Plan(MigrationAction::kFrozen, 77).empty());
  req_.broker_epoch++;
  req_.leader_epoch--;
  EXPECT_TRUE(Plan(MigrationAction::kFrozen, 74).empty());
  req_.leader_epoch++;
  req_.applied_end_offset--;
  EXPECT_TRUE(Plan(MigrationAction::kFrozen, 42).empty());
  req_.applied_end_offset++;
  Apply(Plan(MigrationAction::kFrozen));
  RestartAtSnapshot();
  EXPECT_TRUE(Plan(MigrationAction::kFrozen).empty());
  Report(20, 5);
  req_.source_epoch = 3;
  req_.source_end_offset = 99;
  EXPECT_TRUE(Plan(MigrationAction::kImported, 42).empty());
  req_.source_end_offset = 100;
  req_.digest[0]++;
  EXPECT_TRUE(Plan(MigrationAction::kImported, 42).empty());
  req_.digest[0]--;
  auto records = Plan(MigrationAction::kImported);
  ASSERT_EQ(records.size(), 3u);
  Apply({records.front(), records[1]});
  EXPECT_EQ(loader_.image()->directory.Lookup(42), (SlotOwner{10, 1}));
  EXPECT_EQ(loader_.image()->migrations.LatestCutover(42), nullptr);
  EXPECT_TRUE(loader_.transaction_open());
  Apply({records.back()});
  EXPECT_EQ(loader_.image()->directory.Lookup(42), (SlotOwner{20, 2}));
  EXPECT_EQ(loader_.image()->directory.Lookup(43), (SlotOwner{10, 1}));
  EXPECT_TRUE(Plan(MigrationAction::kImported).empty());
  req_.record_end_offset--;
  EXPECT_TRUE(Plan(MigrationAction::kImported, 42).empty());
  EXPECT_TRUE(Plan(MigrationAction::kCancel, 42).empty());
  RestartAtSnapshot();
  const auto* cutover = loader_.image()->migrations.LatestCutover(42);
  ASSERT_NE(cutover, nullptr);
  EXPECT_EQ(cutover->target_end_offset, 5);
  EXPECT_EQ(loader_.image()->migrations.LatestCutover(43), nullptr);
}

TEST_F(SlotMigrationTest, CancellationHoldsSlotUntilBothGroupsFinishAndFencesOldRetries) {
  Apply(Plan(MigrationAction::kStart));
  Apply(Plan(MigrationAction::kCancel));
  Report(10, 40);
  req_.expected_phase = static_cast<int8_t>(MigrationPhase::kCutover);
  EXPECT_TRUE(Plan(MigrationAction::kFinished, 42).empty());
  req_.expected_phase = static_cast<int8_t>(MigrationPhase::kCancelled);
  Apply(Plan(MigrationAction::kFinished));
  req_.migration_id.back()++;
  EXPECT_TRUE(Plan(MigrationAction::kStart, 42).empty());
  req_.migration_id.back()--;
  Report(20, 9);
  Apply(Plan(MigrationAction::kFinished));
  const auto original = req_;
  req_.migration_id.back()++;
  Apply(Plan(MigrationAction::kStart));
  RestartAtSnapshot();
  auto active = req_.migration_id;
  req_ = original;
  EXPECT_TRUE(Plan(MigrationAction::kFinished).empty());
  EXPECT_TRUE(Plan(MigrationAction::kStatus).empty());
  EXPECT_TRUE(reply_.source_finished && reply_.target_finished);
  EXPECT_EQ(loader_.image()->migrations.active_slots().at(42), active);
  EXPECT_EQ(loader_.image()->directory.Lookup(42), (SlotOwner{10, 1}));
}
}  // namespace
}  // namespace dfly::cluster::raft
