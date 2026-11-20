// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/membership_history.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/escaping.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/log/testdata/snapshot_fixture.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;
namespace fs = filesystem;

class MembershipHistoryTest : public testing::Test {
 protected:
  void SetUp() override {
    string temp = (fs::temp_directory_path() / "dfly-voters-XXXXXX").string();
    ASSERT_NE(mkdtemp(temp.data()), nullptr);
    dir_ = temp;
    log_ = PartitionLog::Open(dir_).value();
    auto& voter = members_.voters.emplace_back();
    voter.voter_id = 1;
    voter.voter_directory_id.back() = 2;
    auto& endpoint = voter.endpoints.emplace_back();
    endpoint.name = "RAFT";
    endpoint.host = "b";
    endpoint.port = 9001;
    voter.k_raft_version_feature.max_supported_version = 1;
  }
  void TearDown() override {
    log_.reset();
    fs::remove_all(dir_);
  }
  string Version(int16_t level) {
    KRaftVersionRecord record;
    record.k_raft_version = level;
    string out;
    EXPECT_EQ(Encode(record, 0, &out), CodecError::kOk);
    return out;
  }
  string Members() {
    string out;
    EXPECT_EQ(Encode(members_, 0, &out), CodecError::kOk);
    return out;
  }
  string Batch(int64_t offset, int16_t type, string_view value) {
    array<ControlRecordValue, 1> records{{{type, value}}};
    return MakeControlBatch(offset, 2, 123, records).value();
  }
  fs::path dir_;
  unique_ptr<PartitionLog> log_;
  MembershipHistory history_;
  VotersRecord members_;
};

TEST_F(MembershipHistoryTest, MatchesKafkaControlRecordsAndAppliesUncommittedChanges) {
  // Complete MemoryRecords batches emitted by testdata/VoterVectors.java.
  string version, voters;
  ASSERT_TRUE(absl::HexStringToBytes(
      "0000000000000000000000410000000202229714b5002000000000000000000000007b000000000000007b"
      "ffffffffffffffffffffffffffff000000011e00000008000000050a000000010000",
      &version));
  ASSERT_TRUE(absl::HexStringToBytes(
      "000000000000000100000065000000020219b10f45002000000000000000000000007b000000000000007b"
      "ffffffffffffffffffffffffffff000000016600000008000000065200000200000001000000000000000000"
      "0000000000000202055241465402622329000000000100000000",
      &voters));
  ASSERT_EQ(version, Batch(0, 5, Version(1)));
  ASSERT_EQ(voters, Batch(1, 6, Members()));
  ASSERT_TRUE(log_->Append(version + voters, 2).ok());
  ASSERT_TRUE(history_.Update(*log_).ok());
  EXPECT_EQ(history_.next_offset(), 2);
  EXPECT_EQ(history_.KraftVersionAt(0), 1);
  EXPECT_FALSE(history_.VotersAt(0));
  ASSERT_NE(history_.VotersAt(1), nullptr);
  EXPECT_EQ(*history_.LatestVoters(), members_);
  EXPECT_EQ(history_.LastVoterOffset(), 1);
}

TEST_F(MembershipHistoryTest, TruncatesUncommittedVoterChangesAndKeepsFloorAtCompaction) {
  ASSERT_TRUE(log_->Append(Batch(0, 5, Version(1)) + Batch(1, 6, Members()), 2).ok());
  members_.voters[0].endpoints[0].port = 9100;
  ASSERT_TRUE(log_->Append(Batch(2, 6, Members()), 2).ok());
  members_.voters[0].endpoints[0].port = 9200;
  ASSERT_TRUE(log_->Append(Batch(3, 6, Members()), 2).ok());
  ASSERT_TRUE(history_.Update(*log_).ok());

  const LogPosition cut{2, 3};
  auto header = history_.SnapshotHeader(cut, 100, 50).value();
  auto footer = MakeSnapshotFooterBatch(DecodeRecordBatch(header)->end.end_offset, 2, 100).value();
  auto snapshot = header + footer;
  auto writer = log_->BeginSnapshot(cut, snapshot.size()).value();
  ASSERT_TRUE(writer->Append(0, snapshot).ok());
  ASSERT_TRUE(log_->InstallSnapshot(writer.get(), cut.end_offset).ok());
  ASSERT_TRUE(log_->DeleteBeforeSnapshot(cut, cut.end_offset).ok());
  ASSERT_TRUE(history_.Update(*log_).ok());
  EXPECT_FALSE(history_.VotersAt(1));
  ASSERT_NE(history_.VotersAt(2), nullptr);
  EXPECT_EQ(history_.VotersAt(2)->voters[0].endpoints[0].port, 9100);
  EXPECT_EQ(history_.LatestVoters()->voters[0].endpoints[0].port, 9200);

  ASSERT_TRUE(log_->TruncateTo(3, 3).ok());
  history_.TruncateNewEntries(3);
  EXPECT_EQ(history_.LastVoterOffset(), 2);
  EXPECT_EQ(history_.LatestVoters()->voters[0].endpoints[0].port, 9100);
  members_.voters[0].endpoints[0].port = 9300;
  ASSERT_TRUE(log_->Append(Batch(3, 6, Members()), 2).ok());
  ASSERT_TRUE(history_.Update(*log_).ok());
  EXPECT_EQ(history_.LatestVoters()->voters[0].endpoints[0].port, 9300);
}

TEST_F(MembershipHistoryTest, RecoversVotersFromSnapshotAtLastContainedOffset) {
  const LogPosition cut{2, 8};
  auto framed = test::SnapshotBytes(cut.epoch);
  auto header_size = RecordBatchSize(framed).value();
  auto middle_size = RecordBatchSize(string_view(framed).substr(header_size)).value();
  auto footer = framed.substr(header_size + middle_size);
  absl::big_endian::Store64(footer.data(), 3);
  auto snapshot =
      framed.substr(0, header_size) + Batch(1, 5, Version(1)) + Batch(2, 6, Members()) + footer;
  auto writer = log_->BeginSnapshot(cut, snapshot.size()).value();
  ASSERT_TRUE(writer->Append(0, snapshot).ok());
  ASSERT_TRUE(log_->InstallSnapshot(writer.get(), 0).ok());
  ASSERT_TRUE(history_.Update(*log_).ok());
  EXPECT_EQ(history_.LastVoterOffset(), 7);
  EXPECT_EQ(history_.KraftVersionAt(6), 0);
  EXPECT_EQ(history_.KraftVersionAt(7), 1);
  ASSERT_NE(history_.LatestVoters(), nullptr);
  EXPECT_EQ(*history_.LatestVoters(), members_);
  ASSERT_TRUE(log_->Append(Batch(8, 6, Members()), 2).ok());
  ASSERT_TRUE(history_.Update(*log_).ok());
  EXPECT_EQ(history_.LastVoterOffset(), 8);
}

TEST_F(MembershipHistoryTest, RejectsBadControlBatchBeforePublishingAnyMembershipChange) {
  string version = Version(1), broken = Members();
  broken.pop_back();
  array<ControlRecordValue, 2> changes{{{5, version}, {6, broken}}};
  ASSERT_TRUE(log_->Append(MakeControlBatch(0, 2, 123, changes).value(), 2).ok());
  EXPECT_FALSE(history_.Update(*log_).ok());
  EXPECT_EQ(history_.next_offset(), 0);
  EXPECT_FALSE(history_.LatestVoters());
  EXPECT_EQ(history_.KraftVersion(), 0);
  ASSERT_TRUE(log_->TruncateTo(0, 0).ok());
  ASSERT_TRUE(log_->Append(Batch(0, 5, Version(2)), 2).ok());
  EXPECT_TRUE(absl::IsFailedPrecondition(history_.Update(*log_)));
}

}  // namespace
}  // namespace dfly::cluster::raft
