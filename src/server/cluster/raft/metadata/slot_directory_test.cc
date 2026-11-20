// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/slot_directory.h"

#include <gtest/gtest.h>

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

SlotAssignmentRecord Assign(SlotId first, SlotId last, SlotOwner old, int32_t group) {
  SlotAssignmentRecord record;
  record.first_slot = first;
  record.last_slot = last;
  record.expected_group = old.group_id;
  record.expected_generation = old.generation;
  record.group_id = group;
  record.generation = old.generation + 1;
  return record;
}

class SlotDirectoryTest : public testing::Test {
 protected:
  void SetUp() override {
    for (int32_t id : {1, 2, 3, 4}) {
      auto& broker = brokers_[id];
      broker.broker_id = id;
      broker.broker_epoch = 5;
      broker.fenced = false;
      auto& ep = broker.end_points.emplace_back();
      ep.name = "RESP";
      ep.host = "node-" + to_string(id);
      ep.port = 6379;
      RedisNodeRecord node;
      node.broker_id = id;
      node.redis_node_id = ep.host;
      ASSERT_TRUE(directory_.Replay(node, brokers_).ok());
    }
    DataGroupRecord group;
    group.group_id = 10;
    group.members = {1, 2};
    ASSERT_TRUE(directory_.Replay(group).ok());
    group.group_id = 20;
    group.members = {3, 4};
    ASSERT_TRUE(directory_.Replay(group).ok());
  }
  GroupLeaderRecord Report(int32_t group, int32_t leader, int32_t term) {
    GroupLeaderRecord record;
    record.group_id = group;
    record.leader_id = leader;
    record.leader_epoch = term;
    record.reporter_id = leader;
    record.reporter_epoch = 5;
    return record;
  }
  SlotDirectory directory_;
  RegisteredBrokers brokers_;
};

TEST_F(SlotDirectoryTest, Slot42MovesWhile43RetainsGenerationAndExistingRedisIdentity) {
  ASSERT_TRUE(directory_.Replay(Report(10, 1, 1), brokers_).ok());
  ASSERT_TRUE(directory_.Replay(Report(20, 3, 1), brokers_).ok());
  ASSERT_TRUE(directory_.Replay(Assign(0, 8191, {}, 10)).ok());
  EXPECT_FALSE(directory_.HasCompleteCoverage());
  EXPECT_FALSE(directory_.Route(42, brokers_));
  ASSERT_TRUE(directory_.Replay(Assign(8192, 16383, {}, 20)).ok());
  ASSERT_TRUE(directory_.HasCompleteCoverage());
  ASSERT_EQ(directory_.Route(42, brokers_)->node_id, "node-1");
  auto move = Assign(42, 42, {10, 1}, 20);
  EXPECT_TRUE(directory_.Replay(move).ok());
  EXPECT_FALSE(directory_.Replay(move).ok());
  ASSERT_EQ(directory_.Lookup(42), (SlotOwner{20, 2}));
  ASSERT_EQ(directory_.Lookup(43), (SlotOwner{10, 1}));
  EXPECT_EQ(directory_.ranges().size(), 4u);
  EXPECT_EQ(directory_.Route(42, brokers_)->node_id, "node-3");
  ASSERT_TRUE(directory_.Replay(Report(10, 2, 2), brokers_).ok());
  ASSERT_TRUE(directory_.Replay(Report(10, 1, 1), brokers_).ok());
  EXPECT_EQ(directory_.Route(43, brokers_)->node_id, "node-2");
  EXPECT_EQ(directory_.Lookup(43), (SlotOwner{10, 1}));
  brokers_[2].broker_epoch++;
  EXPECT_FALSE(directory_.Route(43, brokers_));
  EXPECT_TRUE(directory_.Route(42, brokers_));
}

TEST_F(SlotDirectoryTest, CompareAndSwapCoversEverySlotWithoutPartialRangeUpdates) {
  ASSERT_TRUE(directory_.Replay(Assign(0, 9, {}, 10)).ok());
  ASSERT_TRUE(directory_.Replay(Assign(10, 19, {}, 10)).ok());
  EXPECT_EQ(directory_.ranges().size(), 2u);
  auto before = directory_;
  EXPECT_FALSE(directory_.Replay(Assign(15, 25, {}, 20)).ok());
  EXPECT_EQ(directory_, before);
  EXPECT_FALSE(directory_.Replay(Assign(0, 20, {10, 1}, 20)).ok());
  EXPECT_EQ(directory_, before);
  EXPECT_FALSE(directory_.Replay(Assign(18, 17, {10, 1}, 20)).ok());
  EXPECT_FALSE(directory_.Replay(Assign(20, 16384, {}, 20)).ok());
  EXPECT_FALSE(directory_.Replay(Assign(25, 28, {}, 30)).ok());
  EXPECT_FALSE(directory_.Lookup(16384));
  EXPECT_FALSE(directory_.Lookup(16383));
}

TEST_F(SlotDirectoryTest, RejectsIdentityReuseAndReportsFromOtherGroupsOrFencedProcesses) {
  RedisNodeRecord duplicate;
  duplicate.broker_id = 3;
  duplicate.redis_node_id = "node-1";
  EXPECT_FALSE(directory_.Replay(duplicate, brokers_).ok());
  DataGroupRecord group;
  group.group_id = 30;
  group.members = {4};
  EXPECT_FALSE(directory_.Replay(group).ok());
  auto conflict = Report(10, 3, 1);
  EXPECT_FALSE(directory_.Replay(conflict, brokers_).ok());
  conflict = Report(10, 2, 1);
  conflict.reporter_epoch = 4;
  EXPECT_FALSE(directory_.Replay(conflict, brokers_).ok());
  brokers_[2].fenced = true;
  EXPECT_FALSE(directory_.Replay(Report(10, 2, 1), brokers_).ok());
  brokers_[2].fenced = false;
  ASSERT_TRUE(directory_.Replay(Report(10, 1, 1), brokers_).ok());
  EXPECT_FALSE(directory_.Replay(Report(10, 2, 1), brokers_).ok());
}

}  // namespace
}  // namespace dfly::cluster::raft
