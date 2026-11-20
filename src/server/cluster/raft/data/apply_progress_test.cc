// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/apply_progress.h"

#include <gtest/gtest.h>

namespace dfly::cluster::raft {
namespace {

TEST(DataApplyProgressTest, FlexibleFetchCarriesAppliedIndependentlyOfDurableOffset) {
  protocol::FetchRequest request;
  auto& partition = request.topics.emplace_back().partitions.emplace_back();
  partition.fetch_offset = 21;
  partition.current_leader_epoch = 5;
  DataApplyProgress::Advertise(7, &partition);
  std::string wire;
  ASSERT_EQ(protocol::Encode(request, 17, &wire), protocol::CodecError::kOk);
  protocol::FetchRequest decoded;
  ASSERT_EQ(protocol::Decode(wire, 17, &decoded), protocol::CodecError::kOk);
  auto& peer = decoded.topics.front().partitions.front();
  DataApplyProgress tracker;
  ReplicaKey voter{2, {3}};
  tracker.Observe(voter, peer, 21, 100);
  EXPECT_EQ(tracker.Applied(voter, 5, 110, 20), 7);
  EXPECT_FALSE(tracker.Applied(voter, 6, 110, 20));
  EXPECT_FALSE(tracker.Applied(ReplicaKey{2, {4}}, 5, 110, 20));
  EXPECT_FALSE(tracker.Applied(voter, 5, 121, 20));
  EXPECT_FALSE(tracker.Applied(voter, 5, 90, 20));
  tracker.Observe(voter, peer, 5, 112);
  EXPECT_FALSE(tracker.Applied(voter, 5, 112, 20));
}

TEST(DataApplyProgressTest, MissingOrInvalidReportCannotConfirmAndRestartMayReportLess) {
  DataApplyProgress tracker;
  DataApplyProgress::FetchPartition partition;
  ReplicaKey voter{1, {9}};
  partition.fetch_offset = 30;
  partition.current_leader_epoch = 2;
  DataApplyProgress::Advertise(28, &partition);
  tracker.Observe(voter, partition, 30, 100);
  DataApplyProgress::Advertise(5, &partition);
  tracker.Observe(voter, partition, 30, 101);
  EXPECT_EQ(tracker.Applied(voter, 2, 101, 1), 5);
  ASSERT_EQ(partition.unknown_tags.size(), 1u);
  partition.unknown_tags.begin()->second[0] = 4;
  tracker.Observe(voter, partition, 30, 102);
  EXPECT_FALSE(tracker.Applied(voter, 2, 102, 1));
  DataApplyProgress::Advertise(50, &partition);
  EXPECT_TRUE(partition.unknown_tags.empty());
  tracker.Observe(voter, partition, 30, 103);
  EXPECT_FALSE(tracker.Applied(voter, 2, 103, 10));
}

}  // namespace
}  // namespace dfly::cluster::raft
