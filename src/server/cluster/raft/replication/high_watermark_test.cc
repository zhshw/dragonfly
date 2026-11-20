// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/replication/high_watermark.h"

#include <gtest/gtest.h>

#include <array>

namespace dfly::cluster::raft {
namespace {

using EndOffset = std::optional<LogOffsetMetadata>;

EndOffset At(int64_t offset) {
  return LogOffsetMetadata{offset};
}

// The offsets and epoch starts in these examples are from Kafka LeaderStateTest.
TEST(LeaderHighWatermarkTest, SingleVoterCommitsFirstRecordOfItsTerm) {
  LeaderHighWatermark state(15);
  ASSERT_FALSE(state.value());
  EXPECT_FALSE(state.Update(std::array{At(15)}));
  ASSERT_TRUE(state.Update(std::array{At(16)}));
  EXPECT_EQ(state.value(), At(16));
  EXPECT_FALSE(state.Update(std::array{At(16)}));
  ASSERT_TRUE(state.Update(std::array{At(20)}));
  EXPECT_EQ(state.value(), At(20));
}

TEST(LeaderHighWatermarkTest, RefreshesSegmentPositionAtSameOffset) {
  LeaderHighWatermark state(15);
  EndOffset initial = LogOffsetMetadata{16, SegmentPosition{0, 500}};
  EndOffset rolled = LogOffsetMetadata{16, SegmentPosition{16, 0}};
  ASSERT_TRUE(state.Update(std::array{initial}));
  ASSERT_TRUE(state.Update(std::array{rolled}));
  EXPECT_EQ(state.value(), rolled);
  EXPECT_FALSE(state.Update(std::array{rolled}));
}

TEST(LeaderHighWatermarkTest, TwoVotersBothRequired) {
  LeaderHighWatermark state(10);
  EXPECT_FALSE(state.Update(std::array{At(13), EndOffset{}}));
  EXPECT_FALSE(state.Update(std::array{At(13), At(10)}));
  EXPECT_FALSE(state.value());
  ASSERT_TRUE(state.Update(std::array{At(13), At(11)}));
  EXPECT_EQ(state.value(), At(11));
  ASSERT_TRUE(state.Update(std::array{At(13), At(13)}));
  EXPECT_EQ(state.value(), At(13));
}

TEST(LeaderHighWatermarkTest, ThreeVotersFollowMajority) {
  LeaderHighWatermark state(10);
  EXPECT_FALSE(state.Update(std::array{At(15), EndOffset{}, EndOffset{}}));
  EXPECT_FALSE(state.Update(std::array{At(15), At(10), At(10)}));
  EXPECT_TRUE(state.Update(std::array{At(15), At(10), At(15)}));
  EXPECT_FALSE(state.Update(std::array{At(20), At(10), At(15)}));
  EXPECT_EQ(state.value(), At(15));
  EXPECT_TRUE(state.Update(std::array{At(20), At(20), At(15)}));
  EXPECT_FALSE(state.Update(std::array{At(20), At(20), At(20)}));
  EXPECT_EQ(state.value(), At(20));
}

TEST(LeaderHighWatermarkTest, MembershipChangeCanAdvanceButCannotRewindCommit) {
  LeaderHighWatermark state(5);
  ASSERT_TRUE(state.Update(std::array{At(15), At(10)}));
  EXPECT_TRUE(state.Update(std::array{At(15), At(10), At(15)}));
  EXPECT_EQ(state.value(), At(15));
  EXPECT_FALSE(state.Update(std::array{At(16), At(10), At(15), At(10)}));
  EXPECT_FALSE(state.Update(std::array{At(16), At(13), At(16), At(13)}));
  EXPECT_EQ(state.value(), At(15));
  EXPECT_TRUE(state.Update(std::array{At(16), At(13), At(16), At(16)}));
  EXPECT_EQ(state.value(), At(16));
}

}  // namespace
}  // namespace dfly::cluster::raft
