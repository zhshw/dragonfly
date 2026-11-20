// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/broker_control.h"

#include <gtest/gtest.h>

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

class BrokerControlTest : public testing::Test {
 protected:
  void SetUp() override {
    image_.features = {{"metadata.version", 33}, {"dragonfly.cluster", 1}};
    auto& reserved = image_.brokers[3];
    reserved.broker_id = 3;
    reserved.end_points.push_back({"RAFT", "node", 7100, 0, {}});
    reserved.end_points.push_back({"RESP", "node", 6379, 0, {}});
    ASSERT_TRUE(image_.directory.Replay(RedisNodeRecord{3, "A-r1", {}}, image_.brokers).ok());
    ASSERT_TRUE(image_.directory.Replay(DataGroupRecord{10, {3}, {}}).ok());
    image_.registration_offsets[3] = 1;
    request_.broker_id = 3;
    request_.cluster_id = "c";
    request_.incarnation_id.front() = 2;
    for (const auto& [name, level] : image_.features)
      request_.features.push_back({name, level, level, {}});
    for (const auto& e : reserved.end_points)
      request_.listeners.push_back({e.name, e.host, e.port, e.security_protocol, {}});
    request_.log_dirs.emplace_back().front() = 1;
    control_.Activate(image_, 0);
  }
  void Register() {
    auto records = control_.Register(image_, "c", 10, 90, request_, &registered_);
    ASSERT_EQ(registered_.error_code, 0);
    ASSERT_EQ(registered_.broker_epoch, 10);
    ASSERT_EQ(records.size(), 1u);
    image_.brokers[3] = get<RegisterBrokerRecord>(records[0].message);
    image_.registration_offsets[3] = 10;
    beat_.broker_epoch = 10;
    beat_.broker_id = 3;
  }
  MetadataImage image_;
  BrokerControl control_{100};
  BrokerRegistrationRequest request_;
  BrokerRegistrationResponse registered_;
  BrokerHeartbeatRequest beat_;
  BrokerHeartbeatResponse reply_;
};

TEST_F(BrokerControlTest, ValidateIncarnationFeaturesAndDirectoryBeforeEpochAssignment) {
  auto bad = request_;
  bad.cluster_id = "other";
  EXPECT_TRUE(control_.Register(image_, "c", 5, 0, bad, &registered_).empty());
  EXPECT_EQ(registered_.error_code, 104);
  bad = request_;
  bad.features.clear();
  EXPECT_TRUE(control_.Register(image_, "c", 5, 0, bad, &registered_).empty());
  EXPECT_EQ(registered_.error_code, 119);
  bad = request_;
  bad.features[0].max_supported_version = 0;
  EXPECT_TRUE(control_.Register(image_, "c", 5, 0, bad, &registered_).empty());
  EXPECT_EQ(registered_.error_code, 35);
  bad = request_;
  bad.log_dirs.front() = {};
  EXPECT_TRUE(control_.Register(image_, "c", 5, 0, bad, &registered_).empty());
  EXPECT_EQ(registered_.error_code, 119);
  bad = request_;
  bad.listeners.front().host = "other";
  EXPECT_TRUE(control_.Register(image_, "c", 5, 0, bad, &registered_).empty());
  EXPECT_EQ(registered_.error_code, 119);
}

TEST_F(BrokerControlTest, CatchUpDuplicateProcessExpiryAndControlledShutdown) {
  Register();
  EXPECT_TRUE(image_.brokers[3].fenced);
  beat_.current_metadata_offset = 9;
  EXPECT_TRUE(control_.Heartbeat(image_, 100, beat_, &reply_).empty());
  EXPECT_FALSE(reply_.is_caught_up);
  EXPECT_TRUE(reply_.is_fenced);
  beat_.current_metadata_offset = 10;
  auto records = control_.Heartbeat(image_, 110, beat_, &reply_);
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(get<BrokerRegistrationChangeRecord>(records[0].message).fenced, -1);
  EXPECT_FALSE(reply_.is_fenced);
  image_.brokers[3].fenced = false;
  auto other = request_;
  other.incarnation_id.front()++;
  EXPECT_TRUE(control_.Register(image_, "c", 11, 120, other, &registered_).empty());
  EXPECT_EQ(registered_.error_code, 101);
  EXPECT_FALSE(control_.Expire(image_, 209));
  ASSERT_TRUE(control_.Expire(image_, 210));
  EXPECT_EQ(get<BrokerRegistrationChangeRecord>(control_.Expire(image_, 210)->message).fenced, 1);
  records = control_.Register(image_, "c", 11, 210, other, &registered_);
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(registered_.broker_epoch, 11);
  EXPECT_TRUE(get<RegisterBrokerRecord>(records[0].message).fenced);
  records = control_.Register(image_, "c", 11, 210, request_, &registered_);
  EXPECT_EQ(registered_.broker_epoch, 10);
  EXPECT_FALSE(get<RegisterBrokerRecord>(records.at(0).message).fenced);
  beat_.broker_epoch = 9;
  EXPECT_TRUE(control_.Heartbeat(image_, 211, beat_, &reply_).empty());
  EXPECT_EQ(reply_.error_code, 77);

  GroupLeaderRecord leadership;
  leadership.group_id = 10;
  leadership.reporter_id = leadership.leader_id = 3;
  leadership.reporter_epoch = 10;
  ASSERT_TRUE(image_.directory.Replay(leadership, image_.brokers).ok());
  beat_.broker_epoch = 10;
  beat_.want_shut_down = true;
  records = control_.Heartbeat(image_, 212, beat_, &reply_);
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(get<BrokerRegistrationChangeRecord>(records[0].message).in_controlled_shutdown, 1);
  EXPECT_FALSE(reply_.should_shut_down);
  EXPECT_FALSE(reply_.is_fenced);
}

}  // namespace
}  // namespace dfly::cluster::raft
