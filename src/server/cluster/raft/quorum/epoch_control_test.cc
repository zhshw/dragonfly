// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/epoch_control.h"

#include <gtest/gtest.h>

namespace dfly::cluster::raft {
namespace {

using namespace std;
using namespace protocol;

constexpr ReplicaKey kA{1, {1}}, kB{2, {2}}, kC{3, {3}}, kD{4, {4}};
const vector<CampaignVoter> kVoters{
    {kA, {"node-a", 9101}}, {kB, {"node-b", 9102}}, {kC, {"node-c", 9103}}, {kD, {"node-d", 9104}}};
const LogPosition kEnd{4, 12};

class EpochControlTest : public testing::Test {
 protected:
  ElectionCampaign Make(ReplicaKey local, vector<CampaignVoter> voters = kVoters) {
    return ElectionCampaign({"c", "data", 10, local}, std::move(voters),
                            [](const ElectionState&) { return error_code{}; });
  }

  void Elect(ElectionCampaign* a) {
    ASSERT_FALSE(a->Recover({5, {}, kA, {}}, kEnd));
    for (auto dest : {kB, kC}) {
      auto follower = Make(dest);
      ASSERT_FALSE(follower.Recover({}, kEnd));
      auto req = a->PrepareVote(dest, kEnd);
      ASSERT_TRUE(req);
      VoteResponse response;
      ASSERT_FALSE(follower.ReceiveVote(req->request, 2, kEnd, &response));
      ASSERT_FALSE(a->ReceiveVoteResponse(dest, req->sequence, response));
    }
    ASSERT_EQ(a->state().role, QuorumRole::kLeader);
  }
};

TEST_F(EpochControlTest, BeginNotifiesNewLeaderAndRepeatsUntilFetch) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a);
  ASSERT_FALSE(b.Recover({5, {}, kB, {}}, kEnd));
  EpochControl leader(a, "CONTROLLER", 100, 1000), peer(b, "CONTROLLER", 100, 1000);
  vector<EpochNotification> messages;
  ASSERT_FALSE(leader.Poll(1000, &messages));
  ASSERT_EQ(messages.size(), 3u);
  auto begin = get<BeginQuorumEpochRequest>(messages[0].request);
  for (int16_t version : {0, 1}) {
    string wire;
    ASSERT_EQ(Encode(begin, version, &wire), CodecError::kOk);
    BeginQuorumEpochRequest decoded;
    ASSERT_EQ(Decode(wire, version, &decoded), CodecError::kOk);
    BeginQuorumEpochResponse response;
    ASSERT_FALSE(peer.ReceiveBegin(decoded, version, &response));
    ASSERT_EQ(b.state().role, QuorumRole::kFollower);
    EXPECT_EQ(b.state().election.leader_id, kA.id);
    EXPECT_EQ(b.state().election.voted_key, kB);
    const auto& part = response.topics[0].partitions[0];
    EXPECT_EQ(part.partition_index, 10);
    EXPECT_EQ(part.error_code, 0);
    EXPECT_EQ(part.leader_epoch, 5);
    EXPECT_EQ(response.node_endpoints.size(), version == 0 ? 0u : 1u);
    EXPECT_FALSE(leader.ReceiveBeginResponse(kB, 5, response));
  }
  EXPECT_EQ(leader.UnacknowledgedVoters(), (vector<int32_t>{3, 4}));
  ASSERT_FALSE(leader.Poll(1050, &messages));
  EXPECT_EQ(messages.size(), 3u);
  leader.RecordFetch(kB, 5, 1080, 12);
  ASSERT_FALSE(leader.Poll(1100, &messages));
  ASSERT_EQ(messages.size(), 2u);
  EXPECT_EQ(messages[0].recipient, kC);
}

TEST_F(EpochControlTest, CheckQuorumRequiresFetchFromDistinctConfiguredVoters) {
  auto a = Make(kA);
  Elect(&a);
  EpochControl control(a, "CONTROLLER", 100, 1000);
  vector<EpochNotification> messages;
  ASSERT_FALSE(control.Poll(0, &messages));
  control.RecordFetch(kB, 5, 30, 9);
  control.RecordFetch(kB, 5, 80, 12);
  control.RecordFetch(kA, 5, 110, 12);
  control.RecordFetch(ReplicaKey{8}, 5, 125, 12);
  control.RecordFetch(ReplicaKey{kC.id, {19}}, 5, 127, 12);
  control.RecordFetch(kD, 4, 132, 12);
  EXPECT_FALSE(control.Poll(149, &messages));
  EXPECT_EQ(a.state().role, QuorumRole::kLeader);
  ASSERT_FALSE(control.Poll(150, &messages));
  EXPECT_EQ(a.state().role, QuorumRole::kResigned);
  ASSERT_EQ(messages.size(), 3u);
  for (const auto& item : messages)
    EXPECT_TRUE(holds_alternative<EndQuorumEpochRequest>(item.request));
  ASSERT_FALSE(a.OnElectionTimeout());
  EXPECT_EQ(a.state().role, QuorumRole::kUnattached);
  EXPECT_EQ(a.state().election.epoch, 6);
  EXPECT_FALSE(a.state().election.leader_id);
}

TEST_F(EpochControlTest, MajorityFetchOrSnapshotExtendsQuorumDeadline) {
  auto a = Make(kA);
  Elect(&a);
  EpochControl control(a, "CONTROLLER", 100, 1000);
  vector<EpochNotification> messages;
  ASSERT_FALSE(control.Poll(100, &messages));
  control.RecordFetch(kB, 5, 200, 12);
  control.RecordFetch(kC, 5, 245, nullopt);
  ASSERT_FALSE(control.Poll(394, &messages));
  EXPECT_EQ(a.state().role, QuorumRole::kLeader);
  ASSERT_FALSE(control.Poll(395, &messages));
  EXPECT_EQ(a.state().role, QuorumRole::kResigned);
  auto one = Make(kA, {kVoters[0]});
  ASSERT_FALSE(one.Recover({}, kEnd));
  EpochControl only(one, "CONTROLLER", 100, 1000);
  ASSERT_FALSE(only.Poll(0, &messages));
  ASSERT_FALSE(only.Poll(100000, &messages));
  EXPECT_EQ(one.state().role, QuorumRole::kLeader);
  EXPECT_TRUE(messages.empty());
}

TEST_F(EpochControlTest, ResignationOrdersSuccessorsAndRetriesOnlyMissingAcks) {
  auto a = Make(kA), b = Make(kB), c = Make(kC);
  Elect(&a);
  ASSERT_FALSE(b.Recover({5, kA.id, {}, {}}, kEnd));
  ASSERT_FALSE(c.Recover({5, kA.id, {}, {}}, kEnd));
  EpochControl leader(a, "CONTROLLER", 100, 1000), second(b, "CONTROLLER", 100, 1000),
      first(c, "CONTROLLER", 100, 1000);
  vector<EpochNotification> messages;
  ASSERT_FALSE(leader.Poll(0, &messages));
  leader.RecordFetch(kB, 5, 10, 10);
  leader.RecordFetch(kC, 5, 14, 12);
  ASSERT_FALSE(leader.Resign(20));
  ASSERT_FALSE(leader.Poll(20, &messages));
  ASSERT_EQ(messages.size(), 3u);
  auto end = get<EndQuorumEpochRequest>(messages[0].request);
  EXPECT_EQ(end.topics[0].partitions[0].preferred_successors, (vector<int32_t>{3, 2, 4}));
  for (int16_t version : {0, 1}) {
    string bytes;
    ASSERT_EQ(Encode(end, version, &bytes), CodecError::kOk);
    EndQuorumEpochRequest decoded;
    ASSERT_EQ(Decode(bytes, version, &decoded), CodecError::kOk);
    optional<int32_t> delay;
    EndQuorumEpochResponse response;
    ASSERT_FALSE(second.ReceiveEnd(decoded, version, &response, &delay));
    EXPECT_EQ(delay, 250);
    ASSERT_FALSE(leader.ReceiveEndResponse(kB, 5, response));
    ASSERT_FALSE(first.ReceiveEnd(decoded, version, &response, &delay));
    EXPECT_EQ(delay, 0);
  }
  ASSERT_FALSE(leader.Poll(25, &messages));
  ASSERT_EQ(messages.size(), 2u);
  EXPECT_EQ(messages[0].recipient, kC);
  EXPECT_EQ(messages[1].recipient, kD);
}

TEST_F(EpochControlTest, ValidatesPartitionAndDestinationInKafkaOrder) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a);
  ASSERT_FALSE(b.Recover({}, kEnd));
  EpochControl leader(a, "CONTROLLER", 100, 1000), peer(b, "CONTROLLER", 100, 1000);
  vector<EpochNotification> messages;
  ASSERT_FALSE(leader.Poll(0, &messages));
  auto begin = get<BeginQuorumEpochRequest>(messages[0].request);
  begin.cluster_id = "other";
  BeginQuorumEpochResponse response;
  ASSERT_FALSE(peer.ReceiveBegin(begin, 1, &response));
  EXPECT_EQ(response.error_code, 104);
  EXPECT_TRUE(response.topics.empty());
  begin.cluster_id.reset();
  begin.topics[0].partitions[0].partition_index = 20;
  ASSERT_FALSE(peer.ReceiveBegin(begin, 1, &response));
  EXPECT_EQ(response.error_code, 42);
  EXPECT_TRUE(response.topics.empty());
  EXPECT_EQ(b.state().election.epoch, 4);
  begin.topics[0].partitions[0].partition_index = 10;
  begin.voter_id = kC.id;
  ASSERT_FALSE(peer.ReceiveBegin(begin, 1, &response));
  EXPECT_EQ(response.topics[0].partitions[0].error_code, 125);
  EXPECT_EQ(b.state().election.leader_id, kA.id);
  EXPECT_EQ(b.state().election.epoch, 5);
  begin.topics[0].partitions[0].leader_epoch = 3;
  ASSERT_FALSE(peer.ReceiveBegin(begin, 1, &response));
  EXPECT_EQ(response.topics[0].partitions[0].error_code, 74);
}

}  // namespace
}  // namespace dfly::cluster::raft
