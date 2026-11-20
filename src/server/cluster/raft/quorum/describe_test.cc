// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/describe.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/quorum/fetch_replication.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

constexpr ReplicaKey kLeader{1, {1}}, kFollower{2, {2}};
constexpr Uuid kTopicId{42};
constexpr int64_t kWall = 1'700'000'000'000;
using Partition = DescribeQuorumResponse::TopicData::PartitionData;

class DescribeQuorumTest : public testing::Test {
 protected:
  void SetUp() override {
    string tmp = (filesystem::temp_directory_path() / "dfly-describe-XXXXXX").string();
    ASSERT_NE(mkdtemp(tmp.data()), nullptr);
    dir_ = std::move(tmp);
    auto log = PartitionLog::Open(dir_);
    ASSERT_TRUE(log.ok()) << log.status();
    log_ = *std::move(log);
  }

  void TearDown() override {
    fetch_.reset();
    log_.reset();
    filesystem::remove_all(dir_);
  }

  void Start(bool leader, bool static_voters = false) {
    auto follower = static_voters ? ReplicaKey{kFollower.id} : kFollower;
    vector<CampaignVoter> voters{{static_voters ? ReplicaKey{kLeader.id} : kLeader, {"l", 9093}},
                                 {follower, {"f", 9094}}};
    VoteTarget target{"cluster", "__cluster_metadata", 0, kLeader};
    auto persist = [](const ElectionState&) { return error_code{}; };
    campaign_ = make_unique<ElectionCampaign>(target, voters, persist);
    epochs_ = make_unique<EpochControl>(*campaign_, "CONTROLLER", 500, 1000);
    fetch_ = make_unique<FetchReplication>(*campaign_, *epochs_, *log_, kTopicId);
    ElectionState state;
    state.epoch = 2;
    if (leader)
      state.voted_key = kLeader;
    ASSERT_FALSE(campaign_->Recover(state, {}));
    if (leader) {
      target.local_replica = kFollower;
      ElectionCampaign peer(target, voters, persist);
      ASSERT_FALSE(peer.Recover({}, {}));
      auto vote = campaign_->PrepareVote(follower, {});
      ASSERT_TRUE(vote);
      VoteResponse response;
      ASSERT_FALSE(peer.ReceiveVote(vote->request, 2, {}, &response));
      ASSERT_FALSE(campaign_->ReceiveVoteResponse(kFollower, vote->sequence, response));
      ASSERT_EQ(campaign_->state().role, QuorumRole::kLeader);
    }
    ASSERT_TRUE(fetch_->SyncRole(kWall).ok());
    auto& topic = request_.topics.emplace_back();
    topic.topic_name = target.topic;
    topic.partitions.emplace_back().partition_index = target.partition;
  }

  DescribeQuorumResponse Describe(int64_t wall_ms, int16_t version = 2) {
    DescribeQuorumResponse response, decoded;
    EXPECT_FALSE(DescribeQuorum(*campaign_, *fetch_, version, request_, wall_ms, &response));
    string bytes;
    EXPECT_EQ(Encode(response, version, &bytes), CodecError::kOk);
    EXPECT_EQ(Decode(bytes, version, &decoded), CodecError::kOk);
    return decoded;
  }

  void Fetch(ReplicaKey peer, int64_t offset, int64_t wall_ms) {
    FetchRequest request;
    request.replica_state.replica_id = peer.id;
    request.max_bytes = 8192;
    auto& topic = request.topics.emplace_back();
    topic.topic_id = kTopicId;
    auto& part = topic.partitions.emplace_back();
    part.replica_directory_id = peer.directory_id;
    part.fetch_offset = offset;
    part.current_leader_epoch = campaign_->state().election.epoch;
    part.last_fetched_epoch = offset ? part.current_leader_epoch : 0;
    FetchResponse response;
    bool wait;
    auto status = fetch_->HandleFetch(request, 18, ++monotonic_, wall_ms, &response, &wait);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(response.responses[0].partitions[0].error_code, 0);
  }

  void Append(int count) {
    const auto epoch = campaign_->state().election.epoch;
    vector<string> records(count, "data");
    auto bytes = MakeDataBatch(log_->End().end_offset, epoch, kWall, records);
    ASSERT_TRUE(bytes.ok());
    ASSERT_TRUE(fetch_->Append(*bytes, epoch).ok());
  }

  static Partition PartitionOf(const DescribeQuorumResponse& response) {
    return response.topics.at(0).partitions.at(0);
  }

  filesystem::path dir_;
  unique_ptr<PartitionLog> log_;
  unique_ptr<ElectionCampaign> campaign_;
  unique_ptr<EpochControl> epochs_;
  unique_ptr<FetchReplication> fetch_;
  DescribeQuorumRequest request_;
  int64_t monotonic_ = 100;
};

TEST_F(DescribeQuorumTest, NonLeaderAndUnknownRequestsUsePartitionErrors) {
  Start(false);
  auto response = Describe(kWall);
  EXPECT_EQ(response.error_code, 0);
  auto partition = PartitionOf(response);
  EXPECT_EQ(partition.error_code, 6);
  EXPECT_NE(partition.error_message, "");
  EXPECT_EQ(partition.leader_id, 0);
  EXPECT_EQ(partition.high_watermark, 0);
  EXPECT_TRUE(partition.current_voters.empty());
  EXPECT_TRUE(response.nodes.empty());

  auto& another = request_.topics.emplace_back();
  another.topic_name = "unknown";
  another.partitions.emplace_back().partition_index = 20;
  another.partitions.emplace_back().partition_index = 30;
  response = Describe(kWall);
  ASSERT_EQ(response.topics.size(), 2u);
  EXPECT_EQ(PartitionOf(response).error_code, 3);
  EXPECT_EQ(response.topics[1].topic_name, "unknown");
  ASSERT_EQ(response.topics[1].partitions.size(), 2u);
  EXPECT_EQ(response.topics[1].partitions[1].partition_index, 30);
  EXPECT_EQ(response.topics[1].partitions[1].error_message,
            "This server does not host this topic-partition.");
  request_.topics.clear();
  EXPECT_TRUE(Describe(kWall).topics.empty());
}

TEST_F(DescribeQuorumTest, StaticVotersUseNoDirectoryAndVersionedTimestampsAndEndpoints) {
  Start(true, true);
  for (int16_t version : {0, 1, 2}) {
    auto response = Describe(kWall, version);
    auto partition = PartitionOf(response);
    ASSERT_EQ(partition.current_voters.size(), 2u);
    const auto& leader = partition.current_voters[0];
    EXPECT_EQ(partition.leader_epoch, 2);
    EXPECT_EQ(partition.high_watermark, -1);
    EXPECT_EQ(leader.log_end_offset, 1);
    EXPECT_EQ(leader.replica_directory_id, Uuid{});
    EXPECT_EQ(leader.last_fetch_timestamp, version ? kWall : -1);
    EXPECT_EQ(leader.last_caught_up_timestamp, version ? kWall : -1);
    EXPECT_EQ(partition.current_voters[1].log_end_offset, -1);
    EXPECT_EQ(partition.current_voters[1].last_fetch_timestamp, -1);
    ASSERT_EQ(response.nodes.size(), version >= 2 ? 2u : 0u);
    if (version >= 2) {
      const auto& listener = response.nodes[1].listeners.at(0);
      EXPECT_EQ(listener.name, "CONTROLLER");
      EXPECT_EQ(listener.host, "f");
      EXPECT_EQ(listener.port, 9094);
    }
  }

  Fetch(kFollower, 1, kWall + 10);
  auto partition = PartitionOf(Describe(kWall + 20));
  EXPECT_EQ(partition.high_watermark, 1);
  EXPECT_EQ(partition.current_voters[1].replica_directory_id, Uuid{});
  EXPECT_EQ(partition.current_voters[1].last_caught_up_timestamp, kWall + 10);
  EXPECT_TRUE(partition.observers.empty());
}

TEST_F(DescribeQuorumTest, FollowerTimesUsePreviousLeaderEndAndRegressingFetchDoesNotRewindHW) {
  Start(true);
  Fetch(kFollower, 0, kWall + 10);
  Append(2);
  Fetch(kFollower, 1, kWall + 20);
  auto partition = PartitionOf(Describe(kWall + 30));
  EXPECT_EQ(partition.high_watermark, 1);
  EXPECT_EQ(partition.current_voters[1].last_caught_up_timestamp, kWall + 10);
  EXPECT_EQ(partition.current_voters[1].last_fetch_timestamp, kWall + 20);
  EXPECT_EQ(partition.current_voters[0].log_end_offset, 3);
  Fetch(kFollower, 3, kWall + 40);
  Fetch(kFollower, 2, kWall + 50);
  partition = PartitionOf(Describe(kWall + 60));
  EXPECT_EQ(partition.high_watermark, 3);
  EXPECT_EQ(partition.current_voters[1].log_end_offset, 1);
  EXPECT_EQ(partition.current_voters[1].last_caught_up_timestamp, kWall + 40);
  EXPECT_EQ(partition.current_voters[1].last_fetch_timestamp, kWall + 50);
  ASSERT_FALSE(epochs_->Resign(1000));
  EXPECT_EQ(PartitionOf(Describe(kWall + 60)).error_code, 6);
}

TEST_F(DescribeQuorumTest, ObserverDirectoriesExpireSeparatelyAndNeverVote) {
  Start(true);
  ReplicaKey newer{kFollower.id, {99}};
  Fetch(newer, 1, kWall + 10);
  Fetch({3, {3}}, 0, kWall + 20);
  Fetch({-1}, 0, kWall + 25);
  auto response = Describe(kWall + 30);
  auto partition = PartitionOf(response);
  EXPECT_EQ(partition.high_watermark, -1);
  EXPECT_EQ(partition.current_voters[1].replica_directory_id, kFollower.directory_id);
  EXPECT_EQ(partition.current_voters[1].log_end_offset, -1);
  ASSERT_EQ(partition.observers.size(), 2u);
  EXPECT_EQ(partition.observers[0].replica_directory_id, newer.directory_id);
  EXPECT_EQ(partition.observers[0].last_caught_up_timestamp, kWall + 10);
  EXPECT_EQ(partition.observers[1].last_caught_up_timestamp, -1);
  EXPECT_EQ(response.nodes.size(), 2u);
  partition = PartitionOf(Describe(kWall + 300010));
  ASSERT_EQ(partition.observers.size(), 1u);
  EXPECT_EQ(partition.observers[0].replica_id, 3);
  EXPECT_TRUE(PartitionOf(Describe(kWall + 300020)).observers.empty());
}

}  // namespace
}  // namespace dfly::cluster::raft
