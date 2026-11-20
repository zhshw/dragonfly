// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/voter_changes.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;
namespace fs = filesystem;
constexpr ReplicaKey kA{1, {1}}, kB{2, {2}};
constexpr Uuid kTopic{17};
const CampaignVoter kLocal{kA, {"a", 9000}};

class VoterChangesTest : public testing::Test {
 protected:
  void SetUp() override {
    string temp = (fs::temp_directory_path() / "dfly-voter-change-XXXXXX").string();
    ASSERT_NE(mkdtemp(temp.data()), nullptr);
    dir_ = temp;
    log_ = PartitionLog::Open(dir_).value();
    fetch_ = make_unique<FetchReplication>(campaign_, epochs_, *log_, kTopic);
    changes_ = make_unique<VoterChanges>(campaign_, epochs_, *fetch_, 500);
  }

  void Lead(bool dynamic = true) {
    if (dynamic) {
      KRaftVersionRecord level;
      level.k_raft_version = 1;
      VotersRecord voters;
      auto& voter = voters.voters.emplace_back();
      voter.voter_id = kA.id;
      voter.voter_directory_id = kA.directory_id;
      voter.endpoints.push_back({"RAFT", "a", 9000, {}});
      voter.k_raft_version_feature.max_supported_version = 1;
      string first, second;
      ASSERT_EQ(Encode(level, 0, &first), CodecError::kOk);
      ASSERT_EQ(Encode(voters, 0, &second), CodecError::kOk);
      array<ControlRecordValue, 2> controls{{{5, first}, {6, second}}};
      ASSERT_TRUE(log_->Append(MakeControlBatch(0, 1, 100, controls).value(), 1).ok());
    }
    ASSERT_TRUE(fetch_->RefreshMembership().ok());
    ASSERT_FALSE(campaign_.Recover({}, log_->End()));
    ASSERT_TRUE(fetch_->SyncRole(100).ok());
  }

  void TearDown() override {
    changes_.reset();
    fetch_.reset();
    log_.reset();
    if (!dir_.empty())
      fs::remove_all(dir_);
  }

  void Report(ReplicaKey key) {
    FetchRequest request;
    request.replica_state.replica_id = key.id;
    auto& topic = request.topics.emplace_back();
    topic.topic_id = kTopic;
    auto& part = topic.partitions.emplace_back();
    part.current_leader_epoch = campaign_.state().election.epoch;
    part.last_fetched_epoch = log_->End().epoch;
    part.fetch_offset = log_->End().end_offset;
    part.replica_directory_id = key.directory_id;
    FetchResponse response;
    bool can_wait;
    ASSERT_TRUE(fetch_->HandleFetch(request, 18, 10, 1200, &response, &can_wait).ok());
    ASSERT_EQ(response.responses[0].partitions[0].error_code, 0);
  }

  AddRaftVoterRequest Add(bool wait) {
    AddRaftVoterRequest add;
    add.cluster_id = "c";
    add.voter_id = kB.id;
    add.voter_directory_id = kB.directory_id;
    add.listeners.push_back({"raft", "b", 9001, {}});
    add.ack_when_committed = wait;
    return add;
  }

  ApiVersionsResponse Versions() {
    ApiVersionsResponse response;
    response.supported_features.push_back({"kraft.version", 0, 1, {}});
    return response;
  }

  void ExchangeVersions() {
    auto probe = changes_->TakeProbe();
    ASSERT_TRUE(probe);
    EXPECT_FALSE(changes_->TakeProbe());
    ASSERT_TRUE(changes_->OnApiVersions(*probe, Versions(), 20, 1500).ok());
  }

  fs::path dir_;
  unique_ptr<PartitionLog> log_;
  ElectionCampaign campaign_{
      {"c", "topic", 0, kA}, {kLocal}, [](const auto&) { return error_code{}; }};
  EpochControl epochs_{campaign_, "RAFT", 300, 300};
  unique_ptr<FetchReplication> fetch_;
  unique_ptr<VoterChanges> changes_;
};

TEST_F(VoterChangesTest, AddAckWhenCommittedBlocksOtherOperationsUntilNewMajority) {
  Lead();
  Report(kB);
  auto add = Add(true);
  auto result = changes_->Begin(add, 10, 1400);
  EXPECT_FALSE(result->error);
  EXPECT_EQ(changes_->Begin(add, 11, 1401)->error, 7);
  ExchangeVersions();
  EXPECT_EQ(campaign_.voters().size(), 2u);
  EXPECT_FALSE(result->error);
  Report(kB);
  changes_->Poll(31);
  EXPECT_EQ(result->error, 0);
  EXPECT_EQ(changes_->Begin(add, 32, 1420)->error, 126);
}

TEST_F(VoterChangesTest, EarlyAddAckStillRequiresNewMajorityToStartRemoval) {
  Lead();
  Report(kB);
  auto result = changes_->Begin(Add(false), 10, 1400);
  ExchangeVersions();
  EXPECT_EQ(result->error, 0);
  RemoveRaftVoterRequest remove;
  remove.cluster_id = "c";
  remove.voter_id = kA.id;
  remove.voter_directory_id = kA.directory_id;
  EXPECT_EQ(changes_->Begin(remove, 25, 1501)->error, 7);
  Report(kB);
  auto departed = changes_->Begin(remove, 35, 1600);
  EXPECT_FALSE(departed->error);
  ASSERT_EQ(campaign_.voters().size(), 1u);
  EXPECT_EQ(campaign_.state().role, QuorumRole::kLeader);
  Report(kB);
  changes_->Poll(45);
  EXPECT_EQ(departed->error, 0);
  EXPECT_EQ(campaign_.state().role, QuorumRole::kResigned);
}

TEST_F(VoterChangesTest, ProbeRequiresMatchingFeatureAndRecentObserverIdentity) {
  Lead();
  auto add = Add(true);
  auto r = changes_->Begin(add, 0, 1400);
  auto probe = changes_->TakeProbe().value();
  auto other = probe;
  other.key.directory_id = {9};
  ASSERT_TRUE(changes_->OnApiVersions(other, Versions(), 5, 1405).ok());
  EXPECT_FALSE(r->error);
  ASSERT_TRUE(changes_->OnApiVersions(probe, {}, 6, 1406).ok());
  EXPECT_EQ(r->error, 42);
  Report({kB.id, {8}});
  r = changes_->Begin(add, 10, 1410);
  ExchangeVersions();
  EXPECT_EQ(r->error, 7);
  r = changes_->Begin(add, 21, 1421);
  probe = changes_->TakeProbe().value();
  changes_->Poll(521);
  EXPECT_EQ(r->error, 7);
  ASSERT_TRUE(changes_->OnApiVersions(probe, Versions(), 522, 1999).ok());
  EXPECT_EQ(campaign_.voters().size(), 1u);
}

TEST_F(VoterChangesTest, UpdateChecksEpochIdentityAndNegotiatedFeatureBeforeAppending) {
  Lead();
  Report(kB);
  auto added = changes_->Begin(Add(true), 1, 1001);
  ExchangeVersions();
  Report(kB);
  changes_->Poll(21);
  ASSERT_EQ(added->error, 0);
  UpdateRaftVoterRequest update;
  update.cluster_id = "c";
  update.current_leader_epoch = campaign_.state().election.epoch - 1;
  update.voter_id = kB.id;
  update.voter_directory_id = kB.directory_id;
  update.listeners.push_back({"RAFT", "b-2", 9002, {}});
  EXPECT_EQ(changes_->Begin(update, 31, 1010)->error, 74);
  update.current_leader_epoch++;
  auto invalid = changes_->Begin(update, 32, 1015);
  auto probe = changes_->TakeProbe().value();
  EXPECT_EQ(probe.endpoint.host, "b-2");
  ASSERT_TRUE(changes_->OnApiVersions(probe, Versions(), 33, 1020).ok());
  EXPECT_EQ(invalid->error, 42);
  update.k_raft_version_feature.max_supported_version = 1;
  auto accepted = changes_->Begin(update, 35, 1025);
  probe = changes_->TakeProbe().value();
  ASSERT_TRUE(changes_->OnApiVersions(probe, Versions(), 36, 1030).ok());
  EXPECT_EQ(accepted->error, 0);
  EXPECT_EQ(campaign_.voters()[1].endpoint.port, 9002);
  EXPECT_EQ(changes_->Begin(update, 37, 1035)->error, 7);
}

TEST_F(VoterChangesTest, LastVoterCannotBeRemovedAndStaticQuorumRemainsStatic) {
  Lead(false);
  EXPECT_EQ(changes_->Begin(Add(false), 0, 1000)->error, 35);
  RemoveRaftVoterRequest remove;
  remove.cluster_id = "c";
  remove.voter_id = kA.id;
  remove.voter_directory_id = kA.directory_id;
  EXPECT_EQ(changes_->Begin(remove, 1, 1000)->error, 35);
  UpdateRaftVoterRequest req;
  req.cluster_id = "c";
  req.current_leader_epoch = campaign_.state().election.epoch;
  req.voter_id = kA.id;
  req.voter_directory_id = kA.directory_id;
  req.listeners.push_back({"RAFT", "other", 9001, {}});
  req.k_raft_version_feature.max_supported_version = 1;
  auto result = changes_->Begin(req, 2, 1002);
  auto probe = changes_->TakeProbe().value();
  ASSERT_TRUE(changes_->OnApiVersions(probe, Versions(), 3, 1003).ok());
  EXPECT_EQ(result->error, 0);
  EXPECT_EQ(fetch_->LogEndOffset(), 1);
  EXPECT_EQ(campaign_.voters()[0].endpoint.host, "a");
}

TEST_F(VoterChangesTest, RemovalRequiresExactDirectoryAndOneRemainingVoter) {
  Lead();
  RemoveRaftVoterRequest remove;
  remove.cluster_id = "c";
  remove.voter_id = kA.id;
  remove.voter_directory_id = kA.directory_id;
  EXPECT_EQ(changes_->Begin(remove, 1, 1000)->error, 127);
  remove.voter_id = kB.id;
  EXPECT_EQ(changes_->Begin(remove, 2, 1000)->error, 127);
  remove.cluster_id = "other";
  EXPECT_EQ(changes_->Begin(remove, 3, 1000)->error, 104);
}

}  // namespace
}  // namespace dfly::cluster::raft
