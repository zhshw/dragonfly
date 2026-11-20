// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/vote_handler.h"

#include <gtest/gtest.h>
#include <stdlib.h>

namespace dfly::cluster::raft {
namespace {

class VoteHandlerTest : public testing::Test {
 protected:
  using Request = protocol::VoteRequest;
  using Response = protocol::VoteResponse;

  const VoteTarget target_{"cluster-id", "__cluster_metadata", 0, {1, {9}}};
  const ReplicaKey candidate_{2, {10}};
  const ReplicaKey other_{3, {11}};
  std::vector<ElectionState> writes_;
  PersistElection persist_ = [this](const ElectionState& state) {
    writes_.push_back(state);
    return std::error_code{};
  };
  std::optional<Response::NodeEndpoint> endpoint_;
  LogPosition log_{4, 8};
  QuorumVoteState state_{ElectionState{5, std::nullopt, std::nullopt, {1, 2, 3}}};
  Response response_;

  Request MakeRequest(int32_t epoch, bool pre_vote, int16_t version = 2) const {
    Request request;
    request.cluster_id = target_.cluster_id;
    auto& topic = request.topics.emplace_back();
    topic.topic_name = target_.topic;
    auto& part = topic.partitions.emplace_back();
    part.partition_index = target_.partition;
    part.replica_epoch = epoch;
    part.replica_id = candidate_.id;
    part.last_offset_epoch = log_.epoch;
    part.last_offset = log_.end_offset;
    part.pre_vote = pre_vote;
    if (version >= 1) {
      request.voter_id = target_.local_replica.id;
      part.voter_directory_id = target_.local_replica.directory_id;
      part.replica_directory_id = candidate_.directory_id;
    }
    return request;
  }

  std::error_code Dispatch(const Request& request, int16_t version = 2) {
    return HandleVoteRequest(target_, request, version, log_, endpoint_, persist_, &state_,
                             &response_);
  }

  void ExpectPartition(int32_t epoch, int32_t leader, int16_t error, bool granted) const {
    ASSERT_EQ(response_.error_code, 0);
    ASSERT_EQ(response_.topics.size(), 1u);
    EXPECT_EQ(response_.topics[0].topic_name, target_.topic);
    ASSERT_EQ(response_.topics[0].partitions.size(), 1u);
    const auto& result = response_.topics[0].partitions[0];
    EXPECT_EQ(result.partition_index, target_.partition);
    EXPECT_EQ(result.leader_epoch, epoch);
    EXPECT_EQ(result.leader_id, leader);
    EXPECT_EQ(result.error_code, error);
    EXPECT_EQ(result.vote_granted, granted);
  }
};

TEST_F(VoteHandlerTest, ValidateClusterAndSingletonGroupBeforeAdvancingEpoch) {
  auto request = MakeRequest(9, false);
  for (const char* invalid_id : {"", "another-cluster"}) {
    request.cluster_id = invalid_id;
    ASSERT_FALSE(Dispatch(request));
    EXPECT_EQ(response_.error_code, 104);
    EXPECT_TRUE(response_.topics.empty());
  }
  for (int i = 0; i < 5; ++i) {
    request = MakeRequest(9, false);
    switch (i) {
      case 0:
        request.topics.clear();
        break;
      case 1:
        request.topics.emplace_back();
        break;
      case 2:
        request.topics.front().topic_name = "other";
        break;
      case 3:
        request.topics.front().partitions.emplace_back();
        break;
      case 4:
        request.topics.front().partitions.front().partition_index = 1;
        break;
    }
    ASSERT_FALSE(Dispatch(request));
    EXPECT_EQ(response_.error_code, 42);
    EXPECT_TRUE(response_.topics.empty());
  }
  EXPECT_TRUE(writes_.empty());
  EXPECT_EQ(state_.election.epoch, 5);
  request = MakeRequest(5, true);
  request.cluster_id.reset();
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(5, -1, 0, true);
}

TEST_F(VoteHandlerTest, FencedEpochAndInvalidCandidateLog) {
  state_.role = QuorumRole::kFollower;
  state_.election.leader_id = other_.id;
  endpoint_.emplace();
  endpoint_->node_id = other_.id;
  endpoint_->host = "broker-3";
  endpoint_->port = 9093;
  auto request = MakeRequest(3, true);
  auto& part = request.topics[0].partitions[0];
  part.last_offset_epoch = 3;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(5, 3, 74, false);
  EXPECT_EQ(response_.node_endpoints, (std::vector<Response::NodeEndpoint>{*endpoint_}));
  part.replica_epoch = 6;
  part.last_offset = -1;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(5, 3, 42, false);
  part.last_offset = 9;
  part.last_offset_epoch = -1;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(5, 3, 42, false);
  part.last_offset_epoch = 6;
  part.pre_vote = false;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(5, 3, 42, false);
  part.pre_vote = true;
  part.last_offset_epoch = 7;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(5, 3, 42, false);
  EXPECT_TRUE(writes_.empty());
}

TEST_F(VoteHandlerTest, NewEpochPersistsBeforeDestinationCheck) {
  state_.role = QuorumRole::kLeader;
  state_.election.leader_id = target_.local_replica.id;
  state_.election.voted_key = target_.local_replica;
  auto request = MakeRequest(6, false);
  request.voter_id = 4;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(6, -1, 125, false);
  ASSERT_EQ(writes_.size(), 1u);
  EXPECT_FALSE(writes_[0].voted_key);
  EXPECT_FALSE(writes_[0].leader_id);
  EXPECT_EQ(writes_[0].voters, state_.election.voters);
  EXPECT_EQ(state_.role, QuorumRole::kUnattached);
  request.voter_id = target_.local_replica.id;
  request.topics[0].partitions[0].voter_directory_id = other_.directory_id;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(6, -1, 125, false);
  EXPECT_EQ(writes_.size(), 1u);
}

TEST_F(VoteHandlerTest, PreVoteMayAdvanceEpochWithoutCastingStandardVote) {
  state_.role = QuorumRole::kFollower;
  state_.fetched_from_leader = true;
  state_.election.leader_id = other_.id;
  auto request = MakeRequest(6, true);
  request.topics[0].partitions[0].last_offset_epoch = 6;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(6, -1, 0, true);
  ASSERT_EQ(writes_.size(), 1u);
  EXPECT_FALSE(writes_[0].voted_key);
  EXPECT_FALSE(state_.fetched_from_leader);
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(6, -1, 0, true);
  EXPECT_EQ(writes_.size(), 1u);
}

TEST_F(VoteHandlerTest, NewVotePersistsBeforeGrantAndDuplicateDoesNotWriteAgain) {
  state_.role = QuorumRole::kProspective;
  auto request = MakeRequest(6, false);
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(6, -1, 0, true);
  ASSERT_EQ(writes_.size(), 2u);
  EXPECT_FALSE(writes_[0].voted_key);
  EXPECT_EQ(writes_[1].voted_key, candidate_);
  log_.end_offset += 10;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(6, -1, 0, true);
  request.topics[0].partitions[0].replica_directory_id = other_.directory_id;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(6, -1, 0, false);
  EXPECT_EQ(writes_.size(), 2u);
}

TEST_F(VoteHandlerTest, UncertainWriteFencesQuorumUntilRecovered) {
  for (int fail_at : {1, 2}) {
    auto request = MakeRequest(6, false);
    state_ = QuorumVoteState{ElectionState{5, {}, {}, {1, 2, 3}}};
    int calls = 0;
    persist_ = [&](const ElectionState&) {
      return ++calls == fail_at ? std::make_error_code(std::errc::io_error) : std::error_code{};
    };
    ASSERT_EQ(Dispatch(request), std::errc::io_error);
    EXPECT_TRUE(response_.topics.empty());
    request.topics[0].partitions[0].replica_id = other_.id;
    ASSERT_EQ(Dispatch(request), std::errc::io_error);
    ASSERT_EQ(Dispatch(MakeRequest(7, true)), std::errc::io_error);
    EXPECT_EQ(calls, fail_at);
    EXPECT_TRUE(response_.topics.empty());
  }
}

// Kafka can answer a vote from or to an observer. Only the configured voter set can
// contribute to a nominee's majority. The nonzero partition exercises data-group routing.
TEST_F(VoteHandlerTest, ReportsDataGroupAndOnlyItsOwnVoterSet) {
  const VoteTarget data_group{"cluster-id", "data", 10, target_.local_replica};
  state_.election.voters = {3, 4, 5};
  auto request = MakeRequest(7, false);
  request.topics.front().topic_name = "data";
  request.topics.front().partitions.front().partition_index = 10;
  ASSERT_FALSE(
      HandleVoteRequest(data_group, request, 2, log_, endpoint_, persist_, &state_, &response_));
  ASSERT_EQ(response_.topics.size(), 1u);
  ASSERT_EQ(response_.topics.front().partitions.size(), 1u);
  EXPECT_TRUE(response_.topics.front().partitions.front().vote_granted);
  EXPECT_EQ(response_.topics.front().partitions.front().partition_index, 10);
  EXPECT_EQ(state_.election.voted_key, candidate_);
  EXPECT_EQ(state_.election.voters, (std::set<int32_t>{3, 4, 5}));
}

TEST_F(VoteHandlerTest, RejectMalformedSelfVoteAndPreserveStoredVote) {
  auto request = MakeRequest(6, false);
  auto& part = request.topics[0].partitions[0];
  part.replica_id = -1;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(5, -1, 42, false);
  part.replica_id = target_.local_replica.id;
  ASSERT_FALSE(Dispatch(request));
  ExpectPartition(6, -1, 42, false);
  EXPECT_FALSE(state_.election.voted_key);
  EXPECT_EQ(writes_.size(), 1u);
}

TEST_F(VoteHandlerTest, RequiresRecoveredLogPosition) {
  log_ = {9, 10};
  EXPECT_EQ(Dispatch(MakeRequest(10, true)), std::errc::state_not_recoverable);
  EXPECT_TRUE(writes_.empty());
  EXPECT_TRUE(response_.topics.empty());
}

TEST_F(VoteHandlerTest, DecodedLegacyVoteAndRecoveryFromDisk) {
  auto dir = (std::filesystem::temp_directory_path() / "dfly-raft-vote-XXXXXX").string();
  ASSERT_NE(mkdtemp(dir.data()), nullptr);
  FileQuorumStateStore store(dir);
  for (int16_t version : {0, 1, 2}) {
    std::string bytes;
    Request decoded;
    state_ = QuorumVoteState{ElectionState{5, {}, {}, {1, 2, 3}}};
    const int16_t kraft_version = version == 0 ? 0 : 1;
    persist_ = [&](const ElectionState& next) {
      return store.WriteElectionState(next, kraft_version);
    };
    ASSERT_EQ(Encode(MakeRequest(6, false, version), version, &bytes), protocol::CodecError::kOk);
    ASSERT_EQ(Decode(bytes, version, &decoded), protocol::CodecError::kOk);
    ASSERT_FALSE(Dispatch(decoded, version));
    ExpectPartition(6, -1, 0, true);
    EXPECT_EQ(Encode(response_, version, &bytes), protocol::CodecError::kOk);
    std::optional<ElectionState> recovered;
    ASSERT_FALSE(store.ReadElectionState(&recovered));
    ASSERT_TRUE(recovered);
    EXPECT_EQ(recovered->epoch, 6);
    EXPECT_EQ(recovered->voted_key, version == 0 ? ReplicaKey{candidate_.id} : candidate_);
    state_ = QuorumVoteState{*recovered};
    decoded.topics[0].partitions[0].replica_id = other_.id;
    ASSERT_FALSE(Dispatch(decoded, version));
    ExpectPartition(6, -1, 0, false);
  }
  std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace dfly::cluster::raft
