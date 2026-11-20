// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/election.h"

#include <gtest/gtest.h>

namespace dfly::cluster::raft {
namespace {

constexpr ReplicaKey kLocal{0, {10}};
constexpr ReplicaKey kPeer{1, {20}};
constexpr ReplicaKey kOther{2, {30}};
constexpr LogPosition kLog{4, 10};

using Outcome = EpochElection::Outcome;
using Result = EpochElection::RecordResult;

// Corresponds to Kafka UnattachedStateTest and ProspectiveStateTest grantVote cases.
TEST(RaftVote, UnattachedAndProspectiveCompareLogsByEpochThenEndOffset) {
  for (auto role : {QuorumRole::kUnattached, QuorumRole::kProspective}) {
    VoteContext context{role, std::nullopt, std::nullopt};
    for (auto type : {VoteType::kVote, VoteType::kPreVote}) {
      EXPECT_TRUE(context.CanGrantVote(kPeer, kLog, kLog, type));
      EXPECT_TRUE(context.CanGrantVote(kPeer, {5, 1}, kLog, type));
      EXPECT_TRUE(context.CanGrantVote(kPeer, {4, 11}, kLog, type));
      EXPECT_FALSE(context.CanGrantVote(kPeer, {3, 100}, kLog, type));
      EXPECT_FALSE(context.CanGrantVote(kPeer, {4, 9}, kLog, type));
    }
    context.leader_id = kOther.id;
    EXPECT_FALSE(context.CanGrantVote(kPeer, kLog, kLog, VoteType::kVote));
    EXPECT_TRUE(context.CanGrantVote(kPeer, kLog, kLog, VoteType::kPreVote));
  }
}

TEST(RaftVote, RepeatedStandardVoteUsesPersistedCandidateIdentity) {
  for (auto role : {QuorumRole::kUnattached, QuorumRole::kProspective}) {
    VoteContext context{role, kOther.id, kPeer};
    const ReplicaKey missing_directory{kPeer.id};
    const ReplicaKey replaced_directory{kPeer.id, {25}};
    const LogPosition stale{2, 1};
    EXPECT_TRUE(context.CanGrantVote(kPeer, stale, kLog, VoteType::kVote));
    EXPECT_FALSE(context.CanGrantVote(kPeer, stale, kLog, VoteType::kPreVote));
    EXPECT_FALSE(context.CanGrantVote(kOther, kLog, kLog, VoteType::kVote));
    EXPECT_FALSE(context.CanGrantVote(missing_directory, kLog, kLog, VoteType::kVote));
    EXPECT_FALSE(context.CanGrantVote(replaced_directory, kLog, kLog, VoteType::kVote));
    EXPECT_TRUE(context.CanGrantVote(kOther, kLog, kLog, VoteType::kPreVote));

    // kraft.version 0 loads a voted node id without its directory UUID.
    context.voted_key = missing_directory;
    EXPECT_TRUE(context.CanGrantVote(kPeer, stale, kLog, VoteType::kVote));
    EXPECT_TRUE(context.CanGrantVote(replaced_directory, stale, kLog, VoteType::kVote));
    EXPECT_FALSE(context.CanGrantVote(kOther, kLog, kLog, VoteType::kVote));
  }
}

// Corresponds to the role-specific testGrantVote and FollowerStateTest's two pre-vote cases.
TEST(RaftVote, CandidateResignedFollowerAndLeader) {
  for (auto role : {QuorumRole::kCandidate, QuorumRole::kResigned, QuorumRole::kFollower,
                    QuorumRole::kLeader}) {
    VoteContext context{role, std::nullopt, std::nullopt};
    EXPECT_FALSE(context.CanGrantVote(kLocal, kLog, kLog, VoteType::kVote));
    EXPECT_FALSE(context.CanGrantVote(kPeer, kLog, kLog, VoteType::kVote));
    EXPECT_EQ(context.CanGrantVote(kPeer, kLog, kLog, VoteType::kPreVote),
              role != QuorumRole::kLeader);
    EXPECT_FALSE(context.CanGrantVote(kPeer, {3, 10}, kLog, VoteType::kPreVote));
  }
  VoteContext follower{QuorumRole::kFollower, kPeer.id, kPeer, true};
  EXPECT_FALSE(follower.CanGrantVote(kOther, kLog, kLog, VoteType::kPreVote));
}

// Kafka CandidateStateTest and ProspectiveStateTest exercise these quorum sizes.
TEST(RaftEpochElection, SingleTwoAndThreeVoters) {
  const std::array voters{kLocal, kPeer, kOther};
  for (auto type : {VoteType::kPreVote, VoteType::kVote}) {
    auto one = EpochElection::Start(std::span{voters}.first(1), kLocal, type);
    ASSERT_TRUE(one.has_value());
    EXPECT_EQ(one->GetOutcome(), Outcome::kGranted);
    EXPECT_TRUE(one->UnrecordedVoters().empty());
    for (bool grant : {false, true}) {
      auto two = EpochElection::Start(std::span{voters}.first(2), kLocal, type);
      ASSERT_TRUE(two.has_value());
      EXPECT_EQ(two->GetOutcome(), Outcome::kPending);
      EXPECT_EQ(two->UnrecordedVoters(), std::vector<ReplicaKey>{kPeer});
      EXPECT_EQ(two->RecordVote(kPeer.id, grant), Result::kFirst);
      EXPECT_EQ(two->GetOutcome(), grant ? Outcome::kGranted : Outcome::kRejected);
      EXPECT_TRUE(two->UnrecordedVoters().empty());

      auto three = EpochElection::Start(voters, kLocal, type);
      ASSERT_TRUE(three.has_value());
      EXPECT_EQ(three->GetOutcome(), Outcome::kPending);
      EXPECT_EQ(three->RecordVote(kOther.id, false), Result::kFirst);
      EXPECT_EQ(three->GetOutcome(), Outcome::kPending);
      EXPECT_EQ(three->RecordVote(kPeer.id, grant), Result::kFirst);
      EXPECT_EQ(three->GetOutcome(), grant ? Outcome::kGranted : Outcome::kRejected);
    }
  }
}

TEST(RaftEpochElection, InvalidOrDuplicateResponsesPreserveTally) {
  const std::array voters{kLocal, kPeer, kOther};
  for (const bool grant : {false, true}) {
    auto election = EpochElection::Start(voters, kLocal, VoteType::kVote);
    ASSERT_TRUE(election.has_value());
    EXPECT_EQ(election->RecordVote(kLocal.id, false), Result::kInvalid);
    EXPECT_EQ(election->RecordVote(42, grant), Result::kInvalid);
    EXPECT_EQ(election->RecordVote(kPeer.id, grant), Result::kFirst);
    EXPECT_EQ(election->RecordVote(kPeer.id, grant), Result::kRepeat);
    EXPECT_EQ(election->RecordVote(kPeer.id, !grant), Result::kInvalid);
    EXPECT_EQ(election->GetOutcome(), grant ? Outcome::kGranted : Outcome::kPending);
    EXPECT_EQ(election->UnrecordedVoters(), std::vector<ReplicaKey>{kOther});
  }
}

// ProspectiveStateTest.testCanChangePreVote permits replacing either provisional answer.
TEST(RaftEpochElection, CanChangePreVote) {
  const std::array voters{kLocal, kPeer};
  auto election = EpochElection::Start(voters, kLocal, VoteType::kPreVote);
  ASSERT_TRUE(election.has_value());
  EXPECT_EQ(election->RecordVote(kLocal.id, false), Result::kInvalid);
  EXPECT_EQ(election->RecordVote(kPeer.id, false), Result::kFirst);
  EXPECT_EQ(election->GetOutcome(), Outcome::kRejected);
  EXPECT_EQ(election->RecordVote(kPeer.id, true), Result::kRepeat);
  EXPECT_EQ(election->GetOutcome(), Outcome::kGranted);
  EXPECT_EQ(election->RecordVote(kPeer.id, false), Result::kRepeat);
  EXPECT_EQ(election->GetOutcome(), Outcome::kRejected);
}

// Both types of election require a voter. Old static configurations identify voters by node id.
TEST(RaftEpochElection, LocalVoterAndDirectoryMembership) {
  for (auto type : {VoteType::kVote, VoteType::kPreVote}) {
    EXPECT_FALSE(EpochElection::Start({}, kLocal, type));
    EXPECT_FALSE(EpochElection::Start(std::array{kPeer}, kLocal, type));
    EXPECT_FALSE(EpochElection::Start(std::array{kLocal}, ReplicaKey{kLocal.id, {5}}, type));
    EXPECT_FALSE(
        EpochElection::Start(std::array{kLocal, ReplicaKey{kLocal.id, {5}}}, kLocal, type));
    EXPECT_FALSE(EpochElection::Start(std::array{kLocal, ReplicaKey{-1}}, kLocal, type));
    EXPECT_TRUE(EpochElection::Start(std::array{ReplicaKey{kLocal.id}}, kLocal, type));
  }
}

}  // namespace
}  // namespace dfly::cluster::raft
