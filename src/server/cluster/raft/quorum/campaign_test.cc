// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/campaign.h"

#include <gtest/gtest.h>

namespace dfly::cluster::raft {
namespace {

using namespace std;
using protocol::VoteResponse;

constexpr ReplicaKey kA{1, {1}};
constexpr ReplicaKey kB{2, {2}};
constexpr ReplicaKey kC{3, {3}};
const vector<CampaignVoter> kVoters{{kA, {"A", 9101}}, {kB, {"B", 9102}}, {kC, {"C", 9103}}};
const LogPosition kEnd{2, 10};

class CampaignTest : public testing::Test {
 protected:
  vector<ElectionState> disk_;
  bool fail_write_ = false;

  ElectionCampaign New(ReplicaKey local, vector<CampaignVoter> voters = kVoters) {
    return ElectionCampaign({"cluster", "data", 10, local}, std::move(voters),
                            [this](const ElectionState& state) {
                              if (fail_write_)
                                return make_error_code(errc::io_error);
                              disk_.push_back(state);
                              return error_code{};
                            });
  }

  // Run two real quorum instances through the versioned Kafka request and response body codecs.
  void Exchange(ElectionCampaign* sender, ElectionCampaign* peer, ReplicaKey dest,
                int16_t version = 2) {
    auto pending = sender->PrepareVote(dest, kEnd);
    ASSERT_TRUE(pending);
    string bytes;
    protocol::VoteRequest request;
    ASSERT_EQ(Encode(pending->request, version, &bytes), protocol::CodecError::kOk);
    ASSERT_EQ(Decode(bytes, version, &request), protocol::CodecError::kOk);
    VoteResponse response;
    ASSERT_FALSE(peer->ReceiveVote(request, version, kEnd, &response));
    ASSERT_EQ(Encode(response, version, &bytes), protocol::CodecError::kOk);
    ASSERT_EQ(Decode(bytes, version, &response), protocol::CodecError::kOk);
    ASSERT_FALSE(sender->ReceiveVoteResponse(dest, pending->sequence, response));
  }
};

TEST_F(CampaignTest, TwoRoundsAndOldLeaderRestartsResigned) {
  auto a = New(kA);
  auto b = New(kB);
  ASSERT_FALSE(a.Recover({}, kEnd));
  ASSERT_FALSE(b.Recover({}, kEnd));
  ASSERT_FALSE(a.OnElectionTimeout());
  ASSERT_EQ(a.state().role, QuorumRole::kProspective);
  EXPECT_EQ(a.state().election.epoch, 2);
  Exchange(&a, &b, kB);
  ASSERT_EQ(a.state().role, QuorumRole::kCandidate);
  ASSERT_EQ(a.state().election.epoch, 3);
  ASSERT_FALSE(disk_.empty());
  EXPECT_EQ(disk_.back().voted_key, kA);
  EXPECT_FALSE(disk_.back().leader_id);
  Exchange(&a, &b, kB);
  EXPECT_EQ(a.state().role, QuorumRole::kLeader);
  EXPECT_EQ(a.GrantingVoters(), (vector<int32_t>{1, 2}));
  EXPECT_EQ(disk_.back().leader_id, 1);
  EXPECT_FALSE(disk_.back().voted_key);
  EXPECT_TRUE(a.UnrecordedVoters().empty());

  auto restarted = New(kA);
  ASSERT_FALSE(restarted.Recover(a.state().election, kEnd));
  EXPECT_EQ(restarted.state().role, QuorumRole::kResigned);
  EXPECT_TRUE(restarted.GrantingVoters().empty());
}

TEST_F(CampaignTest, TwoVotersRequireBothAndSingleVoterSelfElects) {
  const vector<CampaignVoter> pair{kVoters[0], kVoters[1]};
  auto a = New(kA, pair);
  auto b = New(kB, pair);
  ASSERT_FALSE(a.Recover({}, kEnd));
  ASSERT_FALSE(b.Recover({}, kEnd));
  ASSERT_FALSE(a.OnElectionTimeout());
  ASSERT_FALSE(a.OnElectionTimeout());
  EXPECT_EQ(a.state().role, QuorumRole::kUnattached);
  EXPECT_EQ(a.state().election.epoch, 2);
  ASSERT_FALSE(a.OnElectionTimeout());
  Exchange(&a, &b, kB);
  EXPECT_EQ(a.state().role, QuorumRole::kCandidate);
  ASSERT_FALSE(a.OnElectionTimeout());
  EXPECT_EQ(a.state().role, QuorumRole::kProspective);
  EXPECT_EQ(a.state().election.epoch, 3);
  EXPECT_EQ(a.state().election.voted_key, kA);

  auto one = New(kA, {kVoters.front()});
  ASSERT_FALSE(one.Recover({}, kEnd));
  EXPECT_EQ(one.state().role, QuorumRole::kLeader);
  EXPECT_EQ(one.state().election.epoch, 3);
  EXPECT_EQ(one.GrantingVoters(), (vector<int32_t>{1}));
}

TEST_F(CampaignTest, UnsupportedPreVoteFallsBackAfterValidCorrelation) {
  auto a = New(kA);
  auto b = New(kB);
  ASSERT_FALSE(a.Recover({}, kEnd));
  ASSERT_FALSE(b.Recover({}, kEnd));
  ASSERT_FALSE(a.OnElectionTimeout());
  auto old = a.PrepareVote(kB, kEnd);
  auto current = a.PrepareVote(kB, kEnd);
  ASSERT_TRUE(current && old);
  VoteResponse unsupported;
  unsupported.error_code = 35;
  ASSERT_FALSE(a.ReceiveVoteResponse(kB, old->sequence, unsupported));
  ASSERT_FALSE(a.ReceiveVoteResponse(kC, current->sequence, unsupported));
  ASSERT_FALSE(a.ReceiveVoteResponse(ReplicaKey{kB.id, {9}}, current->sequence, unsupported));
  EXPECT_EQ(a.state().role, QuorumRole::kProspective);
  ASSERT_FALSE(a.ReceiveVoteResponse(kB, current->sequence, unsupported));
  EXPECT_EQ(a.state().role, QuorumRole::kCandidate);
  ASSERT_FALSE(a.ReceiveVoteResponse(kB, current->sequence, unsupported));
  Exchange(&a, &b, kB, 0);
  EXPECT_EQ(a.state().role, QuorumRole::kLeader);
}

TEST_F(CampaignTest, HigherEpochAndKnownLeaderFenceInFlightCampaign) {
  auto a = New(kA);
  auto b = New(kB);
  ASSERT_FALSE(a.Recover({}, kEnd));
  ASSERT_FALSE(b.Recover({5, kC.id, {}, {}}, kEnd));
  ASSERT_FALSE(a.OnElectionTimeout());
  auto late = a.PrepareVote(kC, kEnd);
  ASSERT_TRUE(late);
  Exchange(&a, &b, kB);
  EXPECT_EQ(a.state().role, QuorumRole::kFollower);
  EXPECT_EQ(a.state().election.epoch, 5);
  ASSERT_TRUE(a.LeaderEndpoint());
  EXPECT_EQ(a.LeaderEndpoint()->host, "C");
  VoteResponse late_grant;
  late_grant.error_code = 35;
  ASSERT_FALSE(a.ReceiveVoteResponse(kC, late->sequence, late_grant));
  EXPECT_EQ(a.state().role, QuorumRole::kFollower);
  ASSERT_FALSE(a.OnElectionTimeout());
  ASSERT_FALSE(a.OnElectionTimeout());
  EXPECT_EQ(a.state().role, QuorumRole::kFollower);
}

TEST_F(CampaignTest, CandidateRecoversSelfVoteAndObserverStaysIneligible) {
  auto a = New(kA);
  ASSERT_FALSE(a.Recover({4, {}, ReplicaKey{kA.id}, {}}, kEnd));
  EXPECT_EQ(a.state().role, QuorumRole::kCandidate);
  EXPECT_EQ(a.state().election.voted_key, kA);
  EXPECT_EQ(a.state().election.epoch, 4);
  EXPECT_EQ(a.UnrecordedVoters(), (vector<ReplicaKey>{kB, kC}));

  auto observer = New(ReplicaKey{8, {8}});
  ASSERT_FALSE(observer.Recover({}, kEnd));
  EXPECT_EQ(observer.OnElectionTimeout(), errc::operation_not_permitted);
  EXPECT_TRUE(observer.UnrecordedVoters().empty());
}

TEST_F(CampaignTest, DeclinedCandidacyWaitsForTimeout) {
  auto a = New(kA);
  auto b = New(kB);
  auto c = New(kC);
  ASSERT_FALSE(a.Recover({3, {}, kA, {}}, kEnd));
  ASSERT_FALSE(b.Recover({3, {}, kC, {}}, kEnd));
  ASSERT_FALSE(c.Recover({3, {}, kB, {}}, kEnd));
  Exchange(&a, &b, kB);
  Exchange(&a, &c, kC);
  EXPECT_EQ(a.state().role, QuorumRole::kCandidate);
  EXPECT_EQ(a.state().election.epoch, 3);
  EXPECT_TRUE(a.UnrecordedVoters().empty());
  ASSERT_FALSE(a.OnElectionTimeout());
  EXPECT_EQ(a.state().role, QuorumRole::kProspective);
  EXPECT_EQ(a.UnrecordedVoters().size(), 2u);
}

TEST_F(CampaignTest, IncomingHigherVoteCancelsRequestsAndPreservesImportedLogCut) {
  auto a = New(kA);
  auto b = New(kB);
  ASSERT_FALSE(a.Recover({2, kA.id, {}, {}}, {4, 30}));
  EXPECT_EQ(a.state().role, QuorumRole::kUnattached);
  EXPECT_EQ(a.state().election.epoch, 4);
  ASSERT_FALSE(a.OnElectionTimeout());
  auto stale = a.PrepareVote(kC, {4, 30});
  ASSERT_TRUE(stale);
  ASSERT_FALSE(b.Recover({6, {}, kB, {}}, {5, 15}));
  auto incoming = b.PrepareVote(kA, {5, 15});
  ASSERT_TRUE(incoming);
  VoteResponse response;
  ASSERT_FALSE(a.ReceiveVote(incoming->request, 2, {4, 30}, &response));
  EXPECT_EQ(a.state().role, QuorumRole::kUnattached);
  EXPECT_EQ(a.state().election.voted_key, kB);
  EXPECT_EQ(a.state().election.epoch, 6);
  EXPECT_TRUE(a.UnrecordedVoters().empty());
  response.error_code = 35;
  ASSERT_FALSE(a.ReceiveVoteResponse(kC, stale->sequence, response));
  EXPECT_EQ(a.state().role, QuorumRole::kUnattached);
}

TEST_F(CampaignTest, SelfVotePersistenceFailureFencesEveryRound) {
  auto a = New(kA);
  auto b = New(kB);
  ASSERT_FALSE(a.Recover({}, kEnd));
  ASSERT_FALSE(b.Recover({}, kEnd));
  ASSERT_FALSE(a.OnElectionTimeout());
  auto req = a.PrepareVote(kB, kEnd);
  ASSERT_TRUE(req);
  VoteResponse answer;
  ASSERT_FALSE(b.ReceiveVote(req->request, 2, kEnd, &answer));
  fail_write_ = true;
  ASSERT_EQ(a.ReceiveVoteResponse(kB, req->sequence, answer), errc::io_error);
  EXPECT_TRUE(a.UnrecordedVoters().empty());
  EXPECT_FALSE(a.PrepareVote(kB, kEnd));
  EXPECT_EQ(a.OnElectionTimeout(), errc::io_error);
}

}  // namespace
}  // namespace dfly::cluster::raft
