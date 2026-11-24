// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/fetch_replication.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include "server/cluster/raft/log/record_batch.h"
#include "server/cluster/raft/log/testdata/controlled_io.h"
#include "server/cluster/raft/log/testdata/snapshot_fixture.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;
namespace fs = filesystem;

constexpr ReplicaKey kA{1, {1}}, kB{2, {2}}, kC{3, {3}};
constexpr Uuid kDataTopic{12, 45};
const vector<CampaignVoter> kVoters{{kA, {"a", 7100}}, {kB, {"b", 7200}}, {kC, {"c", 7300}}};

struct Node {
  Node(fs::path dir, ReplicaKey local, vector<CampaignVoter> voters, int32_t partition,
       optional<int64_t> hw, PartitionLog::AckMode mode = PartitionLog::AckMode::kFlush)
      : log(PartitionLog::Open(dir, 1U << 30, mode).value()),
        campaign({"cluster", "data", partition, local}, std::move(voters),
                 [](const ElectionState&) { return error_code{}; }),
        epochs(campaign, "RAFT", 1000, 1000),
        fetch(campaign, epochs, *log, kDataTopic, hw) {
  }

  unique_ptr<PartitionLog> log;
  ElectionCampaign campaign;
  EpochControl epochs;
  FetchReplication fetch;
};

class FetchReplicationTest : public testing::Test {
 protected:
  void SetUp() override {
    string temp = (fs::temp_directory_path() / "dfly-fetch-XXXXXX").string();
    ASSERT_NE(mkdtemp(temp.data()), nullptr);
    dir_ = temp;
  }
  void TearDown() override {
    fs::remove_all(dir_);
  }
  Node Make(ReplicaKey local, int32_t partition = 10, vector<CampaignVoter> voters = kVoters,
            optional<int64_t> hw = nullopt) {
    return Node(Path(local.id, partition), local, std::move(voters), partition, hw);
  }

  fs::path Path(int32_t id, int32_t partition) {
    return dir_ / (to_string(partition) + "-" + to_string(id));
  }

  void Elect(Node* a, Node* b, int32_t term = 5) {
    ASSERT_FALSE(a->campaign.Recover({term, {}, kA, {}}, a->log->End()));
    ASSERT_FALSE(b->campaign.Recover({}, b->log->End()));
    const auto peers = a->campaign.UnrecordedVoters();
    ASSERT_FALSE(peers.empty());
    ASSERT_EQ(peers.front().id, kB.id);
    auto vote = a->campaign.PrepareVote(peers.front(), a->log->End());
    ASSERT_TRUE(vote);
    VoteResponse response;
    ASSERT_FALSE(b->campaign.ReceiveVote(vote->request, 2, b->log->End(), &response));
    ASSERT_FALSE(a->campaign.ReceiveVoteResponse(kB, vote->sequence, response));
    ASSERT_EQ(a->campaign.state().role, QuorumRole::kLeader);
    ASSERT_FALSE(b->campaign.ObserveLeader(term, kA.id, nullopt));
    ASSERT_TRUE(a->fetch.SyncRole(123).ok());
    ASSERT_TRUE(b->fetch.SyncRole(123).ok());
  }

  FetchResponse Handle(Node* a, const OutboundFetch& fetch, bool* can_wait = nullptr) {
    string wire;
    FetchRequest decoded;
    EXPECT_EQ(Encode(fetch.request, fetch.version, &wire), CodecError::kOk);
    EXPECT_EQ(Decode(wire, fetch.version, &decoded), CodecError::kOk);
    bool wait;
    FetchResponse response, result;
    ++now_;
    EXPECT_TRUE(
        a->fetch.HandleFetch(decoded, fetch.version, now_, now_ + 1000000, &response, &wait).ok());
    if (can_wait)
      *can_wait = wait;
    EXPECT_EQ(Encode(response, fetch.version, &wire), CodecError::kOk);
    EXPECT_EQ(Decode(wire, fetch.version, &result), CodecError::kOk);
    return result;
  }

  string Batch(int64_t offset, int32_t epoch) {
    const array<string, 1> values{"kv stuff"};
    return MakeDataBatch(offset, epoch, 1, values).value();
  }

  string VoterValue(span<const CampaignVoter> voters) {
    VotersRecord record;
    for (const auto& peer : voters) {
      auto& node = record.voters.emplace_back();
      node.voter_id = peer.key.id;
      node.voter_directory_id = peer.key.directory_id;
      node.k_raft_version_feature.max_supported_version = 1;
      node.endpoints.push_back({"RAFT", peer.endpoint.host, peer.endpoint.port, {}});
    }
    string bytes;
    EXPECT_EQ(Encode(record, 0, &bytes), CodecError::kOk);
    return bytes;
  }

  fs::path dir_;
  int64_t now_ = 1000;
};

TEST_F(FetchReplicationTest, NewlyArrivingFetchReadsWrittenTailBeforeLocalPublication) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a, &b);
  for (int i = 0; i < 2; ++i) {
    auto request = b.fetch.PrepareFetch(1, 18, 0, 1024).value();
    ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, request.sequence, Handle(&a, request)).ok());
  }
  test::ControlledIO io;
  a.fetch.SetAsyncIO(io.Submitter());
  auto bytes = Batch(1, 5) + Batch(2, 5);
  const auto expected = bytes;
  const char* data = bytes.data();
  auto buffer = make_shared<const string>(std::move(bytes));
  weak_ptr<const string> lifetime = buffer;
  ASSERT_TRUE(a.fetch.Append(buffer, 5).ok());
  buffer.reset();
  EXPECT_FALSE(lifetime.expired());
  ASSERT_EQ(io.requests.size(), 1u);
  EXPECT_EQ(io.requests.front().bytes.data(), data);
  auto request = b.fetch.PrepareFetch(1, 18, 0, 1024).value();
  EXPECT_TRUE(Handle(&a, request).responses[0].partitions[0].records.data->empty());
  io.Complete();

  // This Fetch arrives after WRITE completion and while the segment sync is still pending.
  request = b.fetch.PrepareFetch(1, 18, 0, 1024).value();
  auto response = Handle(&a, request);
  EXPECT_EQ(response.responses[0].partitions[0].records.data, expected);
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, request.sequence, response).ok());
  EXPECT_EQ(b.log->End(), (LogPosition{5, 3}));
  EXPECT_EQ(a.log->End(), (LogPosition{5, 1}));
  EXPECT_EQ(a.fetch.HighWatermark(), 1);
  auto ack = b.fetch.PrepareFetch(1, 18, 0, 1024).value();
  const auto& part = ack.request.topics[0].partitions[0];
  EXPECT_TRUE(a.log->PendingContains(part.fetch_offset, part.last_fetched_epoch));
  io.Complete();
  EXPECT_EQ(a.fetch.HighWatermark(), 1);
  io.Complete();
  EXPECT_EQ(a.fetch.HighWatermark(), 1);
  EXPECT_FALSE(a.log->PendingContains(part.fetch_offset, part.last_fetched_epoch));
  EXPECT_TRUE(lifetime.expired());
  response = Handle(&a, ack);
  EXPECT_EQ(response.responses[0].partitions[0].diverging_epoch.epoch, -1);
  EXPECT_EQ(a.fetch.HighWatermark(), 3);
}

TEST_F(FetchReplicationTest, AsyncFailureAndLateCompletionCannotCommit) {
  for (bool failure : {false, true}) {
    auto a = Make(kA, failure ? 11 : 10, {kVoters.front()});
    ASSERT_FALSE(a.campaign.Recover({}, {}));
    ASSERT_TRUE(a.fetch.SyncRole(now_).ok());
    const auto epoch = a.campaign.state().election.epoch;
    test::ControlledIO io;
    a.fetch.SetAsyncIO(io.Submitter());
    ASSERT_TRUE(a.fetch.Append(Batch(1, epoch), epoch).ok());
    io.Complete();
    if (failure) {
      io.Complete(-EIO);
      EXPECT_TRUE(a.campaign.state().storage_error);
      EXPECT_FALSE(a.fetch.HighWatermark());
      EXPECT_EQ(a.log->End().end_offset, 1);
    } else {
      ASSERT_FALSE(a.campaign.ObserveLeader(epoch + 1, nullopt, nullopt));
      io.Complete();
      io.Complete();
      EXPECT_EQ(a.campaign.state().election.epoch, epoch + 1);
      EXPECT_EQ(a.fetch.HighWatermark(), 1);
    }
  }
}

TEST_F(FetchReplicationTest, LeaderAppendsOnceAndCommitsOnlyReportedDurableVoterEnds) {
  auto a = Make(kA), b = Make(kB);
  auto c = Make(kC);
  Elect(&a, &b);
  ASSERT_TRUE(a.fetch.SyncRole(124).ok());
  EXPECT_EQ(a.log->End(), (LogPosition{5, 1}));
  EXPECT_FALSE(a.fetch.HighWatermark());
  auto value = Batch(1, 5);
  ASSERT_TRUE(a.fetch.Append(value, 5).ok());

  // The first request acknowledges offset zero. The follower fsyncs both records.
  auto first = b.fetch.PrepareFetch(kA.id, 18, 500, 1 << 20).value();
  auto response = Handle(&a, first);
  EXPECT_EQ(response.responses[0].partitions[0].high_watermark, -1);
  EXPECT_EQ(response.responses[0].partitions[0].partition_index, 10);
  bool reset = false;
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, first.sequence, response, &reset).ok());
  EXPECT_TRUE(reset);
  EXPECT_FALSE(a.fetch.HighWatermark());
  EXPECT_FALSE(b.fetch.HighWatermark());
  EXPECT_EQ(b.log->End(), (LogPosition{5, 2}));
  EXPECT_EQ(b.log->Read(0, 2, SIZE_MAX).value(), a.log->Read(0, 2, SIZE_MAX).value());

  // The next Fetch supplies the new LEO, allowing a majority including B to commit.
  auto next = b.fetch.PrepareFetch(kA.id, 18, 500, 1).value();
  EXPECT_EQ(next.request.topics[0].partitions[0].fetch_offset, 2);
  bool waiting = true;
  auto committed = Handle(&a, next, &waiting);
  EXPECT_FALSE(waiting);
  EXPECT_EQ(a.fetch.HighWatermark(), 2);
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, next.sequence, committed).ok());
  EXPECT_EQ(b.fetch.HighWatermark(), 2);
  auto caught_up = b.fetch.PrepareFetch(kA.id, 18, 500, 1).value();
  EXPECT_EQ(caught_up.request.topics[0].partitions[0].high_watermark, 2);
  Handle(&a, caught_up, &waiting);
  EXPECT_TRUE(waiting);
  EXPECT_EQ(c.log->End().end_offset, 0);
}

TEST_F(FetchReplicationTest, MemoryMajorityCommitsBeforeEitherDataLogFlushes) {
  Node a(Path(kA.id, 10), kA, kVoters, 10, nullopt, PartitionLog::AckMode::kMemory);
  Node b(Path(kB.id, 10), kB, kVoters, 10, nullopt, PartitionLog::AckMode::kMemory);
  Elect(&a, &b);
  for (int i = 0; i < 2; ++i) {
    auto fetch = b.fetch.PrepareFetch(kA.id, 18, 0, 1 << 20).value();
    ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, fetch.sequence, Handle(&a, fetch)).ok());
  }
  ASSERT_EQ(a.fetch.HighWatermark(), 1);
  ASSERT_EQ(b.fetch.HighWatermark(), 1);

  // Leader-change records remain durable even when ordinary data uses memory acknowledgement.
  ASSERT_EQ(a.log->DurableEnd().end_offset, 1);
  ASSERT_EQ(b.log->DurableEnd().end_offset, 1);
  test::ControlledIO io;
  a.fetch.SetAsyncIO(io.Submitter());
  ASSERT_TRUE(a.fetch.Append(Batch(1, 5), 5).ok());
  ASSERT_EQ(io.requests.size(), 1u);
  io.Complete();
  EXPECT_TRUE(io.requests.empty());
  EXPECT_EQ(a.log->End().end_offset, 2);
  EXPECT_EQ(a.log->DurableEnd().end_offset, 1);
  EXPECT_EQ(a.fetch.HighWatermark(), 1);

  auto copy = b.fetch.PrepareFetch(kA.id, 18, 0, 1 << 20).value();
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, copy.sequence, Handle(&a, copy)).ok());
  EXPECT_EQ(b.log->End().end_offset, 2);
  EXPECT_EQ(b.log->DurableEnd().end_offset, 1);
  EXPECT_EQ(a.fetch.HighWatermark(), 1);
  auto report = b.fetch.PrepareFetch(kA.id, 18, 0, 1 << 20).value();
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, report.sequence, Handle(&a, report)).ok());
  EXPECT_EQ(a.fetch.HighWatermark(), 2);
  EXPECT_EQ(b.fetch.HighWatermark(), 2);
  EXPECT_EQ(a.log->DurableEnd().end_offset, 1);
  EXPECT_EQ(b.log->DurableEnd().end_offset, 1);

  // Both nodes continue ordinary replication while their previous cut is being synced.
  test::ControlledIO leader_flush, follower_flush;
  auto completed = [](absl::Status s) { EXPECT_TRUE(s.ok()); };
  ASSERT_TRUE(a.log->TryFlushBufferedAsync(leader_flush.Submitter(), completed).value());
  ASSERT_TRUE(b.log->TryFlushBufferedAsync(follower_flush.Submitter(), completed).value());
  ASSERT_TRUE(a.fetch.Append(Batch(2, 5), 5).ok());
  io.Complete();
  auto next = b.fetch.PrepareFetch(kA.id, 18, 0, 1 << 20).value();
  auto response = Handle(&a, next);
  ASSERT_TRUE(b.fetch.CanReceiveDuringFlush(kA.id, next.sequence, response));
  EXPECT_FALSE(b.fetch.CanReceiveDuringFlush(kA.id, next.sequence + 1, response));
  EXPECT_FALSE(b.fetch.CanReceiveDuringFlush(kC.id, next.sequence, response));
  for (int change = 0; change < 5; ++change) {
    auto structural = response;
    auto& part = structural.responses.front().partitions.front();
    if (change == 0)
      ++part.current_leader.leader_epoch;
    else if (change == 1)
      part.diverging_epoch.epoch = 5;
    else if (change == 2)
      part.snapshot_id.epoch = 5;
    else if (change == 3)
      part.error_code = 74;
    else {
      const array<int32_t, 1> voters{1};
      part.records.data = MakeLeaderChangeBatch(2, 5, 1, 1, voters, voters).value();
    }
    EXPECT_FALSE(b.fetch.CanReceiveDuringFlush(kA.id, next.sequence, structural));
  }
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, next.sequence, std::move(response)).ok());
  auto confirm = b.fetch.PrepareFetch(kA.id, 18, 0, 1 << 20).value();
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, confirm.sequence, Handle(&a, confirm)).ok());
  EXPECT_EQ(a.fetch.HighWatermark(), 3);
  EXPECT_EQ(b.fetch.HighWatermark(), 3);
  EXPECT_EQ(a.log->DurableEnd().end_offset, 1);
  EXPECT_EQ(b.log->DurableEnd().end_offset, 1);
  leader_flush.Complete();
  follower_flush.Complete();
  leader_flush.Complete();
  follower_flush.Complete();
  EXPECT_EQ(a.log->DurableEnd().end_offset, 2);
  EXPECT_EQ(b.log->DurableEnd().end_offset, 2);
  EXPECT_TRUE(a.log->FlushBuffered().ok());
  EXPECT_TRUE(b.log->FlushBuffered().ok());
  EXPECT_EQ(a.log->DurableEnd(), a.log->End());
  EXPECT_EQ(b.log->DurableEnd(), b.log->End());
}

TEST_F(FetchReplicationTest, IndependentTwoVoterPartitionsLoseMajorityIndependently) {
  auto two = vector<CampaignVoter>(kVoters.begin(), kVoters.begin() + 2);
  auto a10 = Make(kA, 10, two), b10 = Make(kB, 10, two);
  auto a20 = Make(kA, 20, two), b20 = Make(kB, 20, two);
  Elect(&a10, &b10);
  Elect(&a20, &b20);
  auto first = b20.fetch.PrepareFetch(1, 18, 0, 1024).value();
  EXPECT_EQ(Handle(&a10, first).error_code, 42);
  auto response = Handle(&a20, first);
  ASSERT_TRUE(b20.fetch.ReceiveFetchResponse(1, first.sequence, response).ok());
  auto reported = b20.fetch.PrepareFetch(1, 18, 0, 1024).value();
  Handle(&a20, reported);
  EXPECT_EQ(a20.fetch.HighWatermark(), 1);
  EXPECT_FALSE(a10.fetch.HighWatermark());

  vector<EpochNotification> messages;
  ASSERT_FALSE(a10.epochs.Poll(now_, &messages));
  ASSERT_FALSE(a10.epochs.Poll(now_ + 1500, &messages));
  EXPECT_EQ(a10.campaign.state().role, QuorumRole::kResigned);
  EXPECT_FALSE(a10.fetch.Append(Batch(1, 5), 5).ok());
  EXPECT_TRUE(a20.fetch.Append(Batch(1, 5), 5).ok());
}

TEST_F(FetchReplicationTest, CompletingParkedFetchDoesNotRenewQuorumContact) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a, &b);

  // Replicate the leader-change record and let B learn its committed offset.
  for (int i = 0; i < 2; ++i) {
    auto fetch = b.fetch.PrepareFetch(kA.id, 18, 0, 1024).value();
    ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, fetch.sequence, Handle(&a, fetch)).ok());
  }
  int applied_reports = 0;
  a.fetch.SetDataApplyHooks({},
                            [&](ReplicaKey, const auto&, int64_t, int64_t) { ++applied_reports; });
  auto parked = b.fetch.PrepareFetch(kA.id, 18, 3000, 1024).value();
  bool can_wait = false;
  Handle(&a, parked, &can_wait);
  ASSERT_TRUE(can_wait);
  ASSERT_EQ(applied_reports, 1);
  const int64_t last_contact = now_;

  auto batch = Batch(1, 5);
  ASSERT_TRUE(a.fetch.Append(batch, 5).ok());
  now_ += 1000;
  FetchResponse completed;
  ASSERT_TRUE(a.fetch
                  .HandleFetch(parked.request, parked.version, nullopt, now_ + 1000000, &completed,
                               &can_wait)
                  .ok());
  ASSERT_FALSE(can_wait);
  EXPECT_EQ(completed.responses[0].partitions[0].records.data.value(), batch);
  EXPECT_EQ(applied_reports, 1);

  vector<EpochNotification> messages;
  ASSERT_FALSE(a.epochs.Poll(last_contact + 1499, &messages));
  EXPECT_EQ(a.campaign.state().role, QuorumRole::kLeader);
  ASSERT_FALSE(a.epochs.Poll(last_contact + 1500, &messages));
  EXPECT_EQ(a.campaign.state().role, QuorumRole::kResigned);
}

TEST_F(FetchReplicationTest, ParkedFollowerPersistsWrittenDataBeforeLeaderPublishesItsEnd) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a, &b);
  for (int i = 0; i < 2; ++i) {
    auto fetch = b.fetch.PrepareFetch(1, 18, 0, 1024).value();
    ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, fetch.sequence, Handle(&a, fetch)).ok());
  }

  const auto first = Batch(1, 5), second = Batch(2, 5);
  const int32_t budget = first.size() + 1;
  auto pending = b.fetch.PrepareFetch(1, 18, 500, budget).value();
  shared_ptr<const FetchReplication::ParkedFetch> parked;
  FetchResponse response;
  bool sent = false;
  a.fetch.SetFetchWakeup([&] {
    // The leader has written both batches, but its local flush and publication are still ahead.
    ASSERT_EQ(a.fetch.LogEndOffset(), 1);
    ASSERT_EQ(a.fetch.HighWatermark(), 1);
    ASSERT_TRUE(parked);
    EXPECT_EQ(parked->Records(1), first);
    ASSERT_EQ(parked->Records(budget), first + second.substr(0, 1));
    response.responses[0].partitions[0].records.data->assign(*parked->Records(budget));
    ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, pending.sequence, response).ok());
    EXPECT_EQ(b.fetch.LogEndOffset(), 2);
    EXPECT_EQ(b.fetch.HighWatermark(), 1);
    EXPECT_EQ(a.fetch.HighWatermark(), 1);
    sent = true;
  });
  bool can_wait = false;
  ASSERT_TRUE(
      a.fetch.HandleFetch(pending.request, 18, now_, now_, &response, &can_wait, &parked).ok());
  ASSERT_TRUE(can_wait);
  ASSERT_TRUE(parked);
  EXPECT_FALSE(parked->Records(budget));

  bool may_have_appended = true;
  EXPECT_FALSE(a.fetch.Append(Batch(9, 5), 5, &may_have_appended).ok());
  EXPECT_FALSE(may_have_appended);
  EXPECT_FALSE(sent);
  ASSERT_TRUE(a.fetch.Append(first + second, 5, &may_have_appended).ok());
  EXPECT_TRUE(may_have_appended);
  ASSERT_TRUE(sent);
  EXPECT_EQ(a.fetch.LogEndOffset(), 3);
  EXPECT_EQ(a.fetch.HighWatermark(), 1);

  // Only the next Fetch can report the follower's durable end. Its partial second batch
  // is fetched again at offset 2 and still cannot count toward the current majority.
  auto ack = b.fetch.PrepareFetch(1, 18, 0, 1024).value();
  auto suffix = Handle(&a, ack);
  EXPECT_EQ(suffix.responses[0].partitions[0].records.data, second);
  EXPECT_EQ(a.fetch.HighWatermark(), 2);
}

TEST_F(FetchReplicationTest, VersionedIdentityAndMalformedRequestsDoNotCountAsVoters) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a, &b);
  auto pending = b.fetch.PrepareFetch(1, 18, 10, 500).value();
  auto response = Handle(&a, pending);
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, pending.sequence, response).ok());
  for (int16_t version : {13, 14, 15, 16, 17, 18}) {
    auto out = b.fetch.PrepareFetch(1, version, 10, 500).value();
    EXPECT_EQ(out.request.replica_id, version < 15 ? kB.id : -1);
    EXPECT_EQ(out.request.replica_state.replica_id, version >= 15 ? kB.id : -1);
    auto& part = out.request.topics[0].partitions[0];
    if (version >= 17)
      part.replica_directory_id = {99};
    bool wait = false;
    auto handled = Handle(&a, out, &wait);
    EXPECT_TRUE(wait);
    EXPECT_EQ(handled.node_endpoints.size(), version >= 17 ? 1u : 0u);
    EXPECT_FALSE(a.fetch.HighWatermark());
  }
  auto probe = b.fetch.PrepareFetch(1, 18, 10, 500).value();
  probe.request.cluster_id = "different";
  EXPECT_EQ(Handle(&a, probe).error_code, 104);
  probe.request.cluster_id.reset();
  probe.request.topics[0].topic_id[1] = 65;
  EXPECT_EQ(Handle(&a, probe).error_code, 42);
  probe.request.topics[0].topic_id = kDataTopic;
  auto& part = probe.request.topics[0].partitions[0];
  part.current_leader_epoch++;
  EXPECT_EQ(Handle(&a, probe).responses[0].partitions[0].error_code, 75);
  part.last_fetched_epoch = 0;
  part.current_leader_epoch -= 2;
  EXPECT_EQ(Handle(&a, probe).responses[0].partitions[0].error_code, 74);
  part.last_fetched_epoch = -1;
  EXPECT_EQ(Handle(&a, probe).responses[0].partitions[0].error_code, 42);
  EXPECT_FALSE(a.fetch.HighWatermark());
}

TEST_F(FetchReplicationTest, SmallFetchReturnsCompleteFirstBatchAndDiscardsPartialSuffix) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a, &b);
  vector<LogPosition> observed;
  shared_ptr<const string> retained;
  const char* expected_data = nullptr;
  b.fetch.SetFetchedRecordsHook([&](const PartitionLog& log, const shared_ptr<const string>& buffer,
                                    span<const PartitionLog::ValidatedBatch> batches) {
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_EQ(log.End(), batches.back().info.end);
    EXPECT_EQ(buffer->data(), expected_data);
    observed.push_back(batches.back().info.end);
    retained = buffer;
  });
  auto control_size = a.log->Read(0, 1, 1).value().size();
  auto data = Batch(1, 5);
  ASSERT_TRUE(a.fetch.Append(data, 5).ok());
  auto first = b.fetch.PrepareFetch(1, 18, 0, control_size + 12).value();
  auto partial = Handle(&a, first);
  EXPECT_EQ(partial.responses[0].partitions[0].records.data->size(), control_size + 12);
  expected_data = partial.responses[0].partitions[0].records.data->data();
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, first.sequence, std::move(partial)).ok());
  EXPECT_EQ(observed, (vector<LogPosition>{{5, 1}}));
  EXPECT_EQ(b.log->End(), (LogPosition{5, 1}));
  auto retry = b.fetch.PrepareFetch(1, 18, 0, 1).value();
  auto full = Handle(&a, retry);
  EXPECT_EQ(full.responses[0].partitions[0].records.data.value(), data);
  auto broken = full;
  broken.responses[0].partitions[0].records.data->back() ^= 0x10;
  broken.responses[0].partitions[0].high_watermark = 200;
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, retry.sequence, broken).ok());
  EXPECT_EQ(observed.size(), 1u);
  EXPECT_EQ(b.log->End().end_offset, 1);
  EXPECT_EQ(b.fetch.HighWatermark(), 1);
  auto last = b.fetch.PrepareFetch(1, 18, 0, 1).value();
  auto complete = Handle(&a, last);
  expected_data = complete.responses[0].partitions[0].records.data->data();
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, last.sequence, std::move(complete)).ok());
  EXPECT_EQ(observed, (vector<LogPosition>{{5, 1}, {5, 2}}));
  EXPECT_EQ(*retained, data);

  // An old response must not republish its bytes into the application cache.
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, retry.sequence, full).ok());
  EXPECT_EQ(observed.size(), 2u);
  EXPECT_EQ(b.log->End(), (LogPosition{5, 2}));
}

TEST_F(FetchReplicationTest, FetchMaxBytesWithTwoBatchesResumesAtThirdBatch) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a, &b);
  auto control = b.fetch.PrepareFetch(kA.id, 18, 0, 1024).value();
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, control.sequence, Handle(&a, control)).ok());

  const auto first = MakeDataBatch(1, 5, 124, array<string, 3>{"a", "a", "a"}).value();
  const auto second = MakeDataBatch(4, 5, 124, array<string, 3>{"b", "b", "b"}).value();
  const auto third = MakeDataBatch(7, 5, 124, array<string, 3>{"c", "c", "c"}).value();
  const auto prefix = first + second;
  ASSERT_TRUE(a.fetch.Append(prefix + third, 5).ok());

  // The budget covers two complete batches and one byte of the third.
  const int32_t max_bytes = prefix.size() + 1;
  auto fetch = b.fetch.PrepareFetch(kA.id, 18, 0, max_bytes).value();
  ASSERT_EQ(fetch.request.topics[0].partitions[0].fetch_offset, 1);
  auto response = Handle(&a, fetch);
  const auto& records = response.responses[0].partitions[0].records.data;
  ASSERT_TRUE(records);
  EXPECT_LE(records->size(), static_cast<size_t>(max_bytes));
  EXPECT_EQ(records->substr(0, prefix.size()), prefix);
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, fetch.sequence, response).ok());
  ASSERT_EQ(b.log->End(), (LogPosition{5, 7}));
  EXPECT_EQ(b.log->Read(1, 7, SIZE_MAX).value(), prefix);

  auto next = b.fetch.PrepareFetch(kA.id, 18, 0, max_bytes).value();
  ASSERT_EQ(next.request.topics[0].partitions[0].fetch_offset, 7);
  auto suffix = Handle(&a, next);
  EXPECT_EQ(suffix.responses[0].partitions[0].records.data.value(), third);
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(kA.id, next.sequence, suffix).ok());
  EXPECT_EQ(b.log->End(), (LogPosition{5, 10}));
  EXPECT_EQ(b.log->Read(7, 10, SIZE_MAX).value(), third);
}

TEST_F(FetchReplicationTest, FollowerTruncatesOnlyUncommittedDivergence) {
  auto a = Make(kA), b = Make(kB);
  ASSERT_TRUE(a.log->Append(Batch(0, 1), 1).ok());
  ASSERT_TRUE(b.log->Append(Batch(0, 1), 1).ok());
  Elect(&a, &b);

  // Divergence is created before this follower has installed the new leader's suffix.
  ASSERT_TRUE(b.log->Append(Batch(1, 3), 3).ok());
  auto fetch = b.fetch.PrepareFetch(1, 18, 200, 500).value();
  bool waiting = true;
  auto diverged = Handle(&a, fetch, &waiting);
  EXPECT_FALSE(waiting);
  EXPECT_EQ(diverged.responses[0].partitions[0].diverging_epoch.epoch, 1);
  EXPECT_EQ(diverged.responses[0].partitions[0].diverging_epoch.end_offset, 1);
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, fetch.sequence, diverged).ok());
  EXPECT_EQ(b.log->End(), (LogPosition{1, 1}));
  for (int i = 0; i < 2; ++i) {
    auto next = b.fetch.PrepareFetch(1, 18, 0, 500).value();
    ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, next.sequence, Handle(&a, next)).ok());
  }
  ASSERT_EQ(b.fetch.HighWatermark(), 2);
  auto forbidden = b.fetch.PrepareFetch(1, 18, 0, 500).value();
  EXPECT_FALSE(b.fetch.ReceiveFetchResponse(1, forbidden.sequence, diverged).ok());
  EXPECT_EQ(b.log->End(), (LogPosition{5, 2}));
}

TEST_F(FetchReplicationTest, LaterTermAndReplacedRequestsFenceReplies) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a, &b);
  auto stale = b.fetch.PrepareFetch(1, 18, 500, 512).value();
  auto latest = b.fetch.PrepareFetch(1, 18, 0, 512).value();
  auto response = Handle(&a, stale);
  bool reset = true;
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, stale.sequence, response, &reset).ok());
  EXPECT_FALSE(reset);
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(3, latest.sequence, response, &reset).ok());
  EXPECT_FALSE(reset);
  ASSERT_FALSE(b.campaign.ObserveLeader(6, kC.id, nullopt));
  ASSERT_TRUE(b.fetch.SyncRole(now_).ok());
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, latest.sequence, response, &reset).ok());
  EXPECT_FALSE(reset);
  EXPECT_EQ(b.log->End().end_offset, 0);
  EXPECT_FALSE(b.campaign.state().fetched_from_leader);
}

TEST_F(FetchReplicationTest, MidBatchFetchCannotCommitPartialBatch) {
  auto a = Make(kA), b = Make(kB);
  Elect(&a, &b);
  const array<string, 3> values{"set", "key", "value"};
  ASSERT_TRUE(a.fetch.Append(MakeDataBatch(1, 5, 124, values).value(), 5).ok());
  auto fetch = b.fetch.PrepareFetch(1, 18, 0, 1024).value();
  auto& request = fetch.request.topics[0].partitions[0];
  request.last_fetched_epoch = 5;
  request.fetch_offset = 3;
  auto response = Handle(&a, fetch);
  EXPECT_EQ(response.responses[0].partitions[0].high_watermark, 1);
  request.fetch_offset = 4;
  response = Handle(&a, fetch);
  EXPECT_EQ(response.responses[0].partitions[0].high_watermark, 4);
}

TEST_F(FetchReplicationTest, LegacyStaticVoterReportsDurableOffsetAfterRestart) {
  auto legacy = kVoters;
  for (auto& voter : legacy)
    voter.key.directory_id = {};
  auto a = Make(kA, 10, legacy);
  {
    auto b = Make(kB, 10, legacy);
    Elect(&a, &b);
    auto out = b.fetch.PrepareFetch(1, 13, 100, 4096).value();
    ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, out.sequence, Handle(&a, out)).ok());
    EXPECT_FALSE(a.fetch.HighWatermark());
  }
  auto restored = Make(kB, 10, legacy);
  EXPECT_EQ(restored.log->End(), (LogPosition{5, 1}));
  ASSERT_FALSE(restored.campaign.Recover({5, 1, {}, {}}, restored.log->End()));
  auto reported = restored.fetch.PrepareFetch(1, 13, 100, 4096).value();
  bool wait = false;
  auto response = Handle(&a, reported, &wait);
  EXPECT_EQ(response.responses[0].partitions[0].high_watermark, 1);
  EXPECT_TRUE(wait);
  EXPECT_TRUE(response.node_endpoints.empty());
}

TEST_F(FetchReplicationTest, SnapshotHintPausesFetchUntilDurableInstallAndResumesAtCut) {
  const LogPosition cut{3, 8};
  auto a = Make(kA), b = Make(kB);
  auto bytes = test::SnapshotBytes(3);
  auto writer = a.log->BeginSnapshot(cut, bytes.size()).value();
  ASSERT_TRUE(writer->Append(0, bytes).ok());
  ASSERT_TRUE(a.log->InstallSnapshot(writer.get(), 0).ok());
  Elect(&a, &b);
  auto pending = b.fetch.PrepareFetch(1, 18, 500, 300).value();
  bool wait = true;
  auto hint = Handle(&a, pending, &wait);
  ASSERT_FALSE(wait);
  EXPECT_EQ(hint.responses[0].partitions[0].snapshot_id.end_offset, 8);
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, pending.sequence, hint).ok());
  EXPECT_EQ(b.fetch.RequestedSnapshot(), cut);
  EXPECT_FALSE(b.fetch.PrepareFetch(1, 18, 500, 300));
  EXPECT_FALSE(b.fetch.SnapshotInstalled(cut).ok());
  auto local = b.log->BeginSnapshot(cut, bytes.size()).value();
  ASSERT_TRUE(local->Append(0, bytes).ok());
  ASSERT_TRUE(b.log->InstallSnapshot(local.get(), 0).ok());
  ASSERT_TRUE(b.fetch.SnapshotInstalled(cut).ok());
  auto next = b.fetch.PrepareFetch(1, 18, 500, 300).value();
  EXPECT_EQ(next.request.topics[0].partitions[0].fetch_offset, 8);
  EXPECT_EQ(next.request.topics[0].partitions[0].last_fetched_epoch, 3);
  EXPECT_EQ(next.request.topics[0].partitions[0].high_watermark, 8);
  auto suffix = Handle(&a, next);
  ASSERT_TRUE(b.fetch.ReceiveFetchResponse(1, next.sequence, suffix).ok());
  EXPECT_EQ(b.log->End(), (LogPosition{5, 9}));
}

TEST_F(FetchReplicationTest, SingleVoterCommittedLocallyAndDiskFailureFencesCampaign) {
  auto a = Make(kA, 10, {kVoters.front()});
  ASSERT_FALSE(a.campaign.Recover({}, {}));
  ASSERT_TRUE(a.fetch.SyncRole(now_).ok());
  EXPECT_EQ(a.fetch.HighWatermark(), 1);
  auto epoch = a.campaign.state().election.epoch;
  ASSERT_TRUE(a.fetch.Append(Batch(1, epoch), epoch).ok());
  EXPECT_EQ(a.fetch.HighWatermark(), 2);
  auto marker = Path(1, 10) / "dragonfly-log-end";
  fs::resize_file(marker, fs::file_size(marker) + 1);
  bool may_have_appended = false;
  EXPECT_FALSE(a.fetch.Append(Batch(2, epoch), epoch, &may_have_appended).ok());
  EXPECT_TRUE(may_have_appended);
  EXPECT_TRUE(a.campaign.state().storage_error);
  EXPECT_FALSE(a.fetch.HighWatermark());
  EXPECT_FALSE(a.fetch.SyncRole(now_).ok());
  EXPECT_EQ(a.log->End().end_offset, 2);
}

TEST_F(FetchReplicationTest, NewMajorityGovernsUncommittedRemovalAndLeaderResignsAtCommit) {
  auto a = Make(kA), b = Make(kB), c = Make(kC);
  Elect(&a, &b);
  ASSERT_FALSE(c.campaign.Recover({5, kA.id, {}, {}}, c.log->End()));
  auto exchange = [&](Node* follower) {
    auto out = follower->fetch.PrepareFetch(kA.id, 18, 0, 12000).value();
    ASSERT_TRUE(follower->fetch.ReceiveFetchResponse(1, out.sequence, Handle(&a, out)).ok());
  };
  exchange(&b);
  exchange(&b);
  ASSERT_EQ(a.fetch.HighWatermark(), 1);
  a.fetch.SetFetchWakeup(
      [] { ADD_FAILURE() << "A voter-set change must finish installing first"; });
  auto request = b.fetch.PrepareFetch(kA.id, 18, 100, 1024).value().request;
  shared_ptr<const FetchReplication::ParkedFetch> parked;
  bool can_wait = false;
  FetchResponse response;
  ASSERT_TRUE(a.fetch.HandleFetch(request, 18, now_, now_, &response, &can_wait, &parked).ok());
  ASSERT_TRUE(parked);
  string value = VoterValue(span(kVoters).subspan(1));
  array<ControlRecordValue, 1> control{{{6, value}}};
  ASSERT_TRUE(a.fetch.Append(MakeControlBatch(1, 5, 100, control).value(), 5).ok());
  EXPECT_FALSE(parked->Records(1024));
  EXPECT_EQ(a.campaign.voters().size(), 2u);
  EXPECT_EQ(a.fetch.membership().LastVoterOffset(), 1);
  exchange(&b);
  exchange(&b);
  EXPECT_EQ(a.fetch.HighWatermark(), 1);
  EXPECT_EQ(a.campaign.state().role, QuorumRole::kLeader);
  exchange(&c);
  exchange(&c);
  EXPECT_EQ(a.fetch.HighWatermark(), 2);
  EXPECT_EQ(a.campaign.state().role, QuorumRole::kResigned);
  EXPECT_FALSE(a.fetch.Append(Batch(2, 5), 5).ok());
}

TEST_F(FetchReplicationTest, FirstDynamicLeaderPublishesBootstrapVotersInInitialBatch) {
  vector<CampaignVoter> one{kVoters.front()};
  auto a = Make(kA, 10, one);
  auto envelope = test::SnapshotBytes(0);
  auto prefix = RecordBatchSize(envelope).value();
  auto data = RecordBatchSize(string_view(envelope).substr(prefix)).value();
  auto footer = envelope.substr(prefix + data);
  absl::big_endian::Store64(footer.data(), 3);
  KRaftVersionRecord version;
  version.k_raft_version = 1;
  string version_bytes, members = VoterValue(one);
  ASSERT_EQ(Encode(version, 0, &version_bytes), CodecError::kOk);
  array<ControlRecordValue, 2> controls{{{5, version_bytes}, {6, members}}};
  auto snapshot = envelope.substr(0, prefix) + MakeControlBatch(1, 0, 0, controls).value() + footer;
  auto writer = a.log->BeginSnapshot({0, 0}, snapshot.size()).value();
  ASSERT_TRUE(writer->Append(0, snapshot).ok());
  ASSERT_TRUE(a.log->InstallSnapshot(writer.get(), 0).ok());
  ASSERT_TRUE(a.fetch.RefreshMembership().ok());
  EXPECT_EQ(a.fetch.membership().LastVoterOffset(), -1);
  ASSERT_FALSE(a.campaign.Recover({}, a.log->End()));
  ASSERT_TRUE(a.fetch.SyncRole(100).ok());
  vector<RecordView> records;
  auto bytes = a.log->Read(0, 3, 1).value();
  ASSERT_TRUE(DecodeRecordBatch(bytes, &records).ok());
  ASSERT_EQ(records.size(), 3u);
  EXPECT_EQ(absl::big_endian::Load16(records[0].key->data() + 2), 2);
  EXPECT_EQ(*records[1].value, version_bytes);
  EXPECT_EQ(*records[2].value, members);
  EXPECT_EQ(a.fetch.membership().LastVoterOffset(), 2);
  EXPECT_EQ(a.fetch.HighWatermark(), 3);
}

}  // namespace
}  // namespace dfly::cluster::raft
