// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/quorum/snapshot_transfer.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include <fstream>

#include "server/cluster/raft/log/testdata/snapshot_fixture.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;
namespace fs = filesystem;

constexpr ReplicaKey kLeader{0, {1}}, kPeer{1, {2}};
constexpr LogPosition kCut{3, 8};
const vector<CampaignVoter> kVoters{{kLeader, {"a", 9400}}, {kPeer, {"b", 9401}}};

struct Replica {
  Replica(const fs::path& dir, ReplicaKey key)
      : log(PartitionLog::Open(dir).value()),
        campaign({"c", kDataTopic, 10, key}, kVoters,
                 [](const ElectionState&) { return error_code{}; }),
        epochs(campaign, "RAFT", 100, 100),
        fetch(campaign, epochs, *log, {99}),
        snapshots(campaign, epochs, fetch, *log) {
  }
  unique_ptr<PartitionLog> log;
  ElectionCampaign campaign;
  EpochControl epochs;
  FetchReplication fetch;
  SnapshotTransfer snapshots;
};

class SnapshotTransferTest : public testing::Test {
 protected:
  void SetUp() override {
    string temp = (fs::temp_directory_path() / "dfly-snapshot-transfer-XXXXXX").string();
    ASSERT_NE(mkdtemp(temp.data()), nullptr);
    dir_ = temp;
    a = make_unique<Replica>(dir_ / "a", kLeader);
    b = make_unique<Replica>(dir_ / "b", kPeer);
    data_ = test::SnapshotBytes(kCut.epoch);
    auto writer = a->log->BeginSnapshot(kCut, data_.size()).value();
    ASSERT_TRUE(writer->Append(0, data_).ok());
    ASSERT_TRUE(a->log->InstallSnapshot(writer.get(), 0).ok());
    ASSERT_FALSE(a->campaign.Recover({5, {}, kLeader, {}}, a->log->End()));
    ASSERT_FALSE(b->campaign.Recover({}, b->log->End()));
    auto vote = a->campaign.PrepareVote(kPeer, a->log->End()).value();
    VoteResponse response;
    ASSERT_FALSE(b->campaign.ReceiveVote(vote.request, 2, b->log->End(), &response));
    ASSERT_FALSE(a->campaign.ReceiveVoteResponse(kPeer, vote.sequence, response));
    ASSERT_TRUE(a->fetch.SyncRole(100).ok());
    ASSERT_FALSE(b->campaign.ObserveLeader(5, kLeader.id, nullopt));
    FetchHint();
  }

  void FetchHint() {
    ASSERT_TRUE(b->fetch.SyncRole(110).ok());
    auto req = b->fetch.PrepareFetch(0, 18, 0, 8).value();
    FetchResponse hint;
    bool wait = true;
    ASSERT_TRUE(a->fetch.HandleFetch(req.request, 18, 120, 124000, &hint, &wait).ok());
    ASSERT_TRUE(b->fetch.ReceiveFetchResponse(0, req.sequence, hint).ok());
    ASSERT_EQ(b->fetch.RequestedSnapshot(), kCut);
  }

  void TearDown() override {
    a.reset();
    b.reset();
    if (!dir_.empty())
      fs::remove_all(dir_);
  }

  FetchSnapshotResponse Handle(const OutboundSnapshotFetch& req, int64_t now = 200) {
    string wire;
    FetchSnapshotRequest request;
    EXPECT_EQ(Encode(req.request, req.version, &wire), CodecError::kOk);
    EXPECT_EQ(Decode(wire, req.version, &request), CodecError::kOk);
    FetchSnapshotResponse response, decoded;
    EXPECT_TRUE(a->snapshots.HandleRequest(request, req.version, now, &response).ok());
    EXPECT_EQ(Encode(response, req.version, &wire), CodecError::kOk);
    EXPECT_EQ(Decode(wire, req.version, &decoded), CodecError::kOk);
    return decoded;
  }

  bool HasPartial() const {
    for (const auto& entry : fs::directory_iterator(dir_ / "b")) {
      if (entry.path().extension() == ".part")
        return true;
    }
    return false;
  }

  fs::path dir_;
  string data_;
  unique_ptr<Replica> a, b;
};

TEST_F(SnapshotTransferTest, ChunkPositionsAreByteOffsetsAndInstallIsAtomic) {
  int64_t written = 0;
  const int32_t limit = 11;
  while (written < static_cast<int64_t>(data_.size())) {
    auto out = b->snapshots.Prepare(1, limit).value();
    const auto& partition = out.request.topics[0].partitions[0];
    ASSERT_EQ(partition.position, written);
    EXPECT_EQ(partition.replica_directory_id, kPeer.directory_id);
    EXPECT_FALSE(b->log->LatestSnapshot().has_value());
    EXPECT_EQ(b->log->End().end_offset, 0);
    auto response = Handle(out);
    const auto& part = response.topics[0].partitions[0];
    ASSERT_EQ(part.error_code, 0);
    ASSERT_EQ(part.position, written);
    ASSERT_EQ(response.node_endpoints.size(), 1u);
    ASSERT_EQ(part.unaligned_records.data.value(), data_.substr(written, limit));
    written += part.unaligned_records.data->size();
    bool reset;
    ASSERT_TRUE(b->snapshots.ReceiveResponse(out.peer_id, out.sequence, response, &reset).ok());
    EXPECT_TRUE(reset);
  }
  EXPECT_FALSE(b->fetch.RequestedSnapshot());
  EXPECT_FALSE(b->snapshots.Prepare(1, 10));
  EXPECT_FALSE(HasPartial());
  EXPECT_EQ(b->log->End(), kCut);
  EXPECT_EQ(b->fetch.HighWatermark(), kCut.end_offset);
  EXPECT_EQ(b->fetch.PrepareFetch(0, 18, 0, 10)->request.topics[0].partitions[0].fetch_offset, 8);
  EXPECT_FALSE(a->fetch.HighWatermark());
}

TEST_F(SnapshotTransferTest, MalformedAndUnavailableRequestsHaveCorrectErrorScope) {
  auto out = b->snapshots.Prepare(0, 7).value();
  EXPECT_EQ(out.request.topics[0].partitions[0].replica_directory_id, DirectoryId{});
  auto response = Handle(out);
  EXPECT_TRUE(response.node_endpoints.empty());
  out.request.cluster_id = "wrong";
  EXPECT_EQ(Handle(out).error_code, 104);
  out.request.cluster_id.reset();
  auto& part = out.request.topics[0].partitions[0];
  part.partition = 20;
  response = Handle(out);
  EXPECT_EQ(response.topics[0].partitions[0].error_code, 3);
  EXPECT_EQ(response.topics[0].partitions[0].index, 20);
  part.partition = 10;
  part.current_leader_epoch = 4;
  EXPECT_EQ(Handle(out).topics[0].partitions[0].error_code, 74);
  part.current_leader_epoch = 6;
  EXPECT_EQ(Handle(out).topics[0].partitions[0].error_code, 75);
  part.current_leader_epoch = 5;
  part.position = -1;
  EXPECT_EQ(Handle(out).topics[0].partitions[0].error_code, 99);
  part.position = data_.size();
  EXPECT_EQ(Handle(out).topics[0].partitions[0].error_code, 99);
  part.snapshot_id = {};
  EXPECT_EQ(Handle(out).topics[0].partitions[0].error_code, 98);
  out.request.topics[0].partitions.clear();
  EXPECT_EQ(Handle(out).error_code, 42);
}

TEST_F(SnapshotTransferTest, DataSnapshotSlicesUse64BitBytePositions) {
  // A sparse extent exercises the same file offsets as a multi-GiB KV image.
  const auto position = int64_t{INT32_MAX} + 127;
  fstream file(dir_ / "a" / SnapshotName(kCut), ios::in | ios::out | ios::binary);
  file.seekp(position);
  file.write("tail", 4);
  file.close();
  ASSERT_TRUE(file.good());
  auto request = b->snapshots.Prepare(1, 16).value();
  auto& input = request.request.topics[0].partitions[0];
  input.position = position;
  auto response = Handle(request);
  const auto& slice = response.topics[0].partitions[0];
  ASSERT_EQ(slice.error_code, 0);
  EXPECT_EQ(slice.position, position);
  EXPECT_EQ(slice.size, position + 4);
  EXPECT_EQ(slice.unaligned_records.data.value(), "tail");
  input.position = position + 4;
  EXPECT_EQ(Handle(request).topics[0].partitions[0].error_code, 99);
}

TEST_F(SnapshotTransferTest, InvalidApplicationImagePreservesDurablePrefixAndCanRetry) {
  array<string, 1> values{"prior"};
  string prior = MakeDataBatch(0, 1, 100, values).value();
  ASSERT_TRUE(b->log->AppendAsFollower(prior, 1).ok());
  bool accept = false;
  SnapshotTransfer transfer(b->campaign, b->epochs, b->fetch, *b->log,
                            [&](const SnapshotWriter& file) {
                              EXPECT_EQ(file.id(), kCut);
                              auto bytes = file.Read(0, file.size());
                              EXPECT_TRUE(bytes.ok());
                              if (!bytes.ok())
                                return bytes.status();
                              EXPECT_EQ(bytes->bytes, data_);
                              return accept ? absl::OkStatus() : absl::DataLossError("wrong image");
                            });
  auto request = transfer.Prepare(1, data_.size()).value();
  bool reset;
  EXPECT_TRUE(
      absl::IsDataLoss(transfer.ReceiveResponse(0, request.sequence, Handle(request), &reset)));
  EXPECT_FALSE(reset);
  EXPECT_FALSE(b->log->LatestSnapshot());
  EXPECT_FALSE(HasPartial());
  EXPECT_EQ(b->log->StartOffset(), 0);
  EXPECT_EQ(b->log->Read(0, 1, prior.size()).value(), prior);
  accept = true;
  request = transfer.Prepare(1, data_.size()).value();
  EXPECT_EQ(request.request.topics[0].partitions[0].position, 0);
  ASSERT_TRUE(transfer.ReceiveResponse(0, request.sequence, Handle(request), &reset).ok());
  EXPECT_EQ(b->log->StartOffset(), kCut.end_offset);
  EXPECT_EQ(b->fetch.HighWatermark(), kCut.end_offset);
}

TEST_F(SnapshotTransferTest, SnapshotContactExtendsQuorumDeadlineWithoutAcknowledgingLog) {
  vector<EpochNotification> messages;
  ASSERT_FALSE(a->epochs.Poll(100, &messages));
  auto observer = b->snapshots.Prepare(1, 12).value();
  observer.request.replica_id = 5;
  EXPECT_EQ(Handle(observer, 180).topics[0].partitions[0].error_code, 0);
  auto voter = b->snapshots.Prepare(1, 12).value();
  EXPECT_EQ(Handle(voter, 220).topics[0].partitions[0].error_code, 0);
  ASSERT_FALSE(a->epochs.Poll(369, &messages));
  EXPECT_EQ(a->campaign.state().role, QuorumRole::kLeader);
  EXPECT_FALSE(a->fetch.HighWatermark());
  ASSERT_FALSE(a->epochs.Poll(370, &messages));
  EXPECT_EQ(a->campaign.state().role, QuorumRole::kResigned);
  EXPECT_EQ(Handle(voter, 371).topics[0].partitions[0].error_code, 6);
}

TEST_F(SnapshotTransferTest, RejectsWrongChunkAndReleasesPartialOnNewLeader) {
  auto first = b->snapshots.Prepare(1, 16).value();
  auto response = Handle(first);
  bool reset = false;
  ASSERT_TRUE(b->snapshots.ReceiveResponse(0, first.sequence, response, &reset).ok());
  EXPECT_TRUE(HasPartial());
  auto next = b->snapshots.Prepare(1, 16).value();
  ASSERT_TRUE(b->snapshots.ReceiveResponse(0, first.sequence, response, &reset).ok());
  EXPECT_FALSE(reset);
  auto altered = Handle(next);
  altered.topics[0].partitions[0].size++;
  EXPECT_FALSE(b->snapshots.ReceiveResponse(0, next.sequence, altered, &reset).ok());
  next = b->snapshots.Prepare(1, 16).value();
  EXPECT_EQ(next.request.topics[0].partitions[0].position, 16);
  response = Handle(next);
  response.topics[0].partitions[0].current_leader.leader_epoch++;
  response.topics[0].partitions[0].current_leader.leader_id = -1;
  response.topics[0].partitions[0].error_code = 74;
  ASSERT_TRUE(b->snapshots.ReceiveResponse(0, next.sequence, response, &reset).ok());
  EXPECT_FALSE(reset);
  EXPECT_EQ(b->campaign.state().election.epoch, 6);
  EXPECT_FALSE(b->fetch.RequestedSnapshot());
  EXPECT_FALSE(HasPartial());
  EXPECT_FALSE(b->log->LatestSnapshot());
}

TEST_F(SnapshotTransferTest, InvalidVoterConfigurationPreservesPreviousCut) {
  KRaftVersionRecord version;
  version.k_raft_version = 1;
  string encoded;
  ASSERT_EQ(Encode(version, 0, &encoded), CodecError::kOk);
  const array<ControlRecordValue, 1> controls{{{5, encoded}}};
  auto header = MakeSnapshotHeaderBatch(kCut.epoch, 123, 100, controls).value();
  string invalid = header + MakeSnapshotFooterBatch(2, kCut.epoch, 123).value();

  auto request = b->snapshots.Prepare(1, invalid.size()).value();
  auto response = Handle(request);
  auto& part = response.topics[0].partitions[0];
  part.size = invalid.size();
  part.unaligned_records.data = invalid;
  bool reset = true;
  auto status = b->snapshots.ReceiveResponse(0, request.sequence, response, &reset);
  EXPECT_TRUE(absl::IsDataLoss(status)) << status;
  EXPECT_FALSE(reset);
  EXPECT_FALSE(b->log->LatestSnapshot());
  EXPECT_FALSE(HasPartial());
  EXPECT_EQ(b->log->End().end_offset, 0);
  EXPECT_EQ(b->fetch.RequestedSnapshot(), kCut);
}

TEST_F(SnapshotTransferTest, MissingSnapshotReturnsToFetchAndCorruptFileRestartsFromZero) {
  auto req = b->snapshots.Prepare(1, data_.size()).value();
  auto corrupt = Handle(req);
  corrupt.topics[0].partitions[0].unaligned_records.data->back() ^= 1;
  bool reset = true;
  EXPECT_FALSE(b->snapshots.ReceiveResponse(0, req.sequence, corrupt, &reset).ok());
  EXPECT_FALSE(reset);
  EXPECT_FALSE(b->log->LatestSnapshot());
  EXPECT_FALSE(HasPartial());
  auto retry = b->snapshots.Prepare(1, 20).value();
  EXPECT_EQ(retry.request.topics[0].partitions[0].position, 0);
  ASSERT_TRUE(b->snapshots.ReceiveResponse(0, retry.sequence, Handle(retry), &reset).ok());
  ASSERT_TRUE(HasPartial());
  auto next = b->snapshots.Prepare(1, 20).value();
  auto missing = Handle(next);
  missing.topics[0].partitions[0].error_code = 98;
  ASSERT_TRUE(b->snapshots.ReceiveResponse(0, next.sequence, missing, &reset).ok());
  EXPECT_TRUE(reset);
  EXPECT_FALSE(b->fetch.RequestedSnapshot());
  EXPECT_FALSE(HasPartial());
  EXPECT_EQ(b->fetch.PrepareFetch(0, 18, 0, 256)->request.topics[0].partitions[0].fetch_offset, 0);
}

TEST_F(SnapshotTransferTest, RestartResendsDataFetchBeforeDownloadingSnapshotAgain) {
  auto request = b->snapshots.Prepare(1, 13).value();
  bool reset;
  ASSERT_TRUE(b->snapshots.ReceiveResponse(0, request.sequence, Handle(request), &reset).ok());
  EXPECT_TRUE(HasPartial());
  b.reset();
  b = make_unique<Replica>(dir_ / "b", kPeer);
  EXPECT_FALSE(b->log->LatestSnapshot());
  ASSERT_FALSE(b->campaign.Recover({5, kLeader.id, {}, {}}, b->log->End()));
  FetchHint();
  auto retry = b->snapshots.Prepare(1, 32).value();
  EXPECT_EQ(retry.request.topics[0].partitions[0].position, 0);
}

}  // namespace
}  // namespace dfly::cluster::raft
