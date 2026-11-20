// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/metadata/bootstrap_topology.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include "server/cluster/raft/log/partition_log.h"
#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;

constexpr string_view kTopology = R"([
  {"group_id":10,"slot_ranges":[{"start":0,"end":8191}],
   "master":{"id":"A","ip":"a","port":6379,"broker_id":1,"raft_port":7100},
   "replicas":[{"id":"A-r1","ip":"a-r1","port":6379,"broker_id":2,"raft_port":7100}]},
  {"group_id":20,"slot_ranges":[{"start":8192,"end":16383}],
   "master":{"id":"B","ip":"b","port":6379,"broker_id":3,"raft_port":7100},
   "replicas":[{"id":"B-r1","ip":"b-r1","port":6379,"broker_id":4,"raft_port":7100}]}
])";

TEST(BootstrapTopologyTest, ImportsOriginalShardLayoutIntoOneCommittedTransaction) {
  auto records = ParseBootstrapTopology(kTopology);
  ASSERT_TRUE(records.ok()) << records.status();
  ASSERT_TRUE(holds_alternative<BeginTransactionRecord>(records->front().message));
  ASSERT_TRUE(holds_alternative<EndTransactionRecord>(records->back().message));
  string tmp = (filesystem::temp_directory_path() / "dfly-bootstrap-image-XXXXXX").string();
  ASSERT_NE(mkdtemp(tmp.data()), nullptr);
  auto log = PartitionLog::Open(tmp).value();
  MetadataLoader loader;
  for (const auto& record : *records) {
    auto value = EncodeMetadataRecord(record).value();
    EXPECT_EQ(DecodeMetadataRecord(value).value(), record);
    auto batch = MakeDataBatch(log->End().end_offset, 1, 10, span(&value, 1)).value();
    ASSERT_TRUE(log->Append(batch, 1).ok());
    if (!holds_alternative<EndTransactionRecord>(record.message)) {
      ASSERT_TRUE(loader.Advance(*log, log->End().end_offset).ok());
      EXPECT_TRUE(loader.transaction_open());
      EXPECT_TRUE(loader.image()->brokers.empty());
      EXPECT_TRUE(loader.image()->directory.groups().empty());
    }
  }
  ASSERT_TRUE(loader.Advance(*log, log->End().end_offset).ok());
  auto image = loader.image();
  EXPECT_TRUE(image->directory.HasCompleteCoverage());
  EXPECT_EQ(image->directory.Lookup(42)->group_id, 10);
  EXPECT_EQ(image->directory.Lookup(8192)->group_id, 20);
  EXPECT_EQ(image->directory.nodes().at(2), "A-r1");
  EXPECT_TRUE(image->brokers.at(2).fenced);
  EXPECT_FALSE(image->directory.Route(42, image->brokers));
  GroupLeaderRecord report;
  report.group_id = 10;
  report.leader_id = report.reporter_id = 2;
  report.leader_epoch = 9;
  EXPECT_FALSE(PrepareLeaderReport(*image, 1, report).ok());
  EXPECT_FALSE(PrepareLeaderReport(*image, 2, report).ok());

  auto broker = image->brokers.at(2);
  broker.broker_epoch = 55;
  broker.incarnation_id.back() = 7;
  broker.fenced = false;
  auto value = EncodeMetadataRecord({broker, 4}).value();
  ASSERT_TRUE(
      log->Append(MakeDataBatch(log->End().end_offset, 1, 10, span(&value, 1)).value(), 1).ok());
  ASSERT_TRUE(loader.Advance(*log, log->End().end_offset).ok());
  report.reporter_epoch = 55;
  auto proposed = PrepareLeaderReport(*loader.image(), 2, report);
  ASSERT_TRUE(proposed.ok() && proposed->has_value());
  value = EncodeMetadataRecord(**proposed).value();
  ASSERT_TRUE(
      log->Append(MakeDataBatch(log->End().end_offset, 1, 10, span(&value, 1)).value(), 1).ok());
  ASSERT_TRUE(loader.Advance(*log, log->End().end_offset).ok());
  EXPECT_FALSE(PrepareLeaderReport(*loader.image(), 2, report).value());
  ASSERT_EQ(loader.image()->directory.Route(42, loader.image()->brokers)->node_id, "A-r1");
  EXPECT_FALSE(image->directory.Route(42, image->brokers));
  log.reset();
  filesystem::remove_all(tmp);
}

TEST(BootstrapTopologyTest, RejectsInvalidShardsBeforeProposingAnyRecords) {
  for (auto [from, to] : {pair<string_view, string_view>{"8192", "8193"},
                          {"8192", "8191"},
                          {"16383", "99999"},
                          {"16383", "-1"},
                          {"\"A-r1\"", "\"A\""},
                          {"\"a-r1\"", "\"a\""},
                          {"\"group_id\":20", "\"group_id\":10"},
                          {"\"broker_id\":4", "\"broker_id\":3"},
                          {"\"raft_port\":7100", "\"raft_port\":0"}}) {
    string json(kTopology);
    json.replace(json.find(from), from.size(), to);
    EXPECT_FALSE(ParseBootstrapTopology(json).ok()) << json;
  }
}

}  // namespace
}  // namespace dfly::cluster::raft
