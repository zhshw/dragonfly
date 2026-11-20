// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/admin_command.h"

#include <gtest/gtest.h>

#include <algorithm>

#include "server/cluster/raft/metadata/metadata_image.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
using namespace protocol;
using Kind = AdminReply::Kind;

class AdminCommandTest : public testing::Test {
 protected:
  void SetUp() override {
    image_.features["dragonfly.cluster"] = 1;
    for (int id : {10, 20}) {
      ASSERT_TRUE(image_.directory.Replay(RedisNodeRecord{id, to_string(id), {}}, {}, true).ok());
      ASSERT_TRUE(image_.directory.Replay(DataGroupRecord{id, {id}, {}}).ok());
    }
    ASSERT_TRUE(image_.directory.Replay(SlotAssignmentRecord{0, 8191, -1, 0, 10, 1, {}}).ok());
  }

  vector<MetadataRecord> Plan(initializer_list<string_view> args) {
    return PlanAdminCommand(image_, "c", args, &reply_);
  }
  MetadataImage image_;
  AdminReply reply_;
};

TEST_F(AdminCommandTest, StoresExactBytesWithIntentAndRebuildsRequestIndexFromSnapshot) {
  constexpr string_view value = R"({ "slot": 42, "from":10, "to":20, "generation":1 })";
  constexpr string_view key = "ctl:req:move-42-001";
  auto records = Plan({"set", key, value, "nx"});
  ASSERT_EQ(records.size(), 1u);
  EXPECT_EQ(reply_.kind, Kind::kSimple);
  EXPECT_EQ(reply_.text, "OK");
  EXPECT_TRUE(Plan({"GET", key}).empty());
  EXPECT_EQ(reply_.kind, Kind::kNull);

  auto bytes = EncodeMetadataRecord(records.front());
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  auto copy = DecodeMetadataRecord(*bytes);
  ASSERT_TRUE(copy.ok()) << copy.status();
  auto start = get<SlotMigrationRecord>(copy->message);
  EXPECT_EQ(start.admin_request.request_id, "move-42-001");
  ASSERT_TRUE(image_.migrations.Replay(start, false, &image_.directory).ok());
  EXPECT_TRUE(Plan({"SET", key, "changed", "NX"}).empty());
  EXPECT_EQ(reply_.kind, Kind::kNull);
  EXPECT_TRUE(Plan({"GET", key}).empty());
  EXPECT_EQ(reply_.kind, Kind::kBulk);
  EXPECT_EQ(reply_.text, value);

  SlotMigrationRequest cancel;
  cancel.cluster_id = "c";
  cancel.migration_id = start.migration_id;
  cancel.slot = 42;
  cancel.source_group = 10;
  cancel.target_group = 20;
  cancel.generation = 1;
  cancel.action = static_cast<int8_t>(MigrationAction::kCancel);
  SlotMigrationResponse response;
  auto next = ProposeSlotMigration(image_, "c", cancel, &response);
  ASSERT_EQ(next.size(), 1u);
  auto terminal = get<SlotMigrationRecord>(next.front().message);
  EXPECT_EQ(terminal.admin_request, start.admin_request);
  image_.migrations = {};
  ASSERT_TRUE(image_.migrations.Replay(terminal, true, &image_.directory).ok());
  EXPECT_TRUE(Plan({"GET", key}).empty());
  EXPECT_EQ(reply_.text, value);
  EXPECT_TRUE(Plan({"HGETALL", "ctl:op:move-42-001"}).empty());
  EXPECT_EQ(reply_.kind, Kind::kMap);
  EXPECT_NE(find(reply_.elements.begin(), reply_.elements.end(), "cancelled"),
            reply_.elements.end());
}

TEST_F(AdminCommandTest, ReadsCommittedSlotFieldsAndRejectsDirectOwnershipWrites) {
  EXPECT_TRUE(Plan({"HMGET", "ctl:slot:42", "generation", "missing", "owner"}).empty());
  EXPECT_EQ(reply_.kind, Kind::kArray);
  EXPECT_EQ(reply_.elements, (vector<optional<string>>{"1", nullopt, "10"}));
  EXPECT_TRUE(Plan({"HSET", "ctl:slot:42", "owner", "20"}).empty());
  EXPECT_EQ(reply_.kind, Kind::kError);
  EXPECT_TRUE(Plan({"HGETALL", "ctl:op:unknown"}).empty());
  EXPECT_EQ(reply_.kind, Kind::kMap);
  EXPECT_TRUE(reply_.elements.empty());
  EXPECT_TRUE(Plan({"HMGET", "ctl:slot:16384", "owner"}).empty());
  EXPECT_EQ(reply_.kind, Kind::kError);
  for (string_view value : {R"({"slot":42,"from":10,"to":20,"generation":2})",
                            R"({"slot":16384,"from":10,"to":20,"generation":1})",
                            R"({"slot":42,"from":"10","to":20,"generation":1})", "{}"}) {
    EXPECT_TRUE(Plan({"SET", "ctl:req:new", value, "NX"}).empty());
    EXPECT_EQ(reply_.kind, Kind::kError) << value;
  }
}
}  // namespace
}  // namespace dfly::cluster::raft
