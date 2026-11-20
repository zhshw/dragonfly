// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/facade_test.h"
#include "facade/reply_capture.h"
#include "server/cluster/raft/data/command_handler.h"
#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster/raft/data/set_executor.h"
#include "server/cluster_support.h"
#include "server/journal/executor.h"
#include "server/test_utils.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;

class DataDispatchTest : public BaseFamilyTest, public DataCommandHandler {
 protected:
  void SetUp() override {
    SetTestFlag("cluster_mode", "raft");
    BaseFamilyTest::SetUp();
  }

  vector<facade::Listener*> CreateListeners() override {
    service_->SetRaftDataCommands(this);
    return {};
  }

  absl::Status Set(DbIndex db, string_view key, string_view value,
                   DataWritePosition* pos) override {
    ++sets_;
    if (!ready_)
      return absl::UnavailableError("-TRYAGAIN Group has no committed leader");
    *pos = {10, 3, 8};
    JournalExecutor executor(service_.get());
    return ExecuteSetEffect(executor, {10, db, KeySlot(key), 0, string(key), string(value)});
  }

  absl::Status CheckRead(DbIndex, string_view) override {
    ++reads_;
    return ready_ ? absl::OkStatus() : absl::UnavailableError("-MOVED 42 127.0.0.1:7002");
  }

  absl::StatusOr<DataTopology> Topology() override {
    if (!ready_)
      return DataCommandHandler::Topology();
    DataTopology topology{.shards = {},
                          .local_node_id = "node-a",
                          .known_nodes = known_nodes_,
                          .assigned_groups = assigned_groups_};
    if (!routes_.empty()) {
      topology.shards =
          ClusterShardInfo{.slot_ranges = SlotRanges(routes_),
                           .master = {{"node-a", "127.0.0.1", 7001}, NodeHealth::ONLINE},
                           .replicas = {},
                           .migrations = {}};
    }
    return topology;
  }

  absl::StatusOr<DataReplicationState> ReplicationState() override {
    if (!ready_)
      return DataCommandHandler::ReplicationState();
    return DataReplicationState{.group_id = 10,
                                .epoch = epoch_,
                                .is_leader = leader_,
                                .local_applied_offset = 8,
                                .leader = {"node-a", "127.0.0.1", 7001},
                                .replicas = {{{"node-b", "127.0.0.1", 7002}, applied_}},
                                .connected_to_leader = !leader_};
  }

  int32_t epoch_ = 3;
  bool leader_ = true;
  optional<int64_t> applied_;
  vector<SlotRange> routes_{{0, 16383}};
  size_t known_nodes_ = 1;
  size_t assigned_groups_ = 1;
  bool ready_ = false;
  unsigned sets_ = 0;
  unsigned reads_ = 0;
};

TEST_F(DataDispatchTest, GateRunsBeforeTransactionsAndBypassesOnlyInternalReplay) {
  EXPECT_EQ(Run({"PING"}), "PONG");
  EXPECT_THAT(Run({"SET", "k", "v"}), facade::ErrArg("TRYAGAIN Group has no committed leader"));
  EXPECT_THAT(Run({"GET", "k"}), facade::ErrArg("MOVED 42 127.0.0.1:7002"));
  EXPECT_EQ(sets_, 1u);
  EXPECT_EQ(reads_, 1u);
  ready_ = true;
  EXPECT_THAT(Run({"GET", "k"}), facade::ArgType(facade::RespExpr::NIL));
  EXPECT_EQ(Run({"SET", "k", "v"}), "OK");
  EXPECT_EQ(sets_, 2u);
  EXPECT_EQ(Run({"GET", "k"}), "v");
  EXPECT_EQ(reads_, 3u);
  EXPECT_THAT(Run({"SET", "k", "v2", "NX"}), facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_THAT(Run({"DEL", "k"}), facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_THAT(Run({"MULTI"}), facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_THAT(Run({"EVAL", "return redis.call('DEL','k')", "0"}),
              facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_THAT(RunPrivileged({"DFLYCLUSTER", "CONFIG", "[]"}),
              facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_EQ(Run({"GET", "k"}), "v");
  EXPECT_EQ(sets_, 2u);
}

TEST_F(DataDispatchTest, PipelinedCommandsFallBackToOrderedSynchronousDispatch) {
  ready_ = true;
  RunMany({{"SET", "k", "bypass"}, {"GET", "k"}});
  EXPECT_EQ(sets_, 0u);
  pp_->at(0)->Await([&] {
    ConnectionContext connection(nullptr, acl::UserCredentials{});
    connection.ns = &namespaces->GetDefaultNamespace();
    facade::CapturingReplyBuilder capture;
    CommandContext cmd(&capture, &connection);
    array<string_view, 3> parts{"SET", "k", "value"};
    cmd.Assign(parts.begin(), parts.end(), parts.size());
    EXPECT_EQ(service_->DispatchCommandSimple(&cmd, facade::AsyncPreference::ONLY_ASYNC, nullptr),
              facade::DispatchResult::WOULD_BLOCK);
    EXPECT_FALSE(cmd.IsDeferredReply());
    EXPECT_EQ(sets_, 0u);
  });
  EXPECT_THAT(Run({"GET", "k"}), facade::ArgType(facade::RespExpr::NIL));
}

TEST_F(DataDispatchTest, ClusterDiscoveryUsesOnlyRuntimeTopology) {
  EXPECT_EQ(Run({"CLUSTER", "KEYSLOT", "foo{bar}baz"}), KeySlot("bar"));
  for (string_view sub : {"INFO", "NODES", "SLOTS", "MYID", "SHARDS"})
    EXPECT_THAT(Run({"CLUSTER", sub}),
                facade::ErrArg("CLUSTERDOWN Cluster metadata is unavailable"));
  ready_ = true;
  EXPECT_EQ(Run({"CLUSTER", "MYID"}), "node-a");
  EXPECT_THAT(Run({"CLUSTER", "NODES"}).GetString(),
              testing::HasSubstr("node-a 127.0.0.1:7001@7001 myself,master"));
  EXPECT_THAT(Run({"CLUSTER", "INFO"}).GetString(),
              testing::HasSubstr("cluster_slots_assigned:16384"));
  EXPECT_THAT(Run({"CLUSTER", "SLOTS"}), facade::ArgType(facade::RespExpr::ARRAY));
  EXPECT_THAT(Run({"CLUSTER", "SHARDS"}), facade::ArgType(facade::RespExpr::ARRAY));
}

TEST_F(DataDispatchTest, ClusterInfoDistinguishesAssignedSlotsFromRoutableSlots) {
  ready_ = true;
  known_nodes_ = 4;
  assigned_groups_ = 2;
  routes_ = {{0, 4095}, {8192, 12287}};
  InfoReply partial(Run({"CLUSTER", "INFO"}).GetString());
  EXPECT_EQ(partial.Find("cluster_state"), "fail");
  EXPECT_EQ(partial.GetInt("cluster_slots_assigned"), 16384);
  EXPECT_EQ(partial.GetInt("cluster_slots_ok"), 8192);
  EXPECT_EQ(partial.GetInt("cluster_slots_fail"), 8192);
  EXPECT_EQ(partial.GetInt("cluster_known_nodes"), 4);
  EXPECT_EQ(partial.GetInt("cluster_size"), 2);
  EXPECT_EQ(Run({"CLUSTER", "SLOTS"}).GetVec().size(), 2u);
  routes_.clear();
  InfoReply unroutable(Run({"CLUSTER", "INFO"}).GetString());
  EXPECT_EQ(unroutable.GetInt("cluster_slots_assigned"), 16384);
  EXPECT_EQ(unroutable.GetInt("cluster_slots_ok"), 0);
  EXPECT_EQ(unroutable.GetInt("cluster_slots_fail"), 16384);
  EXPECT_TRUE(Run({"CLUSTER", "SLOTS"}).GetVec().empty());
  EXPECT_EQ(Run({"CLUSTER", "MYID"}), "node-a");
}

TEST_F(DataDispatchTest, WaitUsesThisConnectionsAppliedPositionAndFencesOldTerms) {
  ready_ = true;
  std::array<string_view, 3> set{"SET", "k", "v"};
  std::array<string_view, 3> wait{"WAIT", "1", "1"};
  EXPECT_EQ(Run("writer", set), "OK");
  applied_ = 7;
  EXPECT_EQ(Run("writer", wait), 0);
  EXPECT_EQ(Run("other", wait), 1);
  applied_ = 8;
  EXPECT_EQ(Run("writer", wait), 1);
  ++epoch_;
  EXPECT_THAT(Run("writer", wait),
              facade::ErrArg("TRYAGAIN Data group leadership changed during WAIT"));
  std::array<string_view, 1> reset{"RESET"};
  EXPECT_EQ(Run("writer", reset), "RESET");
  EXPECT_EQ(Run("writer", wait), 1);
  leader_ = false;
  EXPECT_THAT(Run("writer", wait), facade::ErrArg("WAIT cannot be used with replica instances."));
}

TEST_F(DataDispatchTest, RoleAndReplicationInfoUseTheSameLiveGroup) {
  using facade::RespElementsAre;
  EXPECT_THAT(Run({"ROLE"}), facade::ErrArg("CLUSTERDOWN Data group has no known leader"));
  ready_ = true;
  EXPECT_THAT(Run({"ROLE"}), RespElementsAre("master", facade::ArrLen(0)));
  applied_ = 7;
  EXPECT_THAT(
      Run({"ROLE"}),
      RespElementsAre("master", RespElementsAre(RespElementsAre("127.0.0.1", "7002", "online"))));
  auto info = Run({"INFO", "REPLICATION"}).GetString();
  EXPECT_THAT(info, testing::HasSubstr("role:master"));
  EXPECT_THAT(info, testing::HasSubstr("connected_slaves:1"));
  EXPECT_THAT(info, testing::HasSubstr("port=7002,state=online,offset=7"));
  leader_ = false;
  EXPECT_THAT(Run({"ROLE"}), RespElementsAre("slave", "127.0.0.1", "7001", "online"));
  info = Run({"INFO", "REPLICATION"}).GetString();
  EXPECT_THAT(info, testing::HasSubstr("master_port:7001"));
  EXPECT_THAT(info, testing::HasSubstr("master_link_status:up"));
  EXPECT_THAT(info, testing::HasSubstr("slave_repl_offset:8"));
  SetTestFlag("info_replication_valkey_compatible", "false");
  EXPECT_THAT(Run({"ROLE"}), RespElementsAre("replica", "127.0.0.1", "7001", "stable_sync"));
}

}  // namespace
}  // namespace dfly::cluster::raft
