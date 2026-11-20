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

  absl::Status Set(DbIndex db, string_view key, string_view value) override {
    ++sets_;
    if (!ready_)
      return absl::UnavailableError("-TRYAGAIN Group has no committed leader");
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
    ClusterShardInfo shard{.slot_ranges = SlotRanges({{0, 16383}}),
                           .master = {{"node-a", "127.0.0.1", 7001}, NodeHealth::ONLINE},
                           .replicas = {},
                           .migrations = {}};
    return DataTopology{ClusterShardInfos(shard), "node-a"};
  }

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

}  // namespace
}  // namespace dfly::cluster::raft
