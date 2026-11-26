// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/facade_test.h"
#include "facade/reply_capture.h"
#include "server/cluster/raft/data/command_handler.h"
#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster/raft/data/set_executor.h"
#include "server/cluster/raft/data/write_progress.h"
#include "server/cluster_support.h"
#include "server/journal/executor.h"
#include "server/test_utils.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;

TEST(WriteProgressTest, BindingAndEpochFenceProgressAndTerminalErrors) {
  auto epoch = make_shared<WriteEpoch>(4);
  WatermarkWrite first(epoch), rejected(epoch), next_epoch(make_shared<WriteEpoch>(5));
  epoch->reply_offset.store(9);
  EXPECT_FALSE(first.IsReady());
  first.Bind({10, 4, 8});
  EXPECT_TRUE(first.IsReady());
  first.Reject(absl::UnavailableError("late failure"));
  EXPECT_TRUE(first.status.ok());
  rejected.Reject(absl::DeadlineExceededError("expired before binding"));
  rejected.Bind({10, 4, 8});
  EXPECT_TRUE(rejected.IsReady());
  EXPECT_TRUE(absl::IsDeadlineExceeded(rejected.status));
  EXPECT_LT(rejected.position.end_offset, 0);
  next_epoch.Bind({10, 5, 8});
  EXPECT_FALSE(next_epoch.IsReady());
}

TEST(WriteProgressTest, HeadSubscriptionSurvivesNonReadyProgress) {
  auto epoch = make_shared<WriteEpoch>(4);
  WatermarkWrite head(epoch);
  head.Bind({10, 4, 8});
  unsigned wakes = 0;
  auto wake = [&] { ++wakes; };
  util::fb2::detail::Waiter waiter{wake};
  auto subscription = head.Subscribe(&waiter);
  ASSERT_TRUE(subscription);
  epoch->reply_offset.store(7);
  epoch->changed.notifyAll();
  EXPECT_FALSE(head.IsReady());
  epoch->reply_offset.store(8);
  epoch->changed.notifyAll();
  EXPECT_TRUE(head.IsReady());
  EXPECT_EQ(wakes, 2u);
  subscription.reset();
  epoch->changed.notifyAll();
  EXPECT_EQ(wakes, 2u);
}

class DataDispatchTest : public BaseFamilyTest, public DataCommandHandler {
 protected:
  struct TestWrite : PendingDataWrite {
    cmn::BackedArguments input;
    util::fb2::EmbeddedBlockingCounter ready{1};
    bool IsReady() const override {
      return ready.IsCompleted();
    }
    void Wait() override {
      ready.Wait();
    }
    std::optional<util::fb2::EventCount::SubKey> Subscribe(
        util::fb2::detail::Waiter* waiter) override {
      return ready.OnCompletion(waiter);
    }
  };

  void SetUp() override {
    SetTestFlag("cluster_mode", "raft");
    BaseFamilyTest::SetUp();
  }

  vector<facade::Listener*> CreateListeners() override {
    service_->SetRaftDataCommands(this);
    return {};
  }

  bool SupportsAsyncSet() const override {
    return async_enabled_;
  }

  shared_ptr<PendingDataWrite> TrySet(DbIndex, string_view key, string_view value) override {
    if (pending_.size() == 2)
      return nullptr;
    submitted_.emplace_back(key, value);
    pending_.push_back(make_shared<TestWrite>());
    return pending_.back();
  }

  shared_ptr<PendingDataWrite> TrySetOwned(DbIndex db, cmn::BackedArguments* args) override {
    auto write = TrySet(db, (*args)[1], (*args)[2]);
    if (write)
      static_cast<TestWrite&>(*write).input.SwapArgs(*args);
    return write;
  }

  absl::Status Set(DbIndex db, string_view key, string_view value,
                   DataWritePosition* pos) override {
    ++sets_;
    if (!ready_)
      return absl::UnavailableError("-TRYAGAIN Group has no committed leader");
    if (!leader_)
      return absl::FailedPreconditionError("-MOVED 42 127.0.0.1:7002");
    *pos = {10, 3, 8};
    JournalExecutor executor(service_.get());
    return ExecuteSetEffect(executor, {10, db, KeySlot(key), 0, string(key), string(value)});
  }

  absl::StatusOr<bool> SetIf(DbIndex db, string_view key, string_view value, SetCondition condition,
                             DataWritePosition* position) override {
    JournalExecutor executor(service_.get());
    auto exists = ExistsForSet(executor, db, key);
    if (!exists.ok())
      return exists.status();
    if (*exists != (condition == SetCondition::kIfExists))
      return false;
    auto status = Set(db, key, value, position);
    if (!status.ok())
      return status;
    return true;
  }

  absl::StatusOr<optional<string>> Get(DbIndex db, string_view key, DataReadMode mode) override {
    ++reads_;
    if (!ready_ || (!leader_ && mode != DataReadMode::kAllowFollower))
      return absl::UnavailableError("-MOVED 42 127.0.0.1:7002");
    JournalExecutor executor(service_.get());
    return ReadCommittedValue(executor, db, key);
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
                                .connected_to_leader = !leader_,
                                .await_change = replica_wait_};
  }

  int32_t epoch_ = 3;
  bool leader_ = true;
  optional<int64_t> applied_;
  function<void(chrono::steady_clock::time_point)> replica_wait_;
  vector<SlotRange> routes_{{0, 16383}};
  size_t known_nodes_ = 1;
  size_t assigned_groups_ = 1;
  bool async_enabled_ = false;
  vector<shared_ptr<TestWrite>> pending_;
  vector<pair<string, string>> submitted_;
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
  EXPECT_THAT(Run({"SET", "k", "v2", "NX"}), facade::ArgType(facade::RespExpr::NIL));
  EXPECT_THAT(Run({"SET", "k", "v2", "EX", "3"}), facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_THAT(Run({"DEL", "k"}), facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_THAT(Run({"MULTI"}), facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_THAT(Run({"EVAL", "return redis.call('DEL','k')", "0"}),
              facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_THAT(RunPrivileged({"DFLYCLUSTER", "CONFIG", "[]"}),
              facade::ArgType(facade::RespExpr::ERROR));
  EXPECT_EQ(Run({"GET", "k"}), "v");
  EXPECT_EQ(sets_, 2u);
  EXPECT_EQ(Run({"SELECT", "1"}), "OK");
  EXPECT_THAT(Run({"GET", "k"}), facade::ArgType(facade::RespExpr::NIL));
  EXPECT_EQ(Run({"SET", "k", ""}), "OK");
  EXPECT_EQ(Run({"GET", "k"}), "");
  const string binary(2048, '\0');
  EXPECT_EQ(Run({"SET", "k", binary}), "OK");
  EXPECT_EQ(Run({"GET", "k"}).GetString(), binary);
}

TEST_F(DataDispatchTest, ReadOnlyAllowsFollowerReadsOnlyOnThisConnectionUntilReadWriteOrReset) {
  ready_ = true;
  const string binary("value\0\xff", 7);
  ASSERT_EQ(Run({"SET", "k", binary}), "OK");
  leader_ = false;
  auto run = [&](string_view connection, initializer_list<string_view> args) {
    return Run(connection, facade::ArgSlice{args.begin(), args.size()});
  };
  auto moved = facade::ErrArg("MOVED 42 127.0.0.1:7002");
  EXPECT_THAT(run("reader", {"GET", "k"}), moved);
  EXPECT_EQ(run("reader", {"READONLY"}), "OK");
  EXPECT_EQ(run("reader", {"GET", "k"}).GetString(), binary);
  EXPECT_THAT(run("reader", {"GET", "missing"}), facade::ArgType(facade::RespExpr::NIL));
  EXPECT_THAT(run("other", {"GET", "k"}), moved);
  EXPECT_THAT(run("reader", {"SET", "k", "wrong"}), moved);
  EXPECT_EQ(run("reader", {"GET", "k"}).GetString(), binary);
  EXPECT_EQ(run("reader", {"READWRITE"}), "OK");
  EXPECT_THAT(run("reader", {"GET", "k"}), moved);
  EXPECT_EQ(run("reader", {"READONLY"}), "OK");
  EXPECT_EQ(run("reader", {"GET", "k"}).GetString(), binary);
  EXPECT_EQ(run("reader", {"RESET"}), "RESET");
  EXPECT_THAT(run("reader", {"GET", "k"}), moved);

  // READONLY permits replica reads; it does not prohibit writes on the leader.
  leader_ = true;
  EXPECT_EQ(run("reader", {"READONLY"}), "OK");
  EXPECT_EQ(run("reader", {"SET", "k", "leader value"}), "OK");
  EXPECT_EQ(run("reader", {"GET", "k"}), "leader value");
}

TEST_F(DataDispatchTest, ConditionalSetReturnsNullOrOkWithoutOverwritingOnFailure) {
  ready_ = true;
  EXPECT_THAT(Run({"SET", "k", "v1", "XX"}), facade::ArgType(facade::RespExpr::NIL));
  EXPECT_THAT(Run({"GET", "k"}), facade::ArgType(facade::RespExpr::NIL));
  EXPECT_EQ(Run({"SET", "k", "v2", "nx", "NX"}), "OK");
  EXPECT_THAT(Run({"SET", "k", "v3", "NX"}), facade::ArgType(facade::RespExpr::NIL));
  EXPECT_EQ(Run({"GET", "k"}), "v2");
  EXPECT_EQ(Run({"SET", "k", "v4", "xX"}), "OK");
  EXPECT_EQ(Run({"GET", "k"}), "v4");
  EXPECT_THAT(Run({"SET", "k", "v5", "NX", "XX"}), facade::ArgType(facade::RespExpr::ERROR));
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

TEST_F(DataDispatchTest, AsyncSetsSubmitBeforeConfirmationAndKeepBarriers) {
  async_enabled_ = true;
  pp_->at(0)->Await([&] {
    ConnectionContext connection(nullptr, acl::UserCredentials{});
    connection.ns = &namespaces->GetDefaultNamespace();
    connection.skip_acl_validation = true;
    facade::CapturingReplyBuilder capture;
    auto command = [&](initializer_list<string_view> parts) {
      auto cmd = make_unique<CommandContext>(&capture, &connection);
      cmd->Assign(parts.begin(), parts.end(), parts.size());
      return cmd;
    };
    auto first = command({"SET", "same", "first"});
    const string large_value(256U << 10, '\0');
    auto second = command({"SET", "same", large_value});
    const char* value_data = second->back().data();
    EXPECT_EQ(service_->DispatchCommandSimple(first.get(), facade::AsyncPreference::PREFER_ASYNC,
                                              nullptr),
              facade::DispatchResult::OK);
    EXPECT_EQ(
        service_->DispatchCommandSimple(second.get(), facade::AsyncPreference::ONLY_ASYNC, nullptr),
        facade::DispatchResult::OK);
    ASSERT_EQ(pending_.size(), 2u);
    EXPECT_EQ(submitted_[0], make_pair(string("same"), string("first")));
    EXPECT_EQ(submitted_[1], make_pair(string("same"), large_value));
    EXPECT_TRUE(first->empty());
    EXPECT_TRUE(second->empty());
    EXPECT_TRUE(first->tail_args().empty());
    EXPECT_TRUE(second->tail_args().empty());
    EXPECT_EQ(pending_[0]->input[2], "first");
    EXPECT_EQ(pending_[1]->input[2].data(), value_data);
    EXPECT_EQ(pending_[1]->input[2], large_value);
    EXPECT_FALSE(first->CanReply());
    EXPECT_FALSE(second->CanReply());
    EXPECT_FALSE(connection.conn_state.raft_last_write);
    EXPECT_EQ(sets_, 0u);
    EXPECT_EQ(capture.Take().index(), 0u);

    // Saturation leaves the command intact and permits retry before it reaches the head.
    auto full = command({"SET", "other", "third"});
    EXPECT_EQ(
        service_->DispatchCommandSimple(full.get(), facade::AsyncPreference::ONLY_ASYNC, nullptr),
        facade::DispatchResult::ASYNC_BACKPRESSURE);
    EXPECT_FALSE(full->IsDeferredReply());
    EXPECT_EQ(pending_.size(), 2u);
    ASSERT_EQ(full->size(), 3u);
    EXPECT_EQ(full->back(), "third");
    for (auto parts : {initializer_list<string_view>{"GET", "same"},
                       {"WAIT", "1", "1"},
                       {"SET", "same", "conditional", "NX"},
                       {"SET", "same", "conditional", "XX"},
                       {"SELECT", "1"},
                       {"READONLY"},
                       {"READWRITE"},
                       {"RESET"},
                       {"AUTH", "password"},
                       {"SET", "bad"},
                       {"UNKNOWN"}}) {
      auto barrier = command(parts);
      EXPECT_EQ(service_->DispatchCommandSimple(barrier.get(), facade::AsyncPreference::ONLY_ASYNC,
                                                nullptr),
                facade::DispatchResult::WOULD_BLOCK);
      EXPECT_FALSE(barrier->IsDeferredReply());
    }
    EXPECT_EQ(reads_, 0u);
    EXPECT_EQ(capture.Take().index(), 0u);

    // Completion order cannot update WAIT's position; only sending the ordered reply does.
    pending_[1]->position = {10, 3, 9};
    pending_[1]->ready.Dec();
    EXPECT_TRUE(second->CanReply());
    EXPECT_FALSE(first->CanReply());
    EXPECT_FALSE(connection.conn_state.raft_last_write);
    pending_[0]->position = {10, 3, 8};
    pending_[0]->ready.Dec();
    first->SendReply();
    EXPECT_EQ(get<facade::payload::SimpleString>(capture.Take()), "OK");
    EXPECT_EQ(connection.conn_state.raft_last_write->end_offset, 8);
    second->SendReply();
    EXPECT_EQ(get<facade::payload::SimpleString>(capture.Take()), "OK");
    EXPECT_EQ(connection.conn_state.raft_last_write->end_offset, 9);
  });
}

TEST_F(DataDispatchTest, AsyncSetErrorsAndDroppedCommandsKeepOwnedCompletion) {
  async_enabled_ = true;
  pp_->at(0)->Await([&] {
    ConnectionContext connection(nullptr, acl::UserCredentials{});
    connection.ns = &namespaces->GetDefaultNamespace();
    connection.req_auth = true;
    facade::CapturingReplyBuilder capture;
    auto command = [&] {
      auto cmd = make_unique<CommandContext>(&capture, &connection);
      array<string_view, 3> parts{"SET", "key", "value"};
      cmd->Assign(parts.begin(), parts.end(), parts.size());
      return cmd;
    };
    auto denied = command();
    EXPECT_EQ(
        service_->DispatchCommandSimple(denied.get(), facade::AsyncPreference::ONLY_ASYNC, nullptr),
        facade::DispatchResult::ERROR);
    EXPECT_TRUE(denied->IsDeferredReply());
    EXPECT_TRUE(denied->CanReply());
    EXPECT_EQ(capture.Take().index(), 0u);
    denied->SendReply();
    EXPECT_TRUE(facade::CapturingReplyBuilder::TryExtractError(capture.Take()));
    EXPECT_TRUE(pending_.empty());
    connection.req_auth = false;
    connection.skip_acl_validation = true;
    auto failed = command();
    EXPECT_EQ(
        service_->DispatchCommandSimple(failed.get(), facade::AsyncPreference::ONLY_ASYNC, nullptr),
        facade::DispatchResult::OK);
    ASSERT_EQ(pending_.size(), 1u);
    pending_[0]->position = {10, 3, 8};
    pending_[0]->status = absl::UnavailableError("-TRYAGAIN Outcome of Raft write is unknown");
    pending_[0]->ready.Dec();
    failed->SendReply();
    EXPECT_TRUE(facade::CapturingReplyBuilder::TryExtractError(capture.Take()));
    EXPECT_EQ(connection.conn_state.raft_last_write->end_offset, 8);
    auto dropped = command();
    EXPECT_EQ(service_->DispatchCommandSimple(dropped.get(), facade::AsyncPreference::ONLY_ASYNC,
                                              nullptr),
              facade::DispatchResult::OK);
    ASSERT_EQ(pending_.size(), 2u);
    dropped.reset();
    EXPECT_EQ(pending_[1]->input[1], "key");
    EXPECT_EQ(pending_[1]->input[2], "value");
    pending_[1]->position = {10, 3, 9};
    pending_[1]->ready.Dec();
    EXPECT_EQ(connection.conn_state.raft_last_write->end_offset, 8);
    EXPECT_EQ(capture.Take().index(), 0u);
  });
}

TEST_F(DataDispatchTest, AsyncSetExternalArgumentsKeepParsedStorage) {
  async_enabled_ = true;
  pp_->at(0)->Await([&] {
    ConnectionContext connection(nullptr, acl::UserCredentials{});
    connection.ns = &namespaces->GetDefaultNamespace();
    connection.skip_acl_validation = true;
    facade::CapturingReplyBuilder capture;
    CommandContext cmd(&capture, &connection);
    array<string_view, 3> stored{"SET", "original", "kept"};
    cmd.Assign(stored.begin(), stored.end(), stored.size());
    array<string_view, 3> external{"SET", "external", ""};
    EXPECT_EQ(service_->DispatchCommand(facade::ArgSlice{external}, &cmd,
                                        facade::AsyncPreference::ONLY_ASYNC, nullptr),
              facade::DispatchResult::OK);
    ASSERT_EQ(pending_.size(), 1u);
    EXPECT_TRUE(pending_[0]->input.empty());
    EXPECT_EQ(submitted_[0], make_pair(string("external"), string{}));
    ASSERT_EQ(cmd.size(), 3u);
    EXPECT_EQ(cmd[1], "original");
    EXPECT_EQ(cmd[2], "kept");
    pending_[0]->ready.Dec();
    cmd.SendReply();
    EXPECT_EQ(get<facade::payload::SimpleString>(capture.Take()), "OK");
  });
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

TEST_F(DataDispatchTest, WaitRechecksImmediatelyAfterReplicaProgressNotification) {
  ready_ = true;
  std::array<string_view, 3> set{"SET", "k", "v"};
  std::array<string_view, 3> wait{"WAIT", "1", "5000"};
  EXPECT_EQ(Run("writer", set), "OK");
  applied_ = 7;
  unsigned waits = 0;
  replica_wait_ = [&](chrono::steady_clock::time_point) {
    ++waits;
    applied_ = 8;
  };
  EXPECT_EQ(Run("writer", wait), 1);
  EXPECT_EQ(waits, 1u);
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
