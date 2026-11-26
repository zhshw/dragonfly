// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/trace.h"

#include <gtest/gtest.h>

#include "server/cluster/raft/data/admission_budget.h"

namespace dfly::cluster::raft::trace {
namespace {
using namespace std;

optional<int64_t> ValueOf(const Capture& capture, Attribute attribute) {
  for (const auto& event : capture.events()) {
    for (const auto& value : event.values) {
      if (value.attribute == attribute)
        return value.value;
    }
  }
  ADD_FAILURE() << "Missing native attribute";
  return nullopt;
}

TEST(RaftTraceTest, ClipsFsyncUnionAndAccountsForDeliveryAcrossOwners) {
  Time now = 0;
  Capture capture("test", "process", "10", 2, 1U << 20, [&] { return now; });
  const auto first = capture.BeginFsync(1, "Log");
  FetchTrace request(capture, 1, 3, 1, 10);
  request.RpcStarted(1);
  now = 4;
  const auto overlapping = capture.BeginFsync(2, "Log");
  now = 6;
  capture.EndFsync(first, 0);
  now = 8;
  capture.EndFsync(overlapping, 0);

  // The RPC returned at 2 but its notification reaches the group owner only at 9.
  now = 9;
  request.RpcReturned(2, 0, true);
  request.BeginIoWait(now);
  now = 10;
  const auto second = capture.BeginFsync(3, "Recovery marker");
  now = 12;
  request.EndIoWait(now);
  now = 15;
  request.CheckGate(Gate::kAllowed, 0);
  request.ReceiveStarted();
  now = 16;
  request.Finish("Empty");
  now = 20;
  capture.EndFsync(second, 0);
  EXPECT_EQ(ValueOf(capture, Attribute::kReturnWait), 13);
  EXPECT_EQ(ValueOf(capture, Attribute::kElapsed), 15);
  EXPECT_EQ(ValueOf(capture, Attribute::kReceive), 1);
  EXPECT_EQ(ValueOf(capture, Attribute::kFsyncOverlap), 11);
  EXPECT_EQ(ValueOf(capture, Attribute::kIoWait), 3);
  EXPECT_EQ(ValueOf(capture, Attribute::kIoFsyncOverlap), 2);
  EXPECT_EQ(ValueOf(capture, Attribute::kGateRejections), 0);
  for (const auto& event : capture.events()) {
    EXPECT_NE(event.name, "raft.fetch.response");
    EXPECT_NE(event.name, "raft.fetch.flush_gate.ready");
  }
}

TEST(RaftTraceTest, ErrorCancellationAndOverflowNeverBecomeEmptySuccess) {
  for (string outcome : {"Error", "Cancelled", "Unfinished"}) {
    Time now = 0;
    Capture capture("test", "process", "10", 2, 1U << 20, [&] { return now; });
    {
      FetchTrace request(capture, 1, 3, 1, 10);
      request.RpcStarted(now);
      now = 1;
      request.RpcReturned(now, 0, outcome != "Error");
      if (outcome != "Error") {
        request.CheckGate(Gate::kIncompleteSuffix, 1);
        request.BeginIoWait(now);
        now = 3;
        if (outcome == "Cancelled")
          request.Finish(outcome);
      }
    }
    ASSERT_TRUE(capture.drained());
    EXPECT_FALSE(ValueOf(capture, Attribute::kAppendedBytes));
    EXPECT_FALSE(ValueOf(capture, Attribute::kReturnWait));
    EXPECT_FALSE(ValueOf(capture, Attribute::kElapsed));
    EXPECT_EQ(capture.events().back().outcome, outcome);
  }
  Capture bounded("test", "process", "10", 2, 1);
  bounded.Emit(bounded.MakeEvent("point", 0, 0));
  EXPECT_EQ(bounded.dropped(), 1u);
  EXPECT_TRUE(bounded.events().empty());
}

TEST(RaftTraceTest, AdmissionCountsOnlyItsConnectionAndRequiresWalEvidence) {
  Time now = 0;
  auto capture = make_shared<Capture>("test", "process", "10", 1, 1U << 20, [&] { return now; });
  AdmissionBudget budget(100, 2);
  Admission first(capture, 1, budget.progress()), second(capture, 2, budget.progress());
  first.SetEpoch(7);
  second.SetEpoch(7);
  int a, b, blocked;
  auto ticket_a = budget.TryReserve(10);
  auto ticket_b = budget.TryReserve(10);
  ASSERT_TRUE(ticket_a && ticket_b);
  first.Admitted(&a, &*ticket_a);
  second.Admitted(&b, &*ticket_b);
  ASSERT_FALSE(budget.TryReserve(10));
  first.Rejected(&blocked, 10, budget);
  now = 2;
  ticket_b.reset();
  now = 5;
  first.Attempt(&blocked);
  auto replacement = budget.TryReserve(10);
  ASSERT_TRUE(replacement);
  first.Admitted(&blocked, &*replacement);
  EXPECT_EQ(ValueOf(*capture, Attribute::kAdmissionWait), 5);
  EXPECT_EQ(ValueOf(*capture, Attribute::kRetiredTickets), 0);
  EXPECT_EQ(ValueOf(*capture, Attribute::kFullWaveRetired), 0);
  EXPECT_FALSE(ValueOf(*capture, Attribute::kWalIdle));
  now = 6;
  first.SetEpoch(8);
  ticket_a.reset();
  first.Refresh();
  for (const auto& event : capture->events()) {
    EXPECT_EQ(event.attributes.at("node_role"), "Leader");
    EXPECT_EQ(event.attributes.at("epoch"), "7");
  }
}

TEST(RaftTraceTest, PointEventsReadTheClockOnceAndInvalidBoundariesInvalidateCapture) {
  Time now = 0;
  auto capture = make_shared<Capture>("test", "process", "10", 1, 1U << 20, [&] { return ++now; });
  AdmissionBudget budget(10, 1);
  Admission admission(capture, 1, budget.progress());
  int command;
  admission.Rejected(&command, 10, budget);
  admission.Attempt(&command);
  ASSERT_EQ(capture->events().size(), 1u);
  EXPECT_EQ(capture->events()[0].start, capture->events()[0].end);
  FetchTrace fetch(*capture, 2, 1, 1, 0);
  fetch.RpcStarted(capture->Now());
  fetch.RpcReturned(capture->Now(), 0, true);
  const auto io = capture->BeginFsync(1, "Log");
  fetch.CheckGate(Gate::kIncompleteSuffix, 1);
  capture->EndFsync(io, 0);
  fetch.Finish("Cancelled");
  for (const auto& event : capture->events()) {
    EXPECT_GE(event.end, event.start) << event.name;
    if (event.name == "raft.fetch.io_dependency" || event.name == "raft.fetch.io_overlap") {
      EXPECT_EQ(event.start, event.end);
    }
  }
  capture->Emit(capture->MakeEvent("invalid", 2, 1));
  EXPECT_EQ(capture->dropped(), 1u);
}

TEST(RaftTraceTest, DeferredQueueTimingsKeepOriginalBoundsAndRole) {
  Time now = 1;
  Capture capture("test", "process", "10", 11, 16U << 20, [&] { return now; });
  capture.SetNodeRole("Follower", 7);
  Batch batch(capture);
  const auto queued = batch.Now();
  now = 10;
  batch.Record("raft.apply.lane_queue", queued, batch.Now());
  now = 20;
  batch.Record("journal.apply", 10, batch.Now());
  const auto io = capture.BeginFsync(1, "Log");
  now = 1000;
  capture.SetNodeRole("Leader", 8);
  EXPECT_TRUE(capture.events().empty());
  batch.Publish();
  ASSERT_EQ(capture.events().size(), 2u);
  EXPECT_EQ(capture.events()[0].start, 1);
  EXPECT_EQ(capture.events()[0].end, 10);
  EXPECT_EQ(capture.events()[1].start, 10);
  EXPECT_EQ(capture.events()[1].end, 20);
  EXPECT_EQ(capture.events()[1].attributes.at("epoch"), "7");
  EXPECT_EQ(capture.events()[1].attributes.at("node_role"), "Follower");
  batch.Publish();
  EXPECT_EQ(capture.events().size(), 2u);
  capture.EndFsync(io, 0);
  EXPECT_EQ(capture.events().back().attributes.at("node_role"), "Follower");
  EXPECT_EQ(capture.events().back().attributes.at("epoch"), "7");
  EXPECT_EQ(capture.MakePoint("later").attributes.at("node_role"), "Leader");
  for (size_t i = 0; i < 8193; ++i)
    batch.Record("queue", 1, 2);
  batch.Publish();
  EXPECT_EQ(capture.dropped(), 1u);
}

}  // namespace
}  // namespace dfly::cluster::raft::trace
