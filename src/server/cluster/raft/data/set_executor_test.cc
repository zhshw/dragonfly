// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/set_executor.h"

#include "base/gtest.h"
#include "facade/facade_test.h"
#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster_support.h"
#include "server/engine_shard_set.h"
#include "server/journal/executor.h"
#include "server/server_state.h"
#include "server/test_utils.h"

namespace dfly::cluster::raft {
namespace {

class SetExecutorTest : public BaseFamilyTest {};

TEST_F(SetExecutorTest, RealSetTransactionReplacesAnExistingTypeAndClearsItsTTL) {
  std::string key = "{k}set";
  SetEffect effect{10, 0, KeySlot(key), 7, key, std::string("a\0b", 3)};
  EXPECT_EQ(Run({"RPUSH", key, "item"}), 1);
  EXPECT_EQ(Run({"PEXPIRE", key, "60000"}), 1);
  pp_->at(0)->Await([&] {
    JournalExecutor executor(service_.get());
    EXPECT_TRUE(ExistsForSet(executor, 0, key).value());
    EXPECT_FALSE(ExistsForSet(executor, 0, "missing").value());
    EXPECT_FALSE(ExistsForSet(executor, kInvalidDbId, key).ok());
  });
  auto apply = [&] {
    return pp_->at(0)->Await([&] {
      JournalExecutor executor(service_.get());
      return ExecuteSetEffect(executor, effect);
    });
  };
  auto status = apply();
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(Run({"GET", key}), effect.value);
  EXPECT_EQ(Run({"PTTL", key}), -1);

  // Borrowed arguments remain live across shard execution and repeated DB switches. Releasing
  // the input after each call must not affect the stored value, including binary and empty data.
  std::string large(256 << 10, 'v');
  large[123] = '\0';
  pp_->at(0)->Await([&] {
    JournalExecutor executor(service_.get());
    for (DbIndex db : {0, 2, 0, 2}) {
      auto replacement = effect;
      replacement.db = db;
      replacement.value = db == 0 ? large : "";
      ASSERT_TRUE(ExecuteSetEffect(executor, replacement).ok());
      replacement.value.assign("released");
      auto stored = ReadCommittedValue(executor, db, key);
      ASSERT_TRUE(stored.ok()) << stored.status();
      ASSERT_TRUE(stored->has_value());
      EXPECT_EQ(**stored, db == 0 ? large : "");
    }
  });
  EXPECT_EQ(Run({"GET", key}), large);

  pp_->at(0)->Await([&] {
    JournalExecutor executor(service_.get());
    EXPECT_TRUE(ExistsForSet(executor, 0, key).ok());
    const auto previous = used_mem_current.exchange(max_memory_limit.load() + 1);
    auto rejected = ExecuteSetEffect(executor, effect);
    used_mem_current = previous;
    EXPECT_TRUE(absl::IsResourceExhausted(rejected)) << rejected;
  });
  EXPECT_EQ(Run({"GET", key}), large);

  ++effect.slot;
  effect.value = "not applied";
  EXPECT_TRUE(absl::IsDataLoss(apply()));
  effect.slot = KeySlot(key);
  effect.db = kInvalidDbId;
  EXPECT_FALSE(apply().ok());
  EXPECT_NE(Run({"GET", key}), "not applied");
}

TEST_F(SetExecutorTest, DetachedBatchStopsAtFirstFailureAcrossShardRuns) {
  const auto shard_count = shard_set->size();
  ASSERT_GE(shard_count, 2u);
  std::array<std::string, 2> keys;
  pp_->at(0)->Await([&] {
    for (unsigned i = 0; keys[0].empty() || keys[1].empty(); ++i) {
      auto key = "batch-key-" + std::to_string(i);
      auto sid = Shard(key, shard_count);
      if (sid < keys.size())
        keys[sid] = std::move(key);
    }
  });

  auto log = PartitionLog::Open(base::GetTestTempDir()).value();
  CommittedEffects state(10);
  auto append = [&](size_t sid, DbIndex db, std::string value) {
    const auto& key = keys[sid];
    SetEffect effect{10, db, KeySlot(key), 0, key, std::move(value)};
    auto bytes = MakeSetEffectBatch(log->End().end_offset, 1, 0, effect).value();
    ASSERT_TRUE(log->Append(bytes, 1).ok());
    state.RememberAppended(log->End(), bytes.size(), std::move(effect));
  };
  append(1, 0, "first");
  append(1, 2, "other db");
  append(0, 0, "local");
  append(1, 0, "last");
  append(1, kInvalidDbId - 1, "unconfigured db");
  append(1, 0, "same-shard suffix");
  append(0, 0, "cross-shard suffix");

  auto batch = state.PrepareApply(*log, log->End().end_offset).value();
  ASSERT_TRUE(batch);
  ASSERT_EQ(batch->size(), 7u);

  // Force the first remote run to wait on the transaction queue before it can execute.
  auto suspended = ExpectUsedKeys({keys[1]});
  pp_->at(0)->Await([&] {
    JournalExecutor executor(service_.get());
    size_t completed = 0;
    auto status = ExecuteSetBatch(executor, *batch, &completed);
    EXPECT_FALSE(status.ok());
    ASSERT_EQ(completed, 4u);
    EXPECT_TRUE(state.FinishApply(*batch, completed).ok());
    EXPECT_EQ(state.applied_offset(), 4);
    EXPECT_EQ(ReadCommittedValue(executor, 0, keys[0]).value(), "local");
    EXPECT_EQ(ReadCommittedValue(executor, 0, keys[1]).value(), "last");
    EXPECT_EQ(ReadCommittedValue(executor, 2, keys[1]).value(), "other db");
  });
  suspended.Join();
}

}  // namespace
}  // namespace dfly::cluster::raft
