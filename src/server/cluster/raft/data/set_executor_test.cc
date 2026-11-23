// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/set_executor.h"

#include "facade/facade_test.h"
#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster_support.h"
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

}  // namespace
}  // namespace dfly::cluster::raft
