// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/kv_rdb.h"

#include "facade/facade_test.h"
#include "io/io.h"
#include "server/cluster/raft/data/set_effect.h"
#include "server/cluster/raft/data/set_executor.h"
#include "server/cluster/slot_set.h"
#include "server/journal/executor.h"
#include "server/test_utils.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;

class KvRdbTest : public BaseFamilyTest {};

TEST_F(KvRdbTest, SlotCutAcrossDatabasesPreservesAbsoluteExpiryAndIgnoresLaterWrites) {
  string key = "{source}binary", list = "{source}list", other = "{neighbor}key";
  const SlotId slot = KeySlot(key);
  ASSERT_NE(slot, KeySlot(other));
  SlotRanges ranges({{slot, slot}});
  SlotSet slots(ranges);
  string binary(200'000, '\0');
  binary.back() = static_cast<char>(0xff);
  EXPECT_EQ(Run({"SET", key, binary}), "OK");
  EXPECT_EQ(Run({"PEXPIRE", key, "120000"}), 1);
  auto when = Run({"PEXPIRETIME", key}).GetInt();
  ASSERT_TRUE(when);
  EXPECT_EQ(Run({"SET", other, "neighbor"}), "OK");
  EXPECT_EQ(Run({"SELECT", "2"}), "OK");
  EXPECT_EQ(Run({"RPUSH", list, "one", "two"}), 2);
  io::StringSink sink;
  auto status = pp_->at(0)->Await([&] {
    return SaveKvRdb(*service_, slots, &sink, [&] {
      JournalExecutor executor(service_.get());
      ASSERT_TRUE(ExecuteSetEffect(executor, {10, 0, slot, 0, key, "after cut"}).ok());
    });
  });
  ASSERT_TRUE(status.ok()) << status;
  std::string rdb = std::move(sink).str();
  ASSERT_TRUE(rdb.starts_with("REDIS"));
  EXPECT_EQ(Run({"SELECT", "0"}), "OK");
  EXPECT_EQ(Run({"GET", key}), "after cut");
  EXPECT_EQ(Run({"SET", "{source}obsolete", "remove on install"}), "OK");
  EXPECT_EQ(Run({"SET", other, "live neighbor"}), "OK");
  auto install = [&](const SlotRanges& target, string_view bytes, string_view digest) {
    return pp_->at(0)->Await([&] { return InstallKvRdb(service_.get(), target, bytes, digest); });
  };
  const string digest = KvRdbDigest(rdb);
  SlotRanges wrong({{KeySlot(other), KeySlot(other)}});
  EXPECT_FALSE(install(wrong, rdb, digest).ok());
  EXPECT_FALSE(install(ranges, rdb, string(32, 'x')).ok());
  string trailing = rdb + "unframed";
  EXPECT_FALSE(install(ranges, trailing, KvRdbDigest(trailing)).ok());
  EXPECT_EQ(Run({"GET", key}), "after cut");
  EXPECT_EQ(Run({"GET", other}), "live neighbor");
  ASSERT_TRUE(install(ranges, rdb, digest).ok());
  ASSERT_TRUE(install(ranges, rdb, digest).ok());
  EXPECT_EQ(Run({"GET", key}), binary);
  EXPECT_EQ(Run({"PEXPIRETIME", key}), *when);
  EXPECT_EQ(Run({"EXISTS", "{source}obsolete"}), 0);
  EXPECT_EQ(Run({"GET", other}), "live neighbor");
  EXPECT_EQ(Run({"SELECT", "2"}), "OK");
  EXPECT_THAT(Run({"LRANGE", list, "0", "-1"}), facade::RespElementsAre("one", "two"));
}

TEST_F(KvRdbTest, FullGroupRestoreRemovesStaleSlotsAndCanRestoreAnEmptyGroup) {
  string key = "{owned}value", stale = "{lost}key";
  ASSERT_NE(KeySlot(key), KeySlot(stale));
  SlotSet materialized;
  materialized.Set(KeySlot(key), true);
  SlotSet empty;
  io::StringSink before, after;
  EXPECT_EQ(Run({"SET", key, "committed"}), "OK");
  auto capture = [&](const SlotSet& slots, io::StringSink* sink) {
    return pp_->at(0)->Await([&] { return SaveKvRdb(*service_, slots, sink, [] {}); });
  };
  ASSERT_TRUE(capture(empty, &before).ok());
  ASSERT_TRUE(capture(materialized, &after).ok());
  EXPECT_EQ(Run({"SET", stale, "old owner"}), "OK");
  auto restore = [&](const SlotSet& slots, const io::StringSink& sink) {
    return pp_->at(0)->Await([&] { return RestoreGroupKvRdb(*service_, slots, sink.str()); });
  };
  EXPECT_FALSE(restore(empty, after).ok());
  EXPECT_EQ(Run({"GET", stale}), "old owner");
  ASSERT_TRUE(restore(materialized, after).ok());
  EXPECT_EQ(Run({"GET", key}), "committed");
  EXPECT_EQ(Run({"EXISTS", stale}), 0);
  ASSERT_TRUE(restore(empty, before).ok());
  EXPECT_EQ(Run({"DBSIZE"}), 0);
}

#ifdef WITH_EXTENSION_CMDS
TEST_F(KvRdbTest, InvalidObjectFailsValidationBeforeReplacingGroupData) {
  string key = "{owned}doc";
  SlotSet slots;
  slots.Set(KeySlot(key), true);
  ASSERT_EQ(Run({"SELECT", "2"}), "OK");
  ASSERT_EQ(Run({"JSON.SET", key, "$", R"({"v":true})"}), "OK");
  io::StringSink sink;
  auto s = pp_->at(0)->Await([&] { return SaveKvRdb(*service_, slots, &sink, [] {}); });
  ASSERT_TRUE(s.ok()) << s;
  string bytes = std::move(sink).str();
  ASSERT_TRUE(pp_->at(0)->Await([&] { return ValidateKvRdb(*service_, slots, bytes); }).ok());

  // The file still has the correct RDB lengths, but the stored JSON value no longer parses.
  auto pos = bytes.find("true");
  ASSERT_NE(pos, string::npos);
  bytes.replace(pos, 4, "trux");
  ASSERT_EQ(Run({"SET", key, "live value"}), "OK");
  ASSERT_EQ(Run({"SELECT", "0"}), "OK");
  ASSERT_EQ(Run({"SET", "{other}key", "another DB"}), "OK");
  EXPECT_FALSE(pp_->at(0)->Await([&] { return RestoreGroupKvRdb(*service_, slots, bytes); }).ok());
  EXPECT_EQ(Run({"GET", "{other}key"}), "another DB");
  ASSERT_EQ(Run({"SELECT", "2"}), "OK");
  EXPECT_EQ(Run({"GET", key}), "live value");
}

#endif

}  // namespace
}  // namespace dfly::cluster::raft
