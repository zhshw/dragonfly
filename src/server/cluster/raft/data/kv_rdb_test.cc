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

}  // namespace
}  // namespace dfly::cluster::raft
