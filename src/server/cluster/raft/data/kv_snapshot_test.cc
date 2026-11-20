// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/data/kv_snapshot.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include "server/cluster/raft/log/record_batch.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;

TEST(KvSnapshotTest, CarriesScopeDataAndFrozenImportsAcrossChunkTransferAndRestart) {
  string tmp = (filesystem::temp_directory_path() / "raft-kv-snapshot-XXXXXX").string();
  ASSERT_NE(mkdtemp(tmp.data()), nullptr);
  const filesystem::path dir(tmp);
  filesystem::create_directory(dir / "leader");
  filesystem::create_directory(dir / "replica");
  auto log = PartitionLog::Open(dir / "leader").value();
  array<int32_t, 1> voters{1};
  ASSERT_TRUE(log->AppendLeaderChange(3, 2, 1, voters, voters).ok());
  MembershipHistory membership;
  ASSERT_TRUE(membership.Update(*log).ok());
  KvSnapshotScope scope{"cluster1", 10, {3, 1}, {{0, 41, 1}, {42, 42, 2}, {50, 511, 7}}};
  string rdb = "REDIS0011" + string(700'000, '\0');
  string incoming = string(490'000, 'b');
  ASSERT_TRUE(WriteKvSnapshot(scope, rdb, incoming, membership, 1, 25, log.get()).ok());
  EXPECT_FALSE(ReadKvSnapshot(*log, scope.cut, "wrong", 10).ok());
  EXPECT_FALSE(ReadKvSnapshot(*log, scope.cut, "cluster1", 20).ok());
  auto replica = PartitionLog::Open(dir / "replica").value();
  auto first = log->ReadSnapshot(scope.cut, 0, 71'000).value();
  auto writer = replica->BeginSnapshot(scope.cut, first.total_size).value();
  ASSERT_TRUE(writer->Append(0, first.bytes).ok());
  EXPECT_FALSE(writer->Append(0, first.bytes).ok());
  EXPECT_FALSE(replica->InstallSnapshot(writer.get(), 0).ok());
  while (writer->position() < first.total_size) {
    auto chunk = log->ReadSnapshot(scope.cut, writer->position(), 71'000).value();
    ASSERT_TRUE(writer->Append(writer->position(), chunk.bytes).ok());
  }
  ASSERT_TRUE(replica->InstallSnapshot(writer.get(), 0).ok());
  writer.reset();
  replica.reset();
  replica = PartitionLog::Open(dir / "replica").value();
  auto result = ReadKvSnapshot(*replica, scope.cut, "cluster1", 10);
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result->scope, scope);
  EXPECT_EQ(result->rdb, rdb);
  EXPECT_EQ(result->migrations, incoming);
  replica.reset();
  log.reset();
  filesystem::remove_all(dir);
}
}  // namespace
}  // namespace dfly::cluster::raft
