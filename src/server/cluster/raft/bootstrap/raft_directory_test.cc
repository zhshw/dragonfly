// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/bootstrap/raft_directory.h"

#include <gtest/gtest.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fstream>

#include "server/cluster/raft/bootstrap/meta_properties.h"
#include "server/cluster/raft/bootstrap/quorum_bootstrap.h"
#include "server/cluster/raft/quorum/membership_history.h"

namespace dfly::cluster::raft {
namespace {
using namespace std;
namespace fs = std::filesystem;

class RaftDirectoryTest : public testing::Test {
 protected:
  void SetUp() override {
    string temp = (fs::temp_directory_path() / "dfly-raftdir-XXXXXX").string();
    ASSERT_NE(mkdtemp(temp.data()), nullptr);
    temp_ = temp;
    dir_ = temp_ / "node";
  }

  void TearDown() override {
    if (!temp_.empty())
      fs::remove_all(temp_);
  }

  fs::path temp_, dir_;
  const RaftDirectoryConfig cfg_{"my@cluster@id", 3, nullopt};
};

TEST_F(RaftDirectoryTest, FormatVerifyAndRetry) {
  ASSERT_TRUE(absl::IsNotFound(ReadRaftDirectory(dir_).status()));
  auto formatted = FormatRaftDirectory(dir_, cfg_);
  ASSERT_TRUE(formatted.ok()) << formatted.status();
  EXPECT_FALSE(FormatDirectoryId(formatted->directory_id).starts_with('-'));
  auto retry = FormatRaftDirectory(dir_, cfg_);
  ASSERT_TRUE(retry.ok()) << retry.status();
  EXPECT_EQ(*formatted, *retry);
  ASSERT_TRUE(VerifyRaftDirectory(dir_, cfg_).ok());
  EXPECT_TRUE(absl::IsFailedPrecondition(CheckLegacyDirectory(dir_)));
  for (RaftDirectoryConfig wrong :
       {RaftDirectoryConfig{"other", 3, {}}, RaftDirectoryConfig{cfg_.cluster_id, 0, {}},
        RaftDirectoryConfig{cfg_.cluster_id, 3, DirectoryId{12}}}) {
    EXPECT_TRUE(absl::IsFailedPrecondition(VerifyRaftDirectory(dir_, wrong)));
    EXPECT_TRUE(absl::IsFailedPrecondition(FormatRaftDirectory(dir_, wrong).status()));
  }
  EXPECT_TRUE(VerifyRaftDirectory(dir_, cfg_).ok());
}

TEST_F(RaftDirectoryTest, SuppliedVoterDirectoryAndReservedIds) {
  RaftDirectoryConfig cfg = cfg_;
  DirectoryId reserved{};
  for (int low : {0, 1, 2, 99}) {
    reserved.back() = low;
    cfg.directory_id = reserved;
    EXPECT_TRUE(absl::IsInvalidArgument(FormatRaftDirectory(dir_, cfg).status()));
  }
  reserved.back() = 100;
  cfg.directory_id = reserved;
  auto formatted = FormatRaftDirectory(dir_, cfg);
  ASSERT_TRUE(formatted.ok()) << formatted.status();
  EXPECT_EQ(formatted->directory_id, reserved);
}

TEST_F(RaftDirectoryTest, DynamicQuorumBootstrapAndInterruptedRetry) {
  const string local = "3@[::1]:9092:3Adc4FjfTeypRWROmQDNIQ";
  const string other = "4@peer:9093:PIBvV0wFRuay02AARab3Iw";
  auto initial = ParseQuorumBootstrap(other + "," + local, nullopt).value();
  auto formatted = FormatRaftDirectory(dir_, cfg_, &initial);
  ASSERT_TRUE(formatted.ok()) << formatted.status();
  EXPECT_EQ(FormatDirectoryId(formatted->directory_id), "3Adc4FjfTeypRWROmQDNIQ");
  EXPECT_EQ(initial.Partition(), "__cluster_metadata-0");
  {
    auto log = PartitionLog::Open(dir_ / initial.Partition()).value();
    EXPECT_EQ(log->End(), (LogPosition{0, 0}));
    EXPECT_EQ(log->LatestSnapshot(), (LogPosition{0, 0}));
    MembershipHistory restored;
    ASSERT_TRUE(restored.Update(*log).ok());
    EXPECT_EQ(restored.KraftVersion(), 1);
    ASSERT_NE(restored.VotersAt(-1), nullptr);
    EXPECT_EQ(*restored.VotersAt(-1), initial.voters);
    EXPECT_EQ(initial.voters.voters[0].endpoints[0].host, "::1");
  }

  // An interrupted format may have published the snapshot before the final identity.
  ASSERT_TRUE(fs::remove(dir_ / "meta.properties"));
  ASSERT_FALSE(ReadRaftDirectory(dir_).ok());
  auto observer = ParseQuorumBootstrap(nullopt, nullopt).value();
  EXPECT_FALSE(FormatRaftDirectory(dir_, cfg_, &observer).ok());
  EXPECT_FALSE(FormatRaftDirectory(dir_, cfg_).ok());
  auto same = ParseQuorumBootstrap(local + "," + other, nullopt).value();
  auto retry = FormatRaftDirectory(dir_, cfg_, &same);
  ASSERT_TRUE(retry.ok()) << retry.status();
  EXPECT_EQ(*retry, *formatted);
  EXPECT_TRUE(VerifyRaftDirectory(dir_, cfg_).ok());
}

TEST_F(RaftDirectoryTest, BootstrapDataGroupLearnerHasNoVotes) {
  auto joining = ParseQuorumBootstrap(nullopt, 10).value();
  EXPECT_EQ(joining.Partition(), "__dragonfly_data-10");
  ASSERT_TRUE(FormatRaftDirectory(dir_, cfg_, &joining).ok());
  auto log = PartitionLog::Open(dir_ / joining.Partition()).value();
  MembershipHistory restored;
  ASSERT_TRUE(restored.Update(*log).ok());
  EXPECT_EQ(restored.KraftVersion(), 1);
  EXPECT_EQ(restored.LatestVoters(), nullptr);
  EXPECT_FALSE(fs::exists(dir_ / "__cluster_metadata-0"));

  const string good = "3@peer:9092:3Adc4FjfTeypRWROmQDNIQ";
  EXPECT_FALSE(ParseQuorumBootstrap(good + "," + good, 10).ok());
  EXPECT_FALSE(ParseQuorumBootstrap(good, -1).ok());
  for (const char* bad :
       {"", "3@host:9092", "3@host:0:3Adc4FjfTeypRWROmQDNIQ", "3@::1:9092:3Adc4FjfTeypRWROmQDNIQ",
        "1@[::1:42:3Adc4FjfTeypRWROmQDNIQ", "bad@localhost:9000:x"}) {
    EXPECT_FALSE(ParseQuorumBootstrap(bad, nullopt).ok()) << bad;
  }
}

TEST_F(RaftDirectoryTest, LegacyAndForeignContentsArePreserved) {
  EXPECT_TRUE(CheckLegacyDirectory(dir_).ok());
  EXPECT_TRUE(CheckLegacyDirectory("s3://example/dir").ok());
  ASSERT_TRUE(fs::create_directory(dir_));
  for (const auto* name : {"dump.rdb", "meta.properties", "quorum-state"}) {
    auto path = dir_ / name;
    ofstream(path) << "original";
    EXPECT_TRUE(absl::IsFailedPrecondition(FormatRaftDirectory(dir_, cfg_).status()));
    EXPECT_TRUE(CheckLegacyDirectory(dir_).ok());
    EXPECT_FALSE(fs::exists(dir_ / ".dragonfly-raft"));
    string content;
    ifstream(path) >> content;
    EXPECT_EQ(content, "original");
    fs::remove(path);
  }
}

TEST_F(RaftDirectoryTest, InterruptedPublication) {
  ASSERT_TRUE(fs::create_directory(dir_));

  // A formatter that died before publishing the ownership marker left a private temp inode.
  fs::path scratch = dir_ / ".dragonfly-raft-format.abcdef";
  ofstream(scratch) << "partial";
  ASSERT_TRUE(FormatRaftDirectory(dir_, cfg_).ok());
  EXPECT_TRUE(fs::exists(scratch));

  // Crash after marker publication but before installing meta.properties.
  auto installed = ReadRaftDirectory(dir_);
  ASSERT_TRUE(installed.ok()) << installed.status();
  ASSERT_TRUE(fs::remove(dir_ / "meta.properties"));
  EXPECT_TRUE(absl::IsFailedPrecondition(CheckLegacyDirectory(dir_)));
  EXPECT_TRUE(absl::IsFailedPrecondition(ReadRaftDirectory(dir_).status()));
  auto recovered = FormatRaftDirectory(dir_, cfg_);
  ASSERT_TRUE(recovered.ok()) << recovered.status();
  EXPECT_EQ(*installed, *recovered);

  // A conflicting or corrupt published identity cannot be repaired by formatting on top.
  ASSERT_TRUE(fs::remove(dir_ / "meta.properties"));
  ofstream(dir_ / "meta.properties") << "version=1\nnode.id=9\n";
  EXPECT_TRUE(absl::IsDataLoss(FormatRaftDirectory(dir_, cfg_).status()));
  EXPECT_TRUE(absl::IsDataLoss(ReadRaftDirectory(dir_).status()));
}

TEST_F(RaftDirectoryTest, ConflictingConcurrentFormatsHaveOneWinner) {
  int pipes[2];
  ASSERT_EQ(pipe(pipes), 0);
  pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    close(pipes[1]);
    char go;
    if (read(pipes[0], &go, 1) != 1)
      _exit(2);
    auto result = FormatRaftDirectory(dir_, {"child", 2, nullopt});
    _exit(result.ok() ? 0 : absl::IsFailedPrecondition(result.status()) ? 1 : 2);
  }
  close(pipes[0]);
  ASSERT_EQ(write(pipes[1], "x", 1), 1);
  close(pipes[1]);
  auto parent = FormatRaftDirectory(dir_, cfg_);
  int status;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_TRUE(parent.ok() || absl::IsFailedPrecondition(parent.status())) << parent.status();
  ASSERT_EQ(WEXITSTATUS(status), parent.ok() ? 1 : 0);
  auto saved = ReadRaftDirectory(dir_);
  ASSERT_TRUE(saved.ok()) << saved.status();
  EXPECT_EQ(saved->cluster_id, parent.ok() ? cfg_.cluster_id : "child");
}

TEST(MetaPropertiesTest, KafkaV1Representation) {
  auto id = ParseDirectoryId("3Adc4FjfTeypRWROmQDNIQ");
  ASSERT_TRUE(id.ok());
  auto java = DecodeMetaProperties(
      "#Fri Nov 20\r\ncluster.id=my@cluster@id\n"
      "node.id=2\ndirectory.id=3Adc4FjfTeypRWROmQDNIQ\nversion=1");
  ASSERT_TRUE(java.ok()) << java.status();
  EXPECT_EQ(*java, (RaftDirectoryConfig{"my@cluster@id", 2, *id}));
  RaftDirectoryConfig identity{" \t:#\\=!\n集群🧪", 0, *id};
  auto bytes = EncodeMetaProperties(identity);
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  auto roundtrip = DecodeMetaProperties(*bytes);
  ASSERT_TRUE(roundtrip.ok()) << roundtrip.status();
  EXPECT_EQ(*roundtrip, identity);
  auto escaped = DecodeMetaProperties(
      " ! comment\rnode.id : 1\nversion 1\ncluster\\.id=a\\\n"
      "    b\\tc\ndirectory.id=3Adc4FjfTeypRWROmQDNIQ==");
  ASSERT_TRUE(escaped.ok()) << escaped.status();
  EXPECT_EQ(escaped->cluster_id, "ab\tc");
}

TEST(MetaPropertiesTest, VersionOneWithoutDirectoryId) {
  RaftDirectoryConfig input{"my@cluster@id", 2, nullopt};
  auto bytes = EncodeMetaProperties(input);
  ASSERT_TRUE(bytes.ok());
  EXPECT_EQ(bytes->find("directory.id"), string::npos);
  auto decoded = DecodeMetaProperties(*bytes);
  ASSERT_TRUE(decoded.ok());
  EXPECT_EQ(*decoded, input);
}

TEST(MetaPropertiesTest, RejectWrongVersionAndMissingOrInvalidIdentity) {
  EXPECT_TRUE(absl::IsFailedPrecondition(DecodeMetaProperties("version=0").status()));
  EXPECT_TRUE(absl::IsFailedPrecondition(DecodeMetaProperties("version=2").status()));
  const string common = "version=1\ndirectory.id=3Adc4FjfTeypRWROmQDNIQ\n";
  for (const char* fields : {"node.id=0", "cluster.id=c", "node.id=+-1\ncluster.id=c",
                             "node.id=2147483648\ncluster.id=c", "node.id=2\ncluster.id=\\u00xz"})
    EXPECT_TRUE(absl::IsDataLoss(DecodeMetaProperties(common + fields).status())) << fields;
  EXPECT_FALSE(ParseDirectoryId("no uuid").ok());
}

}  // namespace
}  // namespace dfly::cluster::raft
