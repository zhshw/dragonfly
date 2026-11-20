// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/storage/file_quorum_state_store.h"

#include <gtest/gtest.h>
#include <stdlib.h>

#include <fstream>
#include <iterator>
#include <string>

namespace dfly::cluster::raft {
namespace {
using namespace std;

class FileQuorumStateStoreTest : public testing::Test {
 protected:
  void SetUp() override {
    string dir_template = (filesystem::temp_directory_path() / "dfly-quorum-XXXXXX").string();
    ASSERT_NE(mkdtemp(dir_template.data()), nullptr);
    dir_ = dir_template;
  }

  void TearDown() override {
    if (!dir_.empty())
      filesystem::remove_all(dir_);
  }

  filesystem::path File() const {
    return dir_ / "quorum-state";
  }

  FileQuorumStateStore Store() const {
    return FileQuorumStateStore(dir_);
  }

  filesystem::path dir_;
};

// Same transitions and version projections as FileQuorumStateStoreTest.java.
TEST_F(FileQuorumStateStoreTest, ElectionTransitionsAndReload) {
  const ReplicaKey candidate{1, {0xfb, 0xff, 0xfa}};
  for (int16_t version : {0, 1}) {
    ElectionState state{.epoch = 2, .leader_id = {}, .voted_key = {}, .voters = {1, 2, 3}};
    for (int transition = 0; transition < 3; ++transition) {
      if (transition == 1)
        state.voted_key = candidate;
      if (transition == 2)
        state.leader_id = candidate.id;
      SCOPED_TRACE(testing::Message() << "version " << version << ", transition " << transition);
      ASSERT_FALSE(Store().WriteElectionState(state, version));
      EXPECT_FALSE(filesystem::exists(File().string() + ".tmp"));
      optional<ElectionState> recovered;
      ASSERT_FALSE(Store().ReadElectionState(&recovered));
      ElectionState expected = state;
      if (version == 1)
        expected.voters.clear();
      else if (expected.voted_key)
        expected.voted_key->directory_id = {};
      EXPECT_EQ(recovered, expected);
    }
  }
}

TEST_F(FileQuorumStateStoreTest, KafkaLayout) {
  const ReplicaKey candidate{1, {0xfb, 0xff, 0xfa}};
  const ElectionState state{.epoch = 2, .leader_id = 1, .voted_key = candidate, .voters = {1, 2}};
  const string v0 = R"({"clusterId":"","leaderId":1,"leaderEpoch":2,"votedId":1,"appliedOffset":0,)"
                    R"("currentVoters":[{"voterId":1},{"voterId":2}],"data_version":0})";
  const string v1 = R"({"leaderId":1,"leaderEpoch":2,"votedId":1,)"
                    R"("votedDirectoryId":"-__6AAAAAAAAAAAAAAAAAA","data_version":1})";
  const array<string, 2> expected = {v0, v1};
  for (int16_t version : {0, 1}) {
    ASSERT_FALSE(Store().WriteElectionState(state, version));
    ifstream file(File());
    EXPECT_EQ((string{istreambuf_iterator<char>(file), {}}), expected[version]);
  }

  // A Java URL decoder also accepts conventional padding on an input UUID.
  string padded = v1;
  padded.insert(padded.find("AA\",\"data_version") + 2, "==");
  ofstream(File()) << padded;
  optional<ElectionState> recovered;
  ASSERT_FALSE(Store().ReadElectionState(&recovered));
  ASSERT_TRUE(recovered);
  EXPECT_EQ(recovered->voted_key, candidate);
}

TEST_F(FileQuorumStateStoreTest, MissingFileAndUninstalledWrite) {
  optional<ElectionState> state;
  ofstream(File().string() + ".tmp") << "{";
  EXPECT_FALSE(Store().ReadElectionState(&state));
  EXPECT_FALSE(state);
  ElectionState vote{.epoch = 7, .leader_id = {}, .voted_key = ReplicaKey{2}, .voters = {}};
  ASSERT_FALSE(Store().WriteElectionState(vote, 1));

  // A crash before rename may leave a newer, partially written temporary file.
  ofstream(File().string() + ".tmp") << R"({"leaderEpoch":8)";
  ASSERT_FALSE(Store().ReadElectionState(&state));
  EXPECT_EQ(state, vote);
  EXPECT_FALSE(Store().Clear());
  EXPECT_FALSE(filesystem::exists(File()));
  EXPECT_FALSE(filesystem::exists(File().string() + ".tmp"));
}

TEST_F(FileQuorumStateStoreTest, WriteFailurePreservesInstalledState) {
  ElectionState vote{.epoch = 7, .leader_id = {}, .voted_key = ReplicaKey{2}, .voters = {}};
  ASSERT_FALSE(Store().WriteElectionState(vote, 1));
  ASSERT_TRUE(filesystem::create_directory(File().string() + ".tmp"));
  ElectionState next = vote;
  next.epoch++;
  next.voted_key = ReplicaKey{3};
  EXPECT_TRUE(Store().WriteElectionState(next, 1));
  optional<ElectionState> recovered;
  ASSERT_FALSE(Store().ReadElectionState(&recovered));
  EXPECT_EQ(recovered, vote);
}

TEST_F(FileQuorumStateStoreTest, UnsupportedVersionAndCorruptFile) {
  optional<ElectionState> state;
  ofstream(File()) << R"({"leaderId":9990,"leaderEpoch":3012,"votedId":-1,"appliedOffset":0,)"
                      R"("currentVoters":[{"voterId":9990}],"data_version":2})";
  EXPECT_EQ(Store().ReadElectionState(&state), errc::protocol_not_supported);
  EXPECT_EQ(Store().WriteElectionState(ElectionState{}, 2), errc::protocol_not_supported);

  for (const char* invalid : {"", "[]", "{", R"({"data_version":0})",
                              R"({"data_version":1,"leaderId":-1,"leaderEpoch":3,"votedId":4})",
                              R"({"data_version":1,"votedDirectoryId":"bad","leaderId":-1,)"
                              R"("leaderEpoch":3,"votedId":4})",
                              R"({"data_version":1,"votedDirectoryId":"AAAAAAAAAAAAAAAAAAAAAA",)"
                              R"("leaderId":-1,"leaderEpoch":2147483648,"votedId":4})"}) {
    ofstream(File()) << invalid;
    EXPECT_EQ(Store().ReadElectionState(&state), errc::bad_message) << invalid;
  }
}

}  // namespace
}  // namespace dfly::cluster::raft
