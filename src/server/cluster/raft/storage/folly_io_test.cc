// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/storage/folly_io.h"

#include <fcntl.h>
#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <string>

namespace dfly::cluster::raft {
namespace {

TEST(FollyIoTest, ExplicitOffsetsSyncAndErrors) {
  std::string path = (std::filesystem::temp_directory_path() / "raft-folly-XXXXXX").string();
  int fd = mkstemp(path.data());
  ASSERT_GE(fd, 0);
  ASSERT_EQ(unlink(path.c_str()), 0);
  FollyIo io;
  auto submit = [&](int target, int64_t offset, std::string_view bytes) {
    folly::Baton<> ready;
    int result = 0;
    io.Submit(target, offset, bytes, [&](int value) {
      result = value;
      ready.post();
      return true;
    });
    ready.wait();
    return result;
  };
  EXPECT_EQ(submit(fd, 4, "tail"), 4);
  EXPECT_EQ(submit(fd, 0, "head"), 4);
  EXPECT_EQ(submit(fd, -1, {}), 0);
  char bytes[8];
  ASSERT_EQ(pread(fd, bytes, sizeof(bytes), 0), sizeof(bytes));
  EXPECT_EQ(std::string_view(bytes, sizeof(bytes)), "headtail");
  EXPECT_EQ(submit(-1, 0, "bad"), -EBADF);
  EXPECT_EQ(submit(-1, -1, {}), -EBADF);
  EXPECT_EQ(close(fd), 0);
}

TEST(FollyIoTest, FullRecipientQueueDoesNotBlockOtherCompletions) {
  FollyIo io;
  folly::Baton<> accepted;
  bool second_done = false;
  unsigned attempts = 0;
  int first_result = 0, second_result = 0;
  io.Submit(-1, -1, {}, [&](int result) {
    first_result = result;
    ++attempts;
    if (attempts == 1 || !second_done)
      return false;
    accepted.post();
    return true;
  });
  io.Submit(-1, -1, {}, [&](int result) {
    second_result = result;
    second_done = true;
    return true;
  });
  accepted.wait();
  EXPECT_GE(attempts, 2u);
  EXPECT_EQ(first_result, -EBADF);
  EXPECT_EQ(second_result, -EBADF);
}

TEST(FollyIoTest, EncodingLeavesTheGroupLoopAvailableForIoAndDeadline) {
  FollyIo io;
  folly::Baton<> encoding, release, encoded, completed, expired;
  io.Run([&] {
    io.Encode(
        [&] {
          encoding.post();
          release.wait();
        },
        [&] { encoded.post(); });
    io.SetDeadline(std::chrono::steady_clock::now() + std::chrono::hours(1),
                   [] { ADD_FAILURE() << "Replaced deadline fired"; });
    io.SetDeadline(std::chrono::steady_clock::now() + std::chrono::milliseconds(1),
                   [&] { expired.post(); });
    io.Submit(-1, -1, {}, [&](int result) {
      EXPECT_EQ(result, -EBADF);
      completed.post();
      return true;
    });
  });
  encoding.wait();
  completed.wait();
  expired.wait();
  release.post();
  encoded.wait();
  io.Drain();
}

}  // namespace
}  // namespace dfly::cluster::raft
