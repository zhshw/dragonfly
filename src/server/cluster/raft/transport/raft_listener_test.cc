// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/transport/raft_listener.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include "util/accept_server.h"
#include "util/fibers/pool.h"

namespace dfly::cluster::raft {
namespace {

class RaftListenerTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_.reset(util::fb2::Pool::Epoll(1));
    pool_->Run();
    server_ = std::make_unique<util::AcceptServer>(pool_.get(), false);
    auto handler = [](int16_t, const protocol::VoteRequest&, protocol::VoteResponse*) {
      return std::make_error_code(std::errc::io_error);
    };
    protocol::RpcHandlers handlers;
    handlers.vote = handler;
    auto* listener = new RaftListener(4096, std::move(handlers));
    ASSERT_FALSE(server_->AddListener("127.0.0.1", 0, listener));
    const uint16_t port = listener->socket()->LocalEndpoint().port();
    server_->Run();

    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd_, 0);
    const timeval timeout{5, 0};
    ASSERT_EQ(setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    ASSERT_EQ(connect(fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)), 0);
  }

  void TearDown() override {
    if (fd_ >= 0)
      close(fd_);
    server_->Stop(true);
    pool_->Stop();
  }

  int fd_ = -1;
  std::unique_ptr<util::ProactorPool> pool_;
  std::unique_ptr<util::AcceptServer> server_;
};

TEST_F(RaftListenerTest, FragmentedAndPipelinedRequestsKeepCorrelationAndOrder) {
  std::string first, second;
  protocol::RequestMetadata header;
  header.correlation_id = 73;
  ASSERT_EQ(protocol::EncodeRequestFrame(header, protocol::ApiVersionsRequest{}, 0, &first),
            protocol::CodecError::kOk);
  ++header.correlation_id;
  ASSERT_EQ(protocol::EncodeRequestFrame(header, protocol::ApiVersionsRequest{}, 0, &second),
            protocol::CodecError::kOk);
  const std::string requests = first + second;
  ASSERT_EQ(write(fd_, requests.data(), 2), 2);
  ASSERT_EQ(write(fd_, requests.data() + 2, requests.size() - 2),
            static_cast<ssize_t>(requests.size() - 2));

  std::string pending;
  for (int id : {73, 74}) {
    protocol::ApiVersionsResponse response;
    auto frame = protocol::DecodeResponseFrame(pending, 4096, 0, id, &response);
    while (frame.status == protocol::FrameStatus::kNeedMore) {
      char buffer[128];
      const ssize_t count = read(fd_, buffer, sizeof(buffer));
      ASSERT_GT(count, 0);
      pending.append(buffer, count);
      frame = protocol::DecodeResponseFrame(pending, 4096, 0, id, &response);
    }
    ASSERT_EQ(frame.status, protocol::FrameStatus::kComplete);
    EXPECT_EQ(response.error_code, 0);
    pending.erase(0, frame.consumed);
  }
}

TEST_F(RaftListenerTest, OversizedFrameClosedBeforeBodyArrives) {
  const uint32_t length = htonl(4097);
  ASSERT_EQ(write(fd_, &length, sizeof(length)), static_cast<ssize_t>(sizeof(length)));
  char byte;
  EXPECT_EQ(read(fd_, &byte, 1), 0);
}

TEST_F(RaftListenerTest, StorageFailureClosesWithoutAcknowledgement) {
  std::string wire;
  ASSERT_EQ(protocol::EncodeRequestFrame({}, protocol::VoteRequest{}, 0, &wire),
            protocol::CodecError::kOk);
  ASSERT_EQ(write(fd_, wire.data(), wire.size()), static_cast<ssize_t>(wire.size()));
  char byte;
  EXPECT_EQ(read(fd_, &byte, 1), 0);
}

}  // namespace
}  // namespace dfly::cluster::raft
