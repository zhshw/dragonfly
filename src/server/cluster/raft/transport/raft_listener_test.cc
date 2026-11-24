// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/transport/raft_listener.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>

#include "server/cluster/raft/transport/peer_connection.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"
#include "util/fibers/synchronization.h"

namespace dfly::cluster::raft {
namespace {

class RaftListenerTest : public testing::Test {
 protected:
  void SetUp() override {
    auto handler = [](int16_t version, const protocol::VoteRequest& request,
                      protocol::VoteResponse* response) {
      if (request.voter_id == 7 && version >= 1) {
        response->error_code = 125;
        return std::error_code{};
      }
      return std::make_error_code(std::errc::io_error);
    };
    protocol::RpcHandlers handlers;
    handlers.vote = handler;
    StartListener(std::move(handlers));
  }

  void StartListener(protocol::RpcHandlers handlers, std::function<void()> on_pre_shutdown = {}) {
    pool_.reset(util::fb2::Pool::Epoll(1));
    pool_->Run();
    server_ = std::make_unique<util::AcceptServer>(pool_.get(), false);
    listener_ = new RaftListener(4096, std::move(handlers), std::move(on_pre_shutdown));
    ASSERT_FALSE(server_->AddListener("127.0.0.1", 0, listener_));
    port_ = listener_->socket()->LocalEndpoint().port();
    server_->Run();

    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd_, 0);
    const timeval timeout{5, 0};
    ASSERT_EQ(setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port_);
    ASSERT_EQ(connect(fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)), 0);
  }

  void TearDown() override {
    if (fd_ >= 0)
      close(fd_);
    if (server_)
      server_->Stop(true);
    pool_->Stop();
  }

  uint16_t port_ = 0;
  int fd_ = -1;
  std::unique_ptr<util::ProactorPool> pool_;
  std::unique_ptr<util::AcceptServer> server_;
  RaftListener* listener_ = nullptr;
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

TEST_F(RaftListenerTest, PeerNegotiatesAndReconnectsAfterUnacknowledgedRequest) {
  pool_->GetNextProactor()->Await([&] {
    PeerConnection peer({"127.0.0.1", port_}, 1000, 4096);
    int16_t version = -1;
    EXPECT_EQ(peer.Negotiate<protocol::FetchRequest>(13, &version),
              std::errc::protocol_not_supported);
    EXPECT_FALSE(peer.Negotiate<protocol::VoteRequest>(2, &version));
    EXPECT_EQ(version, 2);
    protocol::VoteRequest request;
    protocol::VoteResponse response;
    EXPECT_TRUE(peer.Call(request, 0, &response));
    request.voter_id = 7;
    for (int i = 0; i < 2; ++i) {
      EXPECT_FALSE(peer.Call(request, 1, &response));
      EXPECT_EQ(response.error_code, 125);
    }
    peer.Close();
  });
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

class FetchResponseListenerTest : public RaftListenerTest {
 protected:
  void SetUp() override {
    payload_.resize(256 * 1024);
    for (size_t i = 0; i < payload_.size(); ++i)
      payload_[i] = static_cast<char>(i ^ (i >> 8) ^ (i >> 16));
    protocol::RpcHandlers handlers;
    handlers.fetch = [this](int16_t, const protocol::FetchRequest& request,
                            protocol::FetchResponse* response) {
      auto& part = response->responses.emplace_back().partitions.emplace_back();
      part.records.data = payload_.substr(0, request.max_bytes);
      return std::error_code{};
    };
    StartListener(std::move(handlers));
  }

  std::string payload_;
};

TEST_F(FetchResponseListenerTest, PeerReadsLargeResponseAndReusesConnection) {
  pool_->GetNextProactor()->Await([&] {
    PeerConnection peer({"127.0.0.1", port_}, 2000, 512 * 1024);
    protocol::FetchRequest request;
    protocol::FetchResponse response, expected;
    auto& records = expected.responses.emplace_back().partitions.emplace_back().records.data;
    records = payload_;
    ASSERT_FALSE(peer.Call(request, 18, &response));
    EXPECT_EQ(response, expected);
    ASSERT_TRUE(response.responses[0].partitions[0].records.borrowed);
    EXPECT_FALSE(response.responses[0].partitions[0].records.data);
    auto retained = response;
    std::weak_ptr<const void> lifetime =
        retained.responses[0].partitions[0].records.borrowed->owner;

    // A new connection would be closed, so this Fetch must reuse the stream at the next frame.
    listener_->pause_accepting();
    request.max_bytes = 17;
    records = payload_.substr(0, request.max_bytes);
    ASSERT_FALSE(peer.Call(request, 18, &response));
    EXPECT_EQ(response, expected);
    EXPECT_FALSE(response.responses[0].partitions[0].records.borrowed);
    peer.Close();
    EXPECT_FALSE(lifetime.expired());
    EXPECT_EQ(retained.responses[0].partitions[0].records.View(), payload_);
    retained = {};
    EXPECT_TRUE(lifetime.expired());
  });
}

TEST_F(FetchResponseListenerTest, PeerRejectsOversizedResponseAndReconnects) {
  pool_->GetNextProactor()->Await([&] {
    PeerConnection peer({"127.0.0.1", port_}, 2000, 4096);
    protocol::FetchRequest request;
    protocol::FetchResponse response, expected;
    EXPECT_EQ(peer.Call(request, 18, &response), std::errc::protocol_error);
    EXPECT_EQ(response, expected);

    request.max_bytes = 17;
    expected.responses.emplace_back().partitions.emplace_back().records.data =
        payload_.substr(0, request.max_bytes);
    ASSERT_FALSE(peer.Call(request, 18, &response));
    EXPECT_EQ(response, expected);
    peer.Close();
  });
}

class ParkedFetchListenerTest : public RaftListenerTest {
 protected:
  void SetUp() override {
    protocol::RpcHandlers handlers;
    handlers.fetch = [this](int16_t, const protocol::FetchRequest&, protocol::FetchResponse*) {
      parked_.Notify();
      EXPECT_TRUE(shutdown_.WaitFor(std::chrono::seconds(5)));
      return std::make_error_code(std::errc::operation_canceled);
    };
    StartListener(std::move(handlers), [this] { shutdown_.Notify(); });
  }

  util::fb2::Done parked_, shutdown_;
};

TEST_F(ParkedFetchListenerTest, ShutdownReleasesFetchBeforeDrainingConnections) {
  std::string wire;
  ASSERT_EQ(protocol::EncodeRequestFrame({}, protocol::FetchRequest{}, 18, &wire),
            protocol::CodecError::kOk);
  ASSERT_EQ(write(fd_, wire.data(), wire.size()), static_cast<ssize_t>(wire.size()));
  ASSERT_TRUE(parked_.WaitFor(std::chrono::seconds(5)));
  server_->Stop(true);
  server_.reset();
}

}  // namespace
}  // namespace dfly::cluster::raft
