// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include <chrono>
#include <limits>
#include <memory>

#include "server/cluster/raft/config/quorum_config.h"
#include "server/cluster/raft/protocol/rpc_frame.h"
#include "util/fiber_socket_base.h"

namespace dfly::cluster::raft {
class FollyRpcClient;

// Owned by one peer's request fiber. Negotiate again whenever that peer reconnects.
class PeerConnection {
 public:
  PeerConnection(QuorumEndpoint endpoint, uint32_t timeout_ms, size_t max_frame_size);
  ~PeerConnection();

  // Close on the owning proactor after the last request has completed.
  void Close();

  // Negotiation may connect. Select a schema before populating version-specific request fields.
  template <typename Request> std::error_code Negotiate(int16_t min_version, int16_t* version) {
    if (auto ec = EnsureConnected(); ec)
      return ec;

    for (const auto& api : versions_.api_keys) {
      if (api.api_key != Request::kApiKey)
        continue;
      auto candidate = std::min(api.max_version, Request::kMaxVersion);
      if (candidate >= std::max({min_version, api.min_version, Request::kMinVersion})) {
        *version = candidate;
        return {};
      }
    }
    return std::make_error_code(std::errc::protocol_not_supported);
  }

  template <typename Request, typename Response>
  std::error_code Call(const Request& request, int16_t min_version, Response* response) {
    int16_t version;
    if (auto ec = Negotiate<Request>(min_version, &version); ec)
      return ec;
    return Exchange(request, version, response);
  }

 private:
  std::error_code EnsureConnected();
  std::error_code ExchangeFrame(
      std::string wire,
      absl::FunctionRef<bool(std::string_view, const std::shared_ptr<const void>&, size_t)> decode);
  std::error_code ReceiveFrame(
      std::chrono::steady_clock::time_point deadline,
      absl::FunctionRef<bool(std::string_view, const std::shared_ptr<const void>&, size_t)> decode);

  template <typename Request, typename Response>
  std::error_code Exchange(const Request& request, int16_t version, Response* response) {
    protocol::RequestMetadata header;
    header.correlation_id = sequence_;
    header.client_id = "dragonfly-raft";
    sequence_ = sequence_ == std::numeric_limits<int32_t>::max() ? 0 : sequence_ + 1;
    std::string wire;
    if (protocol::EncodeRequestFrame(header, request, version, &wire) != protocol::CodecError::kOk)
      return std::make_error_code(std::errc::invalid_argument);

    auto ec = ExchangeFrame(std::move(wire), [&](std::string_view bytes,
                                                 const std::shared_ptr<const void>& owner,
                                                 size_t capacity) {
      auto frame = [&] {
        if constexpr (std::is_same_v<Response, protocol::FetchResponse>) {
          if (owner)
            return protocol::DecodeResponseFrame(bytes, max_frame_size_, version,
                                                 header.correlation_id, owner, capacity, response);
        }
        return protocol::DecodeResponseFrame(bytes, max_frame_size_, version, header.correlation_id,
                                             response);
      }();
      return frame.status == protocol::FrameStatus::kComplete && frame.consumed == bytes.size();
    });
    if (ec)
      Close();
    return ec;
  }

  QuorumEndpoint endpoint_;
  uint32_t timeout_ms_;
  size_t max_frame_size_;
  int32_t sequence_ = 0;
  std::unique_ptr<util::FiberSocketBase> socket_;
  std::shared_ptr<FollyRpcClient> folly_;
  bool connected_ = false;
  protocol::ApiVersionsResponse versions_;
};

}  // namespace dfly::cluster::raft
