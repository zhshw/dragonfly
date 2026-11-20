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

// Owned by one peer's request fiber. Negotiate again whenever that peer reconnects.
class PeerConnection {
 public:
  PeerConnection(QuorumEndpoint endpoint, uint32_t timeout_ms, size_t max_frame_size);

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
  std::error_code ReceiveFrame(std::chrono::steady_clock::time_point deadline,
                               absl::FunctionRef<bool(std::string_view)> decode);

  template <typename Request, typename Response>
  std::error_code Exchange(const Request& request, int16_t version, Response* response) {
    protocol::RequestMetadata header;
    header.correlation_id = sequence_;
    header.client_id = "dragonfly-raft";
    sequence_ = sequence_ == std::numeric_limits<int32_t>::max() ? 0 : sequence_ + 1;
    std::string wire;
    if (protocol::EncodeRequestFrame(header, request, version, &wire) != protocol::CodecError::kOk)
      return std::make_error_code(std::errc::invalid_argument);

    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms_);
    socket_->set_timeout(timeout_ms_);
    if (auto ec = socket_->Write(io::Buffer(wire)); ec) {
      Close();
      return ec;
    }
    auto ec = ReceiveFrame(deadline, [&](std::string_view bytes) {
      auto frame = protocol::DecodeResponseFrame(bytes, max_frame_size_, version,
                                                 header.correlation_id, response);
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
  protocol::ApiVersionsResponse versions_;
};

}  // namespace dfly::cluster::raft
