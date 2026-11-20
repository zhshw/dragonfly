// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/transport/peer_connection.h"

#include <arpa/inet.h>

#include <algorithm>

#include "util/fibers/dns_resolve.h"
#include "util/fibers/proactor_base.h"

namespace dfly::cluster::raft {

PeerConnection::PeerConnection(QuorumEndpoint endpoint, uint32_t timeout_ms, size_t max_frame_size)
    : endpoint_(std::move(endpoint)), timeout_ms_(timeout_ms), max_frame_size_(max_frame_size) {
}

void PeerConnection::Close() {
  if (socket_) {
    std::ignore = socket_->Close();
    socket_.reset();
  }
  versions_ = {};
}

std::error_code PeerConnection::ReceiveFrame(std::chrono::steady_clock::time_point deadline,
                                             absl::FunctionRef<bool(std::string_view)> decode) {
  size_t received = 0;
  auto receive = [&](io::MutableBytes buffer) -> std::error_code {
    auto left = deadline - std::chrono::steady_clock::now();
    if (left <= left.zero())
      return std::make_error_code(std::errc::timed_out);
    socket_->set_timeout(
        static_cast<uint32_t>(std::chrono::ceil<std::chrono::milliseconds>(left).count()));
    auto n = socket_->Recv(buffer);
    if (!n || *n == 0)
      return n ? std::make_error_code(std::errc::connection_reset) : n.error();
    received += *n;
    return {};
  };

  // A single read usually contains the entire response for votes and idle Fetches.
  char first[4096];
  do {
    if (auto ec = receive(io::MutableBuffer(first).subspan(received)); ec)
      return ec;
  } while (received < sizeof(int32_t));
  auto size = protocol::RpcFrameSize({first, received}, max_frame_size_);
  if (!size || received > *size)
    return std::make_error_code(std::errc::protocol_error);
  io::MutableBytes buffer = io::MutableBuffer(first);
  std::unique_ptr<uint8_t[]> large;
  if (*size >= buffer.size() && received < *size) {
    // The spare byte detects unsolicited data arriving with the end of the response.
    large = std::make_unique_for_overwrite<uint8_t[]>(*size + 1);
    std::copy_n(buffer.data(), received, large.get());
    buffer = {large.get(), *size + 1};
  }
  while (received < *size) {
    if (auto ec = receive(buffer.subspan(received)); ec)
      return ec;
  }
  if (received != *size || !decode(io::View(buffer.first(received))))
    return std::make_error_code(std::errc::protocol_error);
  return {};
}

std::error_code PeerConnection::EnsureConnected() {
  if (socket_)
    return {};
  auto* owner = util::fb2::ProactorBase::me();
  boost::system::error_code parse_error;
  auto ip = boost::asio::ip::make_address(endpoint_.host, parse_error);
  if (parse_error) {
    char address[INET6_ADDRSTRLEN];
    if (auto ec = util::fb2::DnsResolve(endpoint_.host, timeout_ms_, address, owner); ec)
      return ec;
    ip = boost::asio::ip::make_address(address, parse_error);
    if (parse_error)
      return parse_error;
  }

  socket_.reset(owner->CreateSocket());
  socket_->set_timeout(timeout_ms_);
  if (auto ec = socket_->Connect({ip, endpoint_.port}); ec) {
    Close();
    return ec;
  }

  // ApiVersions v0 works before the peer's flexible-header support is known.
  protocol::ApiVersionsResponse versions;
  if (auto ec = Exchange(protocol::ApiVersionsRequest{}, 0, &versions); ec)
    return ec;
  if (versions.error_code != 0) {
    Close();
    return std::make_error_code(std::errc::protocol_error);
  }
  versions_ = std::move(versions);
  return {};
}

}  // namespace dfly::cluster::raft
