// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/transport/peer_connection.h"

#include <arpa/inet.h>

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
