// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>

namespace dfly::cluster::raft {

// Native Folly io_uring sockets. Callbacks run on the owning EventBase and must not block.
// There is one request in flight per peer; the caller owns protocol negotiation and correlation.
class FollyRpcClient {
 public:
  using Reply = std::function<void(std::error_code, std::string_view,
                                   const std::shared_ptr<const void>&, size_t)>;
  FollyRpcClient();
  ~FollyRpcClient();
  void Exchange(std::string host, uint16_t port, std::string request, size_t max_frame,
                uint32_t timeout_ms, Reply reply);
  void Close(std::function<void()> done);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// AsyncServerSocket accepts on an IoUringBackend; every accepted stream is AsyncIoUringSocket.
// A handler must enqueue work without waiting. False rejects overload by closing the connection.
// Stop closes network producers; drain accepted handlers before destroying the server.
class FollyRpcServer {
 public:
  using Reply = std::function<void(std::error_code, std::string)>;
  using Handler = std::function<bool(std::shared_ptr<const std::string>, Reply)>;
  FollyRpcServer();
  ~FollyRpcServer();
  std::error_code Listen(const std::string& host, uint16_t port, size_t max_frame, Handler handler);
  uint16_t port() const;
  void Pause();
  void Stop();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dfly::cluster::raft
