// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>

#include "server/cluster/raft/protocol/rpc_frame.h"
#include "util/listener_interface.h"

namespace util {
class AcceptServer;
}

namespace dfly::cluster::raft {

// Internal Kafka RPC listener. Keep RESP clients and Raft peers on separate ports.
class RaftListener final : public util::ListenerInterface {
 public:
  RaftListener(size_t max_request_size, protocol::RpcHandlers handlers,
               std::function<void()> on_pre_shutdown = {});

  util::Connection* NewConnection(util::fb2::ProactorBase* proactor) final;

 private:
  void PreShutdown() final;

  size_t max_request_size_;
  protocol::RpcHandlers handlers_;
  std::function<void()> on_pre_shutdown_;
};

// Selects the native node transport at build time. Folly owns its listening socket and all peer
// streams; the host AcceptServer is used only by the alternate transport.
class RaftServer {
 public:
  RaftServer(size_t max_request_size, protocol::RpcHandlers handlers,
             std::function<void()> on_pre_shutdown = {});
  ~RaftServer();
  std::error_code Listen(const std::string& host, uint16_t port, util::AcceptServer* host_server);
  uint16_t port() const;
  void Pause();
  void Stop();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dfly::cluster::raft
