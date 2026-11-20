// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>

#include "server/cluster/raft/protocol/rpc_frame.h"
#include "util/listener_interface.h"

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

}  // namespace dfly::cluster::raft
