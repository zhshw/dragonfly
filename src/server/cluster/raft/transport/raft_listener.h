// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/cluster/raft/protocol/rpc_frame.h"
#include "util/listener_interface.h"

namespace dfly::cluster::raft {

// Internal Kafka RPC listener. Keep RESP clients and Raft peers on separate ports.
class RaftListener final : public util::ListenerInterface {
 public:
  RaftListener(size_t max_request_size, protocol::RpcHandlers handlers);

  util::Connection* NewConnection(util::fb2::ProactorBase* proactor) final;

 private:
  size_t max_request_size_;
  protocol::RpcHandlers handlers_;
};

}  // namespace dfly::cluster::raft
