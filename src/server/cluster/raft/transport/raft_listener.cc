// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/transport/raft_listener.h"

#include "base/logging.h"
#include "util/connection.h"

namespace dfly::cluster::raft {
namespace {

class RaftConnection final : public util::Connection {
 public:
  RaftConnection(size_t max_request_size, const protocol::RpcHandlers& handlers)
      : max_request_size_(max_request_size), handlers_(handlers) {
  }

 private:
  void HandleRequests() final {
    std::string pending;
    char buffer[4096];
    while (true) {
      auto count = socket_->Recv(io::MutableBuffer(buffer));
      if (!count || *count == 0)
        return;
      pending.append(buffer, *count);

      std::string_view next(pending);
      while (!next.empty()) {
        std::string response;
        auto frame = protocol::DispatchRpcFrame(next, max_request_size_, handlers_, &response);
        if (frame.status == protocol::FrameStatus::kNeedMore)
          break;
        if (frame.status != protocol::FrameStatus::kComplete) {
          if (frame.handler_error)
            LOG(ERROR) << "Raft request handler: " << frame.handler_error.message();
          return;
        }
        if (socket_->Write(io::Buffer(response)))
          return;
        next.remove_prefix(frame.consumed);
      }
      pending.erase(0, pending.size() - next.size());
    }
  }

  const size_t max_request_size_;
  const protocol::RpcHandlers& handlers_;
};

}  // namespace

RaftListener::RaftListener(size_t max_request_size, protocol::RpcHandlers handlers,
                           std::function<void()> on_pre_shutdown)
    : max_request_size_(max_request_size),
      handlers_(std::move(handlers)),
      on_pre_shutdown_(std::move(on_pre_shutdown)) {
}

util::Connection* RaftListener::NewConnection(util::fb2::ProactorBase*) {
  return new RaftConnection(max_request_size_, handlers_);
}

void RaftListener::PreShutdown() {
  if (on_pre_shutdown_)
    on_pre_shutdown_();
}

}  // namespace dfly::cluster::raft
