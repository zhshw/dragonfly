// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/transport/raft_listener.h"

#include <array>

#include "base/logging.h"
#include "raft_io_config.h"
#include "util/accept_server.h"
#include "util/connection.h"
#if DFLY_RAFT_USE_FOLLY
#include "server/cluster/raft/transport/folly_rpc.h"
#include "util/fibers/fiberqueue_threadpool.h"
#endif

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

class RaftServer::Impl {
 public:
  Impl(size_t max_request_size, protocol::RpcHandlers handlers, std::function<void()> shutdown)
      : max_request_size_(max_request_size),
        handlers_(std::move(handlers)),
        shutdown_(std::move(shutdown)) {
  }

  std::error_code Listen(const std::string& host, uint16_t port, util::AcceptServer* host_server) {
#if DFLY_RAFT_USE_FOLLY
    for (unsigned worker = 0; worker < 4; ++worker) {
      handlers_pool_.Await(worker, [this, worker] {
        for (unsigned i = worker; i < slots_.size(); i += 4)
          slots_[i].fiber = util::fb2::Fiber("raft-rpc-slot", [this, i] { slots_[i].queue.Run(); });
      });
    }
    server_ = std::make_unique<FollyRpcServer>();
    auto ec = server_->Listen(host, port, max_request_size_, [this](auto input, auto reply) {
      auto task = std::make_shared<std::function<void()>>([this, input = std::move(input),
                                                           reply = std::move(reply)] {
        std::string response;
        auto result = protocol::DispatchRpcFrame(*input, max_request_size_, handlers_, &response);
        auto ec = result.handler_error;
        if (result.status != protocol::FrameStatus::kComplete && !ec)
          ec = std::make_error_code(std::errc::protocol_error);
        reply(ec, std::move(response));
      });
      for (unsigned i = 0; i < slots_.size(); ++i) {
        auto& slot = slots_[(next_worker_ + i) % slots_.size()];
        bool idle = false;
        if (slot.busy.compare_exchange_strong(idle, true)) {
          CHECK(slot.queue.TryAdd([task, slot = &slot] {
            (*task)();
            slot->busy.store(false);
          }));
          next_worker_ = (next_worker_ + i + 1) % slots_.size();
          return true;
        }
      }
      return false;
    });
    port_ = server_->port();
    if (!ec)
      LOG(INFO) << "Raft RPC: Folly AsyncIoUringSocket / IoUringBackend on " << host << ":"
                << port_;
    return ec;
#else
    auto listener = std::make_unique<RaftListener>(max_request_size_, handlers_, shutdown_);
    if (auto ec = host_server->AddListener(host.c_str(), port, listener.get()); ec)
      return ec;
    port_ = listener->socket()->LocalEndpoint().port();
    listener_ = listener.release();
    return {};
#endif
  }

  void Pause() {
#if DFLY_RAFT_USE_FOLLY
    server_->Pause();
#else
    listener_->pause_accepting();
#endif
  }
  void Stop() {
    if (stopped_)
      return;
    stopped_ = true;
    if (shutdown_)
      shutdown_();
#if DFLY_RAFT_USE_FOLLY
    if (server_)
      server_->Stop();
    for (auto& slot : slots_)
      slot.queue.Shutdown();
    for (unsigned worker = 0; worker < 4; ++worker) {
      handlers_pool_.Await(worker, [this, worker] {
        for (unsigned i = worker; i < slots_.size(); i += 4) {
          if (slots_[i].fiber.IsJoinable())
            slots_[i].fiber.Join();
        }
      });
    }
    handlers_pool_.Shutdown();
    server_.reset();
#endif
  }
  uint16_t port() const {
    return port_;
  }

 private:
  const size_t max_request_size_;
  protocol::RpcHandlers handlers_;
  std::function<void()> shutdown_;
  uint16_t port_ = 0;
  bool stopped_ = false;
#if DFLY_RAFT_USE_FOLLY
  struct HandlerSlot {
    util::fb2::FiberQueue queue{2};
    util::fb2::Fiber fiber;
    std::atomic_bool busy{false};
  };

  // Long-poll/control handlers can park independently; none may monopolize a consumer thread
  // and prevent the Fetch that commits it. Slots are fixed and bounded by the connection limit.
  std::array<HandlerSlot, 128> slots_;
  util::fb2::FiberQueueThreadPool handlers_pool_{4, 128};
  std::unique_ptr<FollyRpcServer> server_;
  unsigned next_worker_ = 0;
#else
  RaftListener* listener_ = nullptr;
#endif
};

RaftServer::RaftServer(size_t max_request_size, protocol::RpcHandlers handlers,
                       std::function<void()> on_pre_shutdown)
    : impl_(std::make_unique<Impl>(max_request_size, std::move(handlers),
                                   std::move(on_pre_shutdown))) {
}
RaftServer::~RaftServer() {
  Stop();
}
std::error_code RaftServer::Listen(const std::string& host, uint16_t port,
                                   util::AcceptServer* host_server) {
  return impl_->Listen(host, port, host_server);
}
uint16_t RaftServer::port() const {
  return impl_->port();
}
void RaftServer::Pause() {
  impl_->Pause();
}
void RaftServer::Stop() {
  impl_->Stop();
}

}  // namespace dfly::cluster::raft
