// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/storage/folly_io.h"

#include <folly/io/async/IoUringBackend.h>
#include <folly/io/async/ScopedEventBaseThread.h>

#if !FOLLY_HAS_LIBURING
#error "Raft Folly I/O requires Folly built with liburing support"
#endif

namespace dfly::cluster::raft {
namespace {

class CompletionRetry final : public folly::EventBase::LoopCallback {
 public:
  CompletionRetry(folly::EventBase* event_base, int result, std::function<bool(int)> done)
      : event_base_(event_base), result_(result), done_(std::move(done)) {
    event_base_->runInLoop(this);
  }

  void runLoopCallback() noexcept override {
    if (done_(result_))
      delete this;
    else
      event_base_->runInLoop(this);
  }

 private:
  folly::EventBase* event_base_;
  int result_;
  std::function<bool(int)> done_;
};

}  // namespace

class FollyIo::Impl {
 public:
  Impl()
      : thread_(folly::EventBase::Options().setBackendFactory([] {
          folly::IoUringBackend::Options options;
          options.setCapacity(256).setMaxSubmit(128);
          return std::make_unique<folly::IoUringBackend>(std::move(options));
        }),
                nullptr, "raft-folly-io") {
  }

  void Submit(int fd, int64_t offset, std::string_view bytes, std::function<bool(int)> done) {
    auto* event_base = thread_.getEventBase();
    event_base->runInEventBaseThread(
        [event_base, fd, offset, bytes, done = std::move(done)]() mutable {
          auto* backend = static_cast<folly::IoUringBackend*>(event_base->getBackend());
          auto completion = [event_base, done = std::move(done)](int result) mutable {
            if (!done(result))
              new CompletionRetry(event_base, result, std::move(done));
          };
          if (offset < 0)
            backend->queueFsync(fd, std::move(completion));
          else
            backend->queueWrite(fd, bytes.data(), bytes.size(), offset, std::move(completion));
        });
  }

 private:
  folly::ScopedEventBaseThread thread_;
};

FollyIo::FollyIo() : impl_(std::make_unique<Impl>()) {
}

FollyIo::~FollyIo() = default;

void FollyIo::Submit(int fd, int64_t offset, std::string_view bytes,
                     std::function<bool(int)> done) {
  impl_->Submit(fd, offset, bytes, std::move(done));
}

}  // namespace dfly::cluster::raft
