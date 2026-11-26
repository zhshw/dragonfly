// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/storage/folly_io.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/io/async/AsyncTimeout.h>
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
  class Deadline : public folly::AsyncTimeout {
   public:
    explicit Deadline(folly::EventBase* base) : AsyncTimeout(base) {
    }
    void timeoutExpired() noexcept override {
      auto callback = std::move(task);
      if (callback)
        callback();
    }
    std::function<void()> task;
  };

 public:
  Impl()
      : thread_(folly::EventBase::Options().setBackendFactory([] {
          folly::IoUringBackend::Options options;
          options.setCapacity(256).setMaxSubmit(128);
          return std::make_unique<folly::IoUringBackend>(std::move(options));
        }),
                nullptr, "raft-folly-io") {
    thread_.getEventBase()->runInEventBaseThreadAndWait(
        [this] { deadline_ = std::make_unique<Deadline>(thread_.getEventBase()); });
  }

  ~Impl() {
    thread_.getEventBase()->runInEventBaseThreadAndWait([this] { deadline_.reset(); });
  }

  void Run(std::function<void()> task) {
    thread_.getEventBase()->runInEventBaseThreadAlwaysEnqueue(std::move(task));
  }

  void Drain() {
    thread_.getEventBase()->runInEventBaseThreadAndWait([] {});
  }

  void Encode(std::function<void()> work, std::function<void()> complete) {
    if (!encoder_)
      encoder_ = std::make_unique<folly::CPUThreadPoolExecutor>(1);
    encoder_->add([this, work = std::move(work), complete = std::move(complete)]() mutable {
      work();
      Run(std::move(complete));
    });
  }

  void SetDeadline(std::optional<std::chrono::steady_clock::time_point> deadline,
                   std::function<void()> task) {
    deadline_->cancelTimeout();
    deadline_->task = std::move(task);
    if (deadline) {
      auto delay = std::chrono::ceil<std::chrono::milliseconds>(*deadline -
                                                                std::chrono::steady_clock::now());
      deadline_->scheduleTimeout(std::max(delay, std::chrono::milliseconds(1)));
    }
  }

  void Submit(int fd, int64_t offset, std::string_view bytes, std::function<bool(int)> done,
              QueueObserver queued) {
    auto now = [] {
      return std::chrono::duration_cast<std::chrono::nanoseconds>(
                 std::chrono::steady_clock::now().time_since_epoch())
          .count();
    };
    const auto submitted = queued ? now() : 0;
    auto* event_base = thread_.getEventBase();
    event_base->runInEventBaseThread([event_base, fd, offset, bytes, now, submitted,
                                      queued = std::move(queued),
                                      done = std::move(done)]() mutable {
      if (queued)
        queued(submitted, now());
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
  std::unique_ptr<Deadline> deadline_;
  std::unique_ptr<folly::CPUThreadPoolExecutor> encoder_;
};

FollyIo::FollyIo() : impl_(std::make_unique<Impl>()) {
}

FollyIo::~FollyIo() = default;

void FollyIo::Submit(int fd, int64_t offset, std::string_view bytes, std::function<bool(int)> done,
                     QueueObserver queued) {
  impl_->Submit(fd, offset, bytes, std::move(done), std::move(queued));
}

void FollyIo::Run(std::function<void()> task) {
  impl_->Run(std::move(task));
}
void FollyIo::Drain() {
  impl_->Drain();
}
void FollyIo::Encode(std::function<void()> work, std::function<void()> complete) {
  impl_->Encode(std::move(work), std::move(complete));
}
void FollyIo::SetDeadline(std::optional<std::chrono::steady_clock::time_point> deadline,
                          std::function<void()> task) {
  impl_->SetDeadline(deadline, std::move(task));
}

}  // namespace dfly::cluster::raft
