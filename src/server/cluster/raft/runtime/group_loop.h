// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>

#include "server/cluster/raft/storage/folly_io.h"
#include "util/fibers/detail/result_mover.h"
#include "util/fibers/synchronization.h"

namespace dfly::cluster::raft {

// Bounded bridge into the serial Folly group owner. I/O completions run directly on that loop
// and do not need queue credits, so saturated callers cannot prevent completion or timeout work.
class GroupLoop {
 public:
  ~GroupLoop() {
    io_.Drain();
  }

  template <typename F> bool TryAdd(F&& task) {
    auto pending = pending_.load(std::memory_order_relaxed);
    do {
      if (pending >= 128)
        return false;
    } while (!pending_.compare_exchange_weak(pending, pending + 1, std::memory_order_acq_rel));
    io_.Run([this, task = std::forward<F>(task)]() mutable {
      task();
      pending_.fetch_sub(1, std::memory_order_release);
      capacity_.notifyAll();
    });
    return true;
  }

  template <typename F> auto Await(F&& operation) -> decltype(operation()) {
    util::fb2::Done done;
    util::detail::ResultMover<decltype(operation())> result;
    auto task = [&] {
      result.Apply(operation);
      done.Notify();
    };
    while (true) {
      auto key = capacity_.prepareWait();
      if (TryAdd(task))
        break;
      capacity_.wait(key.epoch());
    }
    done.Wait();
    return std::move(result).get();
  }

  GroupLoop* GetQueue(unsigned) {
    return this;
  }
  FollyIo& io() {
    return io_;
  }

 private:
  std::atomic_size_t pending_{0};
  util::fb2::EventCount capacity_;
  FollyIo io_;
};

}  // namespace dfly::cluster::raft
